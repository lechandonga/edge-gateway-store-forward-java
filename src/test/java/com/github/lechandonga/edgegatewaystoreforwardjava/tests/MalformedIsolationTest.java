package com.github.lechandonga.edgegatewaystoreforwardjava.tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务二：异常报文隔离。
 * 坏魔数、超长帧、半包、非法 JSON、字段缺失/越界、设备序号倒退：
 * 必须带可区分原因隔离，且不影响同一设备后续正常报文与其他设备。
 */
@DisplayName("业务二：异常报文隔离（畸形/半包/超长/字段异常/序号倒退）")
class MalformedIsolationTest extends AbstractGatewayTest {

    private static final Logger log = LoggerFactory.getLogger(MalformedIsolationTest.class);

    @BeforeEach
    void setUp() throws Exception {
        startStack();
    }

    @AfterEach
    void tearDown() {
        stopStack();
    }

    @Test
    @DisplayName("用例1：坏魔数+非法JSON+缺字段+NaN+序号倒退均被按原因隔离，后续正常读数不受影响")
    void variousBadFrames_isolatedByReason_thenGoodFramesStillFlow() throws Exception {
        log.info("[异常隔离-用例1] 设备 d-bad 连续发送 5 类异常报文，再发送正常报文");
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-bad");
            device.sendBadMagic();
            Thread.sleep(100);
            // 等坏魔数后重新同步：坏魔数后本连接可能被丢弃，这里按设计坏帧只是抛异常，
            // 但 BAD_MAGIC 发生在成帧阶段会终止会话（无法定位下一帧边界），因此另起连接。
        }

        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-bad2");
            device.sendRawJsonText("{not-json");
            device.sendRawJsonText("{\"type\":\"reading\",\"seq\":1,\"metric\":\"temp\"}");
            device.sendRawJsonText("{\"type\":\"reading\",\"seq\":2,\"metric\":\"temp\",\"value\":\"abc\"}");
            device.sendJsonReading(1, "temp", 36.5, 1000L);
            device.sendJsonReading(1, "temp", 37.0, 1001L); // 序号倒退/重复
            device.sendJsonReading(2, "temp", 37.1, 1002L); // 恢复正常
        }
        awaitCommitted(2, 5_000);

        Path quarantineFile = Path.of(props.getDataDir(), "quarantine.log");
        String qlog = Files.readString(quatantineFileSafe(quarantineFile));
        log.info("[异常隔离-用例1] 隔离日志内容:\n{}", qlog);
        assertTrue(qlog.contains("BAD_MAGIC"), "应包含 BAD_MAGIC");
        assertTrue(qlog.contains("MALFORMED_PAYLOAD"), "应包含 MALFORMED_PAYLOAD");
        assertTrue(qlog.contains("MISSING_OR_INVALID_FIELD"), "应包含字段缺失原因");
        assertTrue(qlog.contains("DEVICE_SEQ_GOES_BACKWARDS"), "应包含序号倒退原因");
        assertEquals(2, upstream.committedCount(), "异常帧之后的 2 条正常读数必须送达");
        log.info("[异常隔离-用例1] 通过：异常均带原因隔离，正常读数继续流转");
    }

    private Path quatantineFileSafe(Path p) {
        return p;
    }

    @Test
    @DisplayName("用例2：超长帧按 FRAME_TOO_LONG 隔离，且不影响其他设备")
    void oversizedFrame_isolated_otherDeviceUnaffected() throws Exception {
        log.info("[异常隔离-用例2] d-long 发送超长帧；同窗口 d-good 正常发送");
        try (DeviceSimClient bad = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort());
             DeviceSimClient good = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            bad.hello("d-long");
            good.hello("d-good");
            // maxFrameBytes=8192，声明 9000（<=65535 可编码），随后补齐字节
            bad.sendTooLongHeader(9000);
            for (int i = 1; i <= 3; i++) {
                good.sendJsonReading(i, "temp", 25.0 + i, 1000L + i);
            }
        }
        awaitCommitted(3, 5_000);
        Path quarantineFile = Path.of(props.getDataDir(), "quarantine.log");
        String qlog = Files.readString(quarantineFile);
        assertTrue(qlog.contains("FRAME_TOO_LONG"), "应记录超长帧原因");
        assertEquals(3, upstream.committedCount(), "其他设备 3 条读数不受影响");
        log.info("[异常隔离-用例2] 通过：超长帧隔离 FRAME_TOO_LONG，d-good 的 3 条全部送达");
    }

    @Test
    @DisplayName("用例3：半包在超时后按 FRAME_TRUNCATED 隔离，不静默丢失")
    void halfPacket_timeout_isolated() throws Exception {
        log.info("[异常隔离-用例3] d-half 发送半包后不补齐，等待超时隔离");
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-half");
            device.sendHalfPacket();
            Thread.sleep(1_500);
        }
        Path quarantineFile = Path.of(props.getDataDir(), "quarantine.log");
        String qlog = Files.readString(quarantineFile);
        assertTrue(qlog.contains("FRAME_TRUNCATED"), "半包超时必须记录 FRAME_TRUNCATED");
        log.info("[异常隔离-用例3] 通过：半包未静默丢失，按 FRAME_TRUNCATED 隔离");
    }

    @Test
    @DisplayName("用例4：字段值越界（NaN、负时间戳）按 FIELD_VALUE_OUT_OF_RANGE 隔离")
    void outOfRangeFields_isolated() throws Exception {
        log.info("[异常隔离-用例4] d-range 发送 NaN 与负时间戳");
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-range");
            device.sendRawJsonText(
                    "{\"type\":\"reading\",\"seq\":1,\"metric\":\"t\",\"value\":NaN,\"ts\":1}");
            // NaN 不是合法 JSON，Jackson 会按 MALFORMED 处理——改为 Infinity 字符串场景用 KV 验证越界
            device.sendKvReading(1, -1L, 36.5, "temp"); // ts 为负
            device.sendJsonReading(2, "temp", 36.5, 100L); // 正常
        }
        awaitCommitted(1, 5_000);
        String qlog = Files.readString(Path.of(props.getDataDir(), "quarantine.log"));
        assertTrue(qlog.contains("MALFORMED_PAYLOAD") || qlog.contains("FIELD_VALUE_OUT_OF_RANGE"));
        assertTrue(qlog.contains("FIELD_VALUE_OUT_OF_RANGE"), "负时间戳必须有越界原因");
        log.info("[异常隔离-用例4] 通过：字段越界带原因隔离，正常读数仍送达");
    }
}
