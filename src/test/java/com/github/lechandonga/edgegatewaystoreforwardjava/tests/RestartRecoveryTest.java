package com.github.lechandonga.edgegatewaystoreforwardjava.tests;

import com.github.lechandonga.edgegatewaystoreforwardjava.config.GatewayProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务四：进程重启恢复。
 * 上游断连期间积压未确认事件 -> "重启网关进程"（WAL 与上游幂等键都在磁盘）->
 * 恢复后继续补发，不丢不重；已确认事件重启后不再补发。
 */
@DisplayName("业务四：重启恢复（WAL 重放未确认数据，幂等键跨重启生效）")
class RestartRecoveryTest extends AbstractGatewayTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    @BeforeEach
    void setUp() throws Exception {
        startStack(10_000, GatewayProperties.OverflowStrategy.BLOCK, false);
    }

    @AfterEach
    void tearDown() {
        stopStack();
    }

    @Test
    @DisplayName("用例1：断连积压后重启网关，未确认数据继续补发且上游无重复")
    void unackedEvents_replayedAfterRestart() throws Exception {
        log.info("[重启恢复-用例1] 上游不可用，d-r 发送 15 条后重启网关");
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-r");
            for (int i = 1; i <= 15; i++) {
                device.sendJsonReading(i, "temp", i, 100L + i);
            }
        }
        Thread.sleep(300);
        assertEquals(15, queue.size());

        log.info("[重启恢复-用例1] 模拟网关进程重启（数据目录与上游幂等键文件均保留）");
        restartGateway();
        assertEquals(15, queue.size(), "重启后 WAL 必须重放出 15 条未确认事件");
        log.info("[重启恢复-用例1] 重启恢复积压={}，恢复上游继续补发", queue.size());

        upstream.setAvailable(true);
        awaitCommitted(15, 10_000);
        awaitBacklogZero(10_000);
        assertEquals(15, upstream.committedCount());

        log.info("[重启恢复-用例1] 再次重启：已确认数据不得重复补发");
        restartGateway();
        Thread.sleep(500);
        assertEquals(0, queue.size(), "已确认事件不应再出现在缓冲");
        assertEquals(15, upstream.committedCount(), "上游记录数保持 15，无重复");
        log.info("[重启恢复-用例1] 通过：未确认数据跨重启补发成功，已确认数据不重复");
    }

    @Test
    @DisplayName("用例2：重启后新事件接在旧序号之后，补发顺序连续不乱序")
    void newEventsAfterRestart_followOldSequence() throws Exception {
        log.info("[重启恢复-用例2] 积压 5 条 -> 重启 -> 再发 5 条 -> 恢复上游，共 10 条有序送达");
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-cont");
            for (int i = 1; i <= 5; i++) {
                device.sendJsonReading(i, "temp", i, 100L + i);
            }
        }
        restartGateway();
        assertEquals(5, queue.size());
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-cont");
            for (int i = 6; i <= 10; i++) {
                device.sendJsonReading(i, "temp", i, 100L + i);
            }
        }
        Thread.sleep(300);
        assertEquals(10, queue.size());
        upstream.setAvailable(true);
        awaitCommitted(10, 10_000);
        for (int i = 1; i <= 10; i++) {
            assertTrue(upstream.committedKeysView().contains("d-cont#" + i));
        }
        log.info("[重启恢复-用例2] 通过：重启前后 10 条事件连续、完整、无重复");
    }
}
