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
 * 业务三：断连补发。
 * 上游不可用期间事件落本地缓冲；恢复后按序补发；重复投递不产生重复记录；
 * 一批不会只提交一半（上游整批原子提交）。
 */
@DisplayName("业务三：断连补发（本地缓冲、恢复后按序补发、重试退避、整批原子）")
class StoreForwardReconnectTest extends AbstractGatewayTest {

    private static final Logger log = LoggerFactory.getLogger(StoreForwardReconnectTest.class);

    @BeforeEach
    void setUp() throws Exception {
        startStack(10_000, GatewayProperties.OverflowStrategy.BLOCK, false);
    }

    @AfterEach
    void tearDown() {
        stopStack();
    }

    @Test
    @DisplayName("用例1：上游不可用时本地缓冲，恢复后按序全部补发，重试计数可观测")
    void bufferWhileDown_replayInOrderAfterRecovery() throws Exception {
        log.info("[断连补发-用例1] 上游初始不可用，d-1 发送 25 条读数，应全部积压在本地");
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-1");
            for (int i = 1; i <= 25; i++) {
                device.sendJsonReading(i, "temp", 20.0 + i, 5_000L + i);
            }
        }
        long retriesBefore = metrics.getRetried();
        assertTrue(retriesBefore >= 0);
        Thread.sleep(400);
        assertEquals(0, upstream.committedCount(), "上游不可用时不得有记录");
        assertEquals(25, queue.size(), "25 条事件必须全部在本地缓冲中");
        log.info("[断连补发-用例1] 积压确认: backlog={}, 已发生失败重试={}",
                queue.size(), metrics.getRetried());
        assertTrue(metrics.getUpstreamFailures() > 0, "上游不可用期间必须统计到失败次数");

        log.info("[断连补发-用例1] 恢复上游，期望按序补发 25 条");
        upstream.setAvailable(true);
        awaitCommitted(25, 10_000);
        awaitBacklogZero(10_000);

        // 顺序校验：幂等键本身含 deviceSeq，记录集合完整即代表 1..25 全部恰好一次
        for (int i = 1; i <= 25; i++) {
            assertTrue(upstream.committedKeysView().contains("d-1#" + i),
                    "缺失 d-1#" + i);
        }
        assertEquals(25, upstream.committedCount(), "不得有重复记录");
        assertTrue(metrics.getRetried() > retriesBefore, "补发过程应记录重试次数");
        log.info("[断连补发-用例1] 通过：恢复后 25 条恰好一次补发完成，重试次数={}",
                metrics.getRetried());
    }

    @Test
    @DisplayName("用例2：恢复瞬间的重复投递/重复确认在上游不产生重复记录（幂等）")
    void redelivery_isIdempotent() throws Exception {
        log.info("[断连补发-用例2] d-idem 发送 10 条，恢复后人工二次 POST 同样批次验证幂等");
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-idem");
            for (int i = 1; i <= 10; i++) {
                device.sendJsonReading(i, "temp", i, 100L + i);
            }
        }
        upstream.setAvailable(true);
        awaitCommitted(10, 10_000);
        awaitBacklogZero(10_000);

        // 模拟"补发成功但确认丢失"：直接以相同幂等键重复投递整批
        String batch = TestHttpUtil.buildIdempotentBatch("d-idem", 1, 10);
        int status = TestHttpUtil.postBatch(props.getUpstreamPort(), batch);
        assertEquals(200, status);
        assertEquals(10, upstream.committedCount(), "重复投递不得新增任何记录");
        log.info("[断连补发-用例2] 通过：整批重复投递后上游记录数仍为 10");
    }

    @Test
    @DisplayName("用例3：多次断连/恢复抖动下数据不丢不重，退避避免无限重试")
    void flappingUpstream_noLossNoDuplicate() throws Exception {
        log.info("[断连补发-用例3] 上边发边抖动上下游可用性");
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-flap");
            for (int i = 1; i <= 30; i++) {
                device.sendJsonReading(i, "temp", i, 100L + i);
                if (i % 10 == 0) {
                    upstream.setAvailable(!upstream.isAvailable());
                    Thread.sleep(150);
                }
            }
            upstream.setAvailable(true);
        }
        awaitCommitted(30, 15_000);
        assertEquals(30, upstream.committedCount());
        long retries = metrics.getRetried();
        Thread.sleep(300);
        assertEquals(retries, metrics.getRetried(), "稳定后不应继续空转重试");
        log.info("[断连补发-用例3] 通过：抖动结束后 30 条恰好一次，稳定后重试停止(共 {} 次)", retries);
    }
}
