package com.github.lechandonga.edgegatewaystoreforwardjava.edge;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.DecodeResult;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.DeviceEvent;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline.GatewayProperties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：上游断连期间本地缓冲、恢复后按序补发、整批原子与幂等去重。
 */
class DisconnectReplayTest implements BusinessTest {

    private GatewayTestEnv env;

    private GatewayTestEnv env() throws Exception {
        env = new GatewayTestEnv(GatewayTestEnv.defaultProps(GatewayTestEnv.tempDir("replay")),
                GatewayTestEnv.defaultRegistry(), false);
        return env;
    }

    @AfterEach
    void tearDown() {
        if (env != null) {
            env.close();
        }
    }

    private void push(GatewayTestEnv e, String device, long seq) {
        e.pipeline.handle(new DecodeResult.Parsed(device, seq, "temperature", 20 + seq,
                "C", System.currentTimeMillis()), "MODBUS-TXT", "test");
    }

    @Test
    @DisplayName("断连补发-断连期间全部留在 WAL，恢复后按序补发且积压清零")
    void bufferWhileDownAndReplayInOrder() throws Exception {
        GatewayTestEnv e = env();
        e.upstream.setAvailable(false);
        for (long i = 1; i <= 120; i++) {
            push(e, "MOD-1", i);
        }
        assertEquals(120, e.buffer.pendingCount());
        e.forwarder.pumpOnce();
        assertEquals(0, e.upstream.storedCount(), "上游不可用时不能入库");
        assertEquals(120, e.buffer.pendingCount(), "失败后事件必须完整保留在 WAL");
        assertTrue(e.metrics.upstreamFailures() >= 1);
        assertTrue(e.metrics.retryAttempts() >= 1);

        e.upstream.setAvailable(true);
        e.forwarder.pumpOnce();
        assertEquals(120, e.upstream.storedCount());
        assertEquals(0, e.buffer.pendingCount());
        List<DeviceEvent> stored = e.upstream.storedEvents();
        for (int i = 0; i < stored.size(); i++) {
            assertEquals(i + 1, stored.get(i).seq(), "补发必须按序");
        }
    }

    @Test
    @DisplayName("断连补发-同一批重复投递在上游幂等去重，不产生重复记录")
    void duplicateDeliveryIsIdempotent() throws Exception {
        GatewayTestEnv e = env();
        for (long i = 1; i <= 10; i++) {
            push(e, "MOD-1", i);
        }
        e.forwarder.pumpOnce();
        assertEquals(10, e.upstream.storedCount());

        List<DeviceEvent> batch = e.buffer.readPending(50);
        assertTrue(batch.isEmpty(), "已确认数据不应再补发");

        List<DeviceEvent> sameEvents = e.upstream.storedEvents();
        e.upstream.ingestBatch(sameEvents);
        e.upstream.ingestBatch(sameEvents);
        assertEquals(10, e.upstream.storedCount(), "重复投递不得产生重复记录");

        e.buffer.acknowledge(999);
        assertEquals(0, e.buffer.pendingCount(), "重复确认必须安全忽略，不报错");
    }

    @Test
    @DisplayName("断连补发-失败批次不确认，恢复后整批到达，不存在提交一半")
    void noPartialBatch() throws Exception {
        GatewayTestEnv e = env();
        for (long i = 1; i <= 50; i++) {
            push(e, "MOD-1", i);
        }
        e.upstream.setAvailable(false);
        e.forwarder.pumpOnce();
        assertEquals(50, e.buffer.pendingCount(), "整批失败：一条都不能确认");
        e.upstream.setAvailable(true);
        e.forwarder.pumpOnce();
        assertEquals(50, e.upstream.storedCount());
        assertEquals(0, e.buffer.pendingCount());
    }

    @Test
    @DisplayName("断连补发-退避随失败次数指数增长且受上限约束")
    void backoffBounded() throws Exception {
        GatewayTestEnv e = env();
        long d1 = e.forwarder.nextBackoffMs(1);
        long d3 = e.forwarder.nextBackoffMs(3);
        long d100 = e.forwarder.nextBackoffMs(100);
        assertTrue(d3 > d1, "连续失败应退避增长");
        assertTrue(d100 <= e.props.getBackoffMaxMs() * 1.2, "退避必须有上限");
    }

    @Test
    @DisplayName("断连补发-定时器持续 tick 不会造成重试风暴，每个退避窗口只重试一次")
    void noRetryStormFromTicks() throws Exception {
        GatewayTestEnv e = env();
        e.props.setPumpIntervalMs(20);
        e.startForwarder();
        e.upstream.setAvailable(false);
        push(e, "MOD-1", 1);
        Thread.sleep(1500);
        long attemptsAtOneSecond = e.metrics.retryAttempts();
        Thread.sleep(1500);
        long attemptsAtThreeSeconds = e.metrics.retryAttempts();
        assertTrue(attemptsAtOneSecond >= 4, "应至少按退避重试若干次: " + attemptsAtOneSecond);
        assertTrue(attemptsAtThreeSeconds - attemptsAtOneSecond <= 3,
                "退避生效后重试应稀疏，1.5s 内最多 ~2 次，实际增量="
                        + (attemptsAtThreeSeconds - attemptsAtOneSecond));
    }

    @Test
    @DisplayName("断连补发-缓冲写满 reject 策略下新报文按 BUFFER_FULL 隔离而非静默丢弃")
    void bufferFullRejectedWithReason() throws Exception {
        GatewayProperties p = GatewayTestEnv.defaultProps(GatewayTestEnv.tempDir("full"));
        p.setBufferMaxEvents(3);
        p.setBufferFullPolicy(GatewayProperties.FullPolicy.REJECT);
        env = new GatewayTestEnv(p, GatewayTestEnv.defaultRegistry(), false);
        GatewayTestEnv e = env;
        e.upstream.setAvailable(false);
        push(e, "MOD-1", 1);
        push(e, "MOD-1", 2);
        push(e, "MOD-1", 3);
        push(e, "MOD-1", 4);
        assertEquals(3, e.buffer.pendingCount(), "写满后不得覆盖未确认数据");
        assertEquals(RejectReason.BUFFER_FULL, e.quarantined.get(0).reason());
        assertEquals(1, e.metrics.bufferFullRejections());
    }
}
