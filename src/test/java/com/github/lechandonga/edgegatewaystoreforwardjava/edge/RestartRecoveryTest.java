package com.github.lechandonga.edgegatewaystoreforwardjava.edge;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.buffer.WriteAheadBuffer;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.DecodeResult;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline.GatewayProperties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：进程重启恢复——未确认数据在新实例中继续补发，已确认数据不再重投。
 */
class RestartRecoveryTest implements BusinessTest {

    @Test
    @DisplayName("重启恢复-崩溃后未确认事件原样恢复并补发成功")
    void recoverUnacknowledgedAfterRestart() throws Exception {
        Path dir = GatewayTestEnv.tempDir("restart");
        long total = 60;
        try (GatewayTestEnv e = new GatewayTestEnv(GatewayTestEnv.defaultProps(dir),
                GatewayTestEnv.defaultRegistry(), false)) {
            e.upstream.setAvailable(false);
            for (long i = 1; i <= total; i++) {
                e.pipeline.handle(new DecodeResult.Parsed("MOD-2", i, "temperature",
                                20 + (i % 5), "C", System.currentTimeMillis()),
                        "MODBUS-TXT", "test");
            }
            assertEquals(total, e.buffer.pendingCount());
            // 不恢复上游直接关闭：模拟断网期间进程被杀
        }

        try (GatewayTestEnv restarted = new GatewayTestEnv(GatewayTestEnv.defaultProps(dir),
                GatewayTestEnv.defaultRegistry(), false)) {
            assertEquals(total, restarted.buffer.pendingCount(),
                    "重启后未确认数据必须全部恢复");
            var pending = restarted.buffer.readPending((int) total);
            for (int i = 0; i < total; i++) {
                assertEquals(i + 1, pending.get(i).seq(), "恢复后顺序不变");
            }
            restarted.forwarder.pumpOnce();
            assertEquals(total, restarted.upstream.storedCount());
            assertEquals(0, restarted.buffer.pendingCount());
        }
    }

    @Test
    @DisplayName("重启恢复-已确认事件重启后不补发，事件 id 不回退")
    void acknowledgedNotReplayedAndIdAdvances() throws Exception {
        Path dir = GatewayTestEnv.tempDir("restart2");
        long firstIdBeforeRestart;
        try (GatewayTestEnv e = new GatewayTestEnv(GatewayTestEnv.defaultProps(dir),
                GatewayTestEnv.defaultRegistry(), false)) {
            for (long i = 1; i <= 10; i++) {
                e.pipeline.handle(new DecodeResult.Parsed("MOD-2", i, "temperature",
                        21.0, "C", System.currentTimeMillis()), "MODBUS-TXT", "test");
            }
            e.forwarder.pumpOnce();
            assertEquals(0, e.buffer.pendingCount());
            firstIdBeforeRestart = e.upstream.storedEvents().get(0).id();

            e.upstream.setAvailable(false);
            e.pipeline.handle(new DecodeResult.Parsed("MOD-2", 11, "temperature",
                    21.0, "C", System.currentTimeMillis()), "MODBUS-TXT", "test");
            assertEquals(1, e.buffer.pendingCount());
        }

        try (GatewayTestEnv e2 = new GatewayTestEnv(GatewayTestEnv.defaultProps(dir),
                GatewayTestEnv.defaultRegistry(), false)) {
            assertEquals(1, e2.buffer.pendingCount(), "仅未确认的第 11 条恢复");
            e2.forwarder.pumpOnce();
            assertEquals(0, e2.buffer.pendingCount());
            e2.pipeline.handle(new DecodeResult.Parsed("MOD-2", 12, "temperature",
                    21.0, "C", System.currentTimeMillis()), "MODBUS-TXT", "test");
            e2.forwarder.pumpOnce();
            long newId = e2.upstream.storedEvents().get(e2.upstream.storedEvents().size() - 1).id();
            assertTrue(newId > firstIdBeforeRestart + 10, "重启后 id 必须单调不回退");
        }
    }

    @Test
    @DisplayName("重启恢复-直接复用 WAL 组件也能读到未确认数据（组件级验证）")
    void walComponentReopen() throws Exception {
        Path dir = GatewayTestEnv.tempDir("restart3");
        GatewayProperties p = GatewayTestEnv.defaultProps(dir);
        try (WriteAheadBuffer wal = new WriteAheadBuffer(dir, 100, 64 * 1024 * 1024)) {
            wal.append(new com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.DeviceEvent(
                    0, "MOD-1", 1, "temperature", 1.0, "C", 1, 1, "x"));
        }
        try (WriteAheadBuffer wal = new WriteAheadBuffer(dir, 100, 64 * 1024 * 1024)) {
            assertEquals(1, wal.pendingCount());
            assertEquals("MOD-1#1", wal.readPending(10).get(0).dedupKey());
        }
    }
}
