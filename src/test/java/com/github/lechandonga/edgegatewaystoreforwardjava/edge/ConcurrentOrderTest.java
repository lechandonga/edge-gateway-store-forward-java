package com.github.lechandonga.edgegatewaystoreforwardjava.edge;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.demo.DemoDeviceSimulators;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.DeviceEvent;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline.GatewayProperties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：多设备并发接入下的设备内顺序保证与背压。
 */
class ConcurrentOrderTest implements BusinessTest {

    private GatewayTestEnv env;

    @AfterEach
    void tearDown() {
        if (env != null) {
            env.close();
        }
    }

    @Test
    @DisplayName("并发顺序-多设备并发上报，同一设备在上游严格有序，设备间互不阻塞")
    void perDeviceOrderUnderConcurrency() throws Exception {
        env = new GatewayTestEnv(GatewayTestEnv.defaultProps(GatewayTestEnv.tempDir("conc")),
                GatewayTestEnv.defaultRegistry(), true);
        GatewayTestEnv e = env;
        e.startForwarder();

        int devices = 8;
        int perDevice = 200;
        List<Thread> threads = new ArrayList<>();
        for (int d = 0; d < devices; d++) {
            final String jsonId = "MQTT-20" + (d + 1);
            threads.add(new Thread(() -> {
                try (var dev = new DemoDeviceSimulators.JsonDevice(e.jsonServer.boundPort(), jsonId)) {
                    for (int i = 1; i <= perDevice; i++) {
                        dev.sendOne(20 + (i % 10));
                    }
                } catch (Exception ignored) {
                    // connections share two logical devices; failures still asserted via counts
                }
            }, "sim-json-" + d));
        }
        for (int d = 0; d < devices; d++) {
            final int modbusAddr = d + 1;
            threads.add(new Thread(() -> {
                try (var dev = new DemoDeviceSimulators.ModbusDevice(e.modbusServer.boundPort(), modbusAddr)) {
                    for (int i = 1; i <= perDevice; i++) {
                        dev.sendOne(20 + (i % 10));
                    }
                } catch (Exception ignored) {
                    // best effort
                }
            }, "sim-modbus-" + d));
        }
        threads.forEach(Thread::start);
        for (Thread t : threads) {
            t.join(30_000);
        }

        long expectTotal = (long) devices * perDevice * 2L;
        long deadline = System.currentTimeMillis() + 30_000;
        synchronized (this) {
            while (e.upstream.storedCount() < expectTotal && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
        }
        assertEquals(expectTotal, e.upstream.storedCount(),
                "所有设备的全部事件都应入库，畸形数为 0");

        Map<String, List<Long>> byDevice = new HashMap<>();
        for (DeviceEvent ev : e.upstream.storedEvents()) {
            byDevice.computeIfAbsent(ev.deviceId(), k -> new ArrayList<>()).add(ev.seq());
        }
        for (Map.Entry<String, List<Long>> entry : byDevice.entrySet()) {
            List<Long> seqs = entry.getValue();
            for (int i = 1; i < seqs.size(); i++) {
                assertTrue(seqs.get(i) > seqs.get(i - 1),
                        "设备 " + entry.getKey() + " 事件乱序: " + seqs.get(i - 1) + " -> " + seqs.get(i));
            }
        }
        e.close();
        env = null;
    }

    @Test
    @DisplayName("并发顺序-单台设备畸形报文不影响其他设备继续采集")
    void oneBadDeviceDoesNotStopOthers() throws Exception {
        env = new GatewayTestEnv(GatewayTestEnv.defaultProps(GatewayTestEnv.tempDir("isobad")),
                GatewayTestEnv.defaultRegistry(), true);
        GatewayTestEnv e = env;
        e.startForwarder();

        Thread good = new Thread(() -> {
            try (var dev = new DemoDeviceSimulators.ModbusDevice(e.modbusServer.boundPort(), 1)) {
                for (int i = 1; i <= 50; i++) {
                    dev.sendOne(25);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        Thread bad = new Thread(() -> {
            try (var dev = new DemoDeviceSimulators.ModbusDevice(e.modbusServer.boundPort(), 2)) {
                for (int i = 1; i <= 20; i++) {
                    dev.sendGarbage();
                    dev.sendOne(26);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        good.start();
        bad.start();
        good.join(20_000);
        bad.join(20_000);

        long deadline = System.currentTimeMillis() + 20_000;
        while (e.upstream.storedCount() < 70 && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertEquals(70, e.upstream.storedCount(), "好设备 50 条 + 坏设备 20 条正常帧");
        assertTrue(e.quarantined.size() >= 20, "坏设备 20 条畸形帧全部隔离");
        e.close();
        env = null;
    }

    @Test
    @DisplayName("并发顺序-上游慢且缓冲接近上限时触发背压，积压被限制在容量内")
    void backpressureCapsMemory() throws Exception {
        GatewayProperties p =
                GatewayTestEnv.defaultProps(GatewayTestEnv.tempDir("bp"));
        p.setBufferMaxEvents(100);
        p.setBackpressureRatio(0.5);
        env = new GatewayTestEnv(p, GatewayTestEnv.defaultRegistry(), true);
        GatewayTestEnv e = env;
        e.startForwarder();
        e.upstream.setAvailable(false);

        try (var dev = new DemoDeviceSimulators.ModbusDevice(e.modbusServer.boundPort(), 1)) {
            for (int i = 1; i <= 1000; i++) {
                dev.sendOne(25);
                if (i % 50 == 0) {
                    Thread.sleep(20);
                }
            }
        }
        long pending = e.buffer.pendingCount();
        assertTrue(pending <= 100, "背压下积压不得超过容量上限，实际=" + pending);
        e.close();
        env = null;
    }
}
