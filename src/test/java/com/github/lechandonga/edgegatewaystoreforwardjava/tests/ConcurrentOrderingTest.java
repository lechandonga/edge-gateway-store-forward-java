package com.github.lechandonga.edgegatewaystoreforwardjava.tests;

import com.github.lechandonga.edgegatewaystoreforwardjava.config.GatewayProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务五：多设备并发下的顺序与背压。
 * 同一设备事件顺序不乱；REJECT_NEW 策略缓冲写满时按 BUFFER_FULL 拒绝计数，
 * BLOCK 策略下写入被反压，内存不超过容量上限。
 */
@DisplayName("业务五：并发顺序与背压（多设备保序、容量硬上限、满员策略可配置）")
class ConcurrentOrderingTest extends AbstractGatewayTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentOrderingTest.class);

    @AfterEach
    void tearDown() {
        stopStack();
    }

    @Test
    @DisplayName("用例1：8 台设备各发 100 条，全部送达且每台设备序号完整")
    void manyConcurrentDevices_allDelivered_perDeviceComplete() throws Exception {
        startStack();
        int devices = 8;
        int perDevice = 100;
        log.info("[并发顺序-用例1] {} 台设备并发，每台 {} 条", devices, perDevice);
        ExecutorService pool = Executors.newFixedThreadPool(devices);
        List<Future<?>> futures = new ArrayList<>();
        for (int d = 0; d < devices; d++) {
            final String deviceId = "dev-" + d;
            futures.add(pool.submit(() -> {
                try (DeviceSimClient client =
                             new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
                    client.hello(deviceId);
                    for (int i = 1; i <= perDevice; i++) {
                        client.sendJsonReading(i, "temp", i, 100L + i);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();
        awaitCommitted(devices * perDevice, 30_000);
        assertEquals(devices * perDevice, upstream.committedCount());
        for (int d = 0; d < devices; d++) {
            for (int i = 1; i <= perDevice; i++) {
                assertTrue(upstream.committedKeysView().contains("dev-" + d + "#" + i),
                        "缺失 dev-" + d + "#" + i);
            }
        }
        assertEquals(0, metrics.getRejected());
        log.info("[并发顺序-用例1] 通过：{} 条事件全部恰好一次送达", devices * perDevice);
    }

    @Test
    @DisplayName("用例2：REJECT_NEW 策略下缓冲写满时拒绝新事件并按 BUFFER_FULL 隔离，绝不覆盖")
    void rejectNewStrategy_whenFull_rejectsAndCounts() throws Exception {
        startStack(20, GatewayProperties.OverflowStrategy.REJECT_NEW, false);
        log.info("[并发顺序-用例2] 容量=20，上游不可用，发送 30 条：期望 20 入缓冲、10 被拒绝");
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-cap");
            for (int i = 1; i <= 30; i++) {
                device.sendJsonReading(i, "temp", i, 100L + i);
            }
        }
        Thread.sleep(500);
        assertEquals(20, queue.size(), "缓冲中未确认数据必须正好等于容量上限，不得被覆盖");
        String qlog = Files.readString(Path.of(props.getDataDir(), "quarantine.log"));
        long bufferFullCount = qlog.lines().filter(l -> l.contains("BUFFER_FULL")).count();
        assertTrue(bufferFullCount >= 10, "至少 10 条应带 BUFFER_FULL 原因隔离，实际=" + bufferFullCount);

        upstream.setAvailable(true);
        awaitBacklogZero(10_000);
        assertEquals(20, upstream.committedCount(), "缓冲中的 20 条随后仍能正常补发");
        log.info("[并发顺序-用例2] 通过：满员时 {} 条拒绝可观测，缓冲内 20 条完整补发", bufferFullCount);
    }

    @Test
    @DisplayName("用例3：BLOCK 策略下生产者被背压，缓冲始终不超过容量，恢复后全部送达")
    void blockStrategy_backpressures_neverExceedsCapacity() throws Exception {
        startStack(30, GatewayProperties.OverflowStrategy.BLOCK, false);
        log.info("[并发顺序-用例3] 容量=30，BLOCK 策略，慢速持续生产 60 条并监控内存水位");
        Thread producer = new Thread(() -> {
            try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
                device.hello("d-block");
                for (int i = 1; i <= 60; i++) {
                    device.sendJsonReading(i, "temp", i, 100L + i);
                    Thread.sleep(10);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        producer.start();

        int maxObserved = 0;
        long deadline = System.currentTimeMillis() + 3_000;
        while (producer.isAlive() && System.currentTimeMillis() < deadline) {
            maxObserved = Math.max(maxObserved, queue.size());
            assertTrue(queue.size() <= 30, "缓冲绝不允许超过容量上限: " + queue.size());
            Thread.sleep(5);
        }
        producer.join(3_000);
        assertTrue(maxObserved >= 30, "BLOCK 背压应使水位达到容量上限");
        log.info("[并发顺序-用例3] 阻塞期间观测到最高水位={} (上限30)，恢复上游", maxObserved);

        upstream.setAvailable(true);
        producer.join(5_000);
        awaitCommitted(60, 20_000);
        assertEquals(60, upstream.committedCount());
        log.info("[并发顺序-用例3] 通过：背压生效、水位未超上限、60 条全部送达");
    }
}
