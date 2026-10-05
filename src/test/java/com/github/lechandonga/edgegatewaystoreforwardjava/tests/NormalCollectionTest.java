package com.github.lechandonga.edgegatewaystoreforwardjava.tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务一：正常采集。
 * 覆盖 JSON 设备、KV 二进制设备多协议接入、归一化、顺序提交到模拟上游。
 */
@DisplayName("业务一：正常采集（JSON/KV 多协议归一化采集）")
class NormalCollectionTest extends AbstractGatewayTest {

    private static final Logger log = LoggerFactory.getLogger(NormalCollectionTest.class);

    @BeforeEach
    void setUp() throws Exception {
        startStack();
    }

    @AfterEach
    void tearDown() {
        stopStack();
    }

    @Test
    @DisplayName("用例1：JSON 设备读数可被归一化并全部送达上游")
    void jsonDevice_readingsDelivered() throws Exception {
        log.info("[正常采集-用例1] JSON 设备 d-json 接入并发送 5 条读数");
        try (DeviceSimClient device = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            device.hello("d-json");
            for (int i = 1; i <= 5; i++) {
                device.sendJsonReading(i, "temp", 20.0 + i, 1_728_000_000_000L + i);
            }
        }
        awaitCommitted(5, 5_000);
        awaitBacklogZero(5_000);
        assertEquals(5, metrics.getAccepted());
        assertEquals(5, metrics.getPersisted());
        assertEquals(0, metrics.getRejected());
        assertTrue(upstream.committedKeysView().stream().allMatch(k -> k.startsWith("d-json#")));
        log.info("[正常采集-用例1] 通过：上游落库 5 条，缓冲清零，无拒绝");
    }

    @Test
    @DisplayName("用例2：JSON 与 KV 两类设备同时接入，读数统一归一为相同事件模型")
    void mixedProtocols_normalizedToSameModel() throws Exception {
        log.info("[正常采集-用例2] JSON 设备与 KV 设备各发 3 条读数");
        try (DeviceSimClient json = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort());
             DeviceSimClient kv = new DeviceSimClient("127.0.0.1", deviceServer.getBoundPort())) {
            json.hello("d-json");
            kv.hello("d-kv");
            for (int i = 1; i <= 3; i++) {
                json.sendJsonReading(i, "temp", 30.0 + i, 1_728_000_000_000L + i);
                kv.sendKvReading(i, 1_728_000_000_000L + i, 60.0 + i, "humidity");
            }
        }
        awaitCommitted(6, 5_000);
        assertEquals(6, metrics.getAccepted());
        assertEquals(0, metrics.getRejected());
        assertTrue(upstream.committedKeysView().contains("d-json#1"));
        assertTrue(upstream.committedKeysView().contains("d-kv#3"));
        log.info("[正常采集-用例2] 通过：两类协议 6 条读数全部归一化送达，幂等键正确");
    }
}
