package com.github.lechandonga.edgegatewaystoreforwardjava.edge;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.DecodeResult;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.JsonLineDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.ModbusTextDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.DeviceEvent;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 业务：正常采集与归一化。
 */
class NormalCollectionTest implements BusinessTest {

    private GatewayTestEnv env;

    @AfterEach
    void tearDown() {
        if (env != null) {
            env.close();
        }
    }

    private GatewayTestEnv env() throws Exception {
        env = new GatewayTestEnv(GatewayTestEnv.defaultProps(GatewayTestEnv.tempDir("normal")),
                GatewayTestEnv.defaultRegistry(), false);
        return env;
    }

    @Test
    @DisplayName("正常采集-Modbus 文本帧解析并归一化为统一事件")
    void modbusFrameParsed() throws Exception {
        GatewayTestEnv e = env();
        String frame = "#1|SEQ=3|REG=0x00|V=23.75|TS=" + System.currentTimeMillis() + "!\n";
        var decoder = new ModbusTextDecoder(1024, e.registry);
        List<DecodeResult> results = decoder.feed(bytes(frame), frame.length());
        assertEquals(1, results.size());
        DecodeResult.Parsed parsed = assertInstanceOf(DecodeResult.Parsed.class, results.get(0));
        assertEquals("MOD-1", parsed.deviceId());
        assertEquals(3, parsed.seq());
        assertEquals("temperature", parsed.metric());
        assertEquals(23.75, parsed.value(), 1e-9);
        assertEquals("C", parsed.unit());
    }

    @Test
    @DisplayName("正常采集-JSON 行帧解析并归一化为统一事件")
    void jsonFrameParsed() throws Exception {
        GatewayTestEnv e = env();
        String frame = "{\"deviceId\":\"MQTT-101\",\"seq\":9,\"metric\":\"temperature\","
                + "\"value\":18.2,\"ts\":" + System.currentTimeMillis() + "}\n";
        var decoder = new JsonLineDecoder(1024, e.registry);
        List<DecodeResult> results = decoder.feed(bytes(frame), frame.length());
        DecodeResult.Parsed parsed = assertInstanceOf(DecodeResult.Parsed.class, results.get(0));
        assertEquals("MQTT-101", parsed.deviceId());
        assertEquals(9, parsed.seq());
        assertEquals(18.2, parsed.value(), 1e-9);
    }

    @Test
    @DisplayName("正常采集-归一化事件成功入库并被上游确认，积压清零")
    void acceptedThenForwarded() throws Exception {
        GatewayTestEnv e = env();
        DecodeResult.Parsed parsed = new DecodeResult.Parsed("MOD-1", 1, "temperature",
                21.0, "C", System.currentTimeMillis());
        e.pipeline.handle(parsed, "MODBUS-TXT", "127.0.0.1:0");
        assertEquals(1, e.buffer.pendingCount());
        e.forwarder.pumpOnce();
        assertEquals(0, e.buffer.pendingCount());
        assertEquals(1, e.upstream.storedCount());
        DeviceEvent stored = e.upstream.storedEvents().get(0);
        assertEquals("MOD-1#1", stored.dedupKey());
        assertEquals(1, e.metrics.acceptedEvents());
        assertEquals(1, e.metrics.acknowledgedEvents());
    }

    @Test
    @DisplayName("正常采集-半包等待续包，跨字节分片到达仍拼成完整帧")
    void halfFrameAcrossChunks() throws Exception {
        GatewayTestEnv e = env();
        String frame = "#1|SEQ=2|REG=0x00|V=10|TS=" + System.currentTimeMillis() + "!\n";
        var decoder = new ModbusTextDecoder(1024, e.registry);
        int cut = 7;
        byte[] all = bytes(frame);
        byte[] first = new byte[cut];
        System.arraycopy(all, 0, first, 0, cut);
        List<DecodeResult> r1 = decoder.feed(first, first.length);
        assertEquals(0, r1.size(), "半包不应产生结果也不应被拒绝");
        byte[] rest = new byte[all.length - cut];
        System.arraycopy(all, cut, rest, 0, rest.length);
        List<DecodeResult> r2 = decoder.feed(rest, rest.length);
        assertEquals(1, r2.size());
        assertInstanceOf(DecodeResult.Parsed.class, r2.get(0));
        assertEquals(0, e.quarantined.size());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
