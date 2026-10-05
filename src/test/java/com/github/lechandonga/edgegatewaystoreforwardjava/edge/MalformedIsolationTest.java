package com.github.lechandonga.edgegatewaystoreforwardjava.edge;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.DecodeResult;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.FrameDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.ModbusTextDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：异常报文隔离。每类异常都带可区分原因，不静默吞掉，且不影响同连接后续帧。
 */
class MalformedIsolationTest implements BusinessTest {

    private GatewayTestEnv env;

    private GatewayTestEnv env() throws Exception {
        env = new GatewayTestEnv(GatewayTestEnv.defaultProps(GatewayTestEnv.tempDir("isolate")),
                GatewayTestEnv.defaultRegistry(), false);
        return env;
    }

    @AfterEach
    void tearDown() {
        if (env != null) {
            env.close();
        }
    }

    private void feedThroughPipeline(GatewayTestEnv e, FrameDecoder decoder, String... frames) {
        StringBuilder all = new StringBuilder();
        for (String f : frames) {
            all.append(f);
        }
        byte[] data = all.toString().getBytes(StandardCharsets.US_ASCII);
        for (DecodeResult r : decoder.feed(data, data.length)) {
            e.pipeline.handle(r, "MODBUS-TXT", "test");
        }
    }

    private String goodFrame(long seq) {
        return "#1|SEQ=" + seq + "|REG=0x00|V=20|TS=" + System.currentTimeMillis() + "!\n";
    }

    @Test
    @DisplayName("异常隔离-错误帧头按 BAD_HEADER 隔离")
    void badHeader() throws Exception {
        GatewayTestEnv e = env();
        feedThroughPipeline(e, new ModbusTextDecoder(1024, e.registry),
                "GARBAGE-NO-HASH\n");
        expectQuarantine(e, RejectReason.BAD_HEADER);
    }

    @Test
    @DisplayName("异常隔离-缺尾标记/字段缺失给出 MALFORMED_FRAME 与 MISSING_FIELD")
    void malformedAndMissingField() throws Exception {
        GatewayTestEnv e = env();
        feedThroughPipeline(e, new ModbusTextDecoder(1024, e.registry),
                "#1|SEQ=1|REG=0x00|V=20\n",
                "#1|SEQ=2|REG=0x00|TS=" + System.currentTimeMillis() + "!\n");
        assertEquals(2, e.quarantined.size());
        assertEquals(RejectReason.MALFORMED_FRAME, e.quarantined.get(0).reason());
        assertEquals(RejectReason.MISSING_FIELD, e.quarantined.get(1).reason());
        assertTrue(e.quarantined.get(0).rawPayloadHex().length() > 0, "隔离记录须保留原文 hex");
    }

    @Test
    @DisplayName("异常隔离-数值非法/超量程按 BAD_NUMBER 与 VALUE_OUT_OF_RANGE 隔离")
    void badNumberAndRange() throws Exception {
        GatewayTestEnv e = env();
        feedThroughPipeline(e, new ModbusTextDecoder(1024, e.registry),
                "#1|SEQ=1|REG=0x00|V=abc|TS=" + System.currentTimeMillis() + "!\n",
                "#1|SEQ=2|REG=0x00|V=9999|TS=" + System.currentTimeMillis() + "!\n");
        assertEquals(RejectReason.BAD_NUMBER, e.quarantined.get(0).reason());
        assertEquals(RejectReason.VALUE_OUT_OF_RANGE, e.quarantined.get(1).reason());
    }

    @Test
    @DisplayName("异常隔离-未注册设备按 UNKNOWN_DEVICE 隔离")
    void unknownDevice() throws Exception {
        GatewayTestEnv e = env();
        feedThroughPipeline(e, new ModbusTextDecoder(1024, e.registry),
                "#99|SEQ=1|REG=0x00|V=20|TS=" + System.currentTimeMillis() + "!\n");
        RejectedRecordView view = expectQuarantine(e, RejectReason.UNKNOWN_DEVICE);
        assertEquals("MOD-99", view.deviceId);
    }

    @Test
    @DisplayName("异常隔离-超长帧按 FRAME_TOO_LONG 隔离并跳过，后续正常帧不受影响")
    void overlongFrameSkipped() throws Exception {
        GatewayTestEnv e = env();
        FrameDecoder decoder = new ModbusTextDecoder(60, e.registry);
        String longFrame = "#1|SEQ=1|REG=0x00|V=" + "x".repeat(80) + "!\n";
        String good = goodFrame(2);
        String all = longFrame + good;
        byte[] data = all.getBytes(StandardCharsets.US_ASCII);
        List<DecodeResult> results = decoder.feed(data, data.length);
        for (DecodeResult r : results) {
            e.pipeline.handle(r, "MODBUS-TXT", "test");
        }
        expectQuarantine(e, RejectReason.FRAME_TOO_LONG);
        assertEquals(1, e.buffer.pendingCount(), "超长帧后正常帧必须照常入库");
    }

    @Test
    @DisplayName("异常隔离-连接中断时残留半包按 MALFORMED_FRAME 隔离，不静默丢失")
    void halfFrameAtClose() throws Exception {
        GatewayTestEnv e = env();
        FrameDecoder decoder = new ModbusTextDecoder(1024, e.registry);
        byte[] partial = "#1|SEQ=1|REG=0x0".getBytes(StandardCharsets.US_ASCII);
        assertEquals(0, decoder.feed(partial, partial.length).size());
        List<DecodeResult> tail = decoder.endOfStream();
        for (DecodeResult r : tail) {
            e.pipeline.handle(r, "MODBUS-TXT", "test");
        }
        expectQuarantine(e, RejectReason.MALFORMED_FRAME);
    }

    private RejectedRecordView expectQuarantine(GatewayTestEnv e, RejectReason reason) {
        assertEquals(1, e.quarantined.size(), "应有且仅有一条隔离记录");
        var rec = e.quarantined.get(0);
        assertInstanceOf(RejectReason.class, rec.reason());
        assertEquals(reason, rec.reason());
        return new RejectedRecordView(rec.deviceId());
    }

    private record RejectedRecordView(String deviceId) {
    }
}
