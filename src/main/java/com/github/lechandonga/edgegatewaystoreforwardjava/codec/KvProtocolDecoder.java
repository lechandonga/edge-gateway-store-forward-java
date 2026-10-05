package com.github.lechandonga.edgegatewaystoreforwardjava.codec;

import com.github.lechandonga.edgegatewaystoreforwardjava.model.DeviceEvent;
import com.github.lechandonga.edgegatewaystoreforwardjava.model.RejectReason;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;

/**
 * KV 二进制协议（proto=2），载荷布局（大端）：
 * <pre>
 * long deviceSeq | long ts | double value | u16 metricLen | metricUtf8
 * </pre>
 */
public class KvProtocolDecoder implements DeviceProtocolDecoder {

    @Override
    public DeviceEvent decode(String deviceId, byte[] payload) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (payload.length < 26) {
                throw new FrameDecodeException(RejectReason.MALFORMED_PAYLOAD,
                        "kv payload too short: " + payload.length);
            }
            long deviceSeq = in.readLong();
            long ts = in.readLong();
            double value = in.readDouble();
            int metricLen = in.readUnsignedShort();
            if (metricLen == 0 || in.available() < metricLen) {
                throw new FrameDecodeException(RejectReason.MALFORMED_PAYLOAD,
                        "metric length mismatch: declared=" + metricLen
                                + ", available=" + in.available());
            }
            byte[] metricBytes = new byte[metricLen];
            in.readFully(metricBytes);
            String metric = new String(metricBytes, StandardCharsets.UTF_8);
            if (metric.isBlank()) {
                throw new FrameDecodeException(RejectReason.MISSING_OR_INVALID_FIELD,
                        "blank metric");
            }
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw new FrameDecodeException(RejectReason.FIELD_VALUE_OUT_OF_RANGE,
                        "value must be finite");
            }
            if (ts < 0 || deviceSeq < 0) {
                throw new FrameDecodeException(RejectReason.FIELD_VALUE_OUT_OF_RANGE,
                        "ts/seq must be >= 0");
            }
            return new DeviceEvent(-1, deviceId, deviceSeq, metric, value, ts,
                    System.currentTimeMillis());
        } catch (FrameDecodeException e) {
            throw e;
        } catch (Exception e) {
            throw new FrameDecodeException(RejectReason.MALFORMED_PAYLOAD,
                    "kv decode failed: " + e.getMessage());
        }
    }
}
