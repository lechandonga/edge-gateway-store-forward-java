package com.github.lechandonga.edgegatewaystoreforwardjava.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.edgegatewaystoreforwardjava.model.DeviceEvent;
import com.github.lechandonga.edgegatewaystoreforwardjava.model.RejectReason;

/**
 * JSON 协议（proto=1）。两种报文：
 * <ul>
 *   <li>HELLO：{"type":"hello","deviceId":"d-1"}</li>
 *   <li>READING：{"type":"reading","seq":12,"metric":"temp","value":36.5,"ts":1728000000000}</li>
 * </ul>
 */
public class JsonProtocolDecoder implements DeviceProtocolDecoder {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public DeviceEvent decode(String deviceId, byte[] payload) {
        JsonNode node;
        try {
            node = mapper.readTree(payload);
        } catch (Exception e) {
            throw new FrameDecodeException(RejectReason.MALFORMED_PAYLOAD,
                    "invalid json: " + e.getMessage());
        }
        if (node == null || !node.isObject()) {
            throw new FrameDecodeException(RejectReason.MALFORMED_PAYLOAD, "json is not an object");
        }
        String type = text(node, "type", null);
        if ("hello".equals(type)) {
            throw new FrameDecodeException(RejectReason.MISSING_OR_INVALID_FIELD,
                    "hello frame must be handled by session, not decoder");
        }
        long deviceSeq = longField(node, "seq");
        String metric = text(node, "metric", null);
        if (metric == null || metric.isBlank()) {
            throw new FrameDecodeException(RejectReason.MISSING_OR_INVALID_FIELD, "missing metric");
        }
        JsonNode valueNode = node.get("value");
        if (valueNode == null || !valueNode.isNumber()) {
            throw new FrameDecodeException(RejectReason.MISSING_OR_INVALID_FIELD,
                    "value missing or not a number");
        }
        double value = valueNode.asDouble();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new FrameDecodeException(RejectReason.FIELD_VALUE_OUT_OF_RANGE,
                    "value must be finite, got " + value);
        }
        long ts = node.has("ts") && node.get("ts").canConvertToLong()
                ? node.get("ts").asLong() : System.currentTimeMillis();
        if (ts < 0) {
            throw new FrameDecodeException(RejectReason.FIELD_VALUE_OUT_OF_RANGE,
                    "ts must be >= 0");
        }
        if (deviceSeq < 0) {
            throw new FrameDecodeException(RejectReason.FIELD_VALUE_OUT_OF_RANGE,
                    "seq must be >= 0");
        }
        return new DeviceEvent(-1, deviceId, deviceSeq, metric, value, ts,
                System.currentTimeMillis());
    }

    private static long longField(JsonNode node, String name) {
        JsonNode v = node.get(name);
        if (v == null || !v.canConvertToLong()) {
            throw new FrameDecodeException(RejectReason.MISSING_OR_INVALID_FIELD,
                    "missing/integer field: " + name);
        }
        return v.asLong();
    }

    private static String text(JsonNode node, String name, String def) {
        JsonNode v = node.get(name);
        return v == null || v.isNull() ? def : v.asText(def);
    }
}
