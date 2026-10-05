package com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.device.DeviceRegistry;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;

import java.nio.charset.StandardCharsets;

/**
 * 模拟 MQTT/JSON 设备（如传感器网关）协议：每行一个 JSON：
 * <pre>
 * {"deviceId":"MQTT-101","seq":7,"metric":"temperature","value":23.5,"unit":"C","ts":1728000000000}
 * </pre>
 */
public class JsonLineDecoder extends AbstractLineDecoder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DeviceRegistry registry;

    public JsonLineDecoder(int maxFrameBytes, DeviceRegistry registry) {
        super(maxFrameBytes);
        this.registry = registry;
    }

    @Override
    public String protocol() {
        return "JSON-LINE";
    }

    @Override
    protected DecodeResult parseLine(byte[] raw) {
        String hex = Hex.encode(raw);
        JsonNode node;
        try {
            node = MAPPER.readTree(raw);
        } catch (Exception e) {
            return new DecodeResult.Rejected(RejectReason.MALFORMED_FRAME,
                    "invalid JSON: " + e.getMessage(), hex);
        }
        if (!node.isObject()) {
            return new DecodeResult.Rejected(RejectReason.MALFORMED_FRAME,
                    "JSON root must be an object", hex);
        }
        String deviceId = textField(node, "deviceId");
        String metricName = textField(node, "metric");
        if (deviceId == null || deviceId.isBlank()) {
            return new DecodeResult.Rejected(RejectReason.MISSING_FIELD, "missing deviceId", hex);
        }
        if (metricName == null || metricName.isBlank()) {
            return new DecodeResult.Rejected(RejectReason.MISSING_FIELD, "missing metric", hex);
        }
        JsonNode seqNode = node.get("seq");
        JsonNode valueNode = node.get("value");
        JsonNode tsNode = node.get("ts");
        if (seqNode == null || !seqNode.canConvertToLong()) {
            return new DecodeResult.Rejected(RejectReason.BAD_NUMBER, "missing/invalid seq", hex);
        }
        if (valueNode == null || !valueNode.isNumber()) {
            return new DecodeResult.Rejected(RejectReason.BAD_NUMBER, "missing/non-numeric value", hex);
        }
        double value = valueNode.asDouble();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return new DecodeResult.Rejected(RejectReason.VALUE_OUT_OF_RANGE, "value NaN/Infinity", hex);
        }
        long deviceTime = 0L;
        if (tsNode != null) {
            if (!tsNode.canConvertToLong()) {
                return new DecodeResult.Rejected(RejectReason.BAD_TIMESTAMP, "invalid ts", hex);
            }
            deviceTime = tsNode.asLong();
            if (deviceTime < 0) {
                return new DecodeResult.Rejected(RejectReason.BAD_TIMESTAMP, "ts negative", hex);
            }
        }
        return CommonValidation.check(deviceId, seqNode.asLong(), metricName, value,
                deviceTime, raw, registry);
    }

    private static String textField(JsonNode node, String name) {
        JsonNode child = node.get(name);
        return child != null && child.isTextual() ? child.asText() : null;
    }
}
