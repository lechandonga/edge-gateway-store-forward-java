package com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.device.DeviceRegistry;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;

/**
 * 跨协议共用的设备注册与量程校验，保证不同来源归一化后口径一致。
 */
final class CommonValidation {

    private CommonValidation() {
    }

    static DecodeResult check(String deviceId, long seq, String metricName, double value,
                              long deviceTime, byte[] raw, DeviceRegistry registry) {
        DeviceRegistry.DeviceSpec device = registry.find(deviceId).orElse(null);
        if (device == null) {
            return new DecodeResult.Rejected(
                    RejectReason.UNKNOWN_DEVICE, "device not registered: " + deviceId, Hex.encode(raw));
        }
        DeviceRegistry.MetricSpec spec = device.metrics().get(metricName);
        if (spec == null) {
            return new DecodeResult.Rejected(
                    RejectReason.MISSING_FIELD,
                    "metric '" + metricName + "' not defined for device " + deviceId, Hex.encode(raw));
        }
        if (value < spec.min() || value > spec.max()) {
            return new DecodeResult.Rejected(
                    RejectReason.VALUE_OUT_OF_RANGE,
                    "value " + value + " out of range [" + spec.min() + "," + spec.max() + "] for " + metricName,
                    Hex.encode(raw));
        }
        if (seq < 0) {
            return new DecodeResult.Rejected(
                    RejectReason.MALFORMED_FRAME, "negative SEQ: " + seq, Hex.encode(raw));
        }
        return new DecodeResult.Parsed(deviceId, seq, metricName, value, spec.unit(), deviceTime);
    }
}
