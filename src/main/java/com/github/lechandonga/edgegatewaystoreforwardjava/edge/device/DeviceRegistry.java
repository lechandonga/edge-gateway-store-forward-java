package com.github.lechandonga.edgegatewaystoreforwardjava.edge.device;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备与指标元数据（量程/单位）注册表。模拟"设备接入配置"，
 * 未注册设备的报文按 {@code UNKNOWN_DEVICE} 隔离。
 */
public class DeviceRegistry {

    public record MetricSpec(String name, String unit, double min, double max) {
    }

    public record DeviceSpec(String deviceId, String type, Map<String, MetricSpec> metrics) {
    }

    private final Map<String, DeviceSpec> devices = new ConcurrentHashMap<>();

    public void register(DeviceSpec spec) {
        devices.put(spec.deviceId(), spec);
    }

    public Optional<DeviceSpec> find(String deviceId) {
        return Optional.ofNullable(devices.get(deviceId));
    }

    public Optional<MetricSpec> metric(String deviceId, String metricName) {
        return find(deviceId).flatMap(s -> Optional.ofNullable(s.metrics().get(metricName)));
    }

    public int size() {
        return devices.size();
    }
}
