package com.github.lechandonga.edgegatewaystoreforwardjava.edge.model;

/**
 * 归一化后的统一设备事件。
 *
 * @param id         网关全局唯一事件 id（持久化后分配）
 * @param deviceId   设备 id
 * @param seq        设备内单调递增序号，用于上游幂等去重
 * @param metric     指标名（归一化后的英文指标）
 * @param value      读数
 * @param unit       单位
 * @param deviceTime 设备上送时间（epoch milli），0 表示设备未上送
 * @param ingestTime 网关接收时间（epoch milli）
 * @param source     来源协议
 */
public record DeviceEvent(
        long id,
        String deviceId,
        long seq,
        String metric,
        double value,
        String unit,
        long deviceTime,
        long ingestTime,
        String source
) {
    /** 上游幂等键：同一设备同序号只允许产生一条记录。 */
    public String dedupKey() {
        return deviceId + "#" + seq;
    }
}
