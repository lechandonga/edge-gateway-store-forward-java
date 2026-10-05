package com.github.lechandonga.edgegatewaystoreforwardjava.model;

/**
 * 归一化后的统一设备事件。
 *
 * <p>无论设备采用哪种协议（JSON / KV 二进制帧），解码后都转换为本结构。
 * {@code seq} 是该事件在全局缓冲队列中的单调递增序号，由缓冲层分配，
 * 同时作为上游幂等去重键的组成部分（deviceId + deviceSeq 唯一，seq 用于排序）。
 */
public record DeviceEvent(
        long seq,
        String deviceId,
        long deviceSeq,
        String metric,
        double value,
        long eventTime,
        long ingestTime
) {
    /** 幂等键：同一设备的同一设备侧序号只允许上游落库一次。 */
    public String idempotencyKey() {
        return deviceId + "#" + deviceSeq;
    }
}
