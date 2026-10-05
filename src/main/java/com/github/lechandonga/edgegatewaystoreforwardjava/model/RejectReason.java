package com.github.lechandonga.edgegatewaystoreforwardjava.model;

/**
 * 报文被拒绝 / 隔离的可区分原因。每类异常单独计数，避免静默吞掉。
 */
public enum RejectReason {
    /** 帧长度超过协议允许上限（超长报文）。 */
    FRAME_TOO_LONG,
    /** 帧头声明长度与实际字节不符（半包在超时内未补齐）。 */
    FRAME_TRUNCATED,
    /** 帧头魔数错误，非本协议报文。 */
    BAD_MAGIC,
    /** JSON 语法非法 / 无法解析。 */
    MALFORMED_PAYLOAD,
    /** 缺少必填字段或类型错误。 */
    MISSING_OR_INVALID_FIELD,
    /** 字段取值非法（如 NaN/Infinity、时间戳为负）。 */
    FIELD_VALUE_OUT_OF_RANGE,
    /** 设备侧序号倒退，违反同一设备单调递增约束。 */
    DEVICE_SEQ_GOES_BACKWARDS,
    /** 本地缓冲已满且策略为 REJECT_NEW（BLOCK 策略会阻塞而不是拒绝）。 */
    BUFFER_FULL
}
