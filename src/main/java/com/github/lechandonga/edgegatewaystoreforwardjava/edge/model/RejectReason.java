package com.github.lechandonga.edgegatewaystoreforwardjava.edge.model;

/**
 * 可区分的报文拒绝/隔离原因。
 */
public enum RejectReason {
    /** 协议帧头不合法 */
    BAD_HEADER,
    /** 缺少结束符（半包超时，等待后续字节）不会隔离；带结束符但结构残缺 */
    MALFORMED_FRAME,
    /** 帧长度字段与实际不符 */
    LENGTH_MISMATCH,
    /** 报文超过协议允许的最大长度 */
    FRAME_TOO_LONG,
    /** 必填字段缺失 */
    MISSING_FIELD,
    /** 数值字段无法解析 */
    BAD_NUMBER,
    /** 设备号非法（未注册/格式错误） */
    UNKNOWN_DEVICE,
    /** 读数值超出量程或为 NaN/无穷 */
    VALUE_OUT_OF_RANGE,
    /** 时间戳非法 */
    BAD_TIMESTAMP,
    /** 本地缓冲写满，按 reject-full 策略隔离 */
    BUFFER_FULL
}
