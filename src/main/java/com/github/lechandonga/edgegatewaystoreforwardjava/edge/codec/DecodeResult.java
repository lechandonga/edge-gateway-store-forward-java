package com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;

/**
 * 解码器对一段字节流处理后的单条结果。
 */
public sealed interface DecodeResult {

    /** 半包：未收到帧结束符，继续等待后续字节，不拒绝。 */
    record NeedMore() implements DecodeResult {
        public static final NeedMore INSTANCE = new NeedMore();
    }

    /** 解析成功的归一化事件骨架（id / ingestTime 由入口层补全）。 */
    record Parsed(String deviceId, long seq, String metric, double value,
                  String unit, long deviceTime) implements DecodeResult {
    }

    /** 拒绝：带明确原因，绝不能静默丢弃。 */
    record Rejected(RejectReason reason, String detail, String rawHex) implements DecodeResult {
    }
}
