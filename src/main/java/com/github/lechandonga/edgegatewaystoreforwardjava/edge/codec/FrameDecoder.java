package com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec;

import java.util.List;

/**
 * 单连接、有状态的帧解码器。半包数据在解码器内部累积，
 * 超长帧在发现时立即拒绝并跳过，畸形帧与正常帧互不影响。
 */
public interface FrameDecoder {

    /** 协议名，用于隔离记录与指标标签。 */
    String protocol();

    /** 喂入一批字节，返回其中可立即得出的结果（0..n 条）。 */
    List<DecodeResult> feed(byte[] chunk, int length);

    /** 连接断开时调用：残留半包作为畸形帧隔离，避免静默吞掉。 */
    List<DecodeResult> endOfStream();
}
