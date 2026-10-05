package com.github.lechandonga.edgegatewaystoreforwardjava.codec;

import com.github.lechandonga.edgegatewaystoreforwardjava.model.RejectReason;

/** 帧 / 报文解码异常，携带可区分的隔离原因。 */
public class FrameDecodeException extends RuntimeException {
    private final RejectReason reason;

    public FrameDecodeException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
