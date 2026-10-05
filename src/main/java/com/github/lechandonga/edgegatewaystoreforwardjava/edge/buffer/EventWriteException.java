package com.github.lechandonga.edgegatewaystoreforwardjava.edge.buffer;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;

/**
 * 缓冲写入失败（容量满）。携带明确原因，由入口决定拒绝隔离还是阻塞背压。
 */
public class EventWriteException extends RuntimeException {

    private final RejectReason reason;

    public EventWriteException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
