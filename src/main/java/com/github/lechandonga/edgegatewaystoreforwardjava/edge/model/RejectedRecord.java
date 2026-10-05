package com.github.lechandonga.edgegatewaystoreforwardjava.edge.model;

import java.time.Instant;

/**
 * 被隔离的畸形报文记录：不静默吞掉，任何异常都带明确原因与原文。
 */
public record RejectedRecord(
        Instant isolatedAt,
        String deviceId,
        String protocol,
        String remoteAddress,
        RejectReason reason,
        String detail,
        String rawPayloadHex
) {
}
