package com.github.lechandonga.edgegatewaystoreforwardjava.model;

/**
 * 被隔离的异常报文记录，写入 quarantine 日志文件，不进入正常缓冲。
 */
public record RejectedFrame(
        String deviceId,
        RejectReason reason,
        String detail,
        long timestamp
) {
    @Override
    public String toString() {
        String payload = detail == null ? "" : detail.replace('\n', ' ').replace('\r', ' ');
        if (payload.length() > 200) {
            payload = payload.substring(0, 200) + "...";
        }
        return timestamp + "\t" + (deviceId == null ? "unknown" : deviceId)
                + "\t" + reason + "\t" + payload;
    }
}
