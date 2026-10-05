package com.github.lechandonga.edgegatewaystoreforwardjava.ingest;

import com.github.lechandonga.edgegatewaystoreforwardjava.config.GatewayProperties;
import com.github.lechandonga.edgegatewaystoreforwardjava.model.RejectedFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 异常报文隔离区：所有被拒绝的帧带原因落盘到 quarantine.log，并打印 WARN 日志，
 * 保证畸形报文可审计、不静默丢失，也不会污染正常缓冲。
 */
public class QuarantineLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(QuarantineLog.class);
    private final BufferedWriter writer;

    public QuarantineLog(GatewayProperties props) {
        File dir = new File(props.getDataDir());
        dir.mkdirs();
        File file = new File(dir, "quarantine.log");
        try {
            this.writer = new BufferedWriter(new FileWriter(file, true));
        } catch (IOException e) {
            throw new IllegalStateException("无法打开隔离日志: " + file, e);
        }
    }

    public synchronized void record(RejectedFrame frame) {
        try {
            writer.write(frame.toString());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            log.error("隔离日志写入失败: {}", frame, e);
        }
        log.warn("报文隔离 | device={} | reason={} | detail={}",
                frame.deviceId(), frame.reason(), frame.detail());
    }

    @Override
    public synchronized void close() {
        try {
            writer.close();
        } catch (IOException ignored) {
        }
    }
}
