package com.github.lechandonga.edgegatewaystoreforwardjava.edge.isolate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectedRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 隔离报文 JSONL 落盘：重启后仍可审计全部拒绝记录。
 */
public class FileQuarantineSink implements Quarantine.RejectedRecordSink {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path file;

    public FileQuarantineSink(Path dataDir) {
        try {
            Files.createDirectories(dataDir);
            this.file = dataDir.resolve("quarantine.jsonl");
        } catch (IOException e) {
            throw new IllegalStateException("cannot create quarantine dir " + dataDir, e);
        }
    }

    @Override
    public synchronized void write(RejectedRecord record) {
        try {
            byte[] line = MAPPER.writeValueAsBytes(record);
            byte[] nl = "\n".getBytes(StandardCharsets.UTF_8);
            try (var ch = Files.newByteChannel(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(line.length + nl.length);
                buffer.put(line).put(nl).flip();
                ch.write(buffer);
            }
        } catch (IOException e) {
            throw new IllegalStateException("quarantine write failed", e);
        }
    }

    public Path file() {
        return file;
    }
}
