package com.github.lechandonga.edgegatewaystoreforwardjava.edge.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.DeviceEvent;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于单文件 WAL 的持久化事件缓冲。
 *
 * <ul>
 *   <li>写入：在文件尾追加"4字节长度 + JSON"，append 后 force(fsync) 才算提交，
 *       因此未确认数据在进程崩溃/重启后仍可补发，且不会覆盖任何未确认数据。</li>
 *   <li>确认：仅更新 {@code ackedOffset} 指针文件；当队头事件被确认后才截断释放空间，
 *       所以一批数据要么整批确认（offset 越过整批），要么一条都不确认——不存在提交一半。</li>
 *   <li>容量：以"未确认条数 + 磁盘字节"双重上限控制，写满抛 {@link EventWriteException}，
 *       拒绝还是阻塞由上层策略决定（可配置），不静默丢弃。</li>
 * </ul>
 */
public class WriteAheadBuffer implements AutoCloseable {

    static final byte[] MAGIC = "EVT1".getBytes(StandardCharsets.US_ASCII);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Path walFile;
    private final Path offsetFile;
    private final Path idFile;
    private final int maxEvents;
    private final long maxBytes;
    private final RandomAccessFile raf;
    private final AtomicLong liveCount = new AtomicLong();
    private final AtomicLong nextEventId = new AtomicLong(1);
    private long ackedOffset;
    private long writeOffset;

    public WriteAheadBuffer(Path dataDir, int maxEvents, long maxBytes) {
        try {
            Files.createDirectories(dataDir);
            this.walFile = dataDir.resolve("events.wal");
            this.offsetFile = dataDir.resolve("events.offset");
            this.idFile = dataDir.resolve("events.id");
            this.maxEvents = maxEvents;
            this.maxBytes = maxBytes;
            this.raf = new RandomAccessFile(walFile.toFile(), "rw");
            long fileLen = raf.length();
            long restored = Files.exists(offsetFile)
                    ? Long.parseLong(Files.readString(offsetFile).strip())
                    : 0L;
            this.ackedOffset = Math.max(0, Math.min(restored, fileLen));
            long savedId = Files.exists(idFile)
                    ? Long.parseLong(Files.readString(idFile).strip())
                    : 1L;
            this.nextEventId.set(Math.max(1L, savedId));
            scanBoundaries(this.ackedOffset, fileLen);
            this.writeOffset = fileLen;
            this.raf.seek(fileLen);
        } catch (IOException e) {
            throw new IllegalStateException("failed to open WAL in " + dataDir, e);
        }
    }

    private void scanBoundaries(long from, long fileLen) throws IOException {
        long pos = from;
        long count = 0;
        byte[] magic = new byte[4];
        while (pos + 8 <= fileLen) {
            raf.seek(pos);
            raf.readFully(magic);
            int len = raf.readInt();
            if (len <= 0 || pos + 8L + len > fileLen) {
                break;
            }
            pos += 8L + len;
            count++;
        }
        this.liveCount.set(count);
    }

    /**
     * 追加一条事件。容量不足时抛 {@link EventWriteException}(BUFFER_FULL)，
     * 不覆盖、不丢弃任何未确认数据。
     */
    public DeviceEvent append(DeviceEvent skeleton) {
        try {
            synchronized (this) {
                if (liveCount.get() >= maxEvents) {
                    throw new EventWriteException(RejectReason.BUFFER_FULL,
                            "buffer full: pending events " + liveCount.get() + " >= capacity " + maxEvents);
                }
                DeviceEvent event = new DeviceEvent(nextEventId.getAndIncrement(),
                        skeleton.deviceId(), skeleton.seq(), skeleton.metric(), skeleton.value(),
                        skeleton.unit(), skeleton.deviceTime(), skeleton.ingestTime(), skeleton.source());
                byte[] payload = mapper.writeValueAsBytes(event);
                long needWalBytes = raf.length() + payload.length + 8L;
                if (needWalBytes > maxBytes) {
                    throw new EventWriteException(RejectReason.BUFFER_FULL,
                            "buffer full: wal bytes " + needWalBytes + " > max " + maxBytes);
                }
                raf.seek(writeOffset);
                raf.write(MAGIC);
                raf.writeInt(payload.length);
                raf.write(payload);
                raf.getFD().sync();
                Files.writeString(idFile, Long.toString(nextEventId.get()));
                writeOffset += 8 + payload.length;
                liveCount.incrementAndGet();
                return event;
            }
        } catch (EventWriteException e) {
            throw e;
        } catch (IOException e) {
            throw new IllegalStateException("WAL append failed", e);
        }
    }

    /** 顺序读取未确认事件（从最早一条开始），保证补发顺序。 */
    public synchronized List<DeviceEvent> readPending(int limit) {
        try {
            List<DeviceEvent> list = new ArrayList<>(Math.min(limit, 256));
            long pos = ackedOffset;
            byte[] magic = new byte[4];
            for (int i = 0; i < limit && pos < writeOffset; i++) {
                raf.seek(pos);
                raf.readFully(magic);
                int len = raf.readInt();
                if (len <= 0 || pos + 8L + len > writeOffset) {
                    break;
                }
                byte[] payload = new byte[len];
                raf.readFully(payload);
                list.add(mapper.readValue(payload, DeviceEvent.class));
                pos += 8L + len;
            }
            return list;
        } catch (IOException e) {
            throw new IllegalStateException("WAL read failed", e);
        }
    }

    /**
     * 确认从队头开始的前 n 条未确认事件。
     * 只能顺序推进确认点：重复确认（n 超过当前未确认数）直接忽略，不报错、不产生副作用。
     */
    public synchronized long acknowledge(long n) {
        long remaining = n;
        long pos = ackedOffset;
        while (remaining > 0 && pos < writeOffset) {
            long next = nextRecord(pos);
            if (next < 0) {
                break;
            }
            pos = next;
            remaining--;
        }
        long advanced = pos - ackedOffset;
        if (advanced > 0) {
            ackedOffset = pos;
            liveCount.addAndGet(-(int) (n - remaining));
            persistOffset();
            if (ackedOffset >= writeOffset) {
                compact();
            }
        }
        return n - remaining;
    }

    private long nextRecord(long pos) {
        try {
            raf.seek(pos);
            byte[] magic = new byte[4];
            raf.readFully(magic);
            int len = raf.readInt();
            if (len <= 0 || pos + 8L + len > writeOffset) {
                return -1;
            }
            return pos + 8L + len;
        } catch (IOException e) {
            return -1;
        }
    }

    private void compact() {
        try {
            raf.setLength(0);
            writeOffset = 0;
            ackedOffset = 0;
            raf.getFD().sync();
            persistOffset();
        } catch (IOException e) {
            throw new IllegalStateException("WAL compact failed", e);
        }
    }

    private void persistOffset() {
        try {
            Files.writeString(offsetFile, Long.toString(ackedOffset));
        } catch (IOException e) {
            throw new IllegalStateException("offset persist failed", e);
        }
    }

    public long pendingCount() {
        return liveCount.get();
    }

    @Override
    public synchronized void close() {
        try {
            raf.close();
        } catch (IOException ignored) {
            // close best-effort
        }
    }
}
