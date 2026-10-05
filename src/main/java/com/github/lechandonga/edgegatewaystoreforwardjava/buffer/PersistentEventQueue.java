package com.github.lechandonga.edgegatewaystoreforwardjava.buffer;

import com.github.lechandonga.edgegatewaystoreforwardjava.config.GatewayProperties;
import com.github.lechandonga.edgegatewaystoreforwardjava.model.DeviceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * 单文件 WAL 持久化缓冲。
 *
 * <p>WAL 记录格式（大端）：
 * <pre>
 * EVENT: type=1(1) | seq(8) | u16 deviceIdLen + utf8 | deviceSeq(8) |
 *        u16 metricLen + utf8 | value(8) | eventTime(8) | ingestTime(8) | crc32(4)
 * ACK:   type=2(1) | upToSeq(8) | crc32(4)
 * </pre>
 * 每次追加后 fsync；ACK 表示该序号及之前事件已被上游确认，重启时截断。
 * 内存队列有硬上限 {@code bufferCapacity}，绝不覆盖未确认数据。
 */
public class PersistentEventQueue implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PersistentEventQueue.class);
    private static final byte TYPE_EVENT = 1;
    private static final byte TYPE_ACK = 2;

    private final File walFile;
    private final int capacity;
    private final ArrayDeque<DeviceEvent> pending = new ArrayDeque<>();
    private FileOutputStream walOut;
    private long nextSeq;

    public PersistentEventQueue(GatewayProperties props) {
        this.walFile = new File(props.getDataDir(), "queue.wal");
        this.capacity = props.getBufferCapacity();
        recover();
        try {
            this.walOut = new FileOutputStream(walFile, true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 启动恢复：扫描 WAL，重放未确认事件，重写压缩文件，再以追加模式打开。 */
    private void recover() {
        if (!walFile.exists()) {
            walFile.getParentFile().mkdirs();
            nextSeq = 0;
            log.info("WAL 不存在，从空缓冲启动: {}", walFile.getAbsolutePath());
            return;
        }
        long lastAck = -1;
        long maxSeq = -1;
        List<DeviceEvent> recovered = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(walFile)))) {
            while (true) {
                int typeByte;
                try {
                    typeByte = in.read();
                } catch (EOFException e) {
                    break;
                }
                if (typeByte == -1) {
                    break;
                }
                try {
                    if (typeByte == TYPE_EVENT) {
                        long seq = in.readLong();
                        String deviceId = in.readUTF();
                        long deviceSeq = in.readLong();
                        String metric = in.readUTF();
                        double value = in.readDouble();
                        long eventTime = in.readLong();
                        long ingestTime = in.readLong();
                        long storedCrc = in.readInt() & 0xFFFFFFFFL;
                        maxSeq = Math.max(maxSeq, seq);
                        // 对结构性损坏（字段读不完/UTF 长度非法）会抛 EOF/UTFDataFormat，进入尾部截断。
                        recovered.add(new DeviceEvent(seq, deviceId, deviceSeq, metric,
                                value, eventTime, ingestTime));
                    } else if (typeByte == TYPE_ACK) {
                        long upTo = in.readLong();
                        in.readInt(); // crc
                        lastAck = Math.max(lastAck, upTo);
                    } else {
                        log.warn("WAL 出现未知记录类型 {}，停止扫描（尾部视为损坏截断）", typeByte);
                        break;
                    }
                } catch (EOFException e) {
                    log.warn("WAL 尾部存在不完整记录，忽略尾部");
                    break;
                } catch (java.io.UTFDataFormatException e) {
                    log.warn("WAL 存在损坏的 UTF 字段，停止扫描并截断尾部: {}", e.getMessage());
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 恢复失败", e);
        }
        long ack = lastAck;
        recovered.removeIf(e -> e.seq() <= ack);
        recovered.sort((a, b) -> Long.compare(a.seq(), b.seq()));
        pending.addAll(recovered);
        nextSeq = maxSeq + 1;
        compact(recovered, lastAck);
        log.info("WAL 恢复完成: lastAck={}, 待补发事件={}, 下一个 seq={}",
                lastAck, recovered.size(), nextSeq);
    }

    /** 用确认后的存活事件重写 WAL（tmp + 原子 move），避免文件无限膨胀。 */
    private void compact(List<DeviceEvent> alive, long lastAck) {
        File tmp = new File(walFile.getParentFile(), "queue.wal.compact.tmp");
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            if (lastAck >= 0) {
                appendAckTo(out, lastAck);
            }
            for (DeviceEvent e : alive) {
                appendEventTo(out, e);
            }
            out.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 压缩失败", e);
        }
        try {
            Files.move(tmp.toPath(), walFile.toPath(), StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 替换失败", e);
        }
    }

    private static byte[] encodeEvent(DeviceEvent e) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.writeByte(TYPE_EVENT);
        out.writeLong(e.seq());
        out.writeUTF(e.deviceId());
        out.writeLong(e.deviceSeq());
        out.writeUTF(e.metric());
        out.writeDouble(e.value());
        out.writeLong(e.eventTime());
        out.writeLong(e.ingestTime());
        CRC32 crc = new CRC32();
        crc.update(bos.toByteArray());
        out.writeInt((int) crc.getValue());
        return bos.toByteArray();
    }

    private static byte[] encodeAck(long upToSeq) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.writeByte(TYPE_ACK);
        out.writeLong(upToSeq);
        CRC32 crc = new CRC32();
        crc.update(bos.toByteArray());
        out.writeInt((int) crc.getValue());
        return bos.toByteArray();
    }

    private void appendEventTo(FileOutputStream out, DeviceEvent e) throws IOException {
        out.write(encodeEvent(e));
    }

    private void appendAckTo(FileOutputStream out, long upToSeq) throws IOException {
        out.write(encodeAck(upToSeq));
    }

    /**
     * 持久化并入队。容量满时：
     * BLOCK 策略：等待空间（由 ingest 层带超时调用 offerOrBlock 实现背压）；
     * REJECT_NEW 策略：返回 false，由上层按 BUFFER_FULL 隔离计数。
     * 任何情况下都不会覆盖或丢弃已有未确认数据。
     */
    public synchronized DeviceEvent appendAndPersist(String deviceId, long deviceSeq,
                                                      String metric, double value,
                                                      long eventTime) {
        if (pending.size() >= capacity) {
            return null;
        }
        DeviceEvent event = new DeviceEvent(nextSeq, deviceId, deviceSeq, metric, value,
                eventTime, System.currentTimeMillis());
        try {
            walOut.write(encodeEvent(event));
            walOut.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 追加失败，拒绝该事件入内存队列", e);
        }
        pending.addLast(event);
        nextSeq++;
        return event;
    }

    /** 供 BLOCK 策略使用的可中断等待。 */
    public synchronized boolean awaitSpace(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (pending.size() >= capacity) {
            long remain = deadline - System.currentTimeMillis();
            if (remain <= 0) {
                return false;
            }
            wait(Math.min(remain, 100));
        }
        return true;
    }

    /** 顺序取出最多 n 条待补发事件（不移除）。 */
    public synchronized List<DeviceEvent> peekBatch(int n) {
        List<DeviceEvent> batch = new ArrayList<>(Math.min(n, pending.size()));
        for (DeviceEvent e : pending) {
            batch.add(e);
            if (batch.size() >= n) {
                break;
            }
        }
        return batch;
    }

    public synchronized boolean isEmpty() {
        return pending.isEmpty();
    }

    public synchronized int size() {
        return pending.size();
    }

    /**
     * 上游整批成功后调用：仅当批内最大 seq 仍是队列头部连续前缀时才确认，
     * 保证"按序补发"——不会跳过尚未成功的更早事件。
     */
    public synchronized void commitUpTo(long upToSeq) {
        DeviceEvent head = pending.peekFirst();
        if (head == null || upToSeq < head.seq()) {
            return;
        }
        while (!pending.isEmpty() && pending.peekFirst().seq() <= upToSeq) {
            pending.removeFirst();
        }
        try {
            walOut.write(encodeAck(upToSeq));
            walOut.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException("ACK 持久化失败", e);
        }
        notifyAll();
    }

    @Override
    public synchronized void close() {
        try {
            if (walOut != null) {
                walOut.getFD().sync();
                walOut.close();
            }
        } catch (IOException e) {
            log.warn("关闭 WAL 失败: {}", e.getMessage());
        }
    }

}
