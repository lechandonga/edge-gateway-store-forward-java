package com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.buffer.EventWriteException;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.buffer.WriteAheadBuffer;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.DecodeResult;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.isolate.Quarantine;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.metrics.GatewayMetrics;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.DeviceEvent;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectedRecord;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 采集入口管线：解码结果 -> 每设备串行入 WAL。
 * 同一 deviceId 的所有写入由独立锁串行化，保证设备内事件顺序；
 * 缓冲满按可配置策略阻塞背压或隔离，绝不静默丢弃/覆盖。
 */
public class EventPipeline {

    private final WriteAheadBuffer buffer;
    private final Quarantine quarantine;
    private final GatewayMetrics metrics;
    private final GatewayProperties props;

    private final Map<String, Object> deviceLocks = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSeq = new ConcurrentHashMap<>();

    public EventPipeline(WriteAheadBuffer buffer, Quarantine quarantine,
                         GatewayMetrics metrics, GatewayProperties props) {
        this.buffer = buffer;
        this.quarantine = quarantine;
        this.metrics = metrics;
        this.props = props;
    }

    /** 处理一条解码结果（成功入库或带原因隔离），不抛异常给设备 IO 线程。 */
    public void handle(DecodeResult result, String protocol, String remoteAddress) {
        switch (result) {
            case DecodeResult.NeedMore ignore -> {
                // 半包：等待后续字节，不算拒绝
            }
            case DecodeResult.Rejected rejected -> {
                metrics.frameRejected(rejected.reason());
                quarantine(extractDeviceHint(rejected), protocol, remoteAddress,
                        rejected.reason(), rejected.detail(), rejected.rawHex());
            }
            case DecodeResult.Parsed parsed -> {
                metrics.frameReceived();
                ingest(parsed, protocol, remoteAddress);
            }
        }
    }

    private static String extractDeviceHint(DecodeResult.Rejected rejected) {
        String detail = rejected.detail() == null ? "" : rejected.detail();
        int idx = detail.indexOf("device not registered: ");
        if (idx >= 0) {
            return detail.substring(idx + "device not registered: ".length()).trim();
        }
        return "UNKNOWN";
    }

    /** 补发确认释放容量后唤醒因缓冲满而阻塞的设备线程。 */
    public void onCapacityFreed() {
        synchronized (capacityLock) {
            capacityLock.notifyAll();
        }
    }

    private final Object capacityLock = new Object();

    /** 背压信号：积压达到阈值，设备侧应暂停读取 socket（TCP 窗口自然反压到设备端）。 */
    public boolean isBackpressured() {
        return buffer.pendingCount() >= props.getBufferMaxEvents() * props.getBackpressureRatio();
    }

    public long pendingCount() {
        return buffer.pendingCount();
    }

    private void ingest(DecodeResult.Parsed parsed, String protocol, String remoteAddress) {
        Object lock = deviceLocks.computeIfAbsent(parsed.deviceId(), k -> new Object());
        synchronized (lock) {
            Long prev = lastSeq.get(parsed.deviceId());
            if (prev != null && parsed.seq() <= prev) {
                metrics.duplicateSeen();
            } else {
                lastSeq.put(parsed.deviceId(), parsed.seq());
            }
            DeviceEvent skeleton = new DeviceEvent(0, parsed.deviceId(), parsed.seq(),
                    parsed.metric(), parsed.value(), parsed.unit(),
                    parsed.deviceTime() > 0 ? parsed.deviceTime() : System.currentTimeMillis(),
                    System.currentTimeMillis(), protocol);
            try {
                DeviceEvent stored = writeWithPolicy(skeleton);
                metrics.eventAccepted();
                metrics.eventBuffered();
                org.slf4j.LoggerFactory.getLogger(EventPipeline.class)
                        .debug("buffered event id={} device={} seq={} metric={}",
                                stored.id(), stored.deviceId(), stored.seq(), stored.metric());
            } catch (EventWriteException full) {
                metrics.bufferFullRejected();
                metrics.frameRejected(full.reason());
                quarantine(parsed.deviceId(), protocol, remoteAddress,
                        full.reason(), full.getMessage(), "");
            }
        }
    }

    private DeviceEvent writeWithPolicy(DeviceEvent event) {
        if (props.getBufferFullPolicy() == GatewayProperties.FullPolicy.REJECT) {
            return buffer.append(event);
        }
        long deadline = System.currentTimeMillis() + props.getBlockTimeoutMs();
        while (true) {
            try {
                return buffer.append(event);
            } catch (EventWriteException full) {
                long waitLeft = deadline - System.currentTimeMillis();
                if (waitLeft <= 0) {
                    throw full;
                }
                synchronized (capacityLock) {
                    try {
                        capacityLock.wait(Math.min(waitLeft, 500));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw full;
                    }
                }
            }
        }
    }

    private void quarantine(String deviceId, String protocol, String remoteAddress,
                            RejectReason reason, String detail, String rawHex) {
        RejectedRecord record = new RejectedRecord(Instant.now(),
                deviceId == null ? "UNKNOWN" : deviceId,
                protocol, remoteAddress, reason, detail, rawHex == null ? "" : rawHex);
        quarantine.add(record);
        org.slf4j.LoggerFactory.getLogger(EventPipeline.class)
                .warn("frame isolated device={} protocol={} reason={} detail={}",
                        record.deviceId(), protocol, reason, detail);
    }

    public WriteAheadBuffer buffer() {
        return buffer;
    }
}
