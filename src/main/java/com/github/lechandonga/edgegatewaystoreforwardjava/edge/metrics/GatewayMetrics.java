package com.github.lechandonga.edgegatewaystoreforwardjava.edge.metrics;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 网关运行状态（积压量 / 丢弃隔离数 / 重试次数等），
 * 用 JUC 原子量实现，无外部依赖，可通过 admin HTTP 端点与日志观测。
 */
public class GatewayMetrics {

    private final AtomicLong receivedFrames = new AtomicLong();
    private final AtomicLong acceptedEvents = new AtomicLong();
    private final AtomicLong rejectedFrames = new AtomicLong();
    private final AtomicLong bufferedEvents = new AtomicLong();
    private final AtomicLong acknowledgedEvents = new AtomicLong();
    private final AtomicLong retryAttempts = new AtomicLong();
    private final AtomicLong upstreamBatches = new AtomicLong();
    private final AtomicLong upstreamFailures = new AtomicLong();
    private final AtomicLong bufferFullRejections = new AtomicLong();
    private final AtomicLong duplicateInbound = new AtomicLong();
    private final Map<RejectReason, AtomicLong> rejectByReason = new EnumMap<>(RejectReason.class);

    public GatewayMetrics() {
        for (RejectReason reason : RejectReason.values()) {
            rejectByReason.put(reason, new AtomicLong());
        }
    }

    public void frameReceived() {
        receivedFrames.incrementAndGet();
    }

    public void eventAccepted() {
        acceptedEvents.incrementAndGet();
    }

    public void frameRejected(RejectReason reason) {
        rejectedFrames.incrementAndGet();
        rejectByReason.get(reason).incrementAndGet();
    }

    public void eventBuffered() {
        bufferedEvents.incrementAndGet();
    }

    public void eventAcknowledged(long n) {
        acknowledgedEvents.addAndGet(n);
    }

    public void retryAttempted() {
        retryAttempts.incrementAndGet();
    }

    public void batchSent() {
        upstreamBatches.incrementAndGet();
    }

    public void batchFailed() {
        upstreamFailures.incrementAndGet();
    }

    public void bufferFullRejected() {
        bufferFullRejections.incrementAndGet();
    }

    public void duplicateSeen() {
        duplicateInbound.incrementAndGet();
    }

    public long receivedFrames() {
        return receivedFrames.get();
    }

    public long acceptedEvents() {
        return acceptedEvents.get();
    }

    public long rejectedFrames() {
        return rejectedFrames.get();
    }

    public long bufferedEvents() {
        return bufferedEvents.get();
    }

    public long acknowledgedEvents() {
        return acknowledgedEvents.get();
    }

    public long retryAttempts() {
        return retryAttempts.get();
    }

    public long upstreamBatches() {
        return upstreamBatches.get();
    }

    public long upstreamFailures() {
        return upstreamFailures.get();
    }

    public long bufferFullRejections() {
        return bufferFullRejections.get();
    }

    public long duplicateInbound() {
        return duplicateInbound.get();
    }

    public Map<String, Long> snapshot(long currentPending) {
        Map<String, Long> m = new java.util.LinkedHashMap<>();
        m.put("receivedFrames", receivedFrames.get());
        m.put("acceptedEvents", acceptedEvents.get());
        m.put("rejectedFrames", rejectedFrames.get());
        m.put("bufferedEventsTotal", bufferedEvents.get());
        m.put("acknowledgedEventsTotal", acknowledgedEvents.get());
        m.put("pendingEvents", currentPending);
        m.put("retryAttempts", retryAttempts.get());
        m.put("upstreamBatches", upstreamBatches.get());
        m.put("upstreamFailures", upstreamFailures.get());
        m.put("bufferFullRejections", bufferFullRejections.get());
        m.put("duplicateInboundDropped", duplicateInbound.get());
        rejectByReason.forEach((r, c) -> m.put("reject." + r.name(), c.get()));
        return m;
    }
}
