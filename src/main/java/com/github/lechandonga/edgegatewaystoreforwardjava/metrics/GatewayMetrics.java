package com.github.lechandonga.edgegatewaystoreforwardjava.metrics;

import com.github.lechandonga.edgegatewaystoreforwardjava.model.RejectReason;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 网关运行状态：积压量、各类拒绝数、投递/重试/确认计数等。
 * 线程安全，可通过 HTTP /metrics 观测，也定期打印到日志。
 */
public class GatewayMetrics {

    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final Map<RejectReason, AtomicLong> rejectedByReason = new EnumMap<>(RejectReason.class);
    private final AtomicLong persisted = new AtomicLong();
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong duplicateDelivery = new AtomicLong();
    private final AtomicLong retried = new AtomicLong();
    private final AtomicLong upstreamFailures = new AtomicLong();
    private final AtomicLong batchesCommitted = new AtomicLong();
    private final AtomicLong recoveredOnStartup = new AtomicLong();

    public GatewayMetrics() {
        for (RejectReason reason : RejectReason.values()) {
            rejectedByReason.put(reason, new AtomicLong());
        }
    }

    public void incAccepted() { accepted.incrementAndGet(); }
    public void incRejected(RejectReason reason) {
        rejected.incrementAndGet();
        rejectedByReason.get(reason).incrementAndGet();
    }
    public void incPersisted() { persisted.incrementAndGet(); }
    public void incDelivered(long n) { delivered.addAndGet(n); }
    public void incDuplicateDelivery() { duplicateDelivery.incrementAndGet(); }
    public void incRetried() { retried.incrementAndGet(); }
    public void incUpstreamFailure() { upstreamFailures.incrementAndGet(); }
    public void incBatchesCommitted() { batchesCommitted.incrementAndGet(); }
    public void addRecoveredOnStartup(long n) { recoveredOnStartup.addAndGet(n); }

    /** 当前积压量由缓冲层提供，避免两处计数不一致。 */
    public long getAccepted() { return accepted.get(); }
    public long getRejected() { return rejected.get(); }
    public long getPersisted() { return persisted.get(); }
    public long getDelivered() { return delivered.get(); }
    public long getDuplicateDelivery() { return duplicateDelivery.get(); }
    public long getRetried() { return retried.get(); }
    public long getUpstreamFailures() { return upstreamFailures.get(); }
    public long getBatchesCommitted() { return batchesCommitted.get(); }
    public long getRecoveredOnStartup() { return recoveredOnStartup.get(); }

    public Map<String, Long> snapshot(long currentBacklog) {
        Map<String, Long> snap = new java.util.LinkedHashMap<>();
        snap.put("events.accepted", accepted.get());
        snap.put("events.persisted", persisted.get());
        snap.put("events.delivered", delivered.get());
        snap.put("events.duplicateDelivery", duplicateDelivery.get());
        snap.put("events.rejected.total", rejected.get());
        rejectedByReason.forEach((r, c) ->
                snap.put("events.rejected." + r.name(), c.get()));
        snap.put("buffer.backlog", currentBacklog);
        snap.put("forward.retried", retried.get());
        snap.put("forward.upstreamFailures", upstreamFailures.get());
        snap.put("forward.batchesCommitted", batchesCommitted.get());
        snap.put("startup.recovered", recoveredOnStartup.get());
        return snap;
    }
}
