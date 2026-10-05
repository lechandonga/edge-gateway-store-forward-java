package com.github.lechandonga.edgegatewaystoreforwardjava.edge.forward;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.buffer.WriteAheadBuffer;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.metrics.GatewayMetrics;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.DeviceEvent;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline.EventPipeline;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline.GatewayProperties;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.upstream.UpstreamClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 补发器：单线程定时把 WAL 中未确认事件按序批量提交给上游。
 * <ul>
 *   <li>整批成功才顺序推进确认点，失败整批保留，无"提交一半"；</li>
 *   <li>失败指数退避（initial -> max + 抖动），上游恢复瞬间不会无限重试；</li>
 *   <li>成功后立即连续抽空积压，并释放缓冲容量唤醒被背压的设备线程。</li>
 * </ul>
 */
public class Forwarder implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Forwarder.class);

    private final WriteAheadBuffer buffer;
    private final UpstreamClient upstream;
    private final GatewayMetrics metrics;
    private final GatewayProperties props;
    private final EventPipeline pipeline;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "edge-forwarder");
                t.setDaemon(true);
                return t;
            });
    private final ReentrantLock pumpLock = new ReentrantLock();
    private volatile ScheduledFuture<?> tick;
    private volatile long failuresStreak;
    private volatile boolean retryScheduled;

    public Forwarder(WriteAheadBuffer buffer, UpstreamClient upstream, GatewayMetrics metrics,
                     GatewayProperties props, EventPipeline pipeline) {
        this.buffer = buffer;
        this.upstream = upstream;
        this.metrics = metrics;
        this.props = props;
        this.pipeline = pipeline;
    }

    public void start() {
        tick = scheduler.scheduleWithFixedDelay(this::safePump,
                props.getPumpIntervalMs(), props.getPumpIntervalMs(), TimeUnit.MILLISECONDS);
        log.info("forwarder started, batchSize={} backoff={}..{}ms",
                props.getBatchSize(), props.getBackoffInitialMs(), props.getBackoffMaxMs());
    }

    /** 测试/管理面可手动触发一次补发。 */
    public void pumpOnce() {
        retryScheduled = false;
        safePump();
    }

    private void safePump() {
        if (retryScheduled) {
            return;
        }
        if (!pumpLock.tryLock()) {
            return;
        }
        boolean freedAny = false;
        try {
            while (true) {
                List<DeviceEvent> batch = buffer.readPending(props.getBatchSize());
                if (batch.isEmpty()) {
                    failuresStreak = 0;
                    retryScheduled = false;
                    break;
                }
                try {
                    int inserted = upstream.ingestBatch(batch);
                    buffer.acknowledge(batch.size());
                    metrics.eventAcknowledged(batch.size());
                    metrics.batchSent();
                    failuresStreak = 0;
                    retryScheduled = false;
                    freedAny = true;
                    log.info("batch committed: events={}, newlyInserted={}, pending={}",
                            batch.size(), inserted, buffer.pendingCount());
                } catch (Exception e) {
                    metrics.batchFailed();
                    metrics.retryAttempted();
                    long nextDelay = nextBackoffMs(++failuresStreak);
                    log.warn("upstream batch failed (streak={}), keep {} events in WAL, retry in {}ms: {}",
                            failuresStreak, batch.size(), nextDelay, e.getMessage());
                    retryScheduled = true;
                    scheduler.schedule(() -> {
                        retryScheduled = false;
                        safePump();
                    }, nextDelay, TimeUnit.MILLISECONDS);
                    return;
                }
            }
        } finally {
            pumpLock.unlock();
            if (freedAny) {
                pipeline.onCapacityFreed();
            }
        }
    }

    public long nextBackoffMs(long streak) {
        double base = Math.min(props.getBackoffMaxMs(),
                props.getBackoffInitialMs() * (1L << Math.min(streak - 1, 20)));
        double jitterRange = base * props.getBackoffJitter();
        double jitter = Math.random() * jitterRange - jitterRange / 2;
        return Math.max(props.getBackoffInitialMs(), Math.round(base + jitter));
    }

    public long failureStreak() {
        return failuresStreak;
    }

    @Override
    public void close() {
        if (tick != null) {
            tick.cancel(false);
        }
        scheduler.shutdownNow();
    }
}
