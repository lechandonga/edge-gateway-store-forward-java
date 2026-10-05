package com.github.lechandonga.edgegatewaystoreforwardjava.forward;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.edgegatewaystoreforwardjava.buffer.PersistentEventQueue;
import com.github.lechandonga.edgegatewaystoreforwardjava.config.GatewayProperties;
import com.github.lechandonga.edgegatewaystoreforwardjava.metrics.GatewayMetrics;
import com.github.lechandonga.edgegatewaystoreforwardjava.model.DeviceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 补发转发器（单线程）：严格按全局 seq 顺序批量投递。
 *
 * <p>规则：
 * <ul>
 *   <li>每批 peek 后整批 POST；只有 2xx 才 commitUpTo(批次最大 seq)，
 *       任何失败/超时都整批重试，绝不提交一半。</li>
 *   <li>失败采用指数退避（initial * multiplier^n，上限 max，±20% 抖动），
 *       恢复后第一时间按积压补发，避免无限重试打爆上游。</li>
 *   <li>上游按 idempotencyKey 幂等，重复投递/重复确认不会产生重复记录。</li>
 * </ul>
 */
public class EventForwarder implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EventForwarder.class);

    private final GatewayProperties props;
    private final PersistentEventQueue queue;
    private final GatewayMetrics metrics;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();
    private Thread worker;
    private volatile boolean running = true;

    public EventForwarder(GatewayProperties props, PersistentEventQueue queue,
                          GatewayMetrics metrics) {
        this.props = props;
        this.queue = queue;
        this.metrics = metrics;
    }

    public void start() {
        worker = new Thread(this::loop, "event-forwarder");
        worker.setDaemon(true);
        worker.start();
        log.info("补发转发器已启动: batchSize={}, backoff={}ms~{}ms x{}",
                props.getForwardBatchSize(), props.getRetryInitialBackoffMs(),
                props.getRetryMaxBackoffMs(), props.getRetryBackoffMultiplier());
    }

    private URI eventsUri() {
        return URI.create("http://127.0.0.1:" + props.getUpstreamPort() + "/events");
    }

    private void loop() {
        long backoffMs = props.getRetryInitialBackoffMs();
        int failureStreak = 0;
        while (running) {
            try {
                List<DeviceEvent> batch = queue.peekBatch(props.getForwardBatchSize());
                if (batch.isEmpty()) {
                    Thread.sleep(props.getPollTimeoutMs());
                    backoffMs = props.getRetryInitialBackoffMs();
                    failureStreak = 0;
                    continue;
                }
                boolean ok = postBatch(batch);
                if (ok) {
                    long upTo = batch.get(batch.size() - 1).seq();
                    queue.commitUpTo(upTo);
                    metrics.incDelivered(batch.size());
                    metrics.incBatchesCommitted();
                    log.info("批次补发成功并确认: 条数={}, seq {}..{}, 剩余积压={}",
                            batch.size(), batch.get(0).seq(), upTo, queue.size());
                    backoffMs = props.getRetryInitialBackoffMs();
                    failureStreak = 0;
                } else {
                    failureStreak++;
                    metrics.incUpstreamFailure();
                    metrics.incRetried();
                    long sleep = nextBackoff(backoffMs, failureStreak);
                    log.warn("批次投递失败，整批退避重试: 条数={}, 连续失败={}, {}ms 后重试, 积压={}",
                            batch.size(), failureStreak, sleep, queue.size());
                    Thread.sleep(sleep);
                    backoffMs = Math.min(props.getRetryMaxBackoffMs(),
                            Math.round(backoffMs * props.getRetryBackoffMultiplier()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                metrics.incUpstreamFailure();
                log.warn("转发循环异常: {}", e.toString());
                try {
                    Thread.sleep(Math.min(props.getRetryMaxBackoffMs(), backoffMs));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private long nextBackoff(long base, int streak) {
        double jitter = 0.8 + Math.random() * 0.4; // ±20%
        return Math.min(props.getRetryMaxBackoffMs(),
                Math.max(props.getRetryInitialBackoffMs(), Math.round(base * jitter)));
    }

    private boolean postBatch(List<DeviceEvent> batch) {
        try {
            List<Map<String, Object>> payload = batch.stream()
                    .map(e -> Map.<String, Object>of(
                            "idempotencyKey", e.idempotencyKey(),
                            "seq", e.seq(),
                            "deviceId", e.deviceId(),
                            "deviceSeq", e.deviceSeq(),
                            "metric", e.metric(),
                            "value", e.value(),
                            "eventTime", e.eventTime(),
                            "ingestTime", e.ingestTime()))
                    .toList();
            HttpRequest request = HttpRequest.newBuilder(eventsUri())
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(payload)))
                    .build();
            HttpResponse<String> resp = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                try {
                    var tree = mapper.readTree(resp.body());
                    if (tree.path("duplicates").asInt(0) > 0) {
                        for (int i = 0; i < tree.path("duplicates").asInt(); i++) {
                            metrics.incDuplicateDelivery();
                        }
                    }
                } catch (Exception ignored) {
                }
                return true;
            }
            log.debug("上游返回非成功状态: {} {}", resp.statusCode(), resp.body());
            return false;
        } catch (Exception e) {
            log.debug("投递异常: {}", e.toString());
            return false;
        }
    }

    @Override
    public void close() {
        running = false;
        if (worker != null) {
            worker.interrupt();
        }
    }
}
