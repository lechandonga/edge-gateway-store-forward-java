package com.github.lechandonga.edgegatewaystoreforwardjava.web;

import com.github.lechandonga.edgegatewaystoreforwardjava.buffer.PersistentEventQueue;
import com.github.lechandonga.edgegatewaystoreforwardjava.metrics.GatewayMetrics;
import com.github.lechandonga.edgegatewaystoreforwardjava.sim.SimulatedUpstream;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/** 本地观测 / 演示控制接口。 */
@RestController
public class GatewayController {

    private final GatewayMetrics metrics;
    private final PersistentEventQueue queue;
    private final SimulatedUpstream upstream;

    public GatewayController(GatewayMetrics metrics, PersistentEventQueue queue,
                             SimulatedUpstream upstream) {
        this.metrics = metrics;
        this.queue = queue;
        this.upstream = upstream;
    }

    /** 运行状态：积压量、接收/隔离/投递/重试等全部计数。 */
    @GetMapping("/metrics")
    public Map<String, Long> metrics() {
        return metrics.snapshot(queue.size());
    }

    /** 模拟上游断连/恢复。 */
    @GetMapping("/control/upstream")
    public Map<String, Object> upstream(@RequestParam boolean available) {
        upstream.setAvailable(available);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("upstreamAvailable", upstream.isAvailable());
        result.put("committedRecords", upstream.committedCount());
        return result;
    }

    /** 网关侧缓冲与上游记录速览。 */
    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("bufferBacklog", queue.size());
        result.put("upstreamAvailable", upstream.isAvailable());
        result.put("upstreamCommittedRecords", upstream.committedCount());
        result.put("metrics", metrics.snapshot(queue.size()));
        return result;
    }
}
