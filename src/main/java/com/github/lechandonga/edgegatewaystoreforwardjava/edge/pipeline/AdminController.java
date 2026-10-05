package com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.forward.Forwarder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.isolate.Quarantine;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.metrics.GatewayMetrics;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.upstream.UpstreamClient;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 本地可观测与演练端点：
 * GET /admin/metrics 运行状态（积压、隔离、重试等）；
 * GET /admin/quarantine 最近被隔离的报文及原因；
 * POST /admin/upstream?available=false|true 模拟上游断连/恢复；
 * POST /admin/pump 立即触发一次补发。
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final GatewayMetrics metrics;
    private final EventPipeline pipeline;
    private final Quarantine quarantine;
    private final UpstreamClient upstream;
    private final Forwarder forwarder;

    public AdminController(GatewayMetrics metrics, EventPipeline pipeline, Quarantine quarantine,
                           UpstreamClient upstream, Forwarder forwarder) {
        this.metrics = metrics;
        this.pipeline = pipeline;
        this.quarantine = quarantine;
        this.upstream = upstream;
        this.forwarder = forwarder;
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.putAll(metrics.snapshot(pipeline.pendingCount()));
        body.put("upstreamAvailable", upstream.isAvailable());
        body.put("backpressured", pipeline.isBackpressured());
        return body;
    }

    @GetMapping("/quarantine")
    public Object recentQuarantine() {
        return quarantine.recent();
    }

    @PostMapping("/upstream")
    public Map<String, Object> setUpstream(@RequestParam boolean available) {
        upstream.setAvailable(available);
        return Map.of("upstreamAvailable", upstream.isAvailable());
    }

    @PostMapping("/pump")
    public Map<String, Object> pump() {
        forwarder.pumpOnce();
        return new LinkedHashMap<>(metrics.snapshot(pipeline.pendingCount()));
    }
}
