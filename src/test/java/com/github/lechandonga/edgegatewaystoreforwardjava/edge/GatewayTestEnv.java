package com.github.lechandonga.edgegatewaystoreforwardjava.edge;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.buffer.WriteAheadBuffer;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.JsonLineDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.ModbusTextDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.device.DeviceRegistry;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.device.DeviceTcpServer;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.forward.Forwarder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.isolate.Quarantine;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.metrics.GatewayMetrics;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline.EventPipeline;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline.GatewayProperties;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.upstream.InMemoryUpstream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 测试夹具：在临时目录里组装完整链路，不依赖 Spring、外部服务或真实硬件。
 */
public class GatewayTestEnv implements AutoCloseable {

    public final Path dataDir;
    public final DeviceRegistry registry;
    public final WriteAheadBuffer buffer;
    public final InMemoryUpstream upstream;
    public final GatewayMetrics metrics;
    public final List<com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectedRecord>
            quarantined = new ArrayList<>();
    public final Quarantine quarantine;
    public final EventPipeline pipeline;
    public final GatewayProperties props;
    public final Forwarder forwarder;
    public DeviceTcpServer modbusServer;
    public DeviceTcpServer jsonServer;

    public static GatewayProperties defaultProps(Path dataDir) {
        GatewayProperties p = new GatewayProperties();
        p.setDataDir(dataDir.toString());
        p.setBufferMaxEvents(10_000);
        p.setBufferMaxBytes(64 * 1024 * 1024);
        p.setModbusPort(0);
        p.setJsonPort(0);
        p.setBatchSize(50);
        p.setPumpIntervalMs(100);
        p.setBackoffInitialMs(50);
        p.setBackoffMaxMs(500);
        p.setBlockTimeoutMs(5_000);
        return p;
    }

    public static DeviceRegistry defaultRegistry() {
        DeviceRegistry registry = new DeviceRegistry();
        registry.register(new DeviceRegistry.DeviceSpec("MOD-1", "m",
                Map.of("temperature", new DeviceRegistry.MetricSpec("temperature", "C", -50, 150),
                        "humidity", new DeviceRegistry.MetricSpec("humidity", "%", 0, 100))));
        registry.register(new DeviceRegistry.DeviceSpec("MOD-2", "m",
                Map.of("temperature", new DeviceRegistry.MetricSpec("temperature", "C", -50, 150))));
        registry.register(new DeviceRegistry.DeviceSpec("MQTT-101", "j",
                Map.of("temperature", new DeviceRegistry.MetricSpec("temperature", "C", -50, 150))));
        registry.register(new DeviceRegistry.DeviceSpec("MQTT-102", "j",
                Map.of("temperature", new DeviceRegistry.MetricSpec("temperature", "C", -50, 150))));
        for (int i = 1; i <= 8; i++) {
            registry.register(new DeviceRegistry.DeviceSpec("MOD-" + i, "m",
                    Map.of("temperature", new DeviceRegistry.MetricSpec("temperature", "C", -50, 150))));
            registry.register(new DeviceRegistry.DeviceSpec("MQTT-20" + i, "j",
                    Map.of("temperature", new DeviceRegistry.MetricSpec("temperature", "C", -50, 150))));
        }
        return registry;
    }

    public GatewayTestEnv(GatewayProperties props, DeviceRegistry registry, boolean withTcp) {
        this.props = props;
        this.dataDir = Path.of(props.getDataDir());
        this.registry = registry;
        this.buffer = new WriteAheadBuffer(dataDir, props.getBufferMaxEvents(), props.getBufferMaxBytes());
        this.upstream = new InMemoryUpstream();
        this.metrics = new GatewayMetrics();
        this.quarantine = new Quarantine(500, quarantined::add);
        this.pipeline = new EventPipeline(buffer, quarantine, metrics, props);
        this.forwarder = new Forwarder(buffer, upstream, metrics, props, pipeline);
        if (withTcp) {
            this.modbusServer = new DeviceTcpServer("MODBUS", props.getModbusPort(),
                    () -> new ModbusTextDecoder(props.getMaxFrameBytes(), registry), pipeline);
            this.jsonServer = new DeviceTcpServer("JSON", props.getJsonPort(),
                    () -> new JsonLineDecoder(props.getMaxFrameBytes(), registry), pipeline);
            modbusServer.start();
            jsonServer.start();
        }
    }

    public static Path tempDir(String prefix) throws IOException {
        return Files.createTempDirectory("edge-gw-" + prefix + "-");
    }

    public void startForwarder() {
        forwarder.start();
    }

    @Override
    public void close() {
        if (modbusServer != null) {
            modbusServer.close();
        }
        if (jsonServer != null) {
            jsonServer.close();
        }
        forwarder.close();
        buffer.close();
    }
}
