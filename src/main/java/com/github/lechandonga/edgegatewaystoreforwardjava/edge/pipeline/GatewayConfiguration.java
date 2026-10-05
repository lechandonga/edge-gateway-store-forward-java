package com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.buffer.WriteAheadBuffer;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.JsonLineDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.ModbusTextDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.device.DeviceRegistry;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.device.DeviceTcpServer;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.forward.Forwarder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.isolate.FileQuarantineSink;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.isolate.Quarantine;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.metrics.GatewayMetrics;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.upstream.InMemoryUpstream;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.upstream.UpstreamClient;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.Map;

/**
 * 组装整条采集-缓冲-补发链路。设备元数据为本地模拟配置。
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayConfiguration {

    @Bean
    public DeviceRegistry deviceRegistry() {
        DeviceRegistry registry = new DeviceRegistry();
        registry.register(new DeviceRegistry.DeviceSpec("MOD-1", "modbus-sensor",
                Map.of(
                        "temperature", new DeviceRegistry.MetricSpec("temperature", "C", -50, 150),
                        "humidity", new DeviceRegistry.MetricSpec("humidity", "%", 0, 100),
                        "pressure", new DeviceRegistry.MetricSpec("pressure", "kPa", 0, 500))));
        registry.register(new DeviceRegistry.DeviceSpec("MOD-2", "modbus-sensor",
                Map.of(
                        "temperature", new DeviceRegistry.MetricSpec("temperature", "C", -50, 150),
                        "humidity", new DeviceRegistry.MetricSpec("humidity", "%", 0, 100))));
        registry.register(new DeviceRegistry.DeviceSpec("MQTT-101", "json-sensor",
                Map.of(
                        "temperature", new DeviceRegistry.MetricSpec("temperature", "C", -50, 150),
                        "humidity", new DeviceRegistry.MetricSpec("humidity", "%", 0, 100),
                        "pressure", new DeviceRegistry.MetricSpec("pressure", "kPa", 0, 500))));
        registry.register(new DeviceRegistry.DeviceSpec("MQTT-102", "json-sensor",
                Map.of(
                        "temperature", new DeviceRegistry.MetricSpec("temperature", "C", -50, 150))));
        return registry;
    }

    @Bean(destroyMethod = "close")
    public WriteAheadBuffer writeAheadBuffer(GatewayProperties props) {
        return new WriteAheadBuffer(Path.of(props.getDataDir()),
                props.getBufferMaxEvents(), props.getBufferMaxBytes());
    }

    @Bean
    public GatewayMetrics gatewayMetrics() {
        return new GatewayMetrics();
    }

    @Bean
    public UpstreamClient upstreamClient() {
        return new InMemoryUpstream();
    }

    @Bean
    public Quarantine quarantine(GatewayProperties props) {
        return new Quarantine(200, new FileQuarantineSink(Path.of(props.getDataDir())));
    }

    @Bean
    public EventPipeline eventPipeline(WriteAheadBuffer buffer, Quarantine quarantine,
                                       GatewayMetrics metrics, GatewayProperties props) {
        return new EventPipeline(buffer, quarantine, metrics, props);
    }

    @Bean
    public Forwarder forwarder(WriteAheadBuffer buffer, UpstreamClient upstream, GatewayMetrics metrics,
                               GatewayProperties props, EventPipeline pipeline) {
        Forwarder forwarder = new Forwarder(buffer, upstream, metrics, props, pipeline);
        forwarder.start();
        return forwarder;
    }

    @Bean(destroyMethod = "close")
    public DeviceTcpServer modbusServer(GatewayProperties props, DeviceRegistry registry,
                                        EventPipeline pipeline) {
        DeviceTcpServer server = new DeviceTcpServer("MODBUS", props.getModbusPort(),
                () -> new ModbusTextDecoder(props.getMaxFrameBytes(), registry), pipeline);
        server.start();
        return server;
    }

    @Bean(destroyMethod = "close")
    public DeviceTcpServer jsonServer(GatewayProperties props, DeviceRegistry registry,
                                      EventPipeline pipeline) {
        DeviceTcpServer server = new DeviceTcpServer("JSON", props.getJsonPort(),
                () -> new JsonLineDecoder(props.getMaxFrameBytes(), registry), pipeline);
        server.start();
        return server;
    }
}
