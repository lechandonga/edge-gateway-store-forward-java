package com.github.lechandonga.edgegatewaystoreforwardjava.config;

import com.github.lechandonga.edgegatewaystoreforwardjava.buffer.PersistentEventQueue;
import com.github.lechandonga.edgegatewaystoreforwardjava.forward.EventForwarder;
import com.github.lechandonga.edgegatewaystoreforwardjava.ingest.DeviceTcpServer;
import com.github.lechandonga.edgegatewaystoreforwardjava.ingest.QuarantineLog;
import com.github.lechandonga.edgegatewaystoreforwardjava.metrics.GatewayMetrics;
import com.github.lechandonga.edgegatewaystoreforwardjava.sim.SimulatedUpstream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PreDestroy;

/**
 * 本地全链路装配：模拟上游、持久化缓冲、隔离日志、设备接入与补发转发器。
 * 不依赖任何外部中间件 / 真实硬件。
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayConfiguration {

    private static final Logger log = LoggerFactory.getLogger(GatewayConfiguration.class);

    private SimulatedUpstream upstream;
    private PersistentEventQueue queue;
    private QuarantineLog quarantine;
    private DeviceTcpServer deviceServer;
    private EventForwarder forwarder;

    @Bean
    public GatewayMetrics gatewayMetrics() {
        return new GatewayMetrics();
    }

    @Bean
    public SimulatedUpstream simulatedUpstream(GatewayProperties props) throws Exception {
        upstream = new SimulatedUpstream(props.getUpstreamPort(),
                props.isUpstreamInitiallyAvailable(), props.getDataDir());
        return upstream;
    }

    @Bean
    public PersistentEventQueue persistentEventQueue(GatewayProperties props,
                                                     GatewayMetrics metrics) {
        queue = new PersistentEventQueue(props);
        metrics.addRecoveredOnStartup(queue.size());
        if (queue.size() > 0) {
            log.info("进程重启恢复: {} 条未确认事件将继续按序补发", queue.size());
        }
        return queue;
    }

    @Bean
    public QuarantineLog quarantineLog(GatewayProperties props) {
        quarantine = new QuarantineLog(props);
        return quarantine;
    }

    @Bean
    public EventForwarder eventForwarder(GatewayProperties props, PersistentEventQueue queue,
                                         GatewayMetrics metrics, SimulatedUpstream upstream) {
        forwarder = new EventForwarder(props, queue, metrics);
        forwarder.start();
        return forwarder;
    }

    @Bean
    public DeviceTcpServer deviceTcpServer(GatewayProperties props, PersistentEventQueue queue,
                                           QuarantineLog quarantine, GatewayMetrics metrics,
                                           SimulatedUpstream upstream) throws Exception {
        deviceServer = new DeviceTcpServer(props, queue, quarantine, metrics);
        deviceServer.start();
        return deviceServer;
    }

    @PreDestroy
    public void shutdown() {
        closeQuietly(deviceServer);
        closeQuietly(forwarder);
        closeQuietly(queue);
        closeQuietly(quarantine);
        closeQuietly(upstream);
        log.info("网关资源已有序关闭（WAL 已 fsync，未确认数据可在重启后继续补发）");
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception e) {
                log.warn("关闭资源失败: {}", e.getMessage());
            }
        }
    }
}
