package com.github.lechandonga.edgegatewaystoreforwardjava.edge.demo;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.device.DeviceTcpServer;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline.GatewayProperties;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.upstream.UpstreamClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 本地一键复现：edge.gateway.demo-enabled=true 时启动 4 台模拟设备持续上报，
 * 启动后让上游断开 20 秒再恢复，观察缓冲、退避重试、按序补发与隔离日志。
 */
@Component
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final GatewayProperties props;
    private final DeviceTcpServer modbusServer;
    private final DeviceTcpServer jsonServer;
    private final UpstreamClient upstream;

    public DemoRunner(GatewayProperties props, DeviceTcpServer modbusServer,
                      DeviceTcpServer jsonServer, UpstreamClient upstream) {
        this.props = props;
        this.modbusServer = modbusServer;
        this.jsonServer = jsonServer;
        this.upstream = upstream;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!props.isDemoEnabled()) {
            return;
        }
        int modbusPort = modbusServer.boundPort();
        int jsonPort = jsonServer.boundPort();
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(5, r -> {
            Thread t = new Thread(r, "demo-device");
            t.setDaemon(true);
            return t;
        });
        try {
            startDevice(exec, new DemoDeviceSimulators.ModbusDevice(modbusPort, 1));
            startDevice(exec, new DemoDeviceSimulators.ModbusDevice(modbusPort, 2));
            startDevice(exec, new DemoDeviceSimulators.JsonDevice(jsonPort, "MQTT-101"));
            startDevice(exec, new DemoDeviceSimulators.JsonDevice(jsonPort, "MQTT-102"));
        } catch (Exception e) {
            log.error("demo devices failed to connect", e);
            return;
        }

        log.info("=== DEMO: upstream goes DOWN in 5s, stays DOWN 20s, then recovers ===");
        exec.schedule(() -> {
            upstream.setAvailable(false);
            log.warn("=== DEMO: upstream is now DOWN, events buffer locally ===");
        }, 5, TimeUnit.SECONDS);
        exec.schedule(() -> {
            upstream.setAvailable(true);
            log.info("=== DEMO: upstream RECOVERED, replay begins ===");
        }, 25, TimeUnit.SECONDS);
    }

    private void startDevice(ScheduledExecutorService exec, DemoDeviceSimulators.SimDevice device) {
        long periodMs = Math.max(100, 1000L / props.getDemoRatePerDevice());
        exec.scheduleAtFixedRate(() -> {
            try {
                device.sendOne(20 + Math.random() * 10);
            } catch (Exception e) {
                log.debug("demo send failed: {}", e.getMessage());
            }
        }, 1, periodMs, TimeUnit.MILLISECONDS);
        exec.schedule(() -> {
            try {
                device.sendGarbage();
            } catch (Exception ignored) {
                // best effort demo
            }
        }, 3, TimeUnit.SECONDS);
    }
}
