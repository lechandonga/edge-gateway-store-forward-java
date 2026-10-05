package com.github.lechandonga.edgegatewaystoreforwardjava.tests;

import com.github.lechandonga.edgegatewaystoreforwardjava.buffer.PersistentEventQueue;
import com.github.lechandonga.edgegatewaystoreforwardjava.config.GatewayProperties;
import com.github.lechandonga.edgegatewaystoreforwardjava.forward.EventForwarder;
import com.github.lechandonga.edgegatewaystoreforwardjava.ingest.DeviceTcpServer;
import com.github.lechandonga.edgegatewaystoreforwardjava.ingest.QuarantineLog;
import com.github.lechandonga.edgegatewaystoreforwardjava.metrics.GatewayMetrics;
import com.github.lechandonga.edgegatewaystoreforwardjava.sim.SimulatedUpstream;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;

/**
 * 不启动 Spring，直接手工装配全链路（模拟上游 + WAL + 接入 + 补发），
 * 端口随机、数据目录用 JUnit 临时目录，测试间完全隔离。
 */
public abstract class AbstractGatewayTest {

    @TempDir
    Path tempDir;

    protected GatewayProperties props;
    protected SimulatedUpstream upstream;
    protected PersistentEventQueue queue;
    protected QuarantineLog quarantine;
    protected DeviceTcpServer deviceServer;
    protected EventForwarder forwarder;
    protected GatewayMetrics metrics;

    protected int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    protected void startStack() throws Exception {
        startStack(10_000, GatewayProperties.OverflowStrategy.BLOCK, true);
    }

    protected void startStack(int capacity,
                              GatewayProperties.OverflowStrategy strategy,
                              boolean upstreamAvailable) throws Exception {
        props = new GatewayProperties();
        props.setDevicePort(freePort());
        props.setUpstreamPort(freePort());
        props.setDataDir(tempDir.toAbsolutePath().toString());
        props.setBufferCapacity(capacity);
        props.setOverflowStrategy(strategy);
        props.setHalfPacketTimeoutMs(600);
        props.setRetryInitialBackoffMs(50);
        props.setRetryMaxBackoffMs(500);
        props.setRetryBackoffMultiplier(2.0);
        props.setForwardBatchSize(10);
        props.setUpstreamInitiallyAvailable(upstreamAvailable);

        metrics = new GatewayMetrics();
        upstream = new SimulatedUpstream(props.getUpstreamPort(),
                upstreamAvailable, props.getDataDir());
        queue = new PersistentEventQueue(props);
        metrics.addRecoveredOnStartup(queue.size());
        quarantine = new QuarantineLog(props);
        forwarder = new EventForwarder(props, queue, metrics);
        forwarder.start();
        deviceServer = new DeviceTcpServer(props, queue, quarantine, metrics);
        deviceServer.start();
    }

    protected void stopStack() {
        if (deviceServer != null) deviceServer.close();
        if (forwarder != null) forwarder.close();
        if (queue != null) queue.close();
        if (quarantine != null) quarantine.close();
        if (upstream != null) upstream.close();
    }

    protected void restartGateway() throws Exception {
        boolean upstreamWasAvailable = upstream != null && upstream.isAvailable();
        deviceServer.close();
        forwarder.close();
        queue.close();
        quarantine.close();
        upstream.close();
        // 模拟"网关进程重启，但上游仍在"：用相同 dataDir 与上游端口重建
        upstream = new SimulatedUpstream(props.getUpstreamPort(),
                upstreamWasAvailable, props.getDataDir());
        queue = new PersistentEventQueue(props);
        quarantine = new QuarantineLog(props);
        forwarder = new EventForwarder(props, queue, metrics);
        forwarder.start();
        deviceServer = new DeviceTcpServer(props, queue, quarantine, metrics);
        deviceServer.start();
    }

    protected void awaitBacklogZero(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (queue.size() == 0) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("积压未在 " + timeoutMs + "ms 内清空, backlog=" + queue.size());
    }

    protected void awaitCommitted(int expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (upstream.committedCount() >= expected) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("上游记录数未达到 " + expected
                + ", 实际=" + upstream.committedCount());
    }
}
