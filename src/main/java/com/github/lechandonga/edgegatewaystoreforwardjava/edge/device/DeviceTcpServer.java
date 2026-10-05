package com.github.lechandonga.edgegatewaystoreforwardjava.edge.device;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.DecodeResult;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec.FrameDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline.EventPipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 协议监听服务器。每个设备连接一个独立读线程、一个独立解码器实例：
 * 单台设备的半包/超长/畸形报文只影响该连接自身，绝不波及其他设备。
 *
 * <p>背压：网关积压达到阈值后暂停从 socket 读字节，TCP 接收窗口收缩，
 * 反压自然传导到设备端（模拟同步发送的设备会被阻塞），避免内存被打爆。</p>
 */
public class DeviceTcpServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DeviceTcpServer.class);

    private final String name;
    private final int port;
    private final DecoderFactory decoderFactory;
    private final EventPipeline pipeline;
    private final ExecutorService workers = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "device-io-" + DeviceTcpServer.this.name);
        t.setDaemon(true);
        return t;
    });
    private final List<Socket> sessions = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private ServerSocket serverSocket;
    private int boundPort;

    @FunctionalInterface
    public interface DecoderFactory {
        FrameDecoder create();
    }

    public DeviceTcpServer(String name, int port, DecoderFactory decoderFactory, EventPipeline pipeline) {
        this.name = name;
        this.port = port;
        this.decoderFactory = decoderFactory;
        this.pipeline = pipeline;
    }

    public void start() {
        workers.execute(this::acceptLoop);
    }

    private void acceptLoop() {
        try (ServerSocket server = new ServerSocket()) {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", port));
            this.serverSocket = server;
            this.boundPort = server.getLocalPort();
            log.info("[{}] listening on 127.0.0.1:{}", name, boundPort);
            while (running.get()) {
                Socket socket = server.accept();
                sessions.add(socket);
                workers.execute(() -> serve(socket));
            }
        } catch (SocketException e) {
            if (running.get()) {
                log.error("[{}] socket error", name, e);
            }
        } catch (IOException e) {
            log.error("[{}] accept failed", name, e);
        }
    }

    private void serve(Socket socket) {
        String remote = socket.getRemoteSocketAddress().toString();
        FrameDecoder decoder = decoderFactory.create();
        log.info("[{}] device connected: {}", name, remote);
        try {
            socket.setTcpNoDelay(true);
            InputStream in = socket.getInputStream();
            byte[] buf = new byte[2048];
            while (running.get() && !socket.isClosed()) {
                if (pipeline.isBackpressured()) {
                    Thread.sleep(50);
                    continue;
                }
                int n = in.read(buf);
                if (n < 0) {
                    break;
                }
                if (n == 0) {
                    continue;
                }
                dispatch(decoder.feed(buf, n), remote);
            }
            dispatch(decoder.endOfStream(), remote);
        } catch (Exception e) {
            if (running.get()) {
                log.warn("[{}] session {} ended: {}", name, remote, e.getMessage());
            }
        } finally {
            sessions.remove(socket);
            closeQuietly(socket);
            log.info("[{}] device disconnected: {}", name, remote);
        }
    }

    private void dispatch(List<DecodeResult> results, String remote) {
        for (DecodeResult result : results) {
            String protocol = "SERVER-" + name;
            pipeline.handle(result, protocol, remote);
        }
    }

    private void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // best effort
        }
    }

    public int boundPort() {
        long deadline = System.currentTimeMillis() + 3000;
        while (boundPort == 0 && System.currentTimeMillis() < deadline) {
            Thread.onSpinWait();
        }
        return boundPort;
    }

    @Override
    public void close() {
        running.set(false);
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
        sessions.forEach(this::closeQuietly);
        workers.shutdownNow();
    }
}
