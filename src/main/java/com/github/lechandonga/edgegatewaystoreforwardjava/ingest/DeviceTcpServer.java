package com.github.lechandonga.edgegatewaystoreforwardjava.ingest;

import com.github.lechandonga.edgegatewaystoreforwardjava.buffer.PersistentEventQueue;
import com.github.lechandonga.edgegatewaystoreforwardjava.codec.FrameDecodeException;
import com.github.lechandonga.edgegatewaystoreforwardjava.codec.FrameReader;
import com.github.lechandonga.edgegatewaystoreforwardjava.codec.JsonProtocolDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.codec.KvProtocolDecoder;
import com.github.lechandonga.edgegatewaystoreforwardjava.config.GatewayProperties;
import com.github.lechandonga.edgegatewaystoreforwardjava.metrics.GatewayMetrics;
import com.github.lechandonga.edgegatewaystoreforwardjava.model.DeviceEvent;
import com.github.lechandonga.edgegatewaystoreforwardjava.model.RejectReason;
import com.github.lechandonga.edgegatewaystoreforwardjava.model.RejectedFrame;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 设备 TCP 接入服务。
 *
 * <p>每个设备连接由独立线程处理，设备间故障完全隔离：单条连接的畸形/半包/超长报文
 * 只会隔离该帧或关闭该连接，不影响其他连接。同一设备的事件经其会话串行解析，
 * 再顺序进入全局持久队列，保证同一设备不乱序。
 */
public class DeviceTcpServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DeviceTcpServer.class);

    private final GatewayProperties props;
    private final PersistentEventQueue queue;
    private final QuarantineLog quarantine;
    private final GatewayMetrics metrics;
    private final JsonProtocolDecoder jsonDecoder = new JsonProtocolDecoder();
    private final KvProtocolDecoder kvDecoder = new KvProtocolDecoder();
    private final ObjectMapper mapper = new ObjectMapper();
    /** deviceId -> 最近已接受的设备侧序号，检测序号倒退。 */
    private final Map<String, Long> lastDeviceSeq = new ConcurrentHashMap<>();

    private ServerSocket serverSocket;
    private Thread acceptThread;
    private final ExecutorService sessionPool =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "device-session");
                t.setDaemon(true);
                return t;
            });
    private volatile boolean running;

    public DeviceTcpServer(GatewayProperties props, PersistentEventQueue queue,
                           QuarantineLog quarantine, GatewayMetrics metrics) {
        this.props = props;
        this.queue = queue;
        this.quarantine = quarantine;
        this.metrics = metrics;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(props.getDevicePort());
        running = true;
        acceptThread = new Thread(this::acceptLoop, "device-acceptor");
        acceptThread.setDaemon(true);
        acceptThread.start();
        log.info("设备 TCP 接入服务已启动, port={}, maxFrameBytes={}, overflow={}, capacity={}",
                props.getDevicePort(), props.getMaxFrameBytes(),
                props.getOverflowStrategy(), props.getBufferCapacity());
    }

    public int getBoundPort() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                sessionPool.submit(() -> runSession(socket));
            } catch (IOException e) {
                if (running) {
                    log.warn("accept 失败: {}", e.getMessage());
                }
            }
        }
    }

    private void runSession(Socket socket) {
        String deviceId = null;
        try (Socket s = socket) {
            s.setSoTimeout((int) props.getHalfPacketTimeoutMs());
            DataInputStream in = new DataInputStream(s.getInputStream());
            FrameReader reader = new FrameReader(in, props.getMaxFrameBytes());

            // 第一帧必须是 JSON HELLO，绑定设备身份
            FrameReader.RawFrame helloFrame = reader.next();
            if (helloFrame == null) {
                return;
            }
            deviceId = parseHello(helloFrame);
            log.info("设备接入: deviceId={}, remote={}", deviceId, s.getRemoteSocketAddress());

            FrameReader.RawFrame raw;
            while ((raw = reader.next()) != null) {
                try {
                    DeviceEvent normalized = switch (raw.proto()) {
                        case FrameReader.PROTO_JSON ->
                                jsonDecoder.decode(deviceId, raw.payload());
                        case FrameReader.PROTO_KV ->
                                kvDecoder.decode(deviceId, raw.payload());
                        default -> throw new FrameDecodeException(RejectReason.MALFORMED_PAYLOAD,
                                "unknown proto byte: " + raw.proto());
                    };
                    acceptNormalized(normalized);
                } catch (FrameDecodeException e) {
                    metrics.incRejected(e.getReason());
                    quarantine.record(new RejectedFrame(deviceId, e.getReason(),
                            e.getMessage() + " | payload="
                                    + new String(raw.payload(), StandardCharsets.UTF_8),
                            System.currentTimeMillis()));
                }
            }
        } catch (SocketTimeoutException e) {
            metrics.incRejected(RejectReason.FRAME_TRUNCATED);
            quarantine.record(new RejectedFrame(deviceId, RejectReason.FRAME_TRUNCATED,
                    "读帧超时(" + props.getHalfPacketTimeoutMs() + "ms)，按半包处理并断开",
                    System.currentTimeMillis()));
        } catch (FrameDecodeException e) {
            metrics.incRejected(e.getReason());
            quarantine.record(new RejectedFrame(deviceId, e.getReason(),
                    "成帧阶段拒绝: " + e.getMessage(), System.currentTimeMillis()));
        } catch (IOException e) {
            log.debug("设备连接 IO 结束 device={}: {}", deviceId, e.getMessage());
        } finally {
            log.info("设备断开: deviceId={}", deviceId);
        }
    }

    private String parseHello(FrameReader.RawFrame frame) {
        if (frame.proto() != FrameReader.PROTO_JSON) {
            throw new FrameDecodeException(RejectReason.MISSING_OR_INVALID_FIELD,
                    "首帧必须是 JSON HELLO");
        }
        try {
            JsonNode node = mapper.readTree(frame.payload());
            if (node == null || !"hello".equals(node.path("type").asText(null))
                    || node.path("deviceId").asText(null) == null
                    || node.path("deviceId").asText("").isBlank()) {
                throw new FrameDecodeException(RejectReason.MISSING_OR_INVALID_FIELD,
                        "HELLO 必须包含 type=hello 与非空 deviceId");
            }
            String id = node.get("deviceId").asText();
            if (!id.matches("[A-Za-z0-9_\\-.:]{1,64}")) {
                throw new FrameDecodeException(RejectReason.MISSING_OR_INVALID_FIELD,
                        "deviceId 仅允许 1-64 位字母数字 _-.: ");
            }
            return id;
        } catch (FrameDecodeException e) {
            throw e;
        } catch (Exception e) {
            throw new FrameDecodeException(RejectReason.MALFORMED_PAYLOAD,
                    "HELLO JSON 解析失败: " + e.getMessage());
        }
    }

    private void acceptNormalized(DeviceEvent event) throws FrameDecodeException {
        // 同一设备序号必须单调递增（重复 seq 视为乱序/重复投递，隔离之）
        Long prev = lastDeviceSeq.putIfAbsent(event.deviceId(), event.deviceSeq());
        if (prev != null) {
            long last = lastDeviceSeq.get(event.deviceId());
            if (event.deviceSeq() <= last) {
                throw new FrameDecodeException(RejectReason.DEVICE_SEQ_GOES_BACKWARDS,
                        "deviceSeq " + event.deviceSeq() + " <= last " + last);
            }
            lastDeviceSeq.put(event.deviceId(), event.deviceSeq());
        }
        metrics.incAccepted();
        switch (props.getOverflowStrategy()) {
            case REJECT_NEW -> {
                DeviceEvent persisted = queue.appendAndPersist(event.deviceId(),
                        event.deviceSeq(), event.metric(), event.value(), event.eventTime());
                if (persisted == null) {
                    metrics.incRejected(RejectReason.BUFFER_FULL);
                    quarantine.record(new RejectedFrame(event.deviceId(), RejectReason.BUFFER_FULL,
                            "缓冲已满(capacity=" + props.getBufferCapacity()
                                    + ")，策略 REJECT_NEW，拒绝 deviceSeq=" + event.deviceSeq(),
                            System.currentTimeMillis()));
                    return;
                }
                metrics.incPersisted();
            }
            case BLOCK -> {
                try {
                    if (!queue.awaitSpace(30_000)) {
                        metrics.incRejected(RejectReason.BUFFER_FULL);
                        quarantine.record(new RejectedFrame(event.deviceId(),
                                RejectReason.BUFFER_FULL,
                                "BLOCK 等待 30s 仍无空间，断开该设备连接以触发背压",
                                System.currentTimeMillis()));
                        throw new FrameDecodeException(RejectReason.BUFFER_FULL,
                                "backpressure timeout");
                    }
                    DeviceEvent persisted = queue.appendAndPersist(event.deviceId(),
                            event.deviceSeq(), event.metric(), event.value(), event.eventTime());
                    if (persisted == null) {
                        throw new FrameDecodeException(RejectReason.BUFFER_FULL, "race");
                    }
                    metrics.incPersisted();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new FrameDecodeException(RejectReason.BUFFER_FULL, "interrupted");
                }
            }
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
        sessionPool.shutdownNow();
        try {
            sessionPool.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
