package com.github.lechandonga.edgegatewaystoreforwardjava.sim;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * 本地模拟上游服务（基于 JDK 自带 HttpServer，无外部中间件）。
 *
 * <p>语义保证：
 * <ul>
 *   <li>POST /events 接收一批事件，按幂等键 deviceId#deviceSeq 去重；
 *       整批在同一个同步块内完成"查重 + 落库"，因此一批要么全部生效、要么整批失败，
 *       不会出现提交一半。</li>
 *   <li>幂等键持久化到 dataDir/upstream-keys.txt，上游重启后重复投递/重复确认
 *       也不会产生重复记录。</li>
 *   <li>POST /control/available?available=false|true 模拟上游断连与恢复。
 *       不可用时对 /events 返回 503。</li>
 * </ul>
 */
public class SimulatedUpstream implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SimulatedUpstream.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpServer httpServer;
    private final Set<String> committedKeys;
    private final File keysFile;
    private volatile boolean available;

    public SimulatedUpstream(int port, boolean initiallyAvailable, String dataDir)
            throws IOException {
        File dir = new File(dataDir);
        dir.mkdirs();
        this.keysFile = new File(dir, "upstream-keys.txt");
        this.committedKeys = ConcurrentHashMap.newKeySet();
        if (keysFile.exists()) {
            committedKeys.addAll(Files.readAllLines(keysFile.toPath()));
        }
        this.available = initiallyAvailable;
        this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        this.httpServer.createContext("/events", this::handleEvents);
        this.httpServer.createContext("/control/available", this::handleAvailability);
        this.httpServer.createContext("/records", this::handleRecords);
        this.httpServer.setExecutor(Executors.newFixedThreadPool(4));
        this.httpServer.start();
        log.info("模拟上游已启动, port={}, 已持久化幂等键={}, 初始可用={}",
                port, committedKeys.size(), available);
    }

    public int getBoundPort() {
        return httpServer.getAddress().getPort();
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        boolean old = this.available;
        this.available = available;
        if (old != available) {
            log.info("模拟上游可用性切换: {} -> {}", old, available);
        }
    }

    public int committedCount() {
        return committedKeys.size();
    }

    private void handleEvents(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "{\"error\":\"method not allowed\"}");
            return;
        }
        if (!available) {
            respond(ex, 503, "{\"error\":\"upstream unavailable\"}");
            return;
        }
        byte[] body = ex.getRequestBody().readAllBytes();
        JsonNode batch;
        try {
            batch = mapper.readTree(body);
        } catch (Exception e) {
            respond(ex, 400, "{\"error\":\"bad json\"}");
            return;
        }
        // 整批原子提交：同一把锁内完成查重 + 新增 + 持久化
        int committed;
        int duplicates;
        Set<String> newKeys = new HashSet<>();
        synchronized (this) {
            for (JsonNode event : batch) {
                String key = event.path("idempotencyKey").asText(null);
                if (key == null) {
                    respond(ex, 400, "{\"error\":\"missing idempotencyKey\"}");
                    return;
                }
                newKeys.add(key);
            }
            Set<String> toAdd = new HashSet<>();
            int dup = 0;
            for (String key : newKeys) {
                if (committedKeys.contains(key)) {
                    dup++;
                } else {
                    toAdd.add(key);
                }
            }
            // 模拟"提交点"：全部通过检查后一次性落库
            committedKeys.addAll(toAdd);
            persistKeys();
            committed = toAdd.size();
            duplicates = dup;
        }
        log.info("上游原子提交一批: 新记录={}, 重复键去重={}, 累计记录={}",
                committed, duplicates, committedKeys.size());
        respond(ex, 200, "{\"committed\":" + committed + ",\"duplicates\":" + duplicates + "}");
    }

    private void persistKeys() {
        try {
            Files.write(keysFile.toPath(), committedKeys);
        } catch (IOException e) {
            log.error("幂等键持久化失败", e);
        }
    }

    private void handleAvailability(HttpExchange ex) throws IOException {
        String q = ex.getRequestURI().getQuery();
        boolean want = q != null && q.contains("true");
        setAvailable(want);
        respond(ex, 200, "{\"available\":" + want + "}");
    }

    /** 测试辅助：查询上游当前记录数。 */
    private void handleRecords(HttpExchange ex) throws IOException {
        respond(ex, 200, "{\"committedRecords\":" + committedKeys.size() + "}");
    }

    private void respond(HttpExchange ex, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        httpServer.stop(0);
    }

    /** 仅用于测试展示：当前记录键列表。 */
    public List<String> committedKeysView() {
        return List.copyOf(committedKeys);
    }
}
