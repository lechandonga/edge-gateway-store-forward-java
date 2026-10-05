package com.github.lechandonga.edgegatewaystoreforwardjava.edge.upstream;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.DeviceEvent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 模拟云端上游：
 * <ul>
 *   <li>不可用时所有提交直接失败，触发网关缓冲与退避重试；</li>
 *   <li>批量提交在锁内完成校验+写入，保证原子性；</li>
 *   <li>以 dedupKey 去重，同一批内重复或跨批次重复投递都只入库一次。</li>
 * </ul>
 */
public class InMemoryUpstream implements UpstreamClient {

    private final AtomicBoolean available = new AtomicBoolean(true);
    private final Set<String> seenKeys = ConcurrentHashMap.newKeySet();
    private final List<DeviceEvent> stored = new ArrayList<>();

    @Override
    public int ingestBatch(List<DeviceEvent> batch) {
        if (!available.get()) {
            throw new UpstreamUnavailableException("simulated upstream is DOWN");
        }
        synchronized (this) {
            List<String> batchKeys = new ArrayList<>(batch.size());
            for (DeviceEvent event : batch) {
                batchKeys.add(event.dedupKey());
            }
            Set<String> inBatchDup = new HashSet<>();
            List<DeviceEvent> fresh = new ArrayList<>();
            for (int i = 0; i < batch.size(); i++) {
                String key = batchKeys.get(i);
                if (!inBatchDup.add(key)) {
                    continue;
                }
                if (seenKeys.add(key)) {
                    fresh.add(batch.get(i));
                }
            }
            stored.addAll(fresh);
            return fresh.size();
        }
    }

    @Override
    public void setAvailable(boolean up) {
        available.set(up);
    }

    @Override
    public boolean isAvailable() {
        return available.get();
    }

    public synchronized List<DeviceEvent> storedEvents() {
        return new ArrayList<>(stored);
    }

    public synchronized int storedCount() {
        return stored.size();
    }

    /** 仅测试使用：制造"上游收到过但 ACK 丢失"场景时清空已提交记录。 */
    public synchronized void reset() {
        stored.clear();
        seenKeys.clear();
    }

    public static class UpstreamUnavailableException extends RuntimeException {
        public UpstreamUnavailableException(String message) {
            super(message);
        }
    }
}
