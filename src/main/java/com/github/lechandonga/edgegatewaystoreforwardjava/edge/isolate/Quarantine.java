package com.github.lechandonga.edgegatewaystoreforwardjava.edge.isolate;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectedRecord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * 隔离区：所有被拒绝的报文（畸形 / 超长 / 字段异常 / 缓冲写满）都进这里，
 * 内存中保留最近 N 条供 admin 查询，同时落盘 quarantine.log，绝不静默吞掉。
 */
public class Quarantine {

    private final int memoryKeep;
    private final ArrayDeque<RejectedRecord> recent = new ArrayDeque<>();
    private final RejectedRecordSink sink;

    public Quarantine(int memoryKeep, RejectedRecordSink sink) {
        this.memoryKeep = memoryKeep;
        this.sink = sink;
    }

    public synchronized void add(RejectedRecord record) {
        recent.addLast(record);
        while (recent.size() > memoryKeep) {
            recent.removeFirst();
        }
        try {
            sink.write(record);
        } catch (RuntimeException e) {
            org.slf4j.LoggerFactory.getLogger(Quarantine.class)
                    .error("quarantine sink failed, record kept in memory: {}", record, e);
        }
    }

    public synchronized List<RejectedRecord> recent() {
        return new ArrayList<>(recent);
    }

    /** 落盘策略（文件 / 测试中的收集器）。 */
    public interface RejectedRecordSink {
        void write(RejectedRecord record);
    }
}
