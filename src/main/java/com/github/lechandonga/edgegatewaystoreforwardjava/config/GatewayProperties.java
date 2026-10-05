package com.github.lechandonga.edgegatewaystoreforwardjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 全部可调配置项，前缀 gateway。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    /** 缓冲写满策略：BLOCK=反压阻塞设备读取；REJECT_NEW=拒绝新事件并隔离计数。 */
    public enum OverflowStrategy { BLOCK, REJECT_NEW }

    /** TCP 接入端口。 */
    private int devicePort = 9100;
    /** 单帧载荷上限（字节），超出按 FRAME_TOO_LONG 隔离。 */
    private int maxFrameBytes = 8192;
    /** 半包等待补齐时间，超时按 FRAME_TRUNCATED 隔离。 */
    private long halfPacketTimeoutMs = 5_000;

    /** 本地持久化缓冲目录。 */
    private String dataDir = "data/gateway";
    /** 缓冲容量上限（事件条数），写满后按 overflowStrategy 处理，绝不静默覆盖。 */
    private int bufferCapacity = 10_000;
    private OverflowStrategy overflowStrategy = OverflowStrategy.BLOCK;

    /** 补发批量大小：一批要么全部提交成功，要么全部重试，不存在提交一半。 */
    private int forwardBatchSize = 50;
    /** 上游最长不可用 / 无数据时空转等待。 */
    private long pollTimeoutMs = 200;
    /** 重试退避：初始间隔。 */
    private long retryInitialBackoffMs = 200;
    /** 重试退避上限。 */
    private long retryMaxBackoffMs = 10_000;
    /** 退避倍增因子。 */
    private double retryBackoffMultiplier = 2.0;
    /** 模拟上游监听端口。 */
    private int upstreamPort = 9200;
    /** 模拟上游初始是否可用（可在测试/演示中通过 API 切换）。 */
    private boolean upstreamInitiallyAvailable = true;

    public int getDevicePort() { return devicePort; }
    public void setDevicePort(int devicePort) { this.devicePort = devicePort; }
    public int getMaxFrameBytes() { return maxFrameBytes; }
    public void setMaxFrameBytes(int maxFrameBytes) { this.maxFrameBytes = maxFrameBytes; }
    public long getHalfPacketTimeoutMs() { return halfPacketTimeoutMs; }
    public void setHalfPacketTimeoutMs(long v) { this.halfPacketTimeoutMs = v; }
    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public int getBufferCapacity() { return bufferCapacity; }
    public void setBufferCapacity(int bufferCapacity) { this.bufferCapacity = bufferCapacity; }
    public OverflowStrategy getOverflowStrategy() { return overflowStrategy; }
    public void setOverflowStrategy(OverflowStrategy s) { this.overflowStrategy = s; }
    public int getForwardBatchSize() { return forwardBatchSize; }
    public void setForwardBatchSize(int forwardBatchSize) { this.forwardBatchSize = forwardBatchSize; }
    public long getPollTimeoutMs() { return pollTimeoutMs; }
    public void setPollTimeoutMs(long v) { this.pollTimeoutMs = v; }
    public long getRetryInitialBackoffMs() { return retryInitialBackoffMs; }
    public void setRetryInitialBackoffMs(long v) { this.retryInitialBackoffMs = v; }
    public long getRetryMaxBackoffMs() { return retryMaxBackoffMs; }
    public void setRetryMaxBackoffMs(long v) { this.retryMaxBackoffMs = v; }
    public double getRetryBackoffMultiplier() { return retryBackoffMultiplier; }
    public void setRetryBackoffMultiplier(double v) { this.retryBackoffMultiplier = v; }
    public int getUpstreamPort() { return upstreamPort; }
    public void setUpstreamPort(int upstreamPort) { this.upstreamPort = upstreamPort; }
    public boolean isUpstreamInitiallyAvailable() { return upstreamInitiallyAvailable; }
    public void setUpstreamInitiallyAvailable(boolean v) { this.upstreamInitiallyAvailable = v; }
}
