package com.github.lechandonga.edgegatewaystoreforwardjava.edge.pipeline;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关全部可配置项，前缀 {@code edge.gateway}。
 */
@ConfigurationProperties(prefix = "edge.gateway")
public class GatewayProperties {

    /** 缓冲写满策略：block=对设备侧阻塞背压；reject=隔离该报文(BUFFER_FULL)。 */
    public enum FullPolicy { BLOCK, REJECT }

    /** Modbus 文本协议 TCP 端口，0=随机端口。 */
    private int modbusPort = 9001;
    /** JSON-Line 协议 TCP 端口，0=随机端口。 */
    private int jsonPort = 9002;

    /** WAL 数据目录，重启后据此恢复未确认数据。 */
    private String dataDir = "data/gateway";
    /** 未确认事件条数硬上限。 */
    private int bufferMaxEvents = 10_000;
    /** WAL 磁盘字节硬上限（约，含帧头）。 */
    private long bufferMaxBytes = 64L * 1024 * 1024;
    /** 写满策略。 */
    private FullPolicy bufferFullPolicy = FullPolicy.BLOCK;
    /** block 策略下最长等待毫秒，超时仍满则按 reject 兜底，不允许无限挂死。 */
    private long blockTimeoutMs = 30_000;

    /** 单帧最大字节数（含结束符），超过即 FRAME_TOO_LONG。 */
    private int maxFrameBytes = 1024;
    /** 每批补发条数。 */
    private int batchSize = 50;
    /** 补发调度间隔毫秒。 */
    private long pumpIntervalMs = 200;
    /** 退避起始毫秒。 */
    private long backoffInitialMs = 200;
    /** 退避上限毫秒，防止上游恢复瞬间无限重试打爆内存/CPU。 */
    private long backoffMaxMs = 10_000;
    /** 退避抖动比例 0~1。 */
    private double backoffJitter = 0.2;
    /** 上游慢时设备侧读取缓冲背压阈值：积压达到容量的该比例即暂停读 socket。 */
    private double backpressureRatio = 0.8;

    /** 是否启动内置演示设备（本地复现用）。 */
    private boolean demoEnabled = false;
    /** 演示设备每秒上报条数/设备。 */
    private int demoRatePerDevice = 2;

    public int getModbusPort() { return modbusPort; }
    public void setModbusPort(int modbusPort) { this.modbusPort = modbusPort; }
    public int getJsonPort() { return jsonPort; }
    public void setJsonPort(int jsonPort) { this.jsonPort = jsonPort; }
    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public int getBufferMaxEvents() { return bufferMaxEvents; }
    public void setBufferMaxEvents(int bufferMaxEvents) { this.bufferMaxEvents = bufferMaxEvents; }
    public long getBufferMaxBytes() { return bufferMaxBytes; }
    public void setBufferMaxBytes(long bufferMaxBytes) { this.bufferMaxBytes = bufferMaxBytes; }
    public FullPolicy getBufferFullPolicy() { return bufferFullPolicy; }
    public void setBufferFullPolicy(FullPolicy bufferFullPolicy) { this.bufferFullPolicy = bufferFullPolicy; }
    public long getBlockTimeoutMs() { return blockTimeoutMs; }
    public void setBlockTimeoutMs(long blockTimeoutMs) { this.blockTimeoutMs = blockTimeoutMs; }
    public int getMaxFrameBytes() { return maxFrameBytes; }
    public void setMaxFrameBytes(int maxFrameBytes) { this.maxFrameBytes = maxFrameBytes; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    public long getPumpIntervalMs() { return pumpIntervalMs; }
    public void setPumpIntervalMs(long pumpIntervalMs) { this.pumpIntervalMs = pumpIntervalMs; }
    public long getBackoffInitialMs() { return backoffInitialMs; }
    public void setBackoffInitialMs(long backoffInitialMs) { this.backoffInitialMs = backoffInitialMs; }
    public long getBackoffMaxMs() { return backoffMaxMs; }
    public void setBackoffMaxMs(long backoffMaxMs) { this.backoffMaxMs = backoffMaxMs; }
    public double getBackoffJitter() { return backoffJitter; }
    public void setBackoffJitter(double backoffJitter) { this.backoffJitter = backoffJitter; }
    public double getBackpressureRatio() { return backpressureRatio; }
    public void setBackpressureRatio(double backpressureRatio) { this.backpressureRatio = backpressureRatio; }
    public boolean isDemoEnabled() { return demoEnabled; }
    public void setDemoEnabled(boolean demoEnabled) { this.demoEnabled = demoEnabled; }
    public int getDemoRatePerDevice() { return demoRatePerDevice; }
    public void setDemoRatePerDevice(int demoRatePerDevice) { this.demoRatePerDevice = demoRatePerDevice; }
}
