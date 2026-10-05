# 边缘网关：数据采集与断连补发（本地模拟）

纯本地可复现的 store-and-forward 链路：**模拟设备 → TCP 接入 → 协议解码/归一化 → 持久化 WAL 缓冲 → 退避重试 → 模拟上游**。
不依赖外部中间件、真实硬件或其他服务（仅 Spring Boot Web + Jackson + JUnit）。

## 1. 总体架构

```
 模拟设备 (Modbus 文本 / JSON-Line, TCP)
        │  每连接独立线程 + 独立解码器
        ▼
 DeviceTcpServer ──半包累积/超长跳帧/畸形隔离──► Quarantine (内存最近N条 + quarantine.jsonl)
        │ Parsed
        ▼
 EventPipeline ──每 deviceId 一把锁（设备内严格有序）──► WriteAheadBuffer (events.wal + offset + id, fsync)
                                                          │ 容量上限：条数 + 字节；满则 BLOCK 背压 / REJECT 隔离
                                                          ▼
                                              Forwarder（单线程，整批提交，指数退避+抖动+上限）
                                                          ▼
                                              InMemoryUpstream（幂等键去重，批量原子提交）
```

## 2. 接入协议

两种协议都是 TCP + LF（兼容 CRLF）分行文本，分别监听 `edge.gateway.modbus-port`（默认 9001）与 `edge.gateway.json-port`（默认 9002）。

### 2.1 Modbus-TXT（模拟 Modbus-TCP 文本化）

```
#<设备地址>|SEQ=<设备内单调序号>|REG=<寄存器码>|V=<读数>|TS=<设备毫秒时间>!\n
```

- 寄存器码：`0x00`=temperature(℃)，`0x01`=humidity(%)，`0x02`=pressure(kPa)
- 设备 id 归一化为 `MOD-<地址>`，例：`#1|SEQ=3|REG=0x00|V=23.75|TS=1728000000000!\n` → 设备 `MOD-1`

### 2.2 JSON-Line（模拟 MQTT/JSON 传感器）

每行一个 JSON 对象：

```json
{"deviceId":"MQTT-101","seq":7,"metric":"temperature","value":23.5,"ts":1728000000000}
```

### 2.3 统一事件模型

| 字段 | 含义 |
|---|---|
| `id` | 网关全局单调 id（落 WAL 时分配，重启不回退） |
| `deviceId` / `seq` | 设备标识与设备内序号；幂等键 `deviceId#seq` |
| `metric` / `value` / `unit` | 归一化指标名、读数、单位（单位/量程来自设备注册表） |
| `deviceTime` / `ingestTime` | 设备时间 / 网关接收时间 |
| `source` | 来源协议（MODBUS-TXT / JSON-LINE） |

### 2.4 异常报文处理（不影响其他设备、不静默吞掉）

每个 TCP 连接一个独立读线程和独立解码器实例，单连接异常不会传播到其他连接。拒绝原因枚举（`RejectReason`，可区分、可统计）：

| 原因 | 触发条件 |
|---|---|
| `BAD_HEADER` | 帧头不合法（如非 `#` 开头） |
| `MALFORMED_FRAME` | 缺结束标记、字段结构错、空帧、连接关闭时残留半包 |
| `MISSING_FIELD` | 缺 SEQ/REG/V/TS 或设备未定义该指标 |
| `BAD_NUMBER` / `BAD_TIMESTAMP` | 序号/数值/时间戳不可解析 |
| `UNKNOWN_DEVICE` | 设备地址/id 未在注册表登记 |
| `VALUE_OUT_OF_RANGE` | NaN/无穷或超出注册表量程 |
| `FRAME_TOO_LONG` | 单帧超过 `max-frame-bytes`，立即跳至下一换行符，后续帧正常处理 |
| `LENGTH_MISMATCH` | 预留（二进制协议长度字段校验） |
| `BUFFER_FULL` | 本地缓冲写满（见 3.2） |

- **半包**：未收到 LF 前持续在连接级缓冲区累积（`NeedMore`），不算拒绝；连接断开仍有残留则按 `MALFORMED_FRAME` 隔离。
- 隔离记录含时间、设备、协议、远端地址、原因、细节与**原始报文 hex**，写入 `quarantine.jsonl`（JSONL）并在内存保留最近 N 条。

## 3. 缓冲与补发规则

### 3.1 WAL 持久化（`data/<dir>/`）

- `events.wal`：仅追加，记录格式 `EVT1(4B magic) + int 长度 + JSON 事件`；每条 append 后 `fsync` 才算提交。
- `events.offset`：已确认的字节偏移，**只允许从队头顺序推进**；队头确认后截断回收空间。
- `events.id`：全局事件 id 水位，重启后继续递增。
- 启动时从 offset 扫描恢复全部未确认事件，**重启/崩溃后继续补发**。

### 3.2 容量上限与写满策略（可配置，禁止静默丢弃/覆盖）

- 双上限：未确认条数 `buffer-max-events`（默认 10000）与 WAL 字节 `buffer-max-bytes`（默认 64MiB）。
- `buffer-full-policy=BLOCK`（默认）：设备写入线程在容量释放前等待（受 `block-timeout-ms` 约束，超时兜底隔离为 `BUFFER_FULL`）。
- `buffer-full-policy=REJECT`：该报文立即以 `BUFFER_FULL` 进入隔离区，已有未确认数据绝不被覆盖。
- **背压**：积压达到容量的 `backpressure-ratio`（默认 0.8）时，设备读线程暂停读 socket，TCP 接收窗口收缩反压到设备端，内存不会被打爆。

### 3.3 补发、顺序、原子性与幂等

- 单线程 `Forwarder` 每 `pump-interval-ms` 扫描 WAL，从**最早未确认**事件开始，每批最多 `batch-size` 条。
- **整批原子**：上游批量提交成功才顺序确认整批；失败一条都不确认，不存在"只提交一半"。
- **设备内有序**：写入按 `deviceId` 加锁；补发按 WAL 全局顺序（即接收顺序），同一设备事件顺序不乱。
- **幂等**：上游以 `deviceId#seq` 去重，重复投递/重复确认不产生重复记录；`acknowledge(n)` 超过未确认数时安全忽略。
- **退避**：失败按 `backoff-initial-ms` 指数增长（×2）+ 抖动（`backoff-jitter`），封顶 `backoff-max-ms`；退避等待期间忽略周期性 tick，**不会出现重试风暴**。

## 4. 配置项（`application.properties`，前缀 `edge.gateway`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `modbus-port` / `json-port` | 9001 / 9002 | 两种设备协议监听端口 |
| `data-dir` | `data/gateway` | WAL 与隔离文件目录 |
| `buffer-max-events` / `buffer-max-bytes` | 10000 / 64MiB | 缓冲条数 / 字节硬上限 |
| `buffer-full-policy` | `BLOCK` | 写满策略：`BLOCK`（背压）或 `REJECT`（隔离） |
| `block-timeout-ms` | 30000 | BLOCK 最长等待，超时按 BUFFER_FULL 隔离 |
| `max-frame-bytes` | 1024 | 单帧上限，超过即 FRAME_TOO_LONG |
| `batch-size` / `pump-interval-ms` | 50 / 200 | 补发批量 / 扫描间隔 |
| `backoff-initial-ms` / `backoff-max-ms` / `backoff-jitter` | 200 / 10000 / 0.2 | 指数退避与抖动 |
| `backpressure-ratio` | 0.8 | 积压占容量比例阈值，达到即暂停读设备 |
| `demo-enabled` / `demo-rate-per-device` | false / 2 | 内置 4 台模拟设备 + 自动断连恢复演示 |

## 5. 可观测性（HTTP 管理端点）

| 端点 | 说明 |
|---|---|
| `GET /admin/metrics` | 接收/接收/隔离帧数、缓冲累计、未确认积压、已确认总数、重试次数、批量成功/失败数、写满隔离数、重复入站数、按原因分类计数、上游可用与背压状态 |
| `GET /admin/quarantine` | 最近被隔离报文（含原因/细节/原文 hex） |
| `POST /admin/upstream?available=false|true` | 手动模拟上游断连/恢复 |
| `POST /admin/pump` | 立即触发一次补发（绕过退避） |

## 6. 本地验证

前置：JDK 21+、Maven 3.8+。

### 6.1 运行单测

```bash
mvn test
```

日志中以 `BUSINESS-SCENARIO` 打印"业务模块 → 用例开始/通过 → 业务覆盖汇总"。共 23 个用例，5 个业务类：

| 测试类 | 覆盖业务 |
|---|---|
| `NormalCollectionTest` | 正常采集：两种协议解析、归一化入库确认、跨字节半包拼帧 |
| `MalformedIsolationTest` | 异常隔离：坏帧头/缺字段/坏数值/超量程/未注册设备/超长帧/断连残包，原因可区分且不影响后续帧 |
| `DisconnectReplayTest` | 断连补发：断连缓冲、恢复按序补发、整批原子、幂等去重、退避上限与重试风暴防护、写满 REJECT 策略 |
| `RestartRecoveryTest` | 重启恢复：未确认事件重放、已确认不重投、id 不回退、WAL 组件级重开 |
| `ConcurrentOrderTest` | 并发顺序：16 连接 3200 事件设备内严格有序、坏设备不影响好设备、背压将积压限制在容量内 |

### 6.2 一键端到端演示

```bash
mvn package -DskipTests
java -jar target/edge-gateway-store-forward-java-1.0-SNAPSHOT.jar \
  --server.port=8080 --edge.gateway.demo-enabled=true
```

启动 4 台模拟设备（2 Modbus + 2 JSON）持续上报，启动后 5s 模拟上游断连 20s 再恢复。观察日志可见：

- 断连期间 `upstream batch failed (streak=n), keep N events in WAL, retry in <退避>ms`，退避 200ms→10s；
- 恢复时 `batch committed` 将积压按 50 条整批评审放，`pending=0`；
- 第 3 秒每台设备各发一条畸形帧，出现 `frame isolated ... reason=BAD_HEADER/MALFORMED_FRAME`。

另开终端：

```bash
curl -s http://127.0.0.1:8080/admin/metrics | python3 -m json.tool
curl -s http://127.0.0.1:8080/admin/quarantine | python3 -m json.tool
curl -X POST 'http://127.0.0.1:8080/admin/upstream?available=false'
curl -X POST 'http://127.0.0.1:8080/admin/upstream?available=true'
```

落盘文件位于 `data/gateway/`：`events.wal`、`events.offset`、`events.id`、`quarantine.jsonl`。
