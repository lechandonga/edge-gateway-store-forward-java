# 边缘网关：数据采集与断连补发（本地全模拟）

纯本地可复现的边缘网关参考实现：**多协议设备 TCP 接入 → 统一事件归一化 → 有界持久化缓冲 → 模拟上游断连补发**。
不依赖外部中间件（无 Kafka/MQ/数据库）、真实硬件或其他服务；上游用 JDK 内置 HTTP Server 模拟，缓冲用单文件 WAL。

## 架构与数据流

```
模拟设备(JSON/KV 帧 over TCP:9100)
        │  每连接一线程，设备间故障隔离
        ▼
DeviceTcpServer ──异常帧──▶ QuarantineLog (data/quarantine.log，带原因)
        │  归一化 DeviceEvent + 设备序号单调校验
        ▼
PersistentEventQueue (内存有界队列 + queue.wal，追加即 fsync，容量硬上限)
        │  单线程按全局 seq 顺序、批量
        ▼
EventForwarder ──指数退避重试──▶ SimulatedUpstream (HTTP :9200，整批原子 + 幂等键去重)
        ▲
管理/观测 REST (:8080)：/metrics /status /control/upstream
```

## 设备接入协议

统一二进制成帧（两种协议共用）：

| 字段 | magic | proto | length | payload |
|---|---|---|---|---|
| 长度 | 1 字节 | 1 字节 | 2 字节(大端) | length 字节 |
| 取值 | `0xA5` | `1`=JSON，`2`=KV 二进制 | 0–65535，且 ≤ `gateway.max-frame-bytes` | |

- **会话首帧必须是 JSON HELLO**：`{"type":"hello","deviceId":"d-1"}`，绑定设备身份。
- **JSON 读数（proto=1）**：
  `{"type":"reading","seq":12,"metric":"temp","value":36.5,"ts":1728000000000}`
  （`ts` 可省略，取网关时间；`seq` 为设备侧单调递增序号）
- **KV 读数（proto=2，大端）**：
  `long deviceSeq | long ts | double value | u16 metricLen | metricUtf8`

### 异常报文处理（可区分、可审计、不静默）

所有异常帧落 `data/quarantine.log`（时间戳、设备、原因、报文摘要）并打 WARN，同时计入指标：

| 原因（RejectReason） | 触发场景 |
|---|---|
| `BAD_MAGIC` | 帧头魔数不是 0xA5（无法再定位帧边界，隔离后断开该连接，其他设备不受影响） |
| `FRAME_TOO_LONG` | 声明长度 > `gateway.max-frame-bytes`；跳过整帧剩余字节 |
| `FRAME_TRUNCATED` | 半包：声明的载荷在 `gateway.half-packet-timeout-ms` 内未到齐 |
| `MALFORMED_PAYLOAD` | JSON 非法 / KV 长度字段对不上 |
| `MISSING_OR_INVALID_FIELD` | 缺 metric/value/seq、类型错误、HELLO 非法 |
| `FIELD_VALUE_OUT_OF_RANGE` | NaN/Infinity、负时间戳/负序号 |
| `DEVICE_SEQ_GOES_BACKWARDS` | 同一设备 deviceSeq 非递增（乱序/重复） |
| `BUFFER_FULL` | 缓冲写满且策略为 REJECT_NEW（BLOCK 策略下先反压，超时才计此项） |

故障隔离粒度：单个设备连接独立线程；单帧错误只隔离该帧（成帧级错误仅影响该连接），不影响其他设备。

## 缓冲与补发规则

- **有界容量**：`gateway.buffer-capacity`（默认 10000 条）。内存水位永远不超过上限。
- **写满策略**（`gateway.overflow-strategy`，必须显式选择，绝不静默丢弃/覆盖）：
  - `BLOCK`（默认）：阻塞该设备会话读取形成 TCP 背压；等待 30s 仍无空间则断开该连接并记 `BUFFER_FULL`。
  - `REJECT_NEW`：拒绝新事件，按 `BUFFER_FULL` 写入隔离日志并计数；已缓冲的未确认数据原封不动。
- **持久化**：事件先追加 `queue.wal` 并 `fsync` 才入内存队列；上游每确认一批写一条 ACK 记录（同样 fsync）。
- **按序补发**：补发为单线程，严格按全局 `seq` 批量（`gateway.forward-batch-size`）peek；
  只有整批收到 2xx 才 `commitUpTo(批次最大seq)`；失败整批重试，**一批要么全成功要么全重试，不存在提交一半**。
- **退避重试**：初始 `retry-initial-backoff-ms`，按 `retry-backoff-multiplier` 倍增，
  封顶 `retry-max-backoff-ms`，±20% 抖动；上游恢复后立即继续补发，稳定后无空转重试。
- **重启恢复**：启动时扫描 WAL，按最后一条 ACK 截断，重放所有未确认事件并压缩重写 WAL；
  网关重启后继续补发。
- **幂等**：上游以 `deviceId#deviceSeq` 为幂等键，"查重+落库+持久化键"在同一同步块内完成；
  幂等键持久化在 `data/upstream-keys.txt`，因此**重复投递、重复确认、上游重启后重试都不会产生重复记录**。

## 配置项（`src/main/resources/application.properties`，前缀 `gateway`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `device-port` | 9100 | 设备 TCP 接入端口 |
| `max-frame-bytes` | 8192 | 单帧载荷上限，超出按 FRAME_TOO_LONG 隔离 |
| `half-packet-timeout-ms` | 5000 | 半包补齐超时 |
| `data-dir` | data/gateway | WAL、隔离日志、上游幂等键目录 |
| `buffer-capacity` | 10000 | 缓冲容量硬上限（事件条数） |
| `overflow-strategy` | BLOCK | 写满策略：`BLOCK`（背压）/ `REJECT_NEW`（拒绝并隔离） |
| `forward-batch-size` | 50 | 补发批量（整批原子语义） |
| `poll-timeout-ms` | 200 | 无积压时的轮询间隔 |
| `retry-initial-backoff-ms` | 200 | 重试初始退避 |
| `retry-max-backoff-ms` | 10000 | 退避上限 |
| `retry-backoff-multiplier` | 2.0 | 退避倍增因子 |
| `upstream-port` | 9200 | 本地模拟上游 HTTP 端口 |
| `upstream-initially-available` | true | 模拟上游初始是否可用 |

## 可观测性

- `GET http://127.0.0.1:8080/metrics`：接收/持久化/投递/重复投递/各类拒绝/积压/重试/失败/批提交/启动恢复计数。
- `GET /status`：缓冲水位、上游可用性、上游记录数、全部指标。
- `GET /control/upstream?available=false|true`：切换模拟上游断连/恢复（上游本身也支持同语义 `/control/available`）。
- `GET http://127.0.0.1:9200/records`：查询模拟上游落库记录数。

## 本地验证

### 运行单测（覆盖五类业务，日志带业务名与用例名）

```bash
mvn test
```

| 测试类 | 覆盖业务 | 用例 |
|---|---|---|
| `NormalCollectionTest` | 正常采集 | JSON 设备采集；JSON+KV 多协议归一化 |
| `MalformedIsolationTest` | 异常报文隔离 | 坏魔数/非法JSON/缺字段/序号倒退；超长帧不影响他人；半包超时；字段越界 |
| `StoreForwardReconnectTest` | 断连补发 | 不可用积压→恢复按序补发；重复投递幂等；可用性抖动不丢不重、退避收敛 |
| `RestartRecoveryTest` | 重启恢复 | kill 后 WAL 重放未确认数据；重启后新事件序号连续 |
| `ConcurrentOrderingTest` | 并发顺序与背压 | 8 设备×100 条全送达；REJECT_NEW 满员拒绝可观测；BLOCK 水位不超上限 |

测试日志形如 `[断连补发-用例1] ...`，可直接看出每个业务的用例步骤与通过结论；
Surefire 汇总报告在 `target/surefire-reports/`。

### 手工端到端验证

```bash
mvn package -DskipTests
java -jar target/edge-gateway-store-forward-java-1.0-SNAPSHOT.jar

# 1) 制造上游断连
curl "http://127.0.0.1:8080/control/upstream?available=false"
# 2) 用任意 TCP 客户端按协议发 HELLO + 读数（Python 示例）
python3 - <<'EOF'
import socket, struct, json, time
def frame(proto, payload): return bytes([0xA5, proto]) + struct.pack('>H', len(payload)) + payload
s = socket.create_connection(('127.0.0.1', 9100))
s.sendall(frame(1, json.dumps({'type':'hello','deviceId':'demo-1'}).encode()))
for i in range(1, 4):
    s.sendall(frame(1, json.dumps({'type':'reading','seq':i,'metric':'temp',
             'value':20+i,'ts':int(time.time()*1000)}).encode()))
s.close()
EOF
# 3) 观察积压
curl -s http://127.0.0.1:8080/status
# 4) 恢复上游并确认补发、无重复
curl "http://127.0.0.1:8080/control/upstream?available=true"
curl http://127.0.0.1:9200/records
# 5) 验证重启恢复：kill -9 进程后在断连状态重发数据，再重新启动同一 data-dir 的进程
```

## 关键源码索引

- 成帧与协议：`codec/FrameReader.java`、`codec/JsonProtocolDecoder.java`、`codec/KvProtocolDecoder.java`
- 接入与会话隔离：`ingest/DeviceTcpServer.java`、`ingest/QuarantineLog.java`
- 持久化缓冲：`buffer/PersistentEventQueue.java`
- 补发与退避：`forward/EventForwarder.java`
- 模拟上游（原子提交+幂等）：`sim/SimulatedUpstream.java`
- 指标/控制：`metrics/GatewayMetrics.java`、`web/GatewayController.java`
