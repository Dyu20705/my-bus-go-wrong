# PulseLog × TransitPulse
## Master Analysis & Design Document
### Building a Kafka-like Distributed Log From Scratch — từ ý tưởng đến production-inspired deployment

**Trạng thái:** Design baseline v1.0  
**Ngày:** 2026-07-26  
**Ngôn ngữ triển khai mặc định:** Java 21  
**Reference implementation:** `buildthingsuseful/build-your-own-kafka`  
**Reference workload:** TransitPulse — real-time bus telemetry and incident streaming  
**Mức trưởng thành mục tiêu:** Deployable internal/portfolio-grade distributed event broker, single-region  
**Không tuyên bố:** Apache Kafka replacement, Kafka wire compatibility, internet-scale production readiness

---

## 0. Cách sử dụng tài liệu này

Đây là **master document** dùng làm nguồn tham chiếu xuyên suốt vòng đời dự án:

```text
idea
→ research
→ scope
→ architecture
→ protocol
→ storage
→ broker
→ replication
→ client
→ workload integration
→ verification
→ deployment
→ operations
→ production-readiness review
```

Tài liệu có bốn vai trò:

1. **Design authority:** quyết định kiến trúc nào là chính thức.
2. **Scope guard:** ngăn dự án biến thành một bản clone Kafka vô hạn.
3. **Verification contract:** mọi tuyên bố về correctness, durability hoặc performance phải có test/benchmark tương ứng.
4. **Documentation index:** định nghĩa các tài liệu con cần được tách ra khi implementation đủ lớn.

Các từ khóa chuẩn:

- **MUST:** bắt buộc để đạt Definition of Done của milestone.
- **SHOULD:** nên có, chỉ bỏ khi có ADR giải thích.
- **MAY:** tùy chọn.
- **UNKNOWN — REQUIRES VALIDATION:** chưa có bằng chứng, không được coi là sự thật.
- **Tutorial parity:** hành vi bám sát repository hướng dẫn.
- **Production-inspired:** được thiết kế theo nguyên lý hệ thống thực, nhưng chưa có đủ lịch sử vận hành để gọi là production-proven.

---

# 1. Executive summary

## 1.1 Ý tưởng đã được hiệu chỉnh

Dự án không còn là:

> “Xây một ứng dụng TransitPulse dùng Apache Kafka.”

Dự án chính thức là:

> **Xây PulseLog — một distributed append-only log có topic, partition, offset, replication, producer và consumer client; sau đó dùng TransitPulse làm reference workload để chứng minh hệ thống hoạt động đúng dưới tải, restart và broker failure.**

Hai lớp được tách rõ:

```text
┌──────────────────────────────────────────────────────────┐
│ TransitPulse workload                                    │
│ GPS simulator → trip state → incidents → notifications   │
└─────────────────────────────┬────────────────────────────┘
                              │ PulseLog client API
┌─────────────────────────────▼────────────────────────────┐
│ PulseLog data platform                                   │
│ protocol · broker · log storage · replication · offsets  │
└─────────────────────────────┬────────────────────────────┘
                              │ disk / network / metadata
┌─────────────────────────────▼────────────────────────────┐
│ Runtime infrastructure                                   │
│ local processes · Docker Compose · Linux hosts           │
└──────────────────────────────────────────────────────────┘
```

## 1.2 Vì sao TransitPulse là workload phù hợp

TransitPulse tạo ra các đặc tính mà một Kafka-like system phải xử lý:

- telemetry liên tục;
- nhiều producer;
- partition key theo `vehicleId`;
- nhiều consumer độc lập;
- consumer lag;
- replay để tính lại state;
- event đến muộn;
- duplicate delivery;
- broker/service restart;
- incident notification cần idempotency.

Nó không chỉ “trang trí domain” cho broker. Nó là **acceptance workload**.

## 1.3 Kiến trúc phát triển được chốt

Repository hướng dẫn được giữ làm **Learning Track A**:

1. project structure;
2. binary protocol;
3. ZooKeeper integration;
4. segmented log storage;
5. broker;
6. low-level client;
7. producer/consumer facade;
8. manual system test.

Sau đó dự án mở rộng thành **Engineering Track B**:

9. protocol framing và partial I/O correctness;
10. crash-consistent storage;
11. replication có ISR/high watermark/leader epoch;
12. producer reliability và idempotence;
13. consumer groups và durable offsets;
14. retention, compaction và quotas;
15. security, observability và operations;
16. metadata quorum hiện đại hoặc control-plane abstraction;
17. TransitPulse certification suite.

## 1.4 Mục tiêu production thực tế

Không đặt mục tiêu “clone Kafka production-ready” vì đó là scope nhiều năm và cần đội ngũ vận hành thực tế.

Mục tiêu hợp lý:

> **PulseLog L3 — một distributed broker single-region có semantics được tài liệu hóa, chạy được 3 broker, chịu được một broker failure trong cấu hình RF=3/minISR=2, có crash recovery, committed offsets, observability, deployment runbook và automated failure tests.**

---

# 2. Nguồn gốc thiết kế và đánh giá repository hướng dẫn

## 2.1 Những gì repository cung cấp

Reference repository triển khai một SimpleKafka nhỏ bằng Java:

- protocol byte-based cho `PRODUCE`, `FETCH`, `METADATA`, `CREATE_TOPIC`;
- giao tiếp nội bộ cho replication và topic notification;
- ZooKeeper cho broker registration và controller election;
- partition storage bằng `.log` và `.index`;
- broker leader/follower;
- low-level client;
- producer/consumer demo;
- hướng dẫn chạy ba broker và fault test thủ công.

Đây là nền tảng tốt để học **hình dạng kiến trúc**.

## 2.2 Những gì repository chưa chứng minh

Repository không đủ để được gọi là fault-tolerant hoặc production-ready vì thiếu:

- request frame length ở cấp transport;
- protocol versioning;
- correlation ID thực sự được truyền;
- partial socket read/write handling;
- bounded request size;
- CRC/checksum;
- record key, timestamp và headers;
- batching/compression;
- ISR;
- high watermark;
- leader epoch/fencing;
- follower catch-up;
- replication acknowledgment policy;
- durable consumer offset;
- consumer group coordinator;
- retention/compaction;
- authentication/authorization;
- quotas;
- metrics/tracing;
- automated tests;
- deterministic failure tests;
- data corruption recovery;
- rolling upgrade compatibility.

## 2.3 Các lỗi hoặc mâu thuẫn cần sửa khi triển khai

### Protocol/network

- Broker cấp buffer cố định nhỏ và giả định một lần `read()` nhận đủ request.
- Client giả định một lần `write()` gửi hết và một lần `read()` nhận hết response.
- String length được tính theo số ký tự thay vì số byte UTF-8.
- Không có request length prefix nên không thể phân tách nhiều frame trên cùng connection.
- Không có API version hoặc numeric error model.
- Không có timeout, cancellation, backpressure hoặc connection pooling.

### Storage

- Bài viết mô tả sparse index, nhưng code ghi một index entry cho mỗi record.
- `force(true)` trên cả log và index cho từng message làm giảm throughput mạnh.
- Không có CRC để phát hiện corrupted record.
- Không có recovery protocol cho partial tail.
- Lỗi ghi index có thể bị log rồi append vẫn trả thành công.
- Cross-segment read cần được kiểm tra lại; implementation gốc không quản lý channel kế tiếp an toàn.
- Không có retention hoặc compaction.

### Replication

- Leader ACK producer sau local append; replication chạy async.
- Không chờ follower acknowledgment trước khi thành công.
- Follower nhận offset nhưng append theo local offset; không xác minh offset divergence.
- Không có replica fetch loop hoặc truncation.
- Không có ISR, high watermark hoặc committed visibility.
- Failover có thể bầu replica thiếu dữ liệu.
- Không có leader epoch nên stale leader có thể tiếp tục nhận write.

### Metadata/control plane

- ZooKeeper data model rất đơn giản.
- Controller assignment không deterministic và không rack-aware.
- Watcher semantics chưa được harden.
- Không có metadata version, controller epoch hoặc broker fencing.
- Kafka hiện đại đã chuyển sang KRaft; ZooKeeper chỉ nên được giữ như learning stage hoặc control-plane adapter.

### Client semantics

- Producer chọn partition ngẫu nhiên, không hỗ trợ key-based partitioning.
- Không batching, retry policy, delivery timeout hoặc idempotent sequence.
- Consumer chỉ lưu offset trong RAM.
- Không có group membership, rebalance, heartbeat hoặc durable commit.
- Offset callback calculation dùng `indexOf`, không an toàn khi các byte array trùng reference/giá trị.

## 2.4 Kết luận audit

Repository là **tutorial skeleton**, không phải baseline để fork rồi tuyên bố production. Chiến lược đúng:

```text
reproduce tutorial behavior
→ freeze it as a known learning baseline
→ write tests exposing its limits
→ replace subsystems incrementally
→ preserve externally documented semantics
```

---

# 3. Product definition

## 3.1 Tên thành phần

| Thành phần | Tên | Vai trò |
|---|---|---|
| Kafka-like broker | **PulseLog** | Distributed append-only event log |
| Reference workload | **TransitPulse** | Bus telemetry, state, incident, notification |
| CLI/Admin | `pulselogctl` | Topic, metadata, offsets, diagnostics |
| Java client | `pulselog-client` | Producer, consumer, admin API |
| Test harness | `pulselog-chaos` | Failure injection và semantic verification |

## 3.2 User personas

### Broker developer

Cần hiểu:

- protocol framing;
- disk layout;
- replication;
- failover;
- consumer group;
- performance/correctness trade-off.

### Application developer

Cần API ổn định:

```java
producer.send(record);
consumer.subscribe(List.of("vehicle-location.v1"));
consumer.poll(Duration.ofMillis(500));
consumer.commitSync();
```

### Operator

Cần:

- start/stop cluster;
- topic create/describe;
- broker/partition health;
- lag;
- disk usage;
- under-replicated partitions;
- controlled shutdown;
- backup/recovery;
- upgrade runbook.

### Reviewer/recruiter

Cần bằng chứng:

- architecture docs;
- ADR;
- automated tests;
- benchmark;
- failure demo;
- known limitations;
- reproducible local deployment.

---

# 4. Goals, non-goals và maturity levels

## 4.1 Goals

PulseLog MUST:

1. lưu records theo topic-partition;
2. gán offset tăng đơn điệu trong mỗi partition;
3. giữ ordering trong phạm vi partition;
4. hỗ trợ producer append và consumer pull;
5. cho phép replay từ offset;
6. chạy nhiều broker;
7. replicate partition;
8. duy trì committed visibility boundary;
9. phục hồi sau process crash;
10. duy trì durable consumer offsets;
11. cung cấp metrics và admin diagnostics;
12. được kiểm chứng bằng TransitPulse.

## 4.2 Non-goals v1

Không triển khai trong target L3:

- Kafka wire compatibility;
- Kafka Streams;
- transactions/exactly-once end-to-end;
- tiered storage;
- geo-replication;
- multi-region consensus;
- schema registry;
- connectors ecosystem;
- Kubernetes operator;
- dynamic reassignment tối ưu cấp Kafka;
- zero-downtime metadata quorum migration;
- hàng nghìn broker hoặc hàng triệu partition.

## 4.3 Maturity model

| Level | Định nghĩa | Trạng thái mục tiêu |
|---|---|---|
| L0 | In-memory/toy queue | Không dùng |
| L1 | Durable single-broker log | Milestone bắt buộc |
| L2 | 3-broker replicated lab | Milestone bắt buộc |
| L3 | Deployable internal broker, observable, secured, tested | **Final target** |
| L4 | Large-scale production platform | Ngoài scope |
| L5 | Kafka-compatible ecosystem | Ngoài scope |

---

# 5. Requirements

## 5.1 Functional requirements

### Topic và partition

- Tạo topic với `partitionCount` và `replicationFactor`.
- Topic name phải được validate.
- Partition assignment phải deterministic.
- Metadata phải trả về leader, replicas, ISR và leader epoch.
- Không tạo một topic cho mỗi vehicle.

### Produce

- Producer gửi record có key/value/headers/timestamp.
- Broker chọn partition từ explicit partition hoặc key hash.
- Leader gán offset.
- Hỗ trợ `acks=0`, `acks=1`, `acks=all`.
- `acks=all` chỉ thành công khi điều kiện `minISR` được thỏa.
- Retry phải phân biệt retriable và non-retriable errors.
- Idempotent mode chống duplicate do retry.

### Fetch

- Consumer fetch từ `(topic, partition, offset)`.
- Broker chỉ trả records đến committed high watermark cho read-committed mặc định.
- Response giới hạn theo `maxBytes`.
- Fetch dài hạn MAY hỗ trợ long polling.

### Consumer groups

- Join group.
- Heartbeat.
- Partition assignment.
- Rebalance.
- Offset commit/fetch.
- Coordinator failover.
- At-least-once là guarantee mặc định.

### Replication

- Follower pull từ leader.
- Leader theo dõi replica progress.
- ISR được cập nhật từ lag/time thresholds.
- High watermark không vượt quá replica được commit.
- Leader epoch fence stale writes.
- Một follower thiếu dữ liệu phải catch up hoặc truncate theo leader epoch.

### Storage

- Append-only segment.
- Offset index.
- Time index MAY có ở L3.
- CRC32C record/batch integrity.
- Crash recovery và partial-tail truncation.
- Segment rolling.
- Retention by time/size.
- Compaction chỉ cho internal offset topic hoặc milestone sau.

### Administration

- Create/describe/delete topic.
- Describe cluster.
- Describe partition.
- Describe consumer group và lag.
- Controlled shutdown.
- Dump segment/index diagnostics.

## 5.2 Non-functional requirements

Các số dưới đây là **acceptance targets**, không phải kết quả đã đạt.

| Thuộc tính | Target L3 |
|---|---|
| Durability | RF=3, minISR=2, `acks=all`: không mất acknowledged record khi một broker dừng đột ngột |
| Ordering | Strict per-partition offset order |
| Availability | Cluster tiếp tục produce/fetch sau một broker failure nếu quorum/ISR còn đủ |
| Leader recovery | P95 dưới 10 giây trong local 3-node test |
| Produce latency | P99 dưới 50 ms ở 1,000 msg/s, 1 KiB record, RF=3 trên môi trường benchmark chuẩn |
| Throughput | Tối thiểu 10,000 msg/s single broker với batching; UNKNOWN — REQUIRES VALIDATION |
| Recovery | Restart broker không làm thay đổi record đã committed |
| Corruption detection | Record CRC mismatch bị phát hiện và không trả cho consumer |
| Observability | Metrics, structured logs, health, lag và under-replication |
| Security | TLS/mTLS hoặc TLS + token, ACL tối thiểu, quota |
| Reproducibility | `docker compose up` + automated smoke suite |

---

# 6. System architecture

## 6.1 Logical architecture

```text
                            ┌────────────────────┐
                            │ pulselogctl/Admin  │
                            └──────────┬─────────┘
                                       │ Admin API
┌─────────────┐ Produce API   ┌────────▼────────┐
│ Producers   ├──────────────►│ Broker Listener │
└─────────────┘               └───────┬─────────┘
                                      │
                           ┌──────────▼───────────┐
                           │ Request Dispatcher   │
                           └───────┬───────┬──────┘
                                   │       │
                 ┌─────────────────┘       └─────────────────┐
                 ▼                                           ▼
        ┌────────────────┐                         ┌──────────────────┐
        │ Partition Log  │                         │ Group Coordinator │
        │ + Replica Mgr  │                         │ + Offset Store    │
        └───────┬────────┘                         └─────────┬────────┘
                │ follower fetch                            │ internal topic
                ▼                                           ▼
        ┌────────────────┐                         ┌──────────────────┐
        │ Other Brokers  │                         │ __group_offsets  │
        └────────────────┘                         └──────────────────┘

                    ┌───────────────────────────────┐
                    │ Metadata / Controller Plane   │
                    │ ZooKeeper adapter → quorum    │
                    └───────────────────────────────┘
```

## 6.2 Process roles

### Phase A — tutorial parity

Mỗi process vừa là broker vừa có thể trở thành controller; ZooKeeper quản lý broker liveness và controller election.

### Phase B — production-inspired

Tách abstraction:

```java
interface MetadataStore {
    BrokerRegistration registerBroker(...);
    ClusterMetadata snapshot();
    MetadataWatch watch(...);
    long controllerEpoch();
}
```

Implementations:

1. `ZooKeeperMetadataStore` — tương thích tutorial.
2. `InMemoryMetadataStore` — deterministic tests.
3. `RaftMetadataStore` — extension milestone.

Điều này ngăn business logic phụ thuộc trực tiếp ZooKeeper.

## 6.3 Data plane và control plane

### Data plane

- produce;
- fetch;
- replica fetch;
- record persistence;
- high watermark;
- offset commit topic.

### Control plane

- broker membership;
- topic metadata;
- replica assignment;
- leader election;
- leader epoch;
- ACL/config;
- partition reassignment.

Không được trộn state mutation của hai plane trong một `SimpleKafkaBroker.java` khổng lồ.

---

# 7. Repository architecture

```text
pulselog/
├── pom.xml
├── README.md
├── MASTER_DESIGN.md
├── docs/
│   ├── architecture/
│   ├── protocol/
│   ├── storage/
│   ├── replication/
│   ├── consumer-groups/
│   ├── operations/
│   ├── testing/
│   └── adr/
├── pulselog-common/
│   ├── errors
│   ├── identifiers
│   └── utilities
├── pulselog-protocol/
│   ├── framing
│   ├── codec
│   ├── messages
│   └── versions
├── pulselog-storage/
│   ├── record
│   ├── segment
│   ├── index
│   ├── recovery
│   └── retention
├── pulselog-metadata-api/
├── pulselog-metadata-zookeeper/
├── pulselog-metadata-memory/
├── pulselog-replication/
├── pulselog-coordinator/
├── pulselog-broker/
├── pulselog-client/
├── pulselog-admin/
├── pulselog-testkit/
├── pulselog-chaos/
├── workloads/
│   └── transitpulse/
├── deploy/
│   ├── docker/
│   └── compose/
└── benchmarks/
    └── jmh/
```

## 7.1 Boundary rules

- `protocol` không phụ thuộc broker.
- `storage` không phụ thuộc network.
- `replication` chỉ thao tác qua partition/log abstractions.
- `client` không import broker internals.
- `TransitPulse` chỉ phụ thuộc public client API.
- testkit được phép dùng internal diagnostics nhưng application code không được dùng.

---

# 8. Wire protocol v1

## 8.1 Design principles

Protocol MUST:

- framed;
- versioned;
- bounded;
- deterministic;
- UTF-8 explicit;
- partial-I/O safe;
- extensible;
- có numeric errors;
- có correlation ID;
- không dùng Java serialization.

## 8.2 Frame envelope

```text
RequestFrame:
  frameLength        int32
  apiKey             int16
  apiVersion         int16
  correlationId      int32
  clientIdLength     int16
  clientId           bytes[clientIdLength]
  payload            bytes[...]

ResponseFrame:
  frameLength        int32
  correlationId      int32
  errorCode          int16
  throttleTimeMs     int32
  payload            bytes[...]
```

`frameLength` không bao gồm chính field `frameLength`.

## 8.3 API keys v1

| API | Key | Version v1 |
|---|---:|---|
| Produce | 1 | key/value records, acks, timeout |
| Fetch | 2 | offset, maxBytes, waitMs |
| Metadata | 3 | topic filter |
| CreateTopic | 4 | partitions, RF, minISR |
| ReplicaFetch | 5 | leader epoch, follower LEO |
| OffsetCommit | 6 | group/topic/partition/offset |
| OffsetFetch | 7 | committed offsets |
| JoinGroup | 8 | member identity/subscription |
| Heartbeat | 9 | group generation/member |
| LeaveGroup | 10 | group/member |
| DescribeCluster | 11 | brokers/controller |
| DescribeTopic | 12 | leader/replicas/ISR/HW |

## 8.4 Error model

Ví dụ:

```text
0   NONE
1   UNKNOWN_TOPIC
2   UNKNOWN_PARTITION
3   NOT_LEADER
4   OFFSET_OUT_OF_RANGE
5   CORRUPT_RECORD
6   MESSAGE_TOO_LARGE
7   NOT_ENOUGH_REPLICAS
8   NOT_ENOUGH_REPLICAS_AFTER_APPEND
9   STALE_LEADER_EPOCH
10  COORDINATOR_NOT_AVAILABLE
11  REBALANCE_IN_PROGRESS
12  ILLEGAL_GENERATION
13  AUTHENTICATION_FAILED
14  AUTHORIZATION_FAILED
15  REQUEST_TIMED_OUT
16  UNSUPPORTED_VERSION
17  INVALID_REQUEST
```

Mỗi error phải có:

- retriable flag;
- public exception type;
- metric counter;
- test.

## 8.5 Decoder safety

Decoder MUST reject:

- negative lengths;
- frame vượt `max.request.bytes`;
- topic/client ID quá dài;
- unknown API version;
- truncated frame;
- malformed UTF-8 nếu strict mode;
- record count vượt giới hạn;
- integer overflow khi tính allocation.

Không allocate trực tiếp từ untrusted length trước khi validate.

## 8.6 Partial I/O

Network layer cần state machine:

```text
READ_SIZE
→ READ_BODY
→ DECODE
→ DISPATCH
→ ENCODE_RESPONSE
→ WRITE_PARTIAL
→ COMPLETE
```

Không dùng giả định:

```java
channel.read(buffer);   // không đảm bảo đọc đủ
channel.write(buffer);  // không đảm bảo ghi hết
```

---

# 9. Record và storage format

## 9.1 Record batch v1

```text
RecordBatch:
  baseOffset          int64
  batchLength         int32
  leaderEpoch         int32
  magic               int8
  crc32c              uint32
  attributes          int16
  lastOffsetDelta     int32
  baseTimestamp       int64
  maxTimestamp        int64
  producerId          int64
  producerEpoch       int16
  baseSequence        int32
  recordCount         int32
  records             Record[]
```

Không cần sao chép Kafka byte-for-byte, nhưng giữ các khái niệm cần cho:

- corruption detection;
- batching;
- compression extension;
- idempotent producer;
- leader epoch;
- ordered offsets.

## 9.2 Record v1

```text
Record:
  length              varint
  attributes          int8
  timestampDelta      varlong
  offsetDelta         varint
  keyLength           varint
  key                 bytes?
  valueLength         varint
  value               bytes?
  headerCount         varint
  headers             Header[]
```

## 9.3 Segment files

```text
00000000000000000000.log
00000000000000000000.index
00000000000000000000.timeindex
00000000000000000000.epoch
```

### `.log`

Record batches nối tiếp.

### `.index`

Sparse mapping:

```text
relativeOffset:int32 → physicalPosition:int32/64
```

Entry được thêm mỗi `index.interval.bytes`, không phải mọi record.

### `.timeindex`

```text
timestamp:int64 → relativeOffset:int32
```

### `.epoch`

Map leader epoch sang start offset để follower reconcile/truncate.

## 9.4 Append contract

Một append được xem là local-persisted khi:

1. batch codec hợp lệ;
2. CRC được tính;
3. bytes được ghi đầy đủ;
4. log end offset cập nhật;
5. flush policy được thỏa.

Index có thể rebuild từ log; log là source of truth.

## 9.5 Flush policy

Không `fsync` mọi record mặc định.

Modes:

- `SYNC_EACH_BATCH` — correctness test và low throughput;
- `PERIODIC` — production default;
- `OS_PAGE_CACHE` — benchmark only, durability yếu hơn.

Producer acknowledgment và replication không được đồng nhất với `fsync`. Tài liệu phải ghi rõ:

- acknowledged by leader memory/page cache;
- persisted by leader;
- replicated to ISR;
- committed/high-watermark visible.

## 9.6 Crash recovery

Startup sequence:

```text
discover segments
→ validate filenames/order
→ scan active segment
→ validate batch length + CRC
→ truncate partial/corrupt tail if policy allows
→ rebuild missing/corrupt index
→ recover LEO
→ load leader epoch cache
→ expose partition
```

Không được silently skip corruption ở segment giữa; đó là fatal hoặc operator action.

## 9.7 Retention

MVP L3:

- delete by time;
- delete by total partition bytes;
- chỉ delete inactive segments;
- không delete segment chứa high watermark boundary cần thiết;
- metrics cho pending deletion.

Compaction:

- bắt buộc cho internal group offset topic ở milestone consumer groups;
- application topics MAY hỗ trợ sau.

---

# 10. Broker architecture

## 10.1 Main broker components

```text
BrokerRuntime
├── NetworkServer
├── RequestDispatcher
├── MetadataCache
├── ReplicaManager
├── LogManager
├── GroupCoordinator
├── OffsetStore
├── AdminManager
├── SecurityManager
├── QuotaManager
├── MetricsRegistry
└── LifecycleManager
```

## 10.2 Threading model

Không dùng “một thread cho một connection” làm final architecture.

Đề xuất:

- 1 acceptor;
- N selector/network threads;
- bounded request queue;
- M request handler threads;
- dedicated replica fetchers;
- background log flush/retention threads;
- coordinator scheduler;
- metrics exporter.

Tất cả queue phải bounded và có rejection/backpressure policy.

## 10.3 Request lifecycle

```text
socket bytes
→ frame decoder
→ authentication
→ quota
→ request validation
→ metadata/leadership validation
→ handler
→ response
→ metrics
```

## 10.4 Backpressure

Khi overloaded:

- producer request có thể bị throttle;
- request queue đầy trả `BROKER_BUSY`/timeout;
- không spawn thread vô hạn;
- replica traffic có reserved capacity;
- admin request không được làm starvation data plane.

## 10.5 Controlled shutdown

Broker shutdown:

1. stop nhận produce mới;
2. drain request queue có deadline;
3. flush active logs;
4. nếu leader, request preferred transfer nếu có;
5. commit metadata state;
6. close storage;
7. unregister/fence broker.

---

# 11. Metadata và controller

## 11.1 Tutorial model

ZooKeeper lưu:

```text
/brokers/<brokerId>
/controller
/topics/<topic>/partitions/<partitionId>
```

Đây là Stage 3 parity.

## 11.2 Hardened metadata model

Metadata records:

```text
BrokerRegistration
BrokerFence
TopicCreate
TopicDelete
PartitionAssignment
LeaderAndIsr
ConfigUpdate
AclUpdate
ProducerIdBlock
```

Mỗi mutation có:

- monotonically increasing metadata offset;
- controller epoch;
- schema version;
- audit log.

## 11.3 Controller invariants

- Chỉ active controller được mutate metadata.
- Broker phải reject metadata command có stale controller epoch.
- Leader phải reject produce có stale leader epoch.
- Partition assignment phải deterministic.
- Không promote replica ngoài safe candidate set trừ khi operator bật data-loss mode.
- Metadata snapshot phải recoverable.

## 11.4 ZooKeeper versus Raft decision

### Chốt mặc định

- **Milestone đầu:** dùng ZooKeeper để theo đúng guide và học coordination.
- **L3 deployment:** giữ ZooKeeper adapter được harden hoặc thay bằng metadata quorum.
- **Modern extension:** xây Raft metadata subsystem như một subproject riêng.

Không nhúng “tự build Raft” vào MVP broker vì sẽ làm scope mất kiểm soát.

---

# 12. Replication design

## 12.1 Mô hình chính thức

Dùng **follower-pull replication**, không giữ leader-push của tutorial làm final.

```text
Follower:
  fetch(topic, partition, followerLEO, leaderEpoch)
Leader:
  return batches up to local LEO
Follower:
  append exact offsets
  report new LEO
Leader:
  update replica progress
  recompute ISR/HW
```

Lợi ích:

- follower tự điều tiết;
- catch-up tự nhiên;
- leader không cần quản lý outbound push per record;
- dễ batch;
- phát hiện divergence bằng epoch/offset.

## 12.2 Offsets

- **LEO — Log End Offset:** offset kế tiếp sau record cuối local.
- **HW — High Watermark:** offset nhỏ nhất đã được replicated đủ theo commit rule.
- **LSO — Log Start Offset:** record cũ nhất còn retained.
- **Committed offset:** consumer group position, khác HW.

## 12.3 ISR

Replica nằm trong ISR khi:

- kết nối hợp lệ;
- leader epoch đúng;
- lag theo time và bytes trong threshold;
- log không divergent.

ISR shrink/expand là metadata event.

## 12.4 Produce acknowledgment

### `acks=0`

Không response; không durability guarantee.

### `acks=1`

Leader append local rồi response. Có thể mất acknowledged data nếu leader chết trước replication.

### `acks=all`

Response chỉ khi:

- ISR size >= minISR;
- batch được replicated đến tất cả ISR theo policy;
- HW có thể advance qua batch.

## 12.5 Leader election

Candidate priority:

1. in-sync replica;
2. eligible safe replica nếu extension cho phép;
3. unclean replica chỉ khi explicit operator setting bật.

Default:

```text
unclean.leader.election = false
```

## 12.6 Leader epoch và fencing

Mỗi leader transition tăng `leaderEpoch`.

Produce/replica fetch phải chứa epoch. Broker reject stale epoch để tránh stale leader nhận write.

## 12.7 Divergence recovery

Follower gửi `(leaderEpoch, LEO)`.

Leader trả:

- continue fetch;
- truncate to offset;
- snapshot/rebuild required.

Không được append record với offset khác offset leader gửi.

---

# 13. Producer client

## 13.1 API

```java
ProducerRecord<K, V> {
    String topic;
    Integer partition;
    K key;
    V value;
    List<Header> headers;
    Instant timestamp;
}
```

```java
CompletionStage<RecordMetadata> send(ProducerRecord<K,V> record);
void flush();
void close(Duration timeout);
```

## 13.2 Partitioning

Thứ tự:

1. explicit partition;
2. hash(key) mod partition count;
3. sticky partition for keyless batching.

TransitPulse MUST partition `vehicle-location.v1` theo `vehicleId`.

## 13.3 Batching

Batch theo topic-partition với:

- `batch.size`;
- `linger.ms`;
- max request size;
- optional compression extension.

## 13.4 Retry

Retry chỉ cho retriable errors:

- not leader;
- timeout;
- coordinator unavailable;
- not enough replicas after append, tùy policy.

Cần:

- exponential backoff;
- jitter;
- delivery timeout;
- metadata refresh;
- bounded attempts.

## 13.5 Idempotent producer

State:

```text
producerId
producerEpoch
sequenceNumber per topic-partition
```

Broker lưu last accepted sequence theo producer. Duplicate retry trả lại metadata cũ hoặc reject out-of-order sequence.

Transactions không nằm trong L3 v1.

---

# 14. Consumer client và group coordinator

## 14.1 Pull model

Consumer poll:

```text
metadata/coordinator discovery
→ heartbeat
→ fetch assigned partitions
→ deserialize
→ application processing
→ commit
```

## 14.2 Group states

```text
EMPTY
→ PREPARING_REBALANCE
→ ASSIGNING
→ STABLE
→ DEAD
```

## 14.3 Protocol v1

- `JoinGroup`
- `Heartbeat`
- `LeaveGroup`
- `OffsetCommit`
- `OffsetFetch`

Assignment strategy v1:

- Range hoặc RoundRobin.
- Cooperative rebalance là extension.

## 14.4 Offset storage

Internal compacted topic:

```text
__group_offsets
```

Key:

```text
groupId + topic + partition
```

Value:

```text
offset + leaderEpoch + metadata + commitTimestamp
```

Coordinator cache rebuild từ internal topic khi khởi động.

## 14.5 Delivery semantics

### At-most-once

Commit trước processing. Có thể mất processing.

### At-least-once — default

Process rồi commit. Có thể duplicate khi crash giữa side effect và commit.

### Exactly-once

Ngoài scope nếu chưa có transaction coordinator.

TransitPulse services phải dùng idempotency/domain upsert.

---

# 15. TransitPulse reference workload

## 15.1 Workload architecture

```text
GPS Simulator
     │
     ▼
vehicle-location.v1
     ├────────► trip-state
     ├────────► incident-detection
     └────────► telemetry-audit
                    │
       ┌────────────┴────────────┐
       ▼                         ▼
trip-state-changed.v1      incident-detected.v1
                                  │
                                  ▼
                          notification-service
```

## 15.2 Event contract

```json
{
  "eventId": "01J...",
  "vehicleId": "BUS-017",
  "routeId": "ROUTE-01",
  "tripId": "TRIP-2026-07-26-0815",
  "latitude": 21.0285,
  "longitude": 105.8542,
  "speedKph": 14.6,
  "recordedAt": "2026-07-26T08:26:41Z",
  "schemaVersion": 1
}
```

Broker coi payload là opaque bytes. Schema validation nằm ở application layer.

## 15.3 Topic configuration

| Topic | Partitions | RF | minISR | Key | Cleanup |
|---|---:|---:|---:|---|---|
| `vehicle-location.v1` | 6 | 3 | 2 | vehicleId | delete |
| `trip-state-changed.v1` | 3 | 3 | 2 | tripId | compact,delete |
| `incident-detected.v1` | 3 | 3 | 2 | vehicleId | delete |
| `notification-dlt.v1` | 1 | 3 | 2 | notificationId | delete |
| `__group_offsets` | 6 | 3 | 2 | group/partition | compact |

Con số partition là baseline lab; benchmark mới quyết định final.

## 15.4 Application correctness

### Trip state

Upsert có guard:

```sql
UPDATE vehicle_state
SET ...
WHERE last_recorded_at < :incoming_recorded_at;
```

### Incident

`incidentId` deterministic theo rule window hoặc có dedupe table.

### Notification

Idempotency key:

```text
incidentId + channel + recipient
```

## 15.5 Workload acceptance scenarios

1. Hai vehicle produce song song.
2. Event cùng vehicle giữ offset order.
3. Duplicate producer retry không tạo duplicate batch trong idempotent mode.
4. Late GPS event không overwrite state mới.
5. Consumer restart tiếp tục từ committed offset.
6. Kill leader khi đang produce.
7. Cluster giữ acknowledged records với RF=3/minISR=2.
8. Kill consumer sau side effect trước commit; application dedupe hoạt động.
9. Tăng tải làm lag tăng nhưng broker không OOM.
10. Replay toàn bộ telemetry tạo deterministic trip state.

---

# 16. Consistency và guarantees

## 16.1 Guarantees được phép công bố ở L3

- Ordering chỉ trong một partition.
- Producer `acks=all` + RF=3 + minISR=2 chịu được một replica failure nếu safe election.
- Consumer mặc định at-least-once.
- Replay từ retained offset.
- Committed consumer reads không vượt high watermark.
- Metadata/leader changes được fence bằng epoch.

## 16.2 Không được công bố

- Global ordering.
- Exactly-once end-to-end.
- Zero data loss dưới mọi failure.
- Byzantine fault tolerance.
- Multi-region durability.
- Linearizability toàn hệ thống.
- Kafka compatibility.

## 16.3 Failure boundaries

| Failure | Expected behavior |
|---|---|
| Producer retry | idempotent mode không duplicate |
| Client disconnect giữa request | request có thể unknown outcome; correlation/retry semantics rõ |
| Broker process crash | recover log, truncate partial tail |
| Leader crash | elect ISR follower |
| Follower crash | leader tiếp tục nếu minISR còn đủ |
| Disk full | reject produce, preserve existing reads |
| Corrupt active tail | truncate đến last valid batch |
| Corrupt closed segment | fail partition/operator intervention |
| Metadata connection loss | broker fence unsafe mutations |
| Consumer crash | rebalance và resume committed offset |
| Slow consumer | lag tăng, không block producer |
| Poison payload | application DLT, broker không hiểu payload |

---

# 17. Security design

## 17.1 Threat model

Bảo vệ khỏi:

- unauthenticated client;
- unauthorized topic access;
- oversized request memory exhaustion;
- connection flood;
- path traversal qua topic name;
- malicious CRC/length;
- replayed credential;
- log injection;
- secret leakage.

## 17.2 L3 controls

- TLS cho client-broker và broker-broker.
- mTLS cho broker identity.
- Token hoặc mTLS cho clients.
- ACL theo principal/topic/operation.
- Per-client byte/request quota.
- Max connections per principal/IP.
- Strict topic naming.
- No `OPEN_ACL_UNSAFE` trong production profile.
- Secrets qua environment/file permission, không commit.
- Audit log cho admin mutation.

## 17.3 Authorization operations

```text
CLUSTER_DESCRIBE
TOPIC_CREATE
TOPIC_DELETE
TOPIC_READ
TOPIC_WRITE
GROUP_READ
GROUP_COMMIT
ADMIN
```

---

# 18. Observability

## 18.1 Metrics

### Broker

- requests/sec by API;
- request latency p50/p95/p99;
- bytes in/out;
- active connections;
- request queue depth;
- rejected/throttled requests;
- disk usage;
- log flush latency;
- segment count.

### Replication

- under-replicated partitions;
- ISR shrink/expand;
- follower lag bytes/time;
- leader count;
- high watermark gap;
- failed elections;
- stale epoch rejects.

### Consumer groups

- group count/state;
- rebalance count/duration;
- offset commit latency/errors;
- lag per group/topic/partition.

### TransitPulse

- telemetry events/sec;
- processing latency;
- incident count;
- DLT count;
- notification dedupe count.

## 18.2 Logging

Structured JSON fields:

```text
timestamp
level
service
brokerId
correlationId
clientId
principal
apiKey
topic
partition
offset
leaderEpoch
errorCode
durationMs
```

Không log full payload mặc định.

## 18.3 Tracing

Tracing là optional ở broker hot path. TransitPulse services SHOULD propagate trace/correlation IDs qua record headers.

## 18.4 Health endpoints

- liveness: process/event loop sống;
- readiness: metadata loaded, listeners active, storage usable;
- partition health: leader/ISR/disk;
- not-ready khi disk read-only hoặc metadata fenced.

---

# 19. Testing strategy

## 19.1 Test pyramid

```text
unit
→ property-based
→ component
→ integration
→ crash recovery
→ distributed failure
→ workload certification
→ benchmark
```

## 19.2 Unit tests

- codec round-trip;
- malformed frame rejection;
- CRC;
- offset/index lookup;
- segment roll;
- ISR state machine;
- sequence dedupe;
- partitioner;
- group state transitions.

## 19.3 Property-based tests

- arbitrary record encode/decode;
- fragmented frames;
- arbitrary segment boundaries;
- index rebuild;
- append/read equivalence;
- no offset regression;
- deterministic partition assignment.

## 19.4 Storage crash tests

Inject crash tại:

1. trước log write;
2. giữa batch;
3. sau log write trước index;
4. sau index trước metadata;
5. giữa segment roll;
6. khi retention rename/delete.

Postcondition:

- recovered log là prefix hợp lệ;
- no phantom record;
- LEO đúng;
- index rebuild đúng;
- committed record không biến mất theo configured guarantee.

## 19.5 Distributed failure tests

- kill leader;
- kill controller;
- network isolate follower;
- slow disk;
- disk full;
- metadata session expiry;
- delayed replica fetch;
- stale leader reconnect;
- rolling restart.

## 19.6 Semantic history checker

Mỗi produce ghi:

```text
operationId
topic
partition
producerId
sequence
returnedOffset
startTime
endTime
result
```

Checker xác minh:

- offset unique và monotonic;
- acknowledged idempotent sequence không duplicate;
- fetch order đúng;
- read không vượt HW;
- safe failover không mất acknowledged `acks=all` record.

## 19.7 TransitPulse certification

Suite sinh deterministic GPS stream với seed.

Kết quả state phải giống nhau khi:

- chạy một lần;
- chạy lại từ offset 0;
- consumer restart;
- duplicate injection;
- broker failover;
- delayed/out-of-order application event.

## 19.8 Test infrastructure

- JUnit 5;
- jqwik/QuickTheories;
- Testcontainers;
- Toxiproxy;
- Awaitility;
- JMH;
- Docker Compose;
- virtual clock cho incident windows.

Không phụ thuộc network ngoài trong CI.

---

# 20. Performance engineering

## 20.1 Benchmark profile

Cần cố định:

- CPU/RAM;
- disk type;
- OS/JDK;
- broker count;
- RF/minISR;
- record size;
- batch size;
- acks;
- producer count;
- partition count;
- retention/flush policy.

Không so benchmark giữa môi trường khác nhau.

## 20.2 Benchmark cases

1. single broker append;
2. single broker produce+fetch;
3. RF=3 `acks=1`;
4. RF=3 `acks=all`;
5. record 100 B / 1 KiB / 10 KiB;
6. 1 / 6 / 24 partitions;
7. batching on/off;
8. TLS on/off;
9. follower lag;
10. consumer replay.

## 20.3 Performance principles

- sequential append;
- batching;
- sparse index;
- bounded allocation;
- buffer reuse;
- page cache;
- zero-copy MAY được dùng cho fetch sau correctness;
- không tối ưu trước khi profiler xác định bottleneck.

## 20.4 Required outputs

- benchmark command;
- raw CSV/JSON;
- environment manifest;
- chart;
- interpretation;
- regression threshold.

---

# 21. Deployment architecture

## 21.1 Development

```text
1 broker
1 metadata service
local filesystem
no TLS
```

## 21.2 Integration

```text
3 brokers
ZooKeeper ensemble hoặc single ZK test profile
Docker Compose
RF=3
metrics stack
TransitPulse simulator
```

## 21.3 L3 deployment

```text
3 brokers on separate failure domains
3 metadata/controller nodes when quorum track exists
persistent volumes
TLS/mTLS
Prometheus/Grafana
central logs
backup of metadata and configs
systemd or containers
```

Không đặt ba broker trên cùng một disk rồi gọi là fault tolerance.

## 21.4 Configuration profiles

- `dev`
- `test`
- `benchmark`
- `production`

Production profile MUST reject insecure defaults.

## 21.5 Upgrade strategy

- protocol versions N và N-1;
- rolling broker restart;
- metadata version gate;
- storage magic version;
- downgrade limitations documented;
- compatibility tests giữa client/broker versions.

---

# 22. Step-by-step roadmap bám guide

## Stage 0 — Research and contracts

**Output**

- master design;
- glossary;
- initial ADR;
- failure model;
- baseline repo audit.

**DoD**

- không có `TBD` quan trọng;
- target L3 và non-goals được chốt;
- TransitPulse acceptance workload được mô tả.

---

## Stage 1 — Project structure

Bám guide nhưng dùng Maven multi-module và Java 21.

**Build**

- common;
- protocol;
- storage;
- broker;
- client;
- testkit.

**DoD**

- reproducible build;
- formatter/linter;
- unit test skeleton;
- CI;
- dependency locking/check.

---

## Stage 2 — Core protocol

Bắt đầu từ API types của guide, nhưng thêm frame length/version/correlation/error code.

**DoD**

- encode/decode round-trip;
- fragmented frame tests;
- malformed length tests;
- max request limit;
- UTF-8 tests;
- API version negotiation.

---

## Stage 3 — ZooKeeper integration

Bám guide để học broker registration/controller election.

**DoD**

- ephemeral broker registration;
- deterministic controller election;
- session expiry test;
- watcher re-registration;
- controller epoch;
- no open ACL in secure profile.

---

## Stage 4 — Storage layer

Append-only segments và index.

**DoD**

- append/read;
- segment roll;
- CRC;
- partial-tail recovery;
- index rebuild;
- retention baseline;
- no data corruption under crash test.

---

## Stage 5 — Single broker

Produce/fetch/metadata/create topic.

**DoD**

- 100k deterministic records;
- restart/replay;
- no offset regression;
- bounded requests;
- metrics;
- controlled shutdown.

---

## Stage 6 — Client library

Metadata discovery, producer và fetch client.

**DoD**

- leader routing;
- metadata refresh;
- timeout/retry;
- batching;
- key partitioner;
- resource-safe close.

---

## Stage 7 — High-level APIs

Producer facade và manual consumer.

**DoD**

- async send future;
- serializer/deserializer;
- seek;
- at-least-once sample;
- TransitPulse simulator produces events.

---

## Stage 8 — Tutorial parity test

Chạy ba broker giống guide, nhưng không gọi là fault-tolerant pass.

**DoD**

- scripted startup;
- create/produce/fetch;
- manual leader kill ghi lại failure;
- document gaps exposed.

---

## Stage 9 — Network correctness hardening

Thay fixed buffer/single read assumptions.

**DoD**

- selector-based framed server;
- partial write queue;
- max frame;
- slowloris timeout;
- connection quota;
- fuzz tests.

---

## Stage 10 — Crash-consistent storage

**DoD**

- crash matrix pass;
- active tail truncation;
- index rebuild;
- flush policy;
- disk full handling;
- storage compatibility version.

---

## Stage 11 — Replication semantics

Refactor push replication thành follower-pull.

**DoD**

- RF=3;
- ISR;
- HW;
- minISR;
- acks;
- leader epoch;
- safe leader election;
- follower catch-up;
- acknowledged data survival test.

---

## Stage 12 — Producer reliability

**DoD**

- delivery timeout;
- retries;
- idempotent producer;
- duplicate injection test;
- batching benchmark.

---

## Stage 13 — Consumer groups

**DoD**

- join/heartbeat/leave;
- assignment;
- rebalance;
- internal compacted offset topic;
- restart resume;
- group coordinator failover.

---

## Stage 14 — Operations and security

**DoD**

- admin CLI;
- TLS/mTLS;
- ACL;
- quotas;
- metrics dashboards;
- runbooks;
- rolling restart.

---

## Stage 15 — Modern metadata track

Subproject riêng.

**Option A:** harden ZooKeeper adapter và dừng ở L3.  
**Option B:** implement Raft-backed metadata log.

Chỉ chọn B sau khi Stages 0–14 pass.

---

## Stage 16 — TransitPulse certification

**DoD**

- ingestion;
- trip-state;
- unexpected-stop rule;
- notification DLT;
- replay;
- lag dashboard;
- broker/consumer failure tests;
- deterministic end-state evidence.

---

# 23. ADR baseline

## ADR-001 — Không tương thích Kafka wire protocol

**Decision:** PulseLog dùng protocol riêng, Kafka-inspired.

**Reason:** wire compatibility sẽ kéo theo khối lượng API/version/edge case vượt scope.

**Consequence:** không dùng Kafka clients có sẵn.

---

## ADR-002 — Java 21, plain Java core

**Decision:** broker core không dùng Spring Boot.

**Reason:** cần kiểm soát network, storage, threading và lifecycle; tránh che mất cơ chế cần học.

**Consequence:** TransitPulse services MAY dùng Spring Boot nhưng broker không dùng.

---

## ADR-003 — ZooKeeper là learning adapter

**Decision:** Stage 3 dùng ZooKeeper theo guide; control plane nằm sau abstraction.

**Reason:** bám hướng dẫn nhưng tránh khóa kiến trúc vào công nghệ đã rời khỏi Kafka hiện đại.

---

## ADR-004 — Follower-pull replication

**Decision:** final replication là follower-pull.

**Reason:** hỗ trợ batching, catch-up, lag tracking, ISR và truncation rõ hơn.

---

## ADR-005 — At-least-once mặc định

**Decision:** không tuyên bố exactly-once khi chưa có transaction coordinator.

**Reason:** correctness và scope.

---

## ADR-006 — TransitPulse là acceptance workload

**Decision:** mọi milestone lớn phải có scenario TransitPulse tương ứng.

**Reason:** tránh broker chỉ pass unit test nhưng không chứng minh usable.

---

# 24. Risk register

| Risk | Probability | Impact | Mitigation |
|---|---:|---:|---|
| Scope biến thành full Kafka clone | Cao | Rất cao | Maturity target L3, non-goals, stage gates |
| Replication có bug mất dữ liệu | Cao | Rất cao | HW/ISR model, failure history checker |
| Storage corrupt sau crash | Trung bình | Rất cao | CRC, tail recovery, crash injection |
| Network framing bug | Cao | Cao | state-machine decoder, fuzz/fragment tests |
| Consumer rebalance quá phức tạp | Cao | Cao | range assignor trước, cooperative deferred |
| Raft làm chậm toàn dự án | Cao | Rất cao | tách Stage 15 subproject |
| Benchmark thiếu trung thực | Trung bình | Cao | fixed environment manifest, raw results |
| Security bị thêm quá muộn | Trung bình | Cao | protocol identity hooks từ đầu |
| Tutorial code được copy mù quáng | Cao | Cao | baseline audit và regression tests |
| TransitPulse che lấp broker core | Trung bình | Trung bình | workload ở module riêng, opaque payload |
| Overengineering infrastructure | Trung bình | Trung bình | Docker Compose trước, không Kubernetes MVP |

---

# 25. Production-readiness checklist

## Correctness

- [ ] Frame parser xử lý fragmentation/coalescing.
- [ ] Mọi untrusted length được bound check.
- [ ] CRC corruption được phát hiện.
- [ ] Offset không regression.
- [ ] Active partial tail được recover.
- [ ] Index có thể rebuild.
- [ ] Stale leader bị fence.
- [ ] Follower divergence được truncate.
- [ ] HW không vượt safe replica boundary.
- [ ] Idempotent retry không duplicate.

## Availability/durability

- [ ] RF=3/minISR=2 failure suite pass.
- [ ] Leader failover không mất acknowledged `acks=all` records.
- [ ] Controller failover được test.
- [ ] Disk full không corrupt log.
- [ ] Controlled shutdown pass.
- [ ] Restart/rejoin/catch-up pass.

## Client semantics

- [ ] Metadata refresh.
- [ ] Retry/backoff bounded.
- [ ] Delivery timeout.
- [ ] Consumer offsets durable.
- [ ] Rebalance deterministic.
- [ ] At-least-once documented.
- [ ] No exactly-once claim.

## Operations

- [ ] Metrics dashboard.
- [ ] Alert rules.
- [ ] Structured logs.
- [ ] Runbook start/stop/recovery.
- [ ] Topic/admin CLI.
- [ ] Capacity guidance.
- [ ] Backup/restore metadata procedure.
- [ ] Rolling upgrade test.

## Security

- [ ] TLS/mTLS.
- [ ] ACL.
- [ ] Quotas.
- [ ] Secret management.
- [ ] Secure production defaults.
- [ ] Dependency/SBOM scan.
- [ ] No unsafe ZooKeeper ACL.

## Evidence

- [ ] CI green.
- [ ] Failure test artifacts.
- [ ] Benchmark raw data.
- [ ] Architecture diagrams.
- [ ] ADRs.
- [ ] Known limitations.
- [ ] Reproducible demo.
- [ ] TransitPulse certification report.

---

# 26. Definition of Done toàn dự án

Dự án đạt target L3 khi:

```text
1. Ba broker khởi động từ cấu hình reproducible.
2. Topic RF=3/minISR=2 hoạt động.
3. Producer key theo vehicleId giữ ordering trong partition.
4. acks=all không trả success khi ISR dưới minISR.
5. Kill leader không làm mất acknowledged records.
6. Follower restart catch up và trở lại ISR.
7. Consumer group rebalance và resume committed offset.
8. Crash giữa log/index không làm corrupt partition.
9. Corrupted batch bị phát hiện bằng CRC.
10. Request phân mảnh qua nhiều TCP reads vẫn decode đúng.
11. Oversized/malformed request bị reject không OOM.
12. Metrics hiển thị request latency, lag, ISR và disk.
13. TLS + ACL + quota hoạt động trong production profile.
14. TransitPulse replay tạo deterministic state.
15. Notification duplicate được application dedupe.
16. Tất cả guarantee và limitation có bằng chứng.
```

Nếu thiếu một trong các mục 1–12, không được gọi L3.

---

# 27. Issue decomposition đề xuất

## Epic A — Foundation

- A1: Multi-module Maven structure
- A2: Java 21 toolchain and CI
- A3: Error model and identifiers
- A4: Testkit and deterministic clock

## Epic B — Protocol

- B1: Frame envelope
- B2: Produce/fetch/metadata codecs
- B3: Decoder bounds
- B4: Partial I/O server
- B5: Protocol fuzz tests

## Epic C — Storage

- C1: Record batch codec + CRC
- C2: Segment append
- C3: Sparse offset index
- C4: Recovery/truncation
- C5: Retention
- C6: Disk-full handling

## Epic D — Metadata

- D1: MetadataStore API
- D2: In-memory implementation
- D3: ZooKeeper implementation
- D4: Controller epoch
- D5: Topic assignment

## Epic E — Broker

- E1: Lifecycle
- E2: Request dispatcher
- E3: Produce
- E4: Fetch
- E5: Admin/metadata
- E6: Metrics

## Epic F — Replication

- F1: Replica fetch protocol
- F2: Follower append exact offset
- F3: ISR manager
- F4: High watermark
- F5: Leader election/fencing
- F6: Catch-up/truncation
- F7: Failure suite

## Epic G — Clients

- G1: Metadata-aware client
- G2: Producer batching
- G3: Retry/timeout
- G4: Idempotent producer
- G5: Consumer poll/seek

## Epic H — Consumer groups

- H1: Coordinator discovery
- H2: Join/heartbeat
- H3: Assignment
- H4: Offset internal topic
- H5: Commit/fetch
- H6: Rebalance tests

## Epic I — Operations

- I1: Admin CLI
- I2: TLS/mTLS
- I3: ACL
- I4: Quotas
- I5: Dashboards/alerts
- I6: Runbooks
- I7: Rolling upgrade

## Epic J — TransitPulse

- J1: GPS simulator
- J2: Ingestion producer
- J3: Trip-state consumer
- J4: Incident detector
- J5: Notification + DLT
- J6: Replay certification
- J7: Chaos demo

---

# 28. Recommended execution order

```text
A → B → C → D → E
              ↓
              G basic
              ↓
              tutorial parity checkpoint
              ↓
F replication
→ G reliability
→ H consumer groups
→ I operations/security
→ J TransitPulse certification
→ optional metadata quorum
```

Không triển khai TransitPulse đầy đủ trước khi single-broker storage/protocol pass.  
Không triển khai Raft trước khi replicated data plane pass.  
Không tối ưu zero-copy trước khi crash correctness pass.

---

# 29. First implementation slice

Vertical slice đầu tiên:

```text
TransitPulse GPS simulator
→ PulseLog producer
→ framed Produce request
→ single broker
→ append CRC record batch
→ Fetch request from offset 0
→ TransitPulse audit consumer
→ process restart
→ replay identical records
```

## Acceptance criteria

- 10,000 seeded GPS records;
- 1 KiB max average payload;
- offsets 0..9999 liên tục;
- restart broker;
- fetch lại 10,000 records;
- CRC valid;
- payload hash giống input;
- no duplicate/no missing;
- automated integration test;
- metrics cho produce/fetch/storage.

Slice này chưa có:

- ZooKeeper;
- replication;
- consumer groups;
- incident service;
- security.

Đây là điểm bắt đầu tối ưu vì kiểm chứng protocol + storage + client + workload trong một boundary nhỏ.

---

# 30. Documentation set cần duy trì

`MASTER_DESIGN.md` là authority. Khi code phát triển, tách:

```text
docs/
├── architecture/system-context.md
├── architecture/component-model.md
├── protocol/wire-protocol-v1.md
├── protocol/error-codes.md
├── storage/record-format-v1.md
├── storage/crash-recovery.md
├── replication/isr-high-watermark.md
├── replication/leader-election.md
├── consumer-groups/group-protocol-v1.md
├── operations/deployment.md
├── operations/runbook.md
├── operations/security.md
├── testing/failure-matrix.md
├── testing/benchmark-methodology.md
└── adr/ADR-*.md
```

Mỗi PR thay đổi semantics MUST cập nhật tài liệu tương ứng.

---

# 31. References

## Primary project references

1. Build Things Useful, **Building Your Own Kafka-like System From Scratch: A Step-by-Step Guide**  
   https://github.com/buildthingsuseful/build-your-own-kafka

2. Medium series, **Stage 1–8: Build Your Own Kafka**  
   https://buildthingsuseful.medium.com/building-your-own-kafka-like-system-from-scratch-a-step-by-step-guide-d3c5f0a303c0

## Apache Kafka references used for production-inspired design

3. Apache Kafka Protocol  
   https://kafka.apache.org/43/design/protocol/

4. Apache Kafka Message Format  
   https://kafka.apache.org/43/implementation/message-format/

5. Apache Kafka Distribution and Consumer Offset Tracking  
   https://kafka.apache.org/43/implementation/distribution/

6. Apache Kafka Topic Configuration  
   https://kafka.apache.org/43/configuration/topic-configs/

7. Apache Kafka Consumer Rebalance Protocol  
   https://kafka.apache.org/43/operations/consumer-rebalance-protocol/

8. Apache Kafka Security Overview  
   https://kafka.apache.org/43/security/security-overview/

9. Apache Kafka 4.0 release — KRaft-only architecture  
   https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/

---

# 32. Final design decision

**Chốt hướng triển khai:**

> Bám đúng sequence của `build-your-own-kafka` để học từng subsystem, nhưng không fork mù quáng. Tutorial implementation được xem là Stage 1–8 baseline. PulseLog phải thêm protocol framing, crash-consistent storage, follower-pull replication, ISR/high watermark, leader epoch, idempotent producer, consumer groups, observability và security trước khi đạt target L3. TransitPulse là workload chứng minh semantics và fault tolerance, không phải lõi broker.

**Bước tiếp theo chính thức:**

```text
Implement Stage 0–1
→ tạo repository structure
→ protocol/storage contracts
→ testkit
→ vertical slice single-broker 10,000 TransitPulse records
```
