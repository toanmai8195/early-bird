# Booking System Design — Delivery Opportunity Claiming

## Bài toán cốt lõi
Khi 1 booking window mở, rất nhiều driver cùng claim một opportunity có `capacity` giới
hạn, gần như đồng thời. Đóng khung bài toán: **high contention trên một partition đơn**
(`opportunity_id`). Toàn bộ thiết kế là cách *thu nhỏ* contention đó.

Quy mô giả định: mỗi opportunity `capacity` ~1K, ~10K driver claim cùng 1 opportunity
trong cùng thời điểm. Nhiều opportunity chạy song song (chiều dễ — xem mục Scaling).

Yêu cầu bắt buộc:
1. **No overselling** — tổng booking thành công cho 1 `opportunity_id` ≤ `capacity`.
2. **Low latency under heavy concurrency** — fast-reject phần lớn request khi đã đầy.
3. **Fault tolerance** — đúng khi component fail, request retry, message delivered
   nhiều lần (idempotency, at-least-once).

`opportunity_id` là **partition key xuyên suốt** Redis → (Kafka) → PG.

## Tech stack
- **Java + Vert.x**: HTTP service (non-blocking, chịu concurrency cao).
- **Redis + Lua**: atomic gate chống oversell + dedupe, fast-reject 90% request.
- **Kafka**: offload ghi PG bất đồng bộ.
- **PostgreSQL**: source of truth + backstop chống oversell.
- **Resilience4j**: rate limit / circuit breaker khi Redis degrade.
- **MQTT / WebSocket + Push noti**: confirm kết quả về app (online/offline).

## Luồng request (booking attempt)
1. **Vert.x** nhận `POST /opportunities/{id}/bookings` (kèm `idempotency_key`,
   `driver_id`). Đã auth trước.
2. **Redis Lua (atomic)** trên 1 key `claimed_set:{opp_id}`:
   - `SISMEMBER` → đã claim ⇒ trả `DUP` (idempotent, không tốn slot).
   - `SCARD >= capacity` ⇒ trả `FULL` (fast-reject).
   - else `SADD driver_id` ⇒ `OK`. Dùng SET để vừa đếm vừa dedupe trong 1 op.
   → ~90% request bị loại ngay tại đây, không chạm DB.
3. Request `OK`: ghi vào **Redis pending set** (kèm timestamp + retry count) →
   pending set đóng vai trò **outbox**.
4. Publish **Kafka** rồi response **ACCEPTED** cho user (không phải SUCCESS đồng bộ).
   Nếu Kafka publish fail → reconciliation job replay từ pending set.
5. **Consumer** ghi **PG**:
   - `UNIQUE(opportunity_id, driver_id)` chống double-booking khi Kafka deliver lại.
   - Atomic decrement `UPDATE ... SET remaining = remaining-1 WHERE id=? AND
     remaining > 0` — backstop cuối cùng chống oversell.
   - Ghi xong ⇒ remove khỏi Redis pending set, publish confirm cho app
     (MQTT/WebSocket nếu online, push noti nếu offline).
6. **Reconciliation job** định kỳ quét pending set: entry quá `T` giây chưa được xoá
   ⇒ còn slot/đúng thì replay Kafka; hỏng thật thì ghi bảng `failed_bookings` + alert.
   Job phải idempotent (dựa UNIQUE constraint ở mục 5).

## Correctness under concurrency
- **Redis** = lớp chống oversell *nhanh* (atomic single-key Lua).
- **PG** = lớp chống oversell *đúng* (atomic decrement + UNIQUE). Không tin tuyệt đối
  vào Redis.
- **Idempotency** xử lý ở 2 tầng: Redis `claimed_set` (gate) + PG UNIQUE (backstop).

## Failure handling
- **Kafka/step sau fail**: pending set (outbox) + reconciliation job hậu kiểm.
- **Redis counter chết**:
  - Resilience4j hạ rate limit theo số pod để PG không sập, vẫn gửi Kafka.
  - ⚠️ Rate limiter KHÔNG giữ correctness — lúc này **PG atomic decrement mới là cái
    chống oversell**. Tradeoff: mất fairness (driver bấm sau có thể claim trước),
    correctness vẫn giữ.
- **Redis record (claimed_set) chết**: bỏ dedupe ở gate, dựa hoàn toàn vào PG UNIQUE.
  Response bình thường, hậu kiểm sau.

## Kafka partitioning (đã cân nhắc)
- Correctness đã chốt ở Redis ⇒ **không cần ordering** ⇒ **không partition theo
  opportunity_id**.
- Chọn **partition trải đều (round-robin / driver_id)** → tối đa throughput consumer,
  tránh hot partition (opp siêu hot dồn 1 partition = mất song song).
- Message tự chứa đủ `{opportunity_id, driver_id, idempotency_key}` nên consumer nào
  xử lý cũng được.
- *Alternative đã loại*: partition theo opportunity_id để dùng Kafka serialize capacity
  thay Redis — thừa vì đã có Redis gate, lại gây hot partition.

## Scaling (nhiều opportunity_id — chiều dễ)
Các opportunity độc lập → scale ngang tự nhiên, không global lock:
| Tầng | Partition | Contention |
|------|-----------|------------|
| Redis | 1 key/opp | nội bộ 1 opp |
| Kafka | round-robin | — |
| PG | row/opp | row cùng opp |

Cảnh báo còn lại:
- **Redis hot-slot**: 1 key không split across node. Lua single-key ~100K+ ops/s ⇒
  10K vẫn thừa. Nếu cần: shard 1 opp thành N sub-counter (capacity chia N) — alternative.

## Ngôn ngữ
- **client = Go** (load-gen).
- **server / manager / common = Java** (Vert.x, Kafka client, Lettuce/Redis, JDBC/PG,
  Micrometer).

## Cấu trúc repo (Bazel 8 + Bzlmod + rules_oci)
```
MODULE.bazel / .bazelversion(8.1.1) / .bazelrc   # rules_go, gazelle, rules_java, rules_jvm_external, rules_oci, rules_pkg
go.mod                                           # module github.com/tm (chỉ cho client Go; gazelle prefix ở com/tm)
bazel/oci.bzl                                    # go_service_image (distroless/static) + java_service_image (distroless/java)
com/tm/                                           # source root
  infra/                  # docker-compose (redis/kafka/pg) + k8s/migrations (placeholder), TÁCH RIÊNG
  services/
    client/cmd  (Go)      # load-gen client bắn concurrent claim
    server/     (Java)    # Vert.x: nhận request, Redis Lua gate reject sớm → Kafka → 202 ACCEPTED
    manager/    (Java)    # nghe Kafka, commit claim vào PG (source of truth + backstop), notify app
  common/                 # thư viện chung, TÁCH JAVA / GO
    java/                 #   redis (ClaimGate+Lua), kafka, pg (ClaimStore), metric
    go/result/            #   claim Outcome + Tally (client dùng)
  loadtest/               # kết quả loadtest: redis-counter/, pg/
```
Java package = `com.tm.services.server`, `com.tm.common.redis` (Bazel không bắt path khớp
package nên file để dưới `common/java/redis` vẫn giữ package `com.tm.common.redis`).
Go import = `github.com/tm/common/go/result`, `github.com/tm/services/client/cmd`.

### Build commands
```bash
bazel build //com/tm/services/client/cmd:client \
            //com/tm/services/server:server //com/tm/services/manager:manager   # code
bazel build --config=linux-amd64 //com/tm/services/server:server_image          # OCI image
bazel run   --config=linux-amd64 //com/tm/services/server:server_load           # + load vào docker
bazel run   //com/tm/services/client/cmd:client -- --drivers=10000
```
Lưu ý cấu hình đã sửa khi dựng (tất cả build pass):
- Go SDK pin **1.23.4** (rules_go 0.55.1 inject GOEXPERIMENT `coverageredesign`, Go 1.25 reject).
- `.bazelrc` set **Java 21** (`--java_language_version=21`) để dùng record.
- Base image multi-platform → build image PHẢI kèm `--config=linux-amd64` (pin
  `//command_line_option:platforms` về linux/amd64).
- Digest base image (`distroless_static`, `distroless_java`) trong MODULE.bazel đã là
  digest thật.
- Code hiện là skeleton (TODO wiring config/env) — compile + deploy jar + OCI image OK.

## Quy tắc theo path
Rule scoped bằng `.claude/rules/` (load khi Claude đụng file khớp path):
- `com/tm/common/**` → mọi target phải có unit test. Xem
  [.claude/rules/common-unit-tests.md](.claude/rules/common-unit-tests.md).
- `com/tm/services/**`, `com/tm/common/java/{kafka,pg}/**` → API / consumer / producer /
  DAO phải có metric counter (+ latency P99 cho API & DAO). Xem
  [.claude/rules/observability-metrics.md](.claude/rules/observability-metrics.md).

## Output cần tạo
Design document (~2-5 trang, Markdown + Mermaid), gồm:
1. High-level architecture diagram.
2. Request flow cho 1 booking attempt (happy path + reject khi full).
3. Correctness under concurrency.
4. Failure & retry handling.
5. Tradeoffs & alternatives (sync SUCCESS vs async ACCEPTED, Redis gate vs Kafka
   serialize, Kafka partition strategy, Redis degrade modes).
