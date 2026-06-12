# Booking System — Delivery Opportunity Claiming

## Bài toán
1 booking window mở → rất nhiều driver claim opportunity có `capacity` giới hạn, gần như
đồng thời. Đóng khung: **high contention trên một partition đơn** (`opportunity_id`).
Quy mô giả định: capacity ~1K, ~10K driver/opp; nhiều opp chạy song song.

Yêu cầu: **(1) No overselling** (booking ≤ capacity), **(2) Low latency** (fast-reject khi
đầy), **(3) Fault tolerance** (idempotent, at-least-once). `opportunity_id` là partition
key xuyên suốt Redis → (Kafka) → PG.

## Kiến trúc & luồng
Tech: Java+Vert.x (HTTP), Redis+Lua (atomic gate), Kafka (offload), PostgreSQL (source of
truth), Resilience4j (degrade), MQTT/WS+push (confirm về app).

Luồng 1 booking attempt:
1. Vert.x nhận `POST /opportunities/{id}/bookings` (+ `idempotency_key`, `driver_id`).
2. Redis Lua atomic trên key `claimed_set:{opp}`: `SISMEMBER`→`DUP`; `SCARD>=capacity`→
   `FULL`; else `SADD`→`OK`. SET vừa đếm vừa dedupe → ~90% reject tại đây, không chạm DB.
3. `OK` → ghi Redis **pending set** (timestamp+retry) = outbox.
4. Publish Kafka → response **ACCEPTED** (không phải SUCCESS đồng bộ). Publish fail →
   reconciliation replay.
5. Consumer ghi PG: `UNIQUE(opp,driver)` chống double-booking; atomic decrement
   `remaining-1 WHERE remaining>0` = backstop. Xong → xoá pending + confirm về app.
6. Reconciliation job quét pending quá `T`s: còn đúng→replay Kafka; hỏng→`failed_bookings`
   + alert. Idempotent nhờ UNIQUE.

## Nguyên tắc thiết kế
- **Correctness 2 tầng**: Redis = chống oversell *nhanh* (atomic Lua); PG = chống oversell
  *đúng* (atomic decrement + UNIQUE). Không tin tuyệt đối vào Redis.
- **Redis chết**: counter chết → Resilience4j hạ rate limit, vẫn gửi Kafka, **PG là cái
  giữ correctness** (rate limiter KHÔNG giữ); mất fairness, giữ đúng. claimed_set chết →
  bỏ dedupe gate, dựa PG UNIQUE.
- **Kafka partition theo `opportunity_id`** (key = opp): mọi claim cùng opp về 1
  partition/consumer → manager **group theo opp + bulk-settle 1 statement/opp/poll**.
  Đánh đổi: opp siêu hot = hot partition (chấp nhận để bulk-load); correctness vẫn do
  Redis gate + PG quyết, không cần ordering.
- **Scaling**: các opp độc lập, không global lock (Redis 1 key/opp, PG row/opp). Lo còn
  lại: Redis hot-slot (1 key/node) — Lua single-key ~100K ops/s vẫn thừa; nếu cần shard 1
  opp thành N sub-counter. Hot partition Kafka → tăng số partition.

## Ngôn ngữ & cấu trúc (Bazel 8 + Bzlmod + rules_oci)
- **client = Go**; **server / manager / common = Java** (Vert.x, vertx-redis-client,
  vertx-kafka-client, kafka-clients, JDBC, Micrometer, Dagger).
- **server fully non-blocking** trên Vert.x event loop: gate + producer trả `Future`,
  KHÔNG `executeBlocking`. **manager sync có chủ đích** (Kafka poll + JDBC là
  throughput-bound; tối ưu bằng **batch settle 1 tx/poll** + nhiều partition, không phải
  event loop). Settle giữ `remaining` chính xác: per-claim CTE (decrement có khoá +
  dedup rồi mới insert), group/sort theo opportunity_id tránh deadlock.
```
MODULE.bazel/.bazelversion(8.1.1)/.bazelrc   # rules_go, gazelle, rules_java, rules_jvm_external, rules_oci, rules_pkg
go.mod                                       # module github.com/tm (client Go; gazelle prefix ở com/tm)
bazel/oci.bzl                                # go_service_image + java_service_image
tools/dagger/                                # java_plugin(dagger-compiler) + export dagger+javax.inject
com/tm/
  infra/                  # docker-compose (redis/kafka/pg), grafana/dashboards/<svc>.json
  services/
    client/cmd  (Go)      # load-gen
    server/     (Java)    # Vert.x gate → Kafka → 202 ACCEPTED
    manager/    (Java)    # Kafka consumer → PG; Main+ConsumerRunner, di/, config/, dao/, handler/
  common/                 # TÁCH java/ và go/; mỗi class có interface + impl (vd ClaimStore/PgClaimStore)
    java/                 #   config, redis, kafka, pg, metric, exception (custom exceptions chung)
    go/result/            #   Outcome + Tally
  loadtest/               # redis-counter/, pg/
```
- Java package = path (`com.tm.services.server`, `com.tm.common.redis`). Go import =
  `github.com/tm/...`. Bazel không bắt path khớp Java package.
- Mỗi target có **test ở subfolder `test/`** (BUILD riêng).
- DI: constructor `@Inject` cho class mình sở hữu; `ManagerModule` (@Provides) chỉ external;
  `ManagerBindings` (@Binds) bind interface→impl.

## Build / test
```bash
bazel build //com/tm/services/{client/cmd:client,server:server,manager:manager}   # code
bazel build //com/tm/services/server:server_image                                 # OCI image (native arch)
bazel run   //com/tm/services/manager:manager_load                                # + load docker
bazel test  --build_tests_only //com/tm/common/... //com/tm/services/...           # tests
```
Lưu ý: Go SDK pin **1.23.4** (rules_go 0.55.1 inject GOEXPERIMENT `coverageredesign`, Go
1.25 reject); `.bazelrc` set **Java 21** (record); base image pull **multi-arch**
(amd64+arm64/v8) nên image build native trên mac arm64 (không cần `--config`); muốn ép
amd64 cho prod thì thêm `--config=linux-amd64`; digest base trong MODULE.bazel đã là digest thật.

## Rules (path-scoped, lazy-load trong `.claude/rules/`)
Chỉ nạp khi đụng file khớp path — index để biết trước:
- **unit test** — mọi target `common/**` + mọi `services/**/{dao,handler}` có test (subfolder `test/`).
- **observability** — API/consumer/producer/dao có metric counter (+ latency P99 cho API & dao).
- **grafana** — mỗi metric có panel; mỗi service / common-stack có metric = 1 dashboard.
- **interface** — mọi dao/handler/common (có hành vi) expose qua interface (`@Binds`); DTO miễn.

## Deliverable
Design doc (~2-5 trang, Markdown + Mermaid): architecture diagram, request flow, correctness
under concurrency, failure/retry, tradeoffs (sync vs async ACCEPTED, Redis gate vs Kafka
serialize, partition strategy, Redis degrade).
