COMPOSE := docker compose -f com/tm/infra/docker-compose.yaml

# ── Server loadtest flags (override on CLI: make loadtest-server-gate GATE_RPS=5000) ─────
DISABLE_CB      ?= true   # disable Redis CB during loadtest to isolate gate correctness
GATE_RPS        ?= 2000
GATE_REQUESTS   ?= 20000
GATE_ROUNDS     ?= 3
IDEM_BATCH      ?= 200
IDEM_ROUNDS     ?= 5
CAPACITY        ?= 1000
RAMP_STEP       ?= 2m
RAMP_COOLDOWN   ?= 15s

.PHONY: infra-up infra-down infra-logs server manager \
        loadtest-redis loadtest-pg \
        loadtest-server-build loadtest-server-gate loadtest-server-ramp \
        loadtest-server-idempotent loadtest-server-throughput loadtest-server \
        down

# Infra: Redis (claim gate), Kafka (claim events), Postgres (source of truth),
# Prometheus (scrapes server/manager :9404/metrics), Grafana (:3000, dashboards
# provisioned from com/tm/infra/grafana/dashboards).
infra-up:
	$(COMPOSE) up -d redis kafka postgres prometheus grafana
	@curl -sf -X POST http://localhost:9090/-/reload > /dev/null 2>&1 && echo "Prometheus config reloaded." || true

infra-down:
	$(COMPOSE) down

infra-logs:
	$(COMPOSE) logs -f redis kafka postgres prometheus grafana

# Build the server image with Bazel, load it into the local docker daemon, run it.
server:
	bazel run //com/tm/services/server:server_load
	$(COMPOSE) --profile app up -d server

# Build the manager image with Bazel, load it into the local docker daemon, run it.
manager:
	bazel run //com/tm/services/manager:manager_load
	$(COMPOSE) --profile app up -d manager

# Build the redis-counter load-test image, load into Docker, run it once.
# Results and /metrics are printed to stdout; exit code reflects pass/fail.
loadtest-redis:
	bazel run //com/tm/loadtest/redis-counter:redis_counter_load
	$(COMPOSE) --profile loadtest up --no-deps --exit-code-from loadtest-redis-counter loadtest-redis-counter

# Build the pg load-test image, load into Docker, run it once.
loadtest-pg:
	bazel run //com/tm/loadtest/pg:pg_load
	$(COMPOSE) --profile loadtest up --no-deps --exit-code-from loadtest-pg loadtest-pg

down:
	$(COMPOSE) --profile app --profile loadtest down

# ── Server loadtest (full-flow: HTTP → Redis → Kafka → PG) ───────────────────
#
# Prerequisites: infra-up + server + manager must be running.
# Each target builds the loadtest-server image and runs the chosen scenario.
# Exit code is non-zero if any correctness assertion fails (gate / idempotent).
#
# Scenarios:
#   gate        — 3 phases: seed (capacity unique drivers → 202 ACCEPTED),
#                        dup  (replay seed drivers     → 200 DUP),
#                        rest (fill remaining slots + overflow → 409 FULL).
#                 Asserts: accepted==capacity (no oversell), dup==dupN,
#                 full==restN-(cap-seedN). Then calls GET /opportunities/:id
#                 to confirm remaining is consistent with the PG booking count.
#   idempotent  — Round 0: batch B unique drivers → all 202 ACCEPTED.
#                 Round 1..N: replay the same batch → all 200 DUP.
#                 Asserts the Redis gate dedup works end-to-end across the full stack.
#   ramp        — Ramps RPS up (500→50K) across 2 patterns:
#                   contended: a single opp (hot Redis key + hot Kafka partition)
#                   diverse:   N opps round-robin (throughput baseline)
#                 Stops when error%>5% or p99>200ms, reports the ceiling RPS.
#   throughput  — Sustained contended + diverse run indefinitely; metrics on /metrics.
#   all         — gate → idempotent (exit 1 on violation) → throughput.

_LT_FLAGS = \
  --server=http://server:8080 \
  --metrics-addr=:9412 \
  --capacity=$(CAPACITY) \
  --gate-rps=$(GATE_RPS) \
  --gate-requests=$(GATE_REQUESTS) \
  --gate-rounds=$(GATE_ROUNDS) \
  --idempotent-batch=$(IDEM_BATCH) \
  --idempotent-rounds=$(IDEM_ROUNDS) \
  --ramp-step=$(RAMP_STEP) \
  --ramp-cooldown=$(RAMP_COOLDOWN) \
  $(if $(filter true,$(DISABLE_CB)),--disable-cb)

loadtest-server-build:
	bazel run //com/tm/loadtest/server:server_lt_load

## Correctness: oversell gate + capacity enforcement (3 rounds × fresh opp).
loadtest-server-gate: loadtest-server-build
	$(COMPOSE) --profile app --profile loadtest run --rm loadtest-server \
	  $(_LT_FLAGS) --run=gate

## Idempotency: replay same batch N times, assert all DUP.
loadtest-server-idempotent: loadtest-server-build
	$(COMPOSE) --profile app --profile loadtest run --rm loadtest-server \
	  $(_LT_FLAGS) --run=idempotent

## Throughput ceiling: ramp RPS until error%>5% or p99>200ms.
loadtest-server-ramp: loadtest-server-build
	$(COMPOSE) --profile app --profile loadtest run --rm loadtest-server \
	  $(_LT_FLAGS) --run=ramp

## Sustained load (runs until Ctrl-C); metrics live at http://localhost:9412/metrics.
loadtest-server-throughput: loadtest-server-build
	$(COMPOSE) --profile app --profile loadtest run --rm -p 9412:9412 loadtest-server \
	  $(_LT_FLAGS) --run=throughput

## All-in-one: gate → idempotent → ramp → exit (exit 1 on violation).
## Brings up infra + app automatically if not running.
## DISABLE_CB=true (default) disables the Redis circuit breaker so correctness
## checks test the gate logic directly, not CB degradation behavior.
loadtest-server: infra-up server manager loadtest-server-build
	DISABLE_CIRCUIT_BREAKER=$(DISABLE_CB) $(COMPOSE) --profile app up -d --force-recreate server
	@echo "Waiting for server /health..."; \
	for i in $$(seq 1 30); do \
	  curl -sf http://localhost:8080/health > /dev/null 2>&1 && echo "Server ready." && break; \
	  [ $$i -eq 30 ] && echo "Server did not start in time" && exit 1; \
	  echo "  $$i/30..."; sleep 2; \
	done
	DISABLE_CIRCUIT_BREAKER=$(DISABLE_CB) $(COMPOSE) --profile app --profile loadtest run --rm loadtest-server \
	  $(_LT_FLAGS) --run=full
