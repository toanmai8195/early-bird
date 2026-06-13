COMPOSE := docker compose -f com/tm/infra/docker-compose.yaml

.PHONY: infra-up infra-down infra-logs server manager loadtest-redis loadtest-pg down

# Infra: Redis (claim gate), Kafka (claim events), Postgres (source of truth),
# Prometheus (scrapes server/manager :9404/metrics), Grafana (:3000, dashboards
# provisioned from com/tm/infra/grafana/dashboards).
infra-up:
	$(COMPOSE) up -d redis kafka postgres prometheus grafana

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
