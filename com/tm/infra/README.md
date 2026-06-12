# infra

Infrastructure for the booking system, kept separate from service code.

- `docker-compose.yaml` — local Redis / Kafka / Postgres.
- `k8s/` — (placeholder) manifests / Helm for deploying server, manager.
- `migrations/` — (placeholder) Postgres schema (opportunities, bookings, failed_bookings).

## Run locally
```bash
docker compose -f com/tm/infra/docker-compose.yaml up -d
```

## Build & load service images (Bazel + rules_oci)
```bash
bazel run //com/tm/services/server/cmd:server_load
bazel run //com/tm/services/manager/cmd:manager_load
```
