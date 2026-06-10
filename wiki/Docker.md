# Docker

Container images for sidecar and standalone deployments are published to [Docker Hub](https://hub.docker.com/r/sanjuthomas/log-forwarder) (`linux/amd64`, `linux/arm64`).

```bash
docker compose up --build          # local smoke test (file sink, no filter)
docker compose -f docker-compose.smoke.yaml up --build   # filter smoke (see below)
docker compose -f docker-compose.kafka.yaml up --build   # Kafka round-trip (see below)
docker pull sanjuthomas/log-forwarder:latest
```

## Filter smoke test (file sink)

Round-trip test with an **ERROR-only filter**: INFO/WARN lines are dropped, one ERROR record lands in JSONL, and `/metrics` exposes `log_forwarder_lines_filtered`.

```bash
./scripts/docker-smoke.sh
```

Uses `docker-compose.smoke.yaml` and `configs/example-docker-filter.yaml`.

## Kafka smoke test

Round-trip test: forwarder → Kafka topic → consumer verifies JSON. The smoke config applies an ERROR-only filter (WARN lines are dropped).

```bash
./scripts/kafka-smoke.sh
```

Uses `docker-compose.kafka.yaml` (Apache Kafka KRaft + topic init + forwarder). Requires Docker Compose v2 with `compose up --wait`.

GitHub Actions: **Kafka smoke** runs on every pull request to `main` (required check before merge).

See [docs/docker.md](https://github.com/sanjuthomas/log-forwarder/blob/main/docs/docker.md) for volume mounts, Kubernetes sidecar notes, and maintainer publish steps.
