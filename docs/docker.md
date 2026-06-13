# Docker

Run log-forwarder as a container — useful as a **sidecar** next to an application pod or service.

**Image:** [`sanjuthomas/log-forwarder`](https://hub.docker.com/r/sanjuthomas/log-forwarder) on Docker Hub (linux/amd64 and linux/arm64).

## Quick start (local)

```bash
docker compose up --build
```

Then:

```bash
curl -sf http://127.0.0.1:18080/health
cat docker/output/records.jsonl
```

Append a line to `docker/sample-data/app.log` and confirm a new JSONL record appears.

## Filter smoke test (file sink)

Automated file-sink round-trip with an ERROR-only filter:

```bash
./scripts/docker-smoke.sh
```

Stack (`docker-compose.smoke.yaml`):

| Service | Role |
|---------|------|
| `log-forwarder` | Tails `filter-smoke.log` → `/output/records.jsonl` (ERROR only) |

Config: `configs/example-docker-filter.yaml`. Asserts one published record and `log_forwarder_lines_filtered` on `/metrics`.

## Kafka smoke test

Automated round-trip: forwarder publishes to Kafka; a console consumer verifies JSON records.

```bash
./scripts/kafka-smoke.sh
```

Stack (`docker-compose.kafka.yaml`):

| Service | Role |
|---------|------|
| `kafka` | Single-node Apache Kafka (KRaft) |
| `kafka-init` | Creates topic `logs` |
| `log-forwarder` | Tails `docker/sample-data/kafka-smoke.log` → topic `logs` |

Config: `configs/example-docker-kafka.yaml` (PLAINTEXT broker `kafka:9092`, ERROR-only filter).

CI: **Kafka smoke** runs on every pull request to `main` and is required before merge.

## Kafka dead letter smoke test

Verifies `on_flush_failure: dead_letter` against a real Kafka sink: startup passes while Kafka is up, Kafka is stopped, a new log line fails to publish and is written to `/dlq`, and the line does not appear on the topic after Kafka restarts.

```bash
./scripts/kafka-deadletter-smoke.sh
```

Stack (`docker-compose.kafka-dlq.yaml`): same Kafka services plus a `log-forwarder` using `configs/example-docker-kafka-deadletter.yaml` and a writable `/dlq` volume. Tails `docker/sample-data/kafka-dlq-smoke.log` (empty at test start).

## Pull and run

```bash
docker pull sanjuthomas/log-forwarder:latest

docker run --rm \
  -v /path/to/app/logs:/var/log/app:ro \
  -v /path/to/config.yaml:/config/config.yaml:ro \
  -v log-forwarder-state:/state \
  -p 8080:8080 \
  sanjuthomas/log-forwarder:latest
```

Use `configs/example-docker-sidecar.yaml` as a starting point — paths are absolute for container mounts.

## Volume layout (sidecar)

| Mount | Mode | Purpose |
|-------|------|---------|
| `/var/log/app` | ro | Application log directory |
| `/config/config.yaml` | ro | Forwarder YAML config |
| `/state` | rw | Watermark file (`watch.state.path`) |
| `/output` | rw | Only if using `sink.type: file` |

Keep the watermark directory **outside** watched log paths (validated at startup).

The image entrypoint adjusts ownership on `/state` and `/output` at startup so the non-root forwarder user can write to Docker volumes and bind mounts.

## Kubernetes sidecar (sketch)

```yaml
containers:
  - name: app
  # ... your application ...

  - name: log-forwarder
    image: sanjuthomas/log-forwarder:1.0.0
    args: ["-config", "/config/config.yaml"]
    volumeMounts:
      - name: app-logs
        mountPath: /var/log/app
        readOnly: true
      - name: forwarder-config
        mountPath: /config
        readOnly: true
      - name: forwarder-state
        mountPath: /state
    ports:
      - name: metrics
        containerPort: 8080
    livenessProbe:
      httpGet:
        path: /health
        port: metrics
      initialDelaySeconds: 5
      periodSeconds: 30
    readinessProbe:
      httpGet:
        path: /ready
        port: metrics
      initialDelaySeconds: 5
      periodSeconds: 10
```

Share an `emptyDir` (or hostPath) between app and sidecar for logs. Point `sink.kafka.brokers` or `sink.http_noauth.url` at cluster-accessible destinations.

Set `metrics.host: 0.0.0.0` in config so probes and Prometheus can reach the container.

## Build the image locally

```bash
docker build -t sanjuthomas/log-forwarder:local .
```

Multi-arch build (requires buildx):

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t sanjuthomas/log-forwarder:local \
  --load .
```

## Publish to Docker Hub (maintainers)

1. Create a Docker Hub access token for the `sanjuthomas` account (or your org).
2. Add GitHub repository secrets:
   - `DOCKERHUB_USERNAME`
   - `DOCKERHUB_TOKEN`
3. Push a version tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The `Docker` workflow builds multi-arch images and pushes:

- `sanjuthomas/log-forwarder:v0.1.0`
- `sanjuthomas/log-forwarder:latest`

You can also run the workflow manually from the Actions tab (**workflow_dispatch**).

## Custom extensions

The published image is built from `cmd/log-forwarder` (built-in parsers, transformers, enrichers, sinks only). For custom `sink.Register` / `transform.Register` code, build your own image:

```dockerfile
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /out/log-forwarder ./examples/custom

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/log-forwarder /usr/local/bin/log-forwarder
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/log-forwarder"]
```

## Mac and Linux

Docker Desktop on Mac runs Linux containers. Multi-arch images (`amd64` + `arm64`) work on Intel Macs, Apple Silicon, and Linux hosts without extra configuration.
