# log-forwarder

A lightweight Go service that tails log files, transforms lines into structured JSON, enriches records with metadata, and publishes them to Kafka.

```
log files  →  watcher  →  transform  →  enrich  →  Kafka
```

## Requirements

- **Go 1.22+**
- **Kafka** cluster reachable from the host running the forwarder
- Read access to the log directories configured under `watch`

## Build

Clone the repository and build from the project root:

```bash
go mod download
go build -o bin/log-forwarder ./cmd/log-forwarder
```

Cross-compile for Linux (typical server target):

```bash
GOOS=linux GOARCH=amd64 go build -o bin/log-forwarder-linux ./cmd/log-forwarder
```

Build a binary with custom transformers and enrichers (see [Custom extensions](#custom-extensions)):

```bash
go build -o bin/log-forwarder-custom ./examples/custom
```

## Run

### With defaults

If you omit `-config`, the forwarder uses built-in defaults:

- Watches the **current working directory** for `*.log*` files
- Publishes to Kafka at `localhost:9092`, topic `logs`
- Uses the `delimiter` transformer (tab-separated) with `on_error: wrap`
- Adds the host's hostname via the `host` enricher

```bash
./bin/log-forwarder
```

### With a config file

```bash
./bin/log-forwarder -config configs/example.yaml
```

Start the process from a directory where relative paths in the config resolve correctly, or use absolute paths in the YAML file.

The forwarder logs to stderr and exits cleanly on `SIGINT` / `SIGTERM`.

## Configuration

Configuration is YAML. See [`configs/example.yaml`](configs/example.yaml) for a full example.

### `watch`

Controls which log files are tailed.

| Field | Description |
|-------|-------------|
| `poll` | How often to rescan directories (e.g. `1s`, `500ms`) |
| `sources` | Per-directory watch entries, each with its own `patterns` |
| `paths` | Directories to watch when all use the same `patterns` |
| `patterns` | Glob patterns applied to every path in `paths` |

Use **`sources`** when patterns differ per directory, or **`paths`** + **`patterns`** when every directory shares the same globs.

**`sources` example** (different patterns per directory):

```yaml
watch:
  poll: 1s
  sources:
    - path: ./logs/app
      patterns:
        - "*.log"
        - "*.log.*"
        - "*.out"
        - "*.jsonl"
    - path: /var/log/my-service
      patterns:
        - "*.log"
```

**Shared patterns example** (same globs for every path):

```yaml
watch:
  poll: 1s
  paths:
    - ./logs/app
    - ./logs/nginx
  patterns:
    - "*.log"
    - "*.out"
```

The watcher creates missing watch directories, detects new and rotated files (via inode), and tails only **new** lines written after the forwarder starts (or after a rotation).

### `kafka`

| Field | Description |
|-------|-------------|
| `brokers` | List of broker addresses (e.g. `localhost:9092`) |
| `topic` | Topic to publish JSON records to |
| `security` | Optional TLS and SASL settings (see below) |

Omit `security` for unencrypted local development (`PLAINTEXT`).

```yaml
kafka:
  brokers:
    - kafka.example.com:9093
  topic: logs
  security:
    protocol: SASL_SSL
    tls:
      ca_file: /etc/kafka/ca.crt
      cert_file: /etc/kafka/client.crt   # optional — mTLS
      key_file: /etc/kafka/client.key
    sasl:
      mechanism: SCRAM-SHA-512
      username: log-forwarder
      password: secret
```

Supported protocols: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`.

Supported SASL mechanisms: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`. Kerberos (`GSSAPI`) config is accepted but not yet implemented in the sink.

**Example configs for every security mode:** [`examples/kafka/`](examples/kafka/)

### `transform`

Choose a parsing strategy with `type`:

| `type` | Use when |
|--------|----------|
| `delimiter` | Fields are separated by a fixed character (tab, pipe, comma, …) |
| `regex` | Lines match a pattern you can express as a regular expression |
| `tab` | Alias for `delimiter` with tab — kept for backward compatibility |

| Field | Description |
|-------|-------------|
| `type` | `delimiter`, `regex`, `tab`, or a custom registered type |
| `delimiter` | For `delimiter`: field separator (default `\t`). Ignored for `regex`. |
| `columns` | For `delimiter` / `tab`: field names mapped to split columns |
| `pattern` | For `regex`: Go regular expression with named capture groups (required) |
| `on_error` | `wrap` (default) or `skip` — see [Transform errors](#transform-errors) |

**Delimiter example** ([`configs/example.yaml`](configs/example.yaml)):

```yaml
transform:
  type: delimiter
  delimiter: "\t"
  columns:
    - timestamp
    - level
    - message
  on_error: wrap
```

**Regex example** ([`configs/example-regex.yaml`](configs/example-regex.yaml)):

```yaml
transform:
  type: regex
  pattern: '^(?P<timestamp>\S+)\s+(?P<level>\S+)\s+(?P<message>.*)$'
  on_error: wrap
```

### `enrichers`

A list of enrichers applied in order. Each entry has:

| Field | Description |
|-------|-------------|
| `type` | Enricher name: built-in `static`, `host`, or a custom registered type |
| `fields` | For `static`: key/value pairs added to every record |

```yaml
enrichers:
  - type: static
    fields:
      application_id: billing-service
      environment: prod
  - type: host
```

### `pipeline`

| Field | Description |
|-------|-------------|
| `buffer_size` | Buffered channel size between watcher and pipeline (default `1024`) |
| `on_full` | `block` (default) — backpressure when the buffer is full |

### `metrics`

Optional OpenTelemetry metrics exposed over HTTP in Prometheus format. **Disabled by default.**

| Field | Description |
|-------|-------------|
| `enabled` | Start the management HTTP server (default `false`) |
| `host` | Bind address (default `127.0.0.1`) |
| `port` | Listen port (default `8080`) |
| `path` | Metrics scrape path (default `/metrics`) |

```yaml
metrics:
  enabled: true
  host: 127.0.0.1
  port: 8080
  path: /metrics
```

See [Monitoring the forwarder](#monitoring-the-forwarder) for scrape setup, health checks, and alert guidance.

## Built-in transformers

### `delimiter`

Splits each line on a configurable delimiter string. Defaults to tab (`\t`) when `delimiter` is omitted.

- If `columns` is set, values are mapped to those field names; extra columns become `field_N`.
- If `columns` is omitted, fields are named `field_0`, `field_1`, …

**Input:**

```
2024-01-01T00:00:00Z	INFO	service started
```

**Config:**

```yaml
transform:
  type: delimiter
  delimiter: "\t"
  columns:
    - timestamp
    - level
    - message
  on_error: wrap
```

Pipe-delimited example:

```yaml
transform:
  type: delimiter
  delimiter: "|"
  columns:
    - timestamp
    - level
    - message
```

**Output record:**

```json
{
  "timestamp": "2024-01-01T00:00:00Z",
  "level": "INFO",
  "message": "service started",
  "_path": "/path/to/file.log"
}
```

### `tab`

Backward-compatible alias for `delimiter` with tab. Equivalent to `type: delimiter` and `delimiter: "\t"`.

### `regex`

Parses lines with a Go regular expression. Use **named capture groups** for field names.

**Input:**

```
ERROR connection refused
```

**Config:**

```yaml
transform:
  type: regex
  pattern: '^(?P<level>\S+)\s+(?P<message>.*)$'
  on_error: wrap
```

**Output record:**

```json
{
  "level": "ERROR",
  "message": "connection refused",
  "_path": "/path/to/file.log"
}
```

### Transform errors

When a line cannot be parsed:

- **`wrap`** — publish a record with `_raw`, `_path`, and `_error` fields
- **`skip`** — log at debug level and drop the line

Every successfully parsed record also includes `_path` (source file path).

## Built-in enrichers

### `static`

Adds fixed key/value pairs from `fields` to every record.

### `host`

Adds `hostname` (from `os.Hostname()`, or `"unknown"` on failure).

## Custom extensions

Built-in transformers and enrichers are registered in package `init()` functions. To add your own, register factories and build a **custom binary** — the default `./cmd/log-forwarder` entrypoint only includes built-ins.

The full working example lives in [`examples/custom/main.go`](examples/custom/main.go).

### Custom transformer

1. Implement the `transform.Transformer` interface:

```go
type Transformer interface {
    Transform(line []byte) (transform.Record, error)
}
```

2. Register a factory in `init()`:

```go
func init() {
    transform.Register("uppercase_tab", func(cfg config.TransformConfig) (transform.Transformer, error) {
        base, err := transform.New(config.TransformConfig{
            Type:    "tab",
            Columns: cfg.Columns,
        })
        if err != nil {
            return nil, err
        }
        return &uppercaseTab{base: base}, nil
    })
}
```

3. Wrap or replace behavior in your type:

```go
type uppercaseTab struct {
    base transform.Transformer
}

func (u *uppercaseTab) Transform(line []byte) (transform.Record, error) {
    record, err := u.base.Transform(line)
    if err != nil {
        return nil, err
    }
    if msg, ok := record["message"].(string); ok {
        record["message"] = strings.ToUpper(msg)
    }
    return record, nil
}
```

4. Reference the type in config:

```yaml
transform:
  type: uppercase_tab
  columns:
    - timestamp
    - level
    - message
```

The factory receives the full `TransformConfig`, so custom transformers can read `columns`, `pattern`, and `on_error` like built-ins.

### Custom enricher

1. Implement the `enrich.Enricher` interface:

```go
type Enricher interface {
    Enrich(record transform.Record) transform.Record
}
```

2. Register a factory in `init()`:

```go
func init() {
    enrich.Register("region", func(cfg config.EnricherConfig) (enrich.Enricher, error) {
        region := cfg.Fields["region"]
        if region == "" {
            region = "unknown"
        }
        return &regionEnricher{region: region}, nil
    })
}
```

3. Implement enrichment (mutate and return the record):

```go
type regionEnricher struct {
    region string
}

func (r *regionEnricher) Enrich(record transform.Record) transform.Record {
    record["region"] = r.region
    return record
}
```

4. Add to config:

```yaml
enrichers:
  - type: region
    fields:
      region: us-east-1
```

Use `fields` to pass arbitrary string configuration into your enricher factory.

### Build and run the custom binary

```bash
go build -o bin/log-forwarder-custom ./examples/custom
./bin/log-forwarder-custom -config configs/example.yaml
```

Copy [`examples/custom/main.go`](examples/custom/main.go) as a starting point for your own entrypoint — it mirrors `cmd/log-forwarder/main.go` but registers custom types before starting the pipeline.

## Testing

### Unit tests

```bash
go test ./...
```

Verbose output:

```bash
go test -v ./...
```

### End-to-end test with Kafka

**1. Start Kafka** (Docker example):

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka:latest
```

**2. Create a local config** (`configs/local.yaml`):

```yaml
watch:
  poll: 1s
  sources:
    - path: ./logs/app
      patterns:
        - "*.log"

kafka:
  brokers:
    - localhost:9092
  topic: logs

transform:
  type: delimiter
  delimiter: "\t"
  columns:
    - timestamp
    - level
    - message
  on_error: wrap

enrichers:
  - type: static
    fields:
      application_id: test-app
      environment: dev
  - type: host

pipeline:
  buffer_size: 1024
  on_full: block
```

**3. Run the forwarder:**

```bash
mkdir -p logs/app
./bin/log-forwarder -config configs/local.yaml
```

**4. Write a test log line:**

```bash
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)	info	hello from log-forwarder" >> logs/app/test.log
```

**5. Consume from Kafka:**

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic logs \
  --from-beginning
```

You should see JSON output with your transformed fields, enricher metadata, and `_path`.

## Monitoring the forwarder

The forwarder exposes three complementary signals for operations: **HTTP metrics**, **periodic status logs**, and **process health** (via your supervisor or orchestrator).

### 1. Enable metrics

Set `metrics.enabled: true` in your config. The forwarder starts a small HTTP server that serves:

| Endpoint | Purpose |
|----------|---------|
| `GET /metrics` | Prometheus scrape endpoint (OpenTelemetry) |
| `GET /health` | Liveness probe — returns `{"status":"UP"}` |

Both endpoints share the same `metrics.host` and `metrics.port`. `/health` is only available when metrics are enabled.

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/metrics
```

Bind to `127.0.0.1` when Prometheus runs on the same host. Use `0.0.0.0` only if a remote scraper needs direct access, and restrict access at the network layer.

### 2. Scrape with Prometheus

Add a scrape job targeting the forwarder management port:

```yaml
scrape_configs:
  - job_name: log-forwarder
    static_configs:
      - targets: ["localhost:8080"]
    metrics_path: /metrics
    scrape_interval: 15s
```

If you changed `metrics.path` in config, use that value for `metrics_path`.

### 3. Health checks

Use `/health` for liveness probes. The endpoint confirms the management server is running; it does not verify Kafka connectivity on every request (Kafka is checked at startup).

**Kubernetes example:**

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 30
```

Pair this with log monitoring or alerts on `log_forwarder_kafka_publish_failures` to detect downstream Kafka issues after startup.

### 4. Log-based status

Configure periodic status logging to stderr or a log file:

```yaml
logging:
  level: info
  format: json
  status_interval: 30s
```

Every `status_interval`, the forwarder logs a `forwarder status` line with `watched_files` — useful when metrics are disabled or as a secondary signal. Set `status_interval: 0` to disable.

Other useful log lines:

| Log message | Meaning |
|-------------|---------|
| `log forwarder started` | Process is up; includes sources, topic, and metrics address when enabled |
| `kafka connectivity verified` | Startup Kafka check passed |
| `kafka unavailable at startup` | Forwarder refused to start — broker or topic unreachable |
| `kafka publish failed, retrying` | Transient publish error; check Kafka and network |
| `forwarder stopped` | Clean or error shutdown |

### 5. Metrics reference

**Forwarder metrics:**

| Metric | Description |
|--------|-------------|
| `log_forwarder_lines_read` | Lines read from watched files |
| `log_forwarder_lines_published` | Lines published to Kafka |
| `log_forwarder_lines_skipped` | Lines dropped (`transform.on_error: skip`) |
| `log_forwarder_transform_errors` | Transform failures |
| `log_forwarder_kafka_publish_failures` | Failed Kafka publish attempts |
| `log_forwarder_kafka_publish_retries` | Retries after a publish failure |
| `log_forwarder_kafka_publish_duration` | Kafka publish latency (histogram, seconds) |
| `log_forwarder_files_watched` | Files currently being tailed |
| `log_forwarder_pipeline_buffer_depth` | Events queued between watcher and pipeline |
| `log_forwarder_pipeline_buffer_capacity` | Configured `pipeline.buffer_size` |

**Process and runtime metrics:**

| Metric | Description |
|--------|-------------|
| `process_memory_usage` | Process RSS in bytes |
| `process_cpu_time` | Process CPU time (user/system) |
| `go_memory_used` | Go runtime memory in use |
| `go_memory_allocated` | Heap memory allocated by the application |
| `go_cpu_time` | CPU time spent by the Go runtime |
| `go_goroutine_count` | Number of live goroutines |

### 6. What to alert on

| Signal | Suggested condition | Likely cause |
|--------|---------------------|--------------|
| Publish failures | `rate(log_forwarder_kafka_publish_failures[5m]) > 0` sustained | Kafka down, auth/TLS issue, or network partition |
| Publish retries | `rate(log_forwarder_kafka_publish_retries[5m])` rising | Intermittent broker or timeout pressure |
| Publish latency | `histogram_quantile(0.95, rate(log_forwarder_kafka_publish_duration_bucket[5m]))` high | Broker load, network latency, or undersized cluster |
| Buffer backlog | `log_forwarder_pipeline_buffer_depth / log_forwarder_pipeline_buffer_capacity > 0.8` sustained | Pipeline slower than ingest; risk of backpressure |
| No files watched | `log_forwarder_files_watched == 0` while logs are expected | Wrong watch paths, patterns, or permissions |
| Read/publish gap | `rate(log_forwarder_lines_read[5m])` >> `rate(log_forwarder_lines_published[5m])` | Transform skips, persistent publish failures, or pipeline stall |
| Memory growth | `process_memory_usage` or `go_memory_used` trending up without stabilizing | Possible leak or sustained backlog |
| Process down | `/health` failing or scrape target `up == 0` | Crash, OOM kill, or misconfigured port |

### 7. Quick checks

```bash
# Is the management server responding?
curl -sf http://127.0.0.1:8080/health

# Are lines flowing?
curl -s http://127.0.0.1:8080/metrics | grep log_forwarder_lines_

# Is the pipeline backing up?
curl -s http://127.0.0.1:8080/metrics | grep log_forwarder_pipeline_buffer
```

For a single-host deployment, combining Prometheus alerts on publish failures and buffer depth with `status_interval` logs and a systemd `Restart=on-failure` policy gives solid baseline coverage.

## Deployment

There is no packaged deployment artifact in this repository. A typical production setup:

1. Cross-compile the binary for the target OS/arch.
2. Install the binary and config on the host (e.g. `/opt/log-forwarder/`).
3. Ensure the service user can read configured log paths.
4. Point `kafka.brokers` at your production cluster.
5. Run under a process supervisor such as systemd:

```ini
[Unit]
Description=Log Forwarder
After=network.target

[Service]
Type=simple
User=logforwarder
WorkingDirectory=/opt/log-forwarder
ExecStart=/opt/log-forwarder/log-forwarder -config /opt/log-forwarder/config.yaml
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

For custom transformers or enrichers, deploy the binary built from your own entrypoint (e.g. `./examples/custom`) instead of `./cmd/log-forwarder`.

## Project layout

```
cmd/log-forwarder/     Main entrypoint (built-in transformers/enrichers only)
configs/               Example and local config files
examples/custom/       Custom binary with registered extensions
internal/
  config/              YAML loading and validation
  metrics/             OpenTelemetry metrics and HTTP endpoints
  watcher/             File tailing and rotation detection
  transform/           Transformer registry and built-ins (delimiter, regex, tab)
  enrich/              Enricher registry and built-ins (static, host)
  pipeline/            Transform → enrich → publish orchestration
  sink/                Kafka publisher
```

## License

This project is licensed under the [MIT License](LICENSE).
