# log-forwarder

A lightweight Go service that tails log files, parses and transforms lines into structured JSON, enriches records with metadata, and publishes them through a **pluggable sink** to the destination you configure.

**User guide:** For install, configuration, sinks, Spring Boot logs, and watermarks, see the [GitHub Wiki](https://github.com/sanjuthomas/log-forwarder/wiki).

```mermaid
flowchart LR
    subgraph local["Local disk"]
        logs["Log files\n(watch paths)"]
    end

    subgraph host["Forwarder host"]
        watcher["Watcher\ntail · rotate"]
        parser["Parser\nline · multiline"]
        transform["Transform\ndelimiter / regex"]
        enrich["Enrich\nhost · static · …"]
        sinkIf["Sink interface\nPublish · Close · Check"]
        watcher --> parser --> transform --> enrich --> sinkIf
    end

    subgraph sinks["Pluggable sinks (one per config)"]
        kafkaSink["kafka"]
        fileSink["file"]
        httpSink["http-noauth"]
        customSink["custom · bigquery · …"]
    end

    subgraph destinations["Destinations"]
        kafkaDest["Kafka topic"]
        fileDest["JSONL file"]
        httpDest["Open HTTP endpoint"]
        customDest["Your API / warehouse"]
    end

    logs -->|"read new lines"| watcher
    sinkIf -->|"sink.type"| kafkaSink
    sinkIf --> fileSink
    sinkIf --> httpSink
    sinkIf --> customSink
    kafkaSink --> kafkaDest
    fileSink --> fileDest
    httpSink --> httpDest
    customSink --> customDest
```

The pipeline always ends at the same `sink.Sink` interface. `sink.type` in config selects the implementation; only one sink is active at runtime. Register additional types (for example `http-oauth2` or BigQuery streaming) in a custom binary via `sink.Register`.

## Requirements

- **Go 1.22+**
- A reachable **sink destination** (Kafka cluster, writable file path, or open HTTP endpoint) unless you use a custom sink
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

Build a binary with custom parsers, transformers, enrichers, or sinks (see [Custom extensions](#custom-extensions)):

```bash
go build -o bin/log-forwarder-custom ./examples/custom
```

## Run

### With defaults

If you omit `-config`, the forwarder uses built-in defaults:

- Watches the **current working directory** for `*.log*` files
- Publishes to Kafka at `localhost:9092`, topic `logs` (`sink.type: kafka`)
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
| `state.path` | Path to the watermark file (default `.log-forwarder/watermarks.json`) |

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

The watcher creates missing watch directories, detects new and rotated files (via inode), and reads only lines that have not yet been forwarded.

#### Watermarks

Watermarks track **how far this forwarder process has read** in each tailed **source file**. They belong to the **process** (via `watch.state.path`), not to a particular sink — the watermark file stores only file path, byte `offset`, and `inode`. There is no sink type or destination in the watermark.

That means a **restart does not re-publish** lines that were already read and successfully published, even if you change `sink.type` or sink settings before restarting (for example switching from Kafka to file). The forwarder resumes tailing from the saved offset; only **new** lines after that point go to the new sink. Previously shipped lines are **not** automatically re-sent to the new destination.

**Default location:** `.log-forwarder/watermarks.json` in the forwarder's **current working directory** (the directory you start the process from). Relative paths in config are resolved the same way.

If you run without `-config`, the default is still `.log-forwarder/watermarks.json`. The directory is created automatically on first write.

**Change the location** with `watch.state.path`:

```yaml
watch:
  poll: 1s
  state:
    path: /var/lib/log-forwarder/watermarks.json
  sources:
    - path: ./logs/app
      patterns:
        - "*.log"
```

Use an absolute path in production so the file location does not depend on where the service is started from. The forwarder logs the resolved path at startup (`state_path` in the `log forwarder started` message).

**File format** — JSON mapping each tailed file path to a byte offset and inode:

```json
{
  "files": {
    "/var/log/billing/application.log": {
      "offset": 1048576,
      "inode": 883241
    }
  }
}
```

| Field | Meaning |
|-------|---------|
| `offset` | Number of bytes read from the start of the file (including newline characters) |
| `inode` | OS inode of the file when the offset was recorded |

**How it works:**

1. **First run** — no watermark file exists (or no entry for a file). The forwarder tails from the **beginning** of the file.
2. **After each record is published** — the watermark for that source file is updated to the offset of the last processed line (or the last line of a multiline record).
3. **Restart** — if the file's inode matches the stored value, tailing **resumes from `offset`**. The log line `resuming file from watermark` indicates this.
4. **Log rotation** — if the path is reused but the **inode changed** (typical after `logrotate`), the stored offset is ignored and the forwarder tails the new file from the **beginning**. The log line `tailing file from beginning` indicates this.

**Operational notes:**

- **Re-read from the beginning of a file** — remove that file's entry from the `files` object in the watermark JSON (or delete the entire watermark file to reset all watched files). On the next start the forwarder tails from offset `0` and publishes those lines again to the **current** sink.
- **Change sink and restart** — the existing watermark is **respected**; tailing continues where it left off. To backfill the new sink with older log content, you must clear the relevant watermark entry(ies) as above.
- **Different sinks for the same log files** — run **separate forwarder processes**, each with its own config and `watch.state.path`. One process, one sink; one watermark file per process.
- **Do not place the watermark file inside a watched directory** — config validation rejects paths that would be tailed as log input. Keep it outside `watch.paths` / `watch.sources`, similar to `logging.file`.
- Writes are atomic (write to a `.tmp` file, then rename) to reduce the risk of a corrupted state file on crash.

### `sink`

Each forwarder **process** configures exactly **one** sink. There is no multi-sink or fan-out in a single config — `sink.type` selects a single implementation (`kafka`, `file`, `http-noauth`, or a custom registered type), and every published record goes to that destination only.

Built-in types: `kafka` (default), `file`, and `http-noauth`. Register custom sinks (for example BigQuery streaming or HTTP with OAuth2) in a custom binary — see [Custom sink](#custom-sink). Watermark behavior is described [above](#watermarks); it is tied to the process and source files, not to which sink type is active.

| Field | Description |
|-------|-------------|
| `type` | `kafka`, `file`, `http-noauth`, or a custom registered type |
| `kafka` | Settings when `type` is `kafka` |
| `file` | Settings when `type` is `file` |
| `http_noauth` | Settings when `type` is `http-noauth` |
| `options` | Free-form map for custom sink implementations |

#### Kafka sink

```yaml
sink:
  type: kafka
  kafka:
    brokers:
      - kafka.example.com:9093
    topic: logs
    connect_timeout: 10s
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

| `kafka` field | Description |
|---------------|-------------|
| `brokers` | List of broker addresses (e.g. `localhost:9092`) |
| `topic` | Topic to publish JSON records to |
| `connect_timeout` | Startup connectivity check timeout (default `10s`) |
| `security` | Optional TLS and SASL settings |

Omit `security` for unencrypted local development (`PLAINTEXT`).

Supported protocols: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`.

Supported SASL mechanisms: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`. Kerberos (`GSSAPI`) config is accepted but not yet implemented in the sink.

**Example configs for every Kafka security mode:** [`examples/kafka/`](examples/kafka/)

#### File sink

Appends one JSON record per line (JSONL) to a local file. See [`configs/example-file.yaml`](configs/example-file.yaml).

```yaml
sink:
  type: file
  file:
    path: /var/lib/log-forwarder/forwarded.jsonl
```

The parent directory is created if needed. The file path must not be inside a watched directory or match a watch pattern.

#### HTTP sink (no authentication)

POSTs each JSON record to an **open** HTTP ingest endpoint. This built-in sink does not send credentials — no `Authorization` header, API keys, or OIDC token exchange. Use it for local development, trusted internal networks, or endpoints protected by network policy rather than application-level auth.

See [`configs/example-http-noauth.yaml`](configs/example-http-noauth.yaml). For OAuth2 or API-key auth, register a custom sink (a dedicated `http-oauth2` built-in may be added later).

```yaml
sink:
  type: http-noauth
  http_noauth:
    url: http://localhost:8081/ingest
    method: POST
    timeout: 30s
```

Non-2xx responses are treated as publish failures and retried by the pipeline.

### `parser`

Groups physical log lines into logical records before transformation.

| Field | Description |
|-------|-------------|
| `type` | `line` (default) or `multiline` |
| `start_pattern` | For `multiline`: regex that marks the first line of a new record (required) |

**Line parser** (default) — one physical line becomes one record:

```yaml
parser:
  type: line
```

**Multiline parser** — continuation lines are buffered until the next line matching `start_pattern`. Use for stack traces and other multi-line log events (Spring Boot examples below):

```yaml
parser:
  type: multiline
  start_pattern: '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
```

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

**Spring Boot default console format** — multiline parser + regex transform for Logback layout. Example configs per sink:

| Sink | Config |
|------|--------|
| Kafka | [`configs/example-spring-boot-kafka.yaml`](configs/example-spring-boot-kafka.yaml) |
| File (JSONL) | [`configs/example-spring-boot-file.yaml`](configs/example-spring-boot-file.yaml) |
| HTTP (no auth) | [`configs/example-spring-boot-http-noauth.yaml`](configs/example-spring-boot-http-noauth.yaml) |

```yaml
parser:
  type: multiline
  start_pattern: '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'

transform:
  type: regex
  pattern: '^(?s)(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<level>\S+)\s+(?P<pid>\d+)\s+---\s+\[\s*(?P<thread>[^\]]+?)\s*\]\s+(?P<logger>\S+)\s+:\s+(?P<message>.*)$'
  on_error: wrap
```

Only the `sink` block differs between the three Spring Boot examples — parser, transform, and enrichers are shared.

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
| `publish_timeout` | Per-attempt timeout for `sink.Publish` (default `0` = no limit). Applies to all sink types. |
| `publish_retry.initial_backoff` | Delay before the first retry (default `1s`) |
| `publish_retry.max_backoff` | Maximum delay between retries (default `30s`) |
| `publish_retry.max_attempts` | Give up after this many attempts (`0` = retry until shutdown, default) |

When a publish fails, the pipeline retries with exponential backoff (doubling delay up to `max_backoff`). Watermarks are not advanced until publish succeeds.

```yaml
pipeline:
  buffer_size: 1024
  on_full: block
  publish_timeout: 30s
  publish_retry:
    initial_backoff: 1s
    max_backoff: 30s
    max_attempts: 0
```

Sink-specific timeouts still apply where configured (for example `sink.http_noauth.timeout` for each HTTP round trip, `sink.kafka.connect_timeout` for startup checks).

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

## Built-in parsers

### `line`

Default. Each physical line from the watcher becomes one record for the transformer.

### `multiline`

Buffers lines until the next line matches `start_pattern`, then emits the joined record (newline-separated). Incomplete buffers are flushed on shutdown.

## Built-in enrichers

### `static`

Adds fixed key/value pairs from `fields` to every record.

### `host`

Adds `hostname` (from `os.Hostname()`, or `"unknown"` on failure).

## Built-in sinks

Every destination implements the same interface:

```go
type Sink interface {
    Publish(ctx context.Context, payload []byte) error
    Close() error
}
```

Sinks may also implement `sink.Checker` for a startup connectivity probe. Select the implementation with `sink.type` in config (see [`sink`](#sink) above).

| Type | Description |
|------|-------------|
| `kafka` | Publish JSON records to a Kafka topic (default) |
| `file` | Append JSONL to a local file |
| `http-noauth` | POST JSON to an open HTTP endpoint (no credentials) |

Register custom types with `sink.Register` in a custom binary — for example BigQuery streaming or HTTP with OAuth2.

## Custom extensions

Built-in parsers, transformers, and enrichers are registered in package `init()` functions. To add your own, register factories and build a **custom binary** — the default `./cmd/log-forwarder` entrypoint only includes built-ins.

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

### Custom parser

1. Implement the `parser.Parser` interface:

```go
type Parser interface {
    Feed(event watcher.LineEvent) ([]parser.Event, error)
    Flush() ([]parser.Event, error)
}
```

2. Register a factory in `init()`:

```go
func init() {
    parser.Register("my_parser", func(cfg config.ParserConfig) (parser.Parser, error) {
        return &myParser{}, nil
    })
}
```

3. Reference the type in config:

```yaml
parser:
  type: my_parser
```

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

### Custom sink

1. Implement the `sink.Sink` interface:

```go
type Sink interface {
    Publish(ctx context.Context, payload []byte) error
    Close() error
}
```

2. Optionally implement `sink.Checker` for a startup connectivity probe:

```go
type Checker interface {
    Check(ctx context.Context) error
}
```

3. Register a factory in `init()`:

```go
func init() {
    sink.Register("bigquery", func(cfg config.SinkConfig) (sink.Sink, error) {
        project, _ := cfg.Options["project"].(string)
        dataset, _ := cfg.Options["dataset"].(string)
        return newBigQuerySink(project, dataset)
    })
}
```

4. Reference the type in config and pass options:

```yaml
sink:
  type: bigquery
  options:
    project: my-gcp-project
    dataset: application_logs
```

The factory receives the full `SinkConfig`, so custom sinks can read `options` and ignore built-in `kafka` / `file` / `http_noauth` blocks.

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

sink:
  type: kafka
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

Use `/health` for liveness probes. The endpoint confirms the management server is running; it does not verify sink connectivity on every request (the sink is checked at startup when it implements `sink.Checker`).

**Kubernetes example:**

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 30
```

Pair this with log monitoring or alerts on `log_forwarder_kafka_publish_failures` to detect downstream sink issues after startup.

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
| `sink connectivity verified` | Startup sink check passed |
| `sink unavailable at startup` | Forwarder refused to start — destination unreachable |
| `publish failed, retrying` | Transient publish error; check sink destination and network |
| `forwarder stopped` | Clean or error shutdown |

### 5. Metrics reference

**Forwarder metrics:**

| Metric | Description |
|--------|-------------|
| `log_forwarder_lines_read` | Lines read from watched files |
| `log_forwarder_lines_published` | Lines published to the configured sink |
| `log_forwarder_lines_skipped` | Lines dropped (`transform.on_error: skip`) |
| `log_forwarder_transform_errors` | Transform failures |
| `log_forwarder_kafka_publish_failures` | Failed sink publish attempts |
| `log_forwarder_kafka_publish_retries` | Retries after a publish failure |
| `log_forwarder_kafka_publish_duration` | Sink publish latency (histogram, seconds) |
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
| Publish failures | `rate(log_forwarder_kafka_publish_failures[5m]) > 0` sustained | Sink unreachable, auth/TLS issue, or network partition |
| Publish retries | `rate(log_forwarder_kafka_publish_retries[5m])` rising | Intermittent sink or timeout pressure |
| Publish latency | `histogram_quantile(0.95, rate(log_forwarder_kafka_publish_duration_bucket[5m]))` high | Sink load, network latency, or slow endpoint |
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

## Docker

Container images for sidecar and standalone deployments are published to [Docker Hub](https://hub.docker.com/r/sanjuthomas/log-forwarder) (`linux/amd64`, `linux/arm64`).

```bash
docker compose up --build          # local smoke test
docker pull sanjuthomas/log-forwarder:latest
```

See [docs/docker.md](docs/docker.md) for volume mounts, Kubernetes sidecar notes, and maintainer publish steps.

## Deployment

Binary install on the host is still supported. A typical production setup:

1. Cross-compile the binary for the target OS/arch.
2. Install the binary and config on the host (e.g. `/opt/log-forwarder/`).
3. Ensure the service user can read configured log paths.
4. Point `sink.kafka.brokers` at your production cluster (or switch `sink.type` to `file` / `http-noauth`).
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
docker/                Sample log data for docker compose
Dockerfile             Multi-stage image (distroless runtime)
docker-compose.yaml    Local container smoke test
examples/custom/       Custom binary with registered extensions
internal/
  config/              YAML loading and validation
  metrics/             OpenTelemetry metrics and HTTP endpoints
  watcher/             File tailing and rotation detection
  transform/           Transformer registry and built-ins (delimiter, regex, tab)
  enrich/              Enricher registry and built-ins (static, host)
  pipeline/            Transform → enrich → publish orchestration
  sink/                Pluggable sinks (kafka, file, http-noauth)
```

## License

This project is licensed under the [MIT License](LICENSE).
