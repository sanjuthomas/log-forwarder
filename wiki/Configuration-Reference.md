# Configuration

Configuration is YAML. See [`configs/example.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example.yaml) for a full example.

## `watch`

Controls which log files are tailed.

| Field | Description |
|-------|-------------|
| `poll` | How often to rescan directories (e.g. `1s`, `500ms`) |
| `sources` | Per-directory watch entries, each with its own `patterns` |
| `paths` | Directories to watch when all use the same `patterns` |
| `patterns` | Glob patterns applied to every path in `paths` |
| `state.path` | Path to the watermark file (default `.log-forwarder/watermarks.json`) |
| `state.flush_interval` | How often to persist watermarks to disk (default `1s`; set `0` to persist every line) |
| `state.flush_every` | Optional count-based flush: persist after this many in-memory updates (default disabled) |

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

### Watermarks

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
    flush_interval: 1s
    flush_every: 0
  sources:
    - path: ./logs/app
      patterns:
        - "*.log"
```

**Persistence:** Watermark updates are kept in memory on every processed line and written to disk on a schedule (`flush_interval`, default `1s`) or after `flush_every` updates when set. A final flush runs on graceful shutdown (`SIGINT` / `SIGTERM`). Set `flush_interval: 0` to persist after every line (previous behavior, higher disk I/O). On crash or `kill -9`, the on-disk watermark may lag by up to one flush window; already-published lines may be sent again after restart (**at-least-once**).

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
2. **After each committed parser event** — the in-memory watermark for that source file is updated to the event's byte offset (for multiline records, the end of the last physical line in that event). Filtered and transform-skipped lines advance the watermark too; only publish failures stall it. With `parser.type: multiline`, buffered continuation lines do not commit until the next header line or graceful shutdown — see [[Watermarks-and-Restarts#Multiline-parser-and-watermarks|Multiline parser and watermarks]]. The watermark file on disk is updated on the flush schedule (see above).
3. **Restart** — if the file's inode matches the stored value, tailing **resumes from `offset`**. The log line `resuming file from watermark` indicates this.
4. **Log rotation** — if the path is reused but the **inode changed** (typical after `logrotate` with `create` or `rename`), the stored offset is ignored and the forwarder tails the new file from the **beginning**. The log line `tailing file from beginning` indicates this.

**`copytruncate` rotation is not supported.** Some `logrotate` configs truncate the file in place (`copytruncate`) while keeping the same inode. The forwarder detects rotation by **inode change** only; it does not reset tail position when a file shrinks. With `copytruncate`, the forwarder may continue from a stale byte offset and **miss or mis-read** new content. Use `create` / `rename` rotation instead (rename the old file, create a new file at the same path). Integration test E2E-4 covers rename rotation only; see [`docs/integration-test-cases.txt`](https://github.com/sanjuthomas/log-forwarder/blob/main/docs/integration-test-cases.txt).

**Operational notes:**

- **Re-read from the beginning of a file** — remove that file's entry from the `files` object in the watermark JSON (or delete the entire watermark file to reset all watched files). On the next start the forwarder tails from offset `0` and publishes those lines again to the **current** sink.
- **Change sink and restart** — the existing watermark is **respected**; tailing continues where it left off. To backfill the new sink with older log content, you must clear the relevant watermark entry(ies) as above.
- **Different sinks for the same log files** — run **separate forwarder processes**, each with its own config and `watch.state.path`. One process, one sink; one watermark file per process.
- **Do not place the watermark file inside a watched directory** — config validation rejects paths that would be tailed as log input. Keep it outside `watch.paths` / `watch.sources`, similar to `logging.file`.
- Writes are atomic (write to a `.tmp` file, then rename) to reduce the risk of a corrupted state file on crash.

### Multiline parser and watermarks

With `parser.type: multiline`, watermark updates are tied to **committed parser events**, not to every physical line the watcher reads.

| Stage | What happens |
|-------|----------------|
| Header line | Starts a new buffer; the **previous** multiline record (if any) is committed and its watermark is set to the **last byte offset of that record** (the final line that belonged to it). |
| Continuation lines | Appended to the buffer only. No publish and **no watermark update** yet. |
| Trailing record | The last event in a file stays buffered until a new header line arrives or the process shuts down gracefully. |

Implications for operators:

- **Watermark can lag the watcher** — byte offsets in `watermarks.json` reflect the last *committed* multiline event, not necessarily the last line already read from the file. A long stack trace at the end of a log file may be tailed but unpublished until the next header line or shutdown.
- **Last record on shutdown** — graceful stop flushes the parser buffer, publishes the trailing record, and updates the watermark. `kill -9` or crash may leave that final multiline event unpublished; on restart the forwarder resumes from the last committed offset and will re-read and re-publish those lines (**at-least-once**).
- **Sidecar / Kubernetes** — if the forwarder restarts before a trailing multiline event is committed, either rely on graceful termination (preStop hook + `SIGTERM`) or ensure the application emits another header line so the buffered record is flushed. Integration tests often append a sentinel header line for this reason (see E2E-2 in [`docs/integration-test-cases.txt`](https://github.com/sanjuthomas/log-forwarder/blob/main/docs/integration-test-cases.txt)).
- **Line parser for strict per-line watermarks** — use `parser.type: line` when each physical line should commit and advance the watermark immediately (see E2E-3 in the integration test catalog).

The `offset` stored for a committed multiline event is always the end offset of its **last physical line**, not an intermediate line within the stack trace.

## `sink`

Each forwarder **process** configures exactly **one** sink. There is no multi-sink or fan-out in a single config — `sink.type` selects a single implementation (`kafka`, `file`, `http-noauth`, or a custom registered type), and every published record goes to that destination only.

Built-in types: `kafka` (default), `file`, and `http-noauth`. Register custom sinks (for example BigQuery streaming or HTTP with OAuth2) in a custom binary — see [[Custom-Extensions#Custom-sink|Custom sink]]. Watermark behavior is described [[Watermarks-and-Restarts|above]]; it is tied to the process and source files, not to which sink type is active.

| Field | Description |
|-------|-------------|
| `type` | `kafka`, `file`, `http-noauth`, or a custom registered type |
| `kafka` | Settings when `type` is `kafka` |
| `file` | Settings when `type` is `file` |
| `http_noauth` | Settings when `type` is `http-noauth` |
| `options` | Free-form map for custom sink implementations |

### Kafka sink

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

**Example configs for every Kafka security mode:** [`examples/kafka/`](https://github.com/sanjuthomas/log-forwarder/blob/main/examples/kafka/)

### File sink

Appends one JSON record per line (JSONL) to a local file. See [`configs/example-file.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-file.yaml).

```yaml
sink:
  type: file
  file:
    path: /var/lib/log-forwarder/forwarded.jsonl
```

The parent directory is created if needed. The file path must not be inside a watched directory or match a watch pattern.

### HTTP sink (no authentication)

POSTs each JSON record to an **open** HTTP ingest endpoint. This built-in sink does not send credentials — no `Authorization` header, API keys, or OIDC token exchange. Use it for local development, trusted internal networks, or endpoints protected by network policy rather than application-level auth.

See [`configs/example-http-noauth.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-http-noauth.yaml). For OAuth2 or API-key auth, register a custom sink (a dedicated `http-oauth2` built-in may be added later).

```yaml
sink:
  type: http-noauth
  http_noauth:
    url: http://localhost:8081/ingest
    method: POST
    timeout: 30s
```

Non-2xx responses are treated as publish failures and retried by the pipeline.

## `parser`

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

A multiline record is **committed** (passed to transform → filter → enrich → publish) only when:

1. The **next** line matches `start_pattern` (the previous record is flushed first), or
2. The forwarder shuts down gracefully (`SIGINT` / `SIGTERM`), when the parser runs `Flush()` on the trailing buffer.

Until then, continuation lines sit in the parser buffer. The watcher may already have read them from disk, but they are not yet published and **do not advance the watermark**. See [[Watermarks-and-Restarts#Multiline-parser-and-watermarks|Multiline parser and watermarks]] below.

## `transform`

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
| `on_error` | `wrap` (default) or `skip` — see [[Built-in-Components#Transform-errors|Transform errors]] |

**Delimiter example** ([`configs/example.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example.yaml)):

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

**Regex example** ([`configs/example-regex.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-regex.yaml)):

```yaml
transform:
  type: regex
  pattern: '^(?P<timestamp>\S+)\s+(?P<level>\S+)\s+(?P<message>.*)$'
  on_error: wrap
```

**Spring Boot default console format** — multiline parser + regex transform for Logback layout. Example configs per sink:

| Sink | Config |
|------|--------|
| Kafka | [`configs/example-spring-boot-kafka.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-spring-boot-kafka.yaml) |
| File (JSONL) | [`configs/example-spring-boot-file.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-spring-boot-file.yaml) |
| HTTP (no auth) | [`configs/example-spring-boot-http-noauth.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-spring-boot-http-noauth.yaml) |

```yaml
parser:
  type: multiline
  start_pattern: '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'

transform:
  type: regex
  pattern: '^(?s)(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<level>\S+)\s+(?P<pid>\d+)\s+---\s+\[\s*(?P<thread>[^\]]+?)\s*\]\s+(?P<logger>\S+)\s+:\s+(?P<message>.*)$'
  on_error: wrap
```

Only the `sink` block differs between the three Spring Boot examples — parser, transform, enrichers, and optional filter are shared.

## `timestamp`

Optional normalization of the event time field **after transform, before filter**. Omit the block to leave transformed timestamps as opaque strings (default).

| Field | Description |
|-------|-------------|
| `field` | Record key to parse (default `timestamp`) |
| `format` | Optional Go reference time layout (e.g. `2006-01-02 15:04:05.000`). When empty, built-in layouts are tried: RFC3339/RFC3339Nano, `2006-01-02 15:04:05.000`, `2006-01-02 15:04:05` |
| `default_timezone` | IANA zone for parsed times without an offset (default `UTC`) |
| `output` | Normalized UTC format (v1: `rfc3339nano` only) |

On success the configured field is replaced with UTC RFC3339Nano (e.g. `2026-06-08T15:15:23.456Z`). On parse failure (missing, empty, or unparseable value) the field is set to **processing time** (UTC), with `timestamp_raw` preserving the original string when present and `timestamp_source: processing`.

```yaml
timestamp:
  field: timestamp
  format: "2006-01-02 15:04:05.000"
  default_timezone: UTC
```

## `filter`

Optional predicate-based filtering **after timestamp normalization, before enrich**. Omit the entire block to pass all records (default).

Filtered records are **not** enriched or published. Watermarks still advance so tailing does not stall. Each filtered record increments `log_forwarder_lines_filtered`.

| Field | Description |
|-------|-------------|
| `match` | How top-level rules combine: `all` (AND, default) or `any` (OR) |
| `on_missing` | Default for field rules when a referenced field is absent: `drop` (default) or `pass` |
| `rules` | List of predicate rules (see below) |

**Built-in rule types:**

| `type` | Purpose |
|--------|---------|
| `field` | Compare a transformed field value |
| `compound` | Nested rules with their own `match: all` or `any` |

**Field rule fields:**

| Field | Description |
|-------|-------------|
| `field` | Record field name (for example `level`, `message`) |
| `op` | `eq`, `neq`, `in`, or `not_in` |
| `value` | Required for `eq` / `neq` |
| `values` | Required for `in` / `not_in` |
| `ignore_case` | Case-insensitive string comparison (default `false`) |
| `on_missing` | Override filter-level default when the field is absent |

**Errors only** (common for noisy legacy logs — case-insensitive):

```yaml
filter:
  match: all
  rules:
    - type: field
      field: level
      op: in
      values: [ERROR]
      ignore_case: true
```

**Multiple levels (OR):**

```yaml
filter:
  match: any
  rules:
    - type: field
      field: level
      op: in
      values: [INFO, WARN, ERROR]
      ignore_case: true
```

**AND with nested rules:**

```yaml
filter:
  match: all
  rules:
    - type: field
      field: level
      op: eq
      value: ERROR
      ignore_case: true
    - type: field
      field: service
      op: eq
      value: billing
```

See [`configs/example-filter.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-filter.yaml) and [`configs/example-docker-filter.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-docker-filter.yaml). Register custom predicate types with `filter.Register` (see [[Custom-Extensions#Custom-filter|Custom filter]]).

## `enrichers`

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

## `pipeline`

| Field | Description |
|-------|-------------|
| `buffer_size` | Buffered channel size between watcher and pipeline (default `1024`) |
| `on_full` | `block` (default) — watcher waits when the buffer is full; `drop` — discard new lines when the buffer is full (see [[Configuration-Reference#pipelineon_full-block-vs-drop|`on_full: drop` semantics]] below) |
| `publish_timeout` | Per-attempt timeout for `sink.Publish` (default `0` = no limit). Applies to all sink types. |
| `publish_retry.initial_backoff` | Delay before the first retry (default `1s`) |
| `publish_retry.max_backoff` | Maximum delay between retries (default `30s`) |
| `publish_retry.max_attempts` | Give up after this many attempts (`0` = retry until shutdown, default) |
| `max_publish_bytes` | Maximum serialized JSON payload size before publish (default `1048576`, 1 MiB — aligned with Kafka `message.max.bytes`). When exceeded, the configured truncate field is shortened so the record fits. Set to `0` to disable truncation. |
| `truncate_field` | String field to shorten when over the limit (default `message`) |
| `truncate_suffix` | Appended to truncated field text (default `… [truncated]`) |
| `publish_batch.max_bytes` | Sum of serialized JSON sizes in the publish buffer before flush (default `1048576`, 1 MiB). Set to `0` to disable size-based flush. |
| `publish_batch.flush_interval` | Maximum time records wait in the publish buffer (default `100ms`). Set to `0` to disable time-based flush. Set **both** `max_bytes: 0` and `flush_interval: 0` to publish each record synchronously (previous behavior). |
| `publish_batch.on_flush_failure` | Policy when a batch flush fails after retries (default `hibernate`). `hibernate` stops publishing and blocks ingest until cleared. `dead_letter` writes the batch to local JSONL and advances watermarks only after a successful write. |
| `publish_batch.max_attempts` | Per-batch publish attempts before `on_flush_failure` applies (default: `publish_retry.max_attempts`). |
| `publish_batch.hibernate.wake_enabled` | When `true`, periodically retry the stalled batch while hibernating (default `false` — stay hibernating until process restart). |
| `publish_batch.hibernate.wake_interval` | Time between wake retries when `wake_enabled` is `true` (default `10m`). |
| `publish_batch.dead_letter.path` | Directory for failed-batch JSONL files when `on_flush_failure: dead_letter` (required). Validated for write access at startup. Use an ephemeral sidecar volume in Kubernetes. |
| `publish_batch.dead_letter.max_consecutive_batches` | After this many consecutive dead-letter batches without a successful sink publish, transition to hibernate (default `3`). |

`buffer_size` is the **watcher → pipeline** line-event channel depth (event count). `publish_batch` is a separate byte/time buffer **after enrich**, before the sink.

When a publish fails, the pipeline retries with exponential backoff (doubling delay up to `max_backoff`). Watermarks are not advanced until a batch is successfully committed to the sink or dead letter storage. When batch flush retries are exhausted and `on_flush_failure` is `dead_letter`, each failed batch is written to `dead_letter.path` as `{timestamp}_{batch_id}.jsonl`; watermarks advance only after the file is written. If `dead_letter.max_consecutive_batches` is exceeded without a successful sink publish, the forwarder falls back to **hibernate**. When `on_flush_failure` is `hibernate` (the default), the forwarder enters **hibernate** mode: publishing stops, the failed batch’s watermarks stay put, and new lines block on the publish buffer until hibernate is cleared. By default hibernate lasts until the process is restarted; set `publish_batch.hibernate.wake_enabled: true` for periodic self-healing retries against the stalled batch. `/health` stays `200` (the process is alive); `/ready` returns `503` with `reason: sink_hibernating` so load balancers can stop sending traffic without restarting the pod.

Publish batches are flushed on size threshold, timer, or shutdown. While a batch is flushing asynchronously, the pipeline continues appending to a second active buffer; if that buffer fills before the in-flight flush completes, ingest blocks until the sink commit finishes (backpressure). Kafka and file sinks implement `PublishBatch`; other sinks fall back to sequential `Publish` calls per record in the batch.

Oversized records set `publish_truncated: true` and field metadata such as `message_truncated` and `message_original_bytes`. If the record is still too large after truncating the message field, the pipeline tries `_raw` (wrap records) and then the largest string field. UTF-8-safe truncation avoids splitting multibyte characters.

```yaml
pipeline:
  buffer_size: 1024
  on_full: block
  max_publish_bytes: 1048576
  truncate_field: message
  truncate_suffix: "… [truncated]"
  publish_batch:
    max_bytes: 1048576
    flush_interval: 100ms
    on_flush_failure: hibernate
    max_attempts: 0
    hibernate:
      wake_enabled: false
      wake_interval: 10m
  publish_timeout: 30s
  publish_retry:
    initial_backoff: 1s
    max_backoff: 30s
    max_attempts: 0
```

Sink-specific timeouts still apply where configured (for example `sink.http_noauth.timeout` for each HTTP round trip, `sink.kafka.connect_timeout` for startup checks).

### `pipeline.on_full`: block vs drop

**Default to `block` in production.** Use `drop` only when permanent log loss under overload is acceptable.

When the watcher → pipeline channel is full, behavior depends on `on_full`:

| Mode | Watcher file offset | Line reaches pipeline | Watermark advances | Data in sink | Restart replays dropped bytes? |
|------|---------------------|----------------------|--------------------|--------------|-------------------------------|
| `block` | Advances when line is read | Yes (eventually) | Yes, after pipeline processes event | Yes (when publish succeeds) | N/A — no loss |
| `drop` | Advances when line is read | **No** if buffer full | **No** for dropped lines | **No** | **No** — bytes are skipped forever |
| Filter drop | N/A (pipeline layer) | Yes | **Yes** (intentional forward progress) | No | No (by design) |
| Transform skip | N/A (pipeline layer) | Yes | **Yes** | No | No (by design) |
| Publish failure | N/A | Yes | **No** (stalls) | No | **Yes** (retry after restart) |

**`on_full: drop` is permanent data loss, not backpressure shedding.** The watcher reads each line and advances its in-memory file offset **before** attempting to enqueue the event. If the channel is full, the line is discarded (`log_forwarder_pipeline_buffer_dropped` increments; a debug log is emitted). Dropped lines never reach the parser, filter, or sink, and **never update watermarks**. On restart, tailing resumes from the last **processed** watermark — dropped bytes are not replayed.

This differs from filter/transform drops, which are explicit forwarding decisions that still advance watermarks so tailing does not stall on noise. It also differs from publish failures, which stall watermarks so lines can be retried after restart.

**Multiline caveat:** if a continuation line is dropped but later lines are accepted, multiline grouping can be corrupted (orphan lines, split stack traces).

**Metrics:** `log_forwarder_lines_read` still increments for dropped lines, so a gap between `lines_read` and `lines_published` can be misdiagnosed as filter drops or transform skips. Alert separately on `rate(log_forwarder_pipeline_buffer_dropped[5m]) > 0` (see [[Monitoring#What-to-alert-on|What to alert on]]).

If sustained overload fills the buffer, prefer scaling the sink, increasing `buffer_size`, tuning publish batching, or fixing publish latency — not enabling `drop` — unless you explicitly accept gaps in forwarded logs.

## `metrics`

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

See [[Monitoring|Monitoring the forwarder]] for scrape setup, health checks, and alert guidance.
