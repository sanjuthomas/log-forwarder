# Config catalog

Reference for every **YAML config key**: what it does, the default, and when to set it.

For copy-paste example files, see [[Example Configs]] and [`examples/config-catalog.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/examples/config-catalog.yaml) (which example demonstrates which features).

For narrative setup, see [[Configuration Guide]] and [[Configuration-Reference]].

---

## `watch` — file tailing

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `watch.poll` | How often the watcher rescans directories for new or rotated files | `1s` | Lower for faster discovery (more CPU); raise on very large directory trees |
| `watch.paths` | Directories to tail when every directory shares the same glob patterns | cwd (no `-config`) | Simple single- or multi-directory setups with identical patterns |
| `watch.patterns` | Glob patterns applied to every entry in `watch.paths` | `*.log*` (no `-config`) | Match your application's log filenames (`*.log`, `*.out`, `*.jsonl`, …) |
| `watch.sources` | Per-directory watch entries, each with its own `path` and `patterns` | — | Different directories need different globs; preferred in production |
| `watch.sources[].path` | Directory to watch | — | Use **absolute paths** in production |
| `watch.sources[].patterns` | Globs for that directory only | — | Required when using `sources` |
| `watch.state.path` | Watermark file — stores per-file byte offset and inode for **this process** | `.log-forwarder/watermarks.json` | Always set explicitly in production; one file per forwarder process. See [[Watermarks and Restarts]] |
| `watch.state.flush_interval` | How often in-memory watermarks are written to disk | `1s` | `0` = persist after every line (higher I/O, smaller crash window). Raise to reduce disk writes under heavy load |
| `watch.state.flush_every` | Persist watermarks after this many in-memory updates | disabled | Combine with `flush_interval` for count-based flush; useful to cap persist frequency at high line rates |

**Rotation:** rotation is detected by **inode change** only. `copytruncate` logrotate is **not supported** — use rename/create rotation. See [[Watermarks and Restarts]].

---

## `parser` — line grouping

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `parser.type` | How physical lines are grouped into records before transform | `line` | `line` for one line = one event; `multiline` for stack traces and multi-line log events |
| `parser.start_pattern` | Regex marking the **first line** of a new multiline record | — | **Required** when `parser.type: multiline`. Spring Boot: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`. See [[Spring Boot Logs]] |

---

## `transform` — field extraction

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `transform.type` | Parsing strategy | `delimiter` | `delimiter` / `tab` for separated fields; `regex` for fixed or named-capture patterns |
| `transform.delimiter` | Field separator for `delimiter` type | `\t` | Tab, pipe, comma, or any string separator |
| `transform.columns` | Field names mapped to split columns | `field_0`, `field_1`, … | Name your fields (`timestamp`, `level`, `message`, …) |
| `transform.pattern` | Go regex with named capture groups | — | **Required** for `regex`. Use `(?P<name>…)` groups |
| `transform.on_error` | Behavior when a line cannot be parsed | `wrap` | `wrap` = publish `{_raw, _path, _error}`; `skip` = drop line (increments `lines_skipped`) |

---

## `timestamp` — event time normalization

Optional block. Omit entirely to leave transformed timestamps as opaque strings.

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `timestamp.field` | Record key to parse as event time | `timestamp` | When your transform emits a differently named time field |
| `timestamp.format` | Go reference time layout for the source format | built-in layouts tried | Set explicitly for Spring Boot `2006-01-02 15:04:05.000` and other fixed layouts |
| `timestamp.default_timezone` | IANA zone when parsed time has no offset | `UTC` | Logs written in local time without offset (e.g. `America/New_York`) |
| `timestamp.output` | Normalized output format | `rfc3339nano` | v1: only `rfc3339nano` (UTC). On parse failure, field becomes processing time with `timestamp_raw` preserved |

---

## `filter` — predicate filtering

Optional block. Omit to forward all records after transform.

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `filter.match` | How top-level rules combine | `all` (AND) | `any` (OR) for multiple allowed levels or conditions |
| `filter.on_missing` | Default when a field rule references an absent field | `drop` | `pass` to treat missing fields as non-matching instead of filtering out |
| `filter.rules` | List of predicate rules | — | See field and compound rules below |

**Field rule (`type: field`):**

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `field` | Record field to compare | — | e.g. `level`, `message`, `service` |
| `op` | Comparison operator | — | `eq`, `neq`, `in`, `not_in` |
| `value` | Single compare value | — | Required for `eq` / `neq` |
| `values` | List of compare values | — | Required for `in` / `not_in` (e.g. `[ERROR]`, `[INFO, WARN, ERROR]`) |
| `ignore_case` | Case-insensitive string compare | `false` | `true` for `ERROR` vs `error` in log levels |
| `on_missing` | Override filter-level default for this rule | inherits `filter.on_missing` | Per-rule missing-field behavior |

**Compound rule (`type: compound`):** nested `match` + `rules` for AND/OR groups.

Filtered records are **not** published but **do** advance watermarks (tailing does not stall). See [[Built-in-Components#Built-in-filters]].

---

## `enrichers` — metadata

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `enrichers[].type` | Enricher implementation | `host` (no `-config`) | `static` for app/env IDs; `host` for machine hostname; custom via `enrich.Register` |
| `enrichers[].fields` | Key/value pairs for `static` enricher | — | `application_id`, `environment`, `region`, etc. |

---

## `sink` — destination

One sink per forwarder process. See [[Choosing a Sink]].

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `sink.type` | Destination implementation | `kafka` | `kafka`, `file`, `http-noauth`, or custom registered type |
| `sink.options` | Free-form map for custom sinks | — | Custom binary sinks (BigQuery, OAuth HTTP, …). See [[Custom-Extensions]] |

### `sink.kafka`

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `sink.kafka.brokers` | Kafka broker addresses | `localhost:9092` | Production cluster bootstrap servers |
| `sink.kafka.topic` | Topic for JSON records | `logs` | One topic per stream or tenant |
| `sink.kafka.connect_timeout` | Startup connectivity check timeout | `10s` | Fail fast at boot if broker unreachable; lower in smoke tests |
| `sink.kafka.security.protocol` | Wire protocol | `PLAINTEXT` | `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` in secured environments. See [`examples/kafka/`](https://github.com/sanjuthomas/log-forwarder/tree/main/examples/kafka) |
| `sink.kafka.security.tls.ca_file` | CA bundle to verify broker | — | Required for `SSL` / `SASL_SSL` (unless `insecure_skip_verify` for dev) |
| `sink.kafka.security.tls.cert_file` | Client certificate | — | mTLS: set with `key_file` |
| `sink.kafka.security.tls.key_file` | Client private key | — | mTLS: set with `cert_file` |
| `sink.kafka.security.tls.insecure_skip_verify` | Skip TLS verification | `false` | **Dev only** — never in production |
| `sink.kafka.security.sasl.mechanism` | SASL mechanism | — | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`; `GSSAPI` config only (not implemented) |
| `sink.kafka.security.sasl.username` | SASL username | — | PLAIN / SCRAM |
| `sink.kafka.security.sasl.password` | SASL password | — | PLAIN / SCRAM; inject at deploy time |
| `sink.kafka.security.sasl.oauth.token` | Bearer token | — | `OAUTHBEARER` |
| `sink.kafka.security.sasl.kerberos.*` | Kerberos keytab, principal, realm | — | GSSAPI reference config only |

### `sink.file`

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `sink.file.path` | JSONL output file | — | Local debug, sidecar volume, or batch load path. Must **not** be inside a watched directory |

### `sink.http_noauth`

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `sink.http_noauth.url` | Ingest endpoint URL | — | Open HTTP POST target (no credentials sent) |
| `sink.http_noauth.method` | HTTP method | `POST` | Override only if ingest expects non-POST |
| `sink.http_noauth.timeout` | Per-request timeout | `30s` | Tune for slow ingest or WAN latency |

Non-2xx responses are publish failures (retried). Startup `Check()` also requires 2xx.

---

## `pipeline` — buffering, retry, and publish behavior

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `pipeline.buffer_size` | Watcher → pipeline channel depth (line events) | `1024` | Raise under burst ingest; lower (e.g. `64`) in smoke tests. Full channel behavior depends on `on_full` |
| `pipeline.on_full` | Behavior when watcher → pipeline buffer is full | `block` | **`block`** in production (backpressure). **`drop`** only when permanent log loss is acceptable — see [[Configuration-Reference#pipelineon_full-block-vs-drop]] |
| `pipeline.publish_timeout` | Per-attempt deadline for `sink.Publish` | `0` (no limit) | Set (e.g. `30s`) to fail slow sinks instead of hanging forever |
| `pipeline.max_publish_bytes` | Max serialized JSON size before publish | `1048576` (1 MiB) | Align with Kafka `message.max.bytes`; `0` disables truncation |
| `pipeline.truncate_field` | String field shortened when over limit | `message` | Change if `message` is not the oversized field |
| `pipeline.truncate_suffix` | Suffix appended to truncated text | `… [truncated]` | Operator-visible marker in truncated records |

### `pipeline.publish_retry`

Retries a **single record** (or sequential publishes) on transient sink errors. Watermarks do not advance until publish succeeds.

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `pipeline.publish_retry.initial_backoff` | Delay before first retry | `1s` | Lower for fast recovery smoke tests |
| `pipeline.publish_retry.max_backoff` | Cap on exponential backoff | `30s` | Raise for flaky networks; must be ≥ `initial_backoff` |
| `pipeline.publish_retry.max_attempts` | Give up after N attempts | `0` (retry until shutdown) | Set finite value to fail faster; `0` blocks tailing on persistent failure |

### `pipeline.publish_batch`

Byte/time buffer **after enrich**, before sink. Batches flush on size, timer, or shutdown.

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `pipeline.publish_batch.max_bytes` | Sum of JSON sizes before size-based flush | `1048576` | Tune for Kafka batch efficiency; `0` disables size flush |
| `pipeline.publish_batch.flush_interval` | Max time records wait in buffer | `100ms` | Lower for lower latency; `0` disables timer flush. Set **both** `max_bytes: 0` and `flush_interval: 0` for synchronous per-record publish |
| `pipeline.publish_batch.max_attempts` | Per-**batch** attempts before `on_flush_failure` | inherits `publish_retry.max_attempts` | Override when batch retry should differ from single-record retry |
| `pipeline.publish_batch.on_flush_failure` | Policy when batch flush fails after retries | `hibernate` | `hibernate` = stop publishing, stall watermarks, block ingest. `dead_letter` = spill batch to local JSONL then advance watermarks |

### `pipeline.publish_batch.hibernate`

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `pipeline.publish_batch.hibernate.wake_enabled` | Periodically retry stalled batch while hibernating | `false` | `true` for self-healing without pod restart; default requires restart to clear hibernate |
| `pipeline.publish_batch.hibernate.wake_interval` | Time between wake retries | `10m` | Tune recovery speed when `wake_enabled: true` |

When hibernating: `/health` stays `200`, `/ready` returns `503` (`reason: sink_hibernating`).

### `pipeline.publish_batch.dead_letter`

Required when `on_flush_failure: dead_letter`.

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `pipeline.publish_batch.dead_letter.path` | Directory for failed-batch JSONL files | — | Ephemeral sidecar volume in Kubernetes; validated writable at startup |
| `pipeline.publish_batch.dead_letter.max_consecutive_batches` | DLQ batches without successful sink publish before falling back to hibernate | `3` | Lower to hibernate sooner; raise to tolerate longer sink outages |

Example: `configs/example-docker-kafka-deadletter.yaml`. See [[Monitoring#1-Enable-metrics]] for `GET /deadletters`.

---

## `logging` — forwarder's own logs

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `logging.level` | Forwarder log verbosity | `info` | `debug` for troubleshooting tail/publish issues |
| `logging.format` | Forwarder log encoding | `text` | `json` for log aggregation of the forwarder itself |
| `logging.file` | Write forwarder logs to file instead of stderr only | stderr only | Production file logging; must **not** be inside a watched directory |
| `logging.status_interval` | Emit periodic `forwarder status` log line | `30s` | Secondary health signal when metrics disabled; `0` to disable |

---

## `metrics` — Prometheus and HTTP probes

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `metrics.enabled` | Start management HTTP server | `false` | **Enable in production** for Prometheus, `/health`, `/ready` |
| `metrics.host` | Bind address | `127.0.0.1` | `0.0.0.0` only when scraper/probes reach the pod IP (restrict at network layer) |
| `metrics.port` | Listen port | `8080` | Match Kubernetes probe and Prometheus `targets` |
| `metrics.path` | Prometheus scrape path | `/metrics` | Change if your scraper expects a different path |

**Endpoints when enabled:** `GET /metrics`, `GET /health` (liveness), `GET /ready` (readiness), `GET /deadletters` (batch metadata when dead letter path configured). See [[Monitoring]].

### `metrics.readiness`

| Key | What it is for | Default | When to use |
|-----|----------------|---------|-------------|
| `metrics.readiness.path` | Readiness HTTP path | `/ready` | Kubernetes `readinessProbe.httpGet.path` |
| `metrics.readiness.sink_check` | Re-check sink connectivity on `/ready` | `true` | `false` if sink checks are too expensive or handled elsewhere |
| `metrics.readiness.sink_check_timeout` | Timeout for readiness sink check | `10s` (sink connect timeout) | Lower in fast-fail sidecar setups |
| `metrics.readiness.buffer_threshold` | Fail ready when `buffer_depth / capacity` exceeds this | `0.8` | Tune backpressure signaling for load balancers |
| `metrics.readiness.require_files` | Fail ready when no files are being tailed | `false` | `true` when an empty watch list should mark pod not ready |

---

## Quick lookup — operational concerns

| Concern | Primary keys | Wiki |
|---------|--------------|------|
| Resume after restart | `watch.state.path` | [[Watermarks and Restarts]] |
| Debounced watermark persist | `watch.state.flush_interval`, `flush_every` | [[Watermarks and Restarts]] |
| Backpressure under load | `pipeline.buffer_size`, `pipeline.on_full` | [[Configuration-Reference#pipelineon_full-block-vs-drop]] |
| Sink outage / retry | `pipeline.publish_retry`, `publish_timeout` | [[Monitoring]] |
| Batch efficiency | `pipeline.publish_batch.max_bytes`, `flush_interval` | [[Configuration-Reference]] |
| Sink down — stall vs spill | `publish_batch.on_flush_failure`, `hibernate`, `dead_letter` | [[Monitoring]] |
| Self-heal from hibernate | `publish_batch.hibernate.wake_enabled` | [[Configuration-Reference]] |
| Oversized records | `pipeline.max_publish_bytes`, `truncate_field` | [[Configuration-Reference]] |
| Drop noisy log levels | `filter` | [[Built-in-Components#Built-in-filters]] |
| Stack traces as one event | `parser.type: multiline` | [[Spring Boot Logs]] |
| Alerting | `metrics.enabled`, readiness, Prometheus | [[Monitoring#6-What-to-alert-on]] |
