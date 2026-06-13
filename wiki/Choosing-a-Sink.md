# Choosing a Sink

The **sink** is where structured JSON records go after parsing and enrichment. Each forwarder process uses **exactly one** sink.

## Comparison

| Sink type | Best for | Auth | Example config |
|-----------|----------|------|----------------|
| `kafka` | Production streaming, analytics, central log bus | TLS / SASL supported | `configs/example-spring-boot-kafka.yaml` |
| `file` | Local debugging, JSONL archive, ad-hoc analysis | Filesystem permissions only | `configs/example-spring-boot-file.yaml` |
| `http-noauth` | Internal ingest behind network policy | **None** — open endpoint only | `configs/example-spring-boot-http-noauth.yaml` |
| `bigquery` | GCP analytics, long-term storage, SQL over logs | Workload Identity Federation (or ADC) | `configs/example-bigquery.yaml` |

## Kafka (`sink.type: kafka`)

```yaml
sink:
  type: kafka
  kafka:
    brokers:
      - kafka.example.com:9093
    topic: application-logs
    connect_timeout: 10s
```

- Default sink if you use project defaults without a config file
- Startup checks broker connectivity and that the topic exists
- Secured Kafka (TLS, SCRAM, etc.): see [examples/kafka](https://github.com/sanjuthomas/log-forwarder/tree/main/examples/kafka) in the repo

**You need:** network access to brokers, topic created (or auto-create enabled on the cluster).

### Kafka delivery semantics

The built-in Kafka sink is tuned for **at-least-once** delivery with low latency — **not** exactly-once.

| Setting | Value | Effect |
|---------|-------|--------|
| `RequiredAcks` | Leader only (`RequireOne`) | Record is considered published when the partition leader acks. Not all in-sync replicas must ack. |
| `Async` | `false` | Each batch write blocks until the broker responds. |
| Idempotent producer | Not enabled | No broker-side deduplication of producer retries. |
| Watermark persist | Debounced (`watch.state.flush_interval`, default `1s`) | On crash or `kill -9`, the on-disk watermark may lag published records by up to one flush window. |

**Duplicate window:** If the process dies after Kafka accepts a batch but before the watermark file is flushed, restart re-tails those bytes and publishes them again. Graceful shutdown (`SIGINT` / `SIGTERM`) flushes watermarks and shrinks this window.

**Tuning duplicate risk vs I/O:**

- Lower `watch.state.flush_interval` (or set `0` to persist every line) — smaller duplicate window, more disk writes
- Raise `flush_interval` under heavy load — fewer writes, larger duplicate window
- `watch.state.flush_every` — optional count-based flush in addition to the interval

**Consumers** should assume duplicates and dedupe (natural keys in the JSON, Kafka compaction, or downstream idempotent ingest).

**Not configurable today:** `RequireAll` acks and Kafka idempotent producer settings are fixed in code. Stricter durability would be a future enhancement — track via GitHub issues if you need it.

See also [[Watermarks and Restarts]] and [[Configuration-Reference#Kafka sink]].

## File (`sink.type: file`)

```yaml
sink:
  type: file
  file:
    path: /var/lib/log-forwarder/forwarded.jsonl
```

- Appends **one JSON object per line** (JSONL / newline-delimited JSON)
- Creates parent directories if needed
- Output path must **not** be inside a watched log directory

**You need:** write permission on the output path.

## HTTP no auth (`sink.type: http-noauth`)

```yaml
sink:
  type: http-noauth
  http_noauth:
    url: http://log-ingest.internal:8081/ingest
    method: POST
    timeout: 30s
```

- Sends `Content-Type: application/json` POST body per record
- **No credentials** — no `Authorization` header, API keys, or OIDC
- Intended for trusted networks or services that do not require app-level auth

**You need:** a reachable HTTP endpoint that returns 2xx on success.

> **OAuth2 / API keys:** not supported by this built-in sink. Use [[Custom-Extensions#Custom-sink|a custom sink]] or a future dedicated sink type.

## BigQuery (`sink.type: bigquery`)

Uses the [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api) default stream for low-latency, at-least-once ingestion. JSON records are mapped to an existing table schema at startup.

```yaml
sink:
  type: bigquery
  bigquery:
    project_id: my-gcp-project
    dataset: application_logs
    table: forwarder_events
    credentials_file: /etc/log-forwarder/gcp-wif.json
    connect_timeout: 30s
    write_retries: true
```

| Setting | Description |
|---------|-------------|
| `project_id` | GCP project containing the dataset |
| `dataset` | BigQuery dataset ID |
| `table` | Destination table (must exist with a schema matching pipeline JSON fields) |
| `credentials_file` | Optional path to a Workload Identity Federation credential config JSON (from `gcloud iam workload-identity-pools create-cred-config`). When omitted, Application Default Credentials are used (`GOOGLE_APPLICATION_CREDENTIALS`, GCE/GKE metadata, etc.) |
| `connect_timeout` | Startup table metadata check timeout (default `30s`) |
| `write_retries` | Enable Storage Write API append retries (default `true`) |

**Authentication:** Prefer [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation) over long-lived service account keys. Generate a credential config file with `gcloud iam workload-identity-pools create-cred-config`, mount it into the forwarder, and set `credentials_file` (or point `GOOGLE_APPLICATION_CREDENTIALS` at the same file).

**You need:** a pre-created table, `roles/bigquery.dataEditor` (or narrower table-level grant) on the impersonated service account, and network egress to `bigquery.googleapis.com` and `bigquerystorage.googleapis.com`.

See [`configs/example-bigquery.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-bigquery.yaml) and [[Configuration-Reference#BigQuery sink]].

## One sink per process — common patterns

### Production: Kafka only

One forwarder on each app server (or shared log volume), config with `sink.type: kafka`, unique `application_id` in enrichers.

### Development: file sink

Same watch/transform config as prod, but `sink.type: file` and output under `./output/`. Use a **separate** `watch.state.path` so you do not share watermarks with prod.

### Same logs, two destinations

Run **two forwarder processes**:

| Process | Config | Watermark |
|---------|--------|-----------|
| A | `sink.type: kafka` | `/var/lib/forwarder-a/watermarks.json` |
| B | `sink.type: file` | `/var/lib/forwarder-b/watermarks.json` |

Same `watch.sources`, different `sink` and `state.path`.

## Changing sink on restart

Watermarks do **not** include sink information. If you change `sink.type` and restart:

- Tailing **continues from the saved offset**
- Only **new** lines after that point go to the new sink
- Old lines are **not** automatically re-published to the new destination

To backfill the new sink, clear watermarks — [[Watermarks and Restarts]].

## Custom sinks

For S3, authenticated HTTP, or proprietary endpoints, build a custom binary that registers your sink type. BigQuery is a built-in sink — see above. See [[Custom-Extensions#Custom-sink]].

---

## Full sink reference

Each forwarder **process** configures exactly **one** sink. There is no multi-sink or fan-out in a single config — `sink.type` selects a single implementation (`kafka`, `file`, `http-noauth`, or a custom registered type), and every published record goes to that destination only.

Built-in types: `kafka` (default), `file`, `http-noauth`, and `bigquery`. Register custom sinks (for example HTTP with OAuth2) in a custom binary — see [[Custom-Extensions#Custom-sink|Custom sink]]. Watermark behavior is described [[Watermarks-and-Restarts|above]]; it is tied to the process and source files, not to which sink type is active.

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
