# Choosing a Sink

The **sink** is where structured JSON records go after parsing and enrichment. Each forwarder process uses **exactly one** sink.

## Comparison

| Sink type | Best for | Auth | Example config |
|-----------|----------|------|----------------|
| `kafka` | Production streaming, analytics, central log bus | TLS / SASL supported | `configs/example-spring-boot-kafka.yaml` |
| `file` | Local debugging, JSONL archive, ad-hoc analysis | Filesystem permissions only | `configs/example-spring-boot-file.yaml` |
| `http-noauth` | Internal ingest behind network policy | **None** — open endpoint only | `configs/example-spring-boot-http-noauth.yaml` |

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

> **OAuth2 / API keys:** not supported by this built-in sink. Use a [custom sink](https://github.com/sanjuthomas/log-forwarder/blob/main/README.md#custom-sink) or a future dedicated sink type.

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

For BigQuery, S3, authenticated HTTP, etc., build a custom binary that registers your sink type. See the [developer README](https://github.com/sanjuthomas/log-forwarder/blob/main/README.md#custom-sink).
