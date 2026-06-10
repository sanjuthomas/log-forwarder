# Configuration Guide

Configuration is a single **YAML** file passed with `-config`. The sections below follow the order data flows through the pipeline.

> **Full reference:** Every field, example, and edge case is documented in [[Configuration-Reference]]. Use this page for a quick orientation; use the reference when tuning production configs.

## Minimal mental model

```yaml
watch:       # which log files to tail
parser:      # optional — how to group lines (default: one line = one event)
transform:   # how to extract fields from each event
enrichers:   # extra metadata on every record
sink:        # where JSON records go (one destination per process)
pipeline:    # internal buffer settings
logging:     # forwarder's own log output (optional)
metrics:     # Prometheus endpoint (optional)
```

## `watch` — which files to tail

Tell the forwarder **where** to look and **which filenames** match.

```yaml
watch:
  poll: 1s
  state:
    path: /var/lib/log-forwarder/watermarks.json
  sources:
    - path: /var/log/my-app
      patterns:
        - "*.log"
        - "*.log.*"
```

| Field | What to set |
|-------|-------------|
| `poll` | How often to rescan for new/rotated files (`1s`, `500ms`, …) |
| `sources` | List of directories, each with its own `patterns` |
| `state.path` | Watermark file for **this process** — see [[Watermarks and Restarts]] |

**Tip:** Use absolute paths in production for `sources[].path` and `state.path`.

Alternative when every directory uses the same patterns:

```yaml
watch:
  paths: [./logs/app, ./logs/nginx]
  patterns: ["*.log", "*.out"]
```

## `parser` — grouping lines (optional)

| `type` | Use when |
|--------|----------|
| `line` (default) | Each physical line is one event |
| `multiline` | Stack traces or continuation lines belong to the previous header line |

For multiline (typical for Spring Boot errors):

```yaml
parser:
  type: multiline
  start_pattern: '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
```

A new event starts when a line matches `start_pattern`. Everything until the next match is joined with newlines and passed to transform as one blob.

See [[Spring Boot Logs]] for a full example.

## `transform` — structured fields

| `type` | Use when |
|--------|----------|
| `delimiter` | Fields separated by tab, pipe, comma, etc. |
| `regex` | Log format follows a pattern (Spring Boot, nginx, custom) |

**Tab-separated example:**

```yaml
transform:
  type: delimiter
  delimiter: "\t"
  columns: [timestamp, level, message]
  on_error: wrap
```

**Regex example** — use named groups for field names:

```yaml
transform:
  type: regex
  pattern: '^(?P<level>\S+)\s+(?P<message>.*)$'
  on_error: wrap
```

### `on_error` — unparseable lines

| Value | Behavior |
|-------|----------|
| `wrap` (default) | Still publish: `{ "_raw": "...", "_path": "...", "_error": "..." }` |
| `skip` | Drop the line (counted in metrics as skipped) |

## `enrichers` — metadata on every record

```yaml
enrichers:
  - type: static
    fields:
      application_id: billing-service
      environment: prod
  - type: host
```

| Type | Adds |
|------|------|
| `static` | Fixed key/value pairs you define |
| `host` | `hostname` of the machine running the forwarder |

## `sink` — destination (one per process)

See [[Choosing a Sink]] for Kafka vs file vs HTTP and operational notes.

```yaml
sink:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: logs
```

## `pipeline` — buffering

```yaml
pipeline:
  buffer_size: 1024
  on_full: block
```

Usually leave defaults unless you are tuning backpressure under heavy load.

## `logging` — forwarder's own logs

```yaml
logging:
  level: info
  format: text
  file: /var/log/log-forwarder/forwarder.log
  status_interval: 30s
```

Do not put `logging.file` inside a watched directory.

## `metrics` — Prometheus (optional)

```yaml
metrics:
  enabled: true
  host: 127.0.0.1
  port: 8080
  path: /metrics
```

When enabled, `GET /health` is also available on the same port.

## Validation errors at startup

The forwarder validates config before running. Common mistakes:

- Watermark or file sink path **inside** a watched directory
- `sink.kafka` missing when `sink.type: kafka`
- Multiline parser without `start_pattern`
- Regex transform without `pattern`

Fix the YAML and restart.
