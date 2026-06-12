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
  flush_interval: 100ms  # optional; default for multiline — publishes trailing record after quiet time
```

A new event starts when a line matches `start_pattern`. Everything until the next match is joined with newlines and passed to transform as one blob. The trailing record is also flushed after `flush_interval` with no new lines (default `100ms`).

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

**`on_full: block` (default)** — when the watcher → pipeline buffer is full, tailing pauses until the pipeline drains. This applies backpressure and preserves every line.

**`on_full: drop`** — when the buffer is full, new lines are discarded permanently. Dropped lines never reach the sink and **never update watermarks**. Restart does **not** replay them; under sustained overload the forwarder can keep dropping new log bytes indefinitely while `log_forwarder_pipeline_buffer_dropped` rises. Use only when gaps in forwarded logs are acceptable. See [[Configuration-Reference#pipelineon_full-block-vs-drop]] and [[Troubleshooting#Permanent log loss from pipelineon_full-drop]].

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

When enabled, `GET /health` and `GET /ready` are also available on the same port (both include `process_id`). The scrape endpoint includes forwarder counters plus process gauges such as `process_cpu_utilization` (percent of one CPU core, like `top`) and `process_memory_usage` (RSS bytes). See [[Monitoring]] for the full metrics catalog, scrape setup, and alert guidance.

## `atc` — controller registration (optional)

When using **log-forwarder-atc**, enable metrics and register the instance endpoint:

```yaml
metrics:
  enabled: true
  host: 0.0.0.0
  port: 10001

atc:
  enabled: true
  url: http://localhost:8090/api/instances
```

The forwarder calls ATC only at startup (`PUT`) and shutdown (`DELETE`). While running, ATC polls `/health` and `/ready` here. See [[Monitoring#8-log-forwarder-atc-integration]].

## Validation errors at startup

The forwarder validates config before running. Common mistakes:

- Watermark or file sink path **inside** a watched directory
- `sink.kafka` missing when `sink.type: kafka`
- Multiline parser without `start_pattern`
- Regex transform without `pattern`
- `atc.enabled` without `metrics.enabled`

Fix the YAML and restart.
