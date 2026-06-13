# Monitoring the forwarder

The forwarder exposes three complementary signals for operations: **HTTP metrics**, **periodic status logs**, and **process health** (via your supervisor or orchestrator).

## 1. Enable metrics

Set `metrics.enabled: true` in your config. The forwarder starts a small HTTP server that serves:

| Endpoint | Purpose |
|----------|---------|
| `GET /metrics` | Prometheus scrape endpoint (OpenTelemetry) |
| `GET /health` | Liveness probe — returns `{"status":"UP","process_id":<pid>}` |
| `GET /ready` | Readiness probe (sink, buffer, hibernate) — includes `process_id` |
| `GET /deadletters` | Dead letter batch **metadata** only (when `publish_batch.dead_letter.path` is configured) |

Both endpoints share the same `metrics.host` and `metrics.port`. `/health` is only available when metrics are enabled.

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/metrics
curl http://127.0.0.1:8080/deadletters
```

`GET /deadletters` returns a JSON array of batch metadata (`filename`, `created_at`, `event_count`, `bytes`, `failure_reason`, `sink_type`, `batch_attempts`). It does **not** return log record bodies. Retrieve spilled content from the `dead_letter.path` volume (for example `kubectl exec` in Kubernetes). Do not expose the management port to untrusted networks without authentication.

Bind to `127.0.0.1` when Prometheus runs on the same host. Use `0.0.0.0` only if a remote scraper needs direct access, and restrict access at the network layer.

## 2. Scrape with Prometheus

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

## 3. Health and readiness checks

Use `/health` for **liveness** probes. It confirms the management server is running only; it does not verify sink connectivity after startup. Both `/health` and `/ready` include `process_id` (the forwarder OS PID) so controllers and operators can correlate probes with ATC registration.

Example:

```json
{"status":"UP","process_id":12345}
{"status":"READY","process_id":12345}
```

Use `/ready` for **readiness** probes when `metrics.enabled: true`. It returns `503` when:
- the sink fails its connectivity check (when the sink implements `sink.Checker` and `metrics.readiness.sink_check` is true)
- `pipeline buffer depth / capacity` exceeds `metrics.readiness.buffer_threshold` (default `0.8`)
- `metrics.readiness.require_files: true` and no log files are being tailed
- the forwarder is in **hibernate** mode after a failed publish batch (`reason: sink_hibernating`)

Use `/health` for liveness only during hibernate: the process is still running and blocked on backpressure, but it should not receive new traffic until `/ready` is `200` again.

**Kubernetes example:**

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 30
readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

Optional readiness tuning:

```yaml
metrics:
  enabled: true
  host: 0.0.0.0
  port: 8080
  readiness:
    path: /ready
    buffer_threshold: 0.8
    sink_check: true
    require_files: false
    sink_check_timeout: 5s
```

Pair readiness with Prometheus alerts on `log_forwarder_publish_failures` and `log_forwarder_pipeline_buffer_depth` for sustained sink or backlog issues.

## 4. Log-based status

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
| `log forwarder started` | Process is up; includes sources, sink type, metrics address, and `atc_url` when ATC is enabled |
| `sink connectivity verified` | Startup sink check passed |
| `sink unavailable at startup` | Forwarder refused to start — destination unreachable |
| `atc registration status` | ATC lifecycle (`status`: `disabled`, `registering`, `registered`, `failed`, `deregistering`, `deregistered`, `deregistration_failed`) — see [[Log Forwarder ATC]] |
| `shutdown signal received` | SIGINT/SIGTERM received; deregister runs before runner cancel |
| `publish failed, retrying` | Transient publish error; check sink destination and network |
| `forwarder stopped` | Clean or error shutdown |

## 5. Metrics reference

**Forwarder metrics:**

| Metric | Description |
|--------|-------------|
| `log_forwarder_lines_read` | Newly appended log lines read from watched files (excludes at-least-once replays on restart) |
| `log_forwarder_lines_replayed` | Lines re-read after restart because the on-disk watermark lagged behind published data |
| `log_forwarder_lines_published` | Lines published to the configured sink |
| `log_forwarder_lines_filtered` | Lines dropped by configured filters (after transform) |
| `log_forwarder_lines_skipped` | Lines dropped (`transform.on_error: skip`) |
| `log_forwarder_pipeline_buffer_dropped` | Lines dropped when `pipeline.on_full: drop` and buffer is full |
| `log_forwarder_transform_errors` | Transform failures |
| `log_forwarder_timestamp_parse_failures` | Records that fell back to processing time during timestamp normalization |
| `log_forwarder_publish_failures` | Failed sink publish attempts |
| `log_forwarder_publish_truncations` | Records truncated to fit `pipeline.max_publish_bytes` |
| `log_forwarder_publish_batch_flushes` | Publish buffer flushes (`reason`: `size`, `timer`, `shutdown`, `wake`; `result`: `success`, `hibernate`, `dead_letter`, `error`) |
| `log_forwarder_publish_hibernating` | `1` when the forwarder is in sink hibernate mode after a failed batch flush |
| `log_forwarder_publish_dead_letter_batches` | Publish batches written to dead letter storage |
| `log_forwarder_publish_consecutive_dlq_batches` | Consecutive dead-letter batches without a successful sink publish |
| `log_forwarder_publish_batch_size` | Records per publish batch flush (histogram) |
| `log_forwarder_publish_batch_bytes` | Serialized JSON bytes per publish batch flush (histogram) |
| `log_forwarder_publish_buffer_active_bytes` | Serialized JSON bytes waiting in the publish buffer |
| `log_forwarder_publish_retries` | Retries after a publish failure |
| `log_forwarder_publish_duration` | Sink publish latency (histogram, seconds) |
| `log_forwarder_watermark_flush_errors` | Failed watermark persist attempts |
| `log_forwarder_files_watched` | Files currently being tailed |
| `log_forwarder_pipeline_buffer_depth` | Events queued between watcher and pipeline |
| `log_forwarder_pipeline_buffer_capacity` | Configured `pipeline.buffer_size` |

**Line accounting:** `lines_read` counts only bytes appended after the file size observed at tail open (or all lines when tailing from the beginning). On restart with a stale watermark, bytes that were already in the file but not yet persisted to disk are counted as `lines_replayed`, not `lines_read`. Together:

```
lines_read + lines_replayed ≈ lines_published + lines_filtered + lines_skipped + pipeline_buffer_dropped + (lines waiting in publish batch) + (lines waiting in multiline parser buffer)
```

Replayed lines may still be published again (at-least-once delivery). With default `parser.flush_interval` (`100ms` for multiline), the multiline parser buffer is usually empty within ~100ms of the last line — a gap between `lines_read` and `lines_published` alone is not data loss. Check `lines_replayed`, filter/skip/drop counters, publish batch timing, and whether `parser.flush_interval` is `0`.

**Process and runtime metrics:**

| Metric | Description |
|--------|-------------|
| `process_memory_usage` | Process RSS in bytes |
| `process_cpu_utilization` | CPU used as percent of one core (`1.3` = 1.3%; `100` = one core fully busy; can exceed 100 when using multiple cores). Same basis as `top` / psutil. Value is averaged over the interval since the **previous scrape** — align `scrape_interval` with how reactive you want the number; **first scrape is `0`**. |
| `process_cpu_time` | Cumulative process CPU time in seconds (user/system counters) |
| `go_memory_used` | Go runtime memory in use |
| `go_memory_allocated` | Heap memory allocated by the application |
| `go_cpu_time` | CPU time spent by the Go runtime |
| `go_goroutine_count` | Number of live goroutines |

Machine-readable catalog: [`examples/config-catalog.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/examples/config-catalog.yaml) (`metrics_catalog` section).

## 6. What to alert on

| Signal | Suggested condition | Likely cause |
|--------|---------------------|--------------|
| Publish failures | `rate(log_forwarder_publish_failures[5m]) > 0` sustained | Sink unreachable, auth/TLS issue, or network partition |
| Publish retries | `rate(log_forwarder_publish_retries[5m])` rising | Intermittent sink or timeout pressure |
| Publish latency | `histogram_quantile(0.95, rate(log_forwarder_publish_duration_bucket[5m]))` high | Sink load, network latency, or slow endpoint |
| Buffer backlog | `log_forwarder_pipeline_buffer_depth / log_forwarder_pipeline_buffer_capacity > 0.8` sustained | Pipeline slower than ingest; risk of backpressure |
| No files watched | `log_forwarder_files_watched == 0` while logs are expected | Wrong watch paths, patterns, or permissions |
| Buffer drops | `rate(log_forwarder_pipeline_buffer_dropped[5m]) > 0` | `pipeline.on_full: drop` under overload — **permanent** log loss; not recoverable on restart |
| Read/publish gap | `rate(log_forwarder_lines_read[5m])` >> `rate(log_forwarder_lines_published[5m])` | Transform skips, filter drops, buffer drops (`on_full: drop`), persistent publish failures, or pipeline stall |
| Replay after restart | `rate(log_forwarder_lines_replayed[5m]) > 0` | On-disk watermark lagged behind published data (at-least-once replay); tighten `watch.state.flush_interval` or use `flush_interval: 0` if duplicates are costly |
| High filter rate | `rate(log_forwarder_lines_filtered[5m])` high vs `lines_read` | Expected when filtering noisy logs; tune rules if too aggressive |
| Memory growth | `process_memory_usage` or `go_memory_used` trending up without stabilizing | Possible leak or sustained backlog |
| High CPU | `process_cpu_utilization` sustained above your baseline (e.g. `> 80` for one full core) | Heavy regex/multiline parsing, publish backlog, or undersized host |
| Process down | `/health` failing or scrape target `up == 0` | Crash, OOM kill, or misconfigured port |

## 7. Quick checks

```bash
curl -s http://127.0.0.1:8080/metrics | grep -E 'process_cpu_utilization|process_memory_usage'
curl -s http://127.0.0.1:8080/metrics | grep log_forwarder_lines_
curl -s http://127.0.0.1:8080/health
curl -s http://127.0.0.1:8080/ready
curl -s http://127.0.0.1:8080/metrics | head
```

Scrape at least twice (or wait one `scrape_interval`) before expecting a non-zero `process_cpu_utilization`.

## 8. log-forwarder-atc integration

Optional registration with **[log-forwarder-atc](https://github.com/sanjuthomas/log-forwarder-atc)** — a separate fleet controller that tracks agents, polls `/health` and `/ready`, and stores metric snapshots. Disabled by default.

**Full guide (both repos):** [[Log Forwarder ATC]]

### Summary

| Component | Role |
|-----------|------|
| **Forwarder** | `PUT` once after startup; `DELETE` once before shutdown. Exposes `/health`, `/ready`, `/metrics` on `metrics.host`:`metrics.port`. |
| **ATC** | Registry + fleet dashboard. Polls agents every 30s (default). No forwarder traffic while running. |

```yaml
metrics:
  enabled: true
  host: 0.0.0.0
  port: 10001

atc:
  enabled: true
  url: http://localhost:8090/api/instances
```

Registration failures log `atc registration status` at **WARN** and do **not** stop tailing or publishing.

See [[Log Forwarder ATC]], [`configs/example-atc.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-atc.yaml), and [`configs/example-spring-boot-kafka.yaml`](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-spring-boot-kafka.yaml).
