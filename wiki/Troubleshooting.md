# Troubleshooting

## Forwarder refuses to start

### `sink unavailable at startup`

The configured sink failed its connectivity check (Kafka broker/topic or HTTP endpoint unreachable; file sink directory not writable).

**Fix:** Ensure the destination is up and reachable, then restart. For HTTP `http-noauth`, something must be listening on the configured URL.

### Config validation error

Examples:

- Watermark or file sink inside a watched directory
- Missing `sink.kafka` when `type: kafka`
- Invalid regex in `transform.pattern` or `parser.start_pattern`

**Fix:** Read the error message, correct YAML, restart.

### Corrupt watermark file

The startup error mentions `watermark file ... is corrupt or unreadable` and suggests recovery steps.

**Fix:** Restart once with `--reset-watermarks`, or set `watch.state.reset_on_corrupt: true` in config. The corrupt file is archived as `{path}.corrupt.{timestamp}` and tailing starts from the beginning (re-ships already-processed lines). Alternatively, rename or delete the watermark file manually.

## No logs appearing at the sink

| Check | Action |
|-------|--------|
| Watch path | Confirm log files exist under `watch.sources` / `watch.paths` |
| Patterns | Filenames must match globs (`*.log`, etc.) |
| Watermark ahead of content | You may have already tailed past those lines — see [[Watermarks and Restarts]] |
| Transform mismatch | Lines may be wrapped as `_raw` with `_error` — consume sink output and inspect |
| `on_error: skip` | Unparseable lines are dropped silently (debug log only) |

Enable metrics and check:

- `log_forwarder_files_watched` > 0
- `log_forwarder_lines_read` increasing
- `log_forwarder_lines_published` vs `log_forwarder_lines_skipped`

## Duplicate records in Kafka / downstream

Usually caused by:

- Deleting watermarks and re-reading from the beginning
- Running **two processes** with the **same** watermark file
- Log rotation with unexpected inode behavior (rare)

**Fix:** One watermark file per process; avoid resetting watermarks unless you intend to re-ship.

## Spring Boot stack trace split across many records

You need the **multiline parser**. Use an [example-spring-boot-*.yaml](https://github.com/sanjuthomas/log-forwarder/tree/main/configs) config, not a single-line setup.

## HTTP sink: connection refused at startup

Expected if nothing listens on `http_noauth.url`. Start your ingest service first, or use file/Kafka sink for local testing.

## HTTP sink: 4xx / 5xx on publish

Non-2xx responses are retried then fail. Check ingest service logs. The built-in `http-noauth` sink does not send auth headers.

## Kafka: publish failures after startup

Startup only checks connectivity once. Later broker outages show as:

```
publish failed, retrying
```

Monitor `log_forwarder_kafka_publish_failures` and broker health.

## Log rotation surprises

When `logrotate` creates a new file at the same path:

- **New inode** → forwarder tails the **new** file from the **beginning**
- Old file’s watermark does not apply to the new inode

This is usually what you want for rotated logs.

## Getting help

1. Run with `logging.level: debug` (if supported in your build) for more detail
2. Check forwarder stderr / `logging.file`
3. Inspect watermark file and one sample source log line
4. Open an issue on [GitHub](https://github.com/sanjuthomas/log-forwarder/issues) with config (redacted secrets), forwarder version, and symptom
