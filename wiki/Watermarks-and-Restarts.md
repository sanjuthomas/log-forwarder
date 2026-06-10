# Watermarks and Restarts

Watermarks remember **how far this forwarder process has read** in each source log file. They let you restart without re-sending lines that were already published successfully.

## What is stored

Default file: `.log-forwarder/watermarks.json` (override with `watch.state.path`).

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
| Key (`/var/log/...`) | Absolute or resolved path of the **source** log file being tailed |
| `offset` | Bytes read from the start of that file (including newlines) |
| `inode` | OS file identity — used to detect rotation |

**Not stored:** sink type, Kafka topic, HTTP URL, or any destination detail. Watermarks belong to the **process + source file**, not to the sink.

## Lifecycle

| Event | What happens |
|-------|----------------|
| First time tailing a file | No entry → start at **beginning** of file |
| Record published successfully | Watermark updated to end of that event (last line for multiline) |
| Process restart, same file & inode | Resume from saved `offset` — log: `resuming file from watermark` |
| Log rotation (same path, new inode) | Ignore old offset → tail **new** file from beginning |
| Process crash mid-line | Last fully published record’s offset is saved; may re-send partial event on restart depending on timing |

## One process, one watermark file

Each forwarder process should have its own `watch.state.path`. If two processes share a watermark file, they will fight over offsets.

| Setup | Watermark files |
|-------|-----------------|
| One forwarder → Kafka | One file, e.g. `/var/lib/log-forwarder/watermarks.json` |
| Two forwarders, same logs, different sinks | **Two files**, e.g. `watermarks-kafka.json` and `watermarks-file.json` |

## Changing sink and restarting

**Scenario:** You ran with Kafka, then edit config to use `sink.type: file`, and restart.

| What happens | Detail |
|--------------|--------|
| Watermark | **Unchanged** — still at previous offset |
| Tailing | Resumes from that offset |
| Old lines | **Not** re-sent to the file sink |
| New lines | Go to the file sink |

To **backfill** the file sink with historical logs, you must reset the watermark (below).

## How to re-read from the beginning

### One file

Edit the watermark JSON and **remove** that file’s entry from `files`, then restart the forwarder.

### All watched files

Delete the entire watermark file (or move it aside), then restart. Every watched file is tailed from offset `0` and lines are published again to the **current** sink.

**Warning:** Re-publishing can **duplicate** data in Kafka or your ingest pipeline. Coordinate with downstream consumers.

## Production tips

- Use an **absolute path** for `watch.state.path`
- Keep the watermark file **outside** watched log directories (config validation enforces this)
- Back up or document watermark location before major config changes
- After `logrotate`, a new inode means the forwarder reads the new file from the start — expected behavior

## Related

- [[Choosing a Sink]] — one sink per process; separate watermarks when running multiple processes
- [[Troubleshooting]] — “no new records after restart”

## Detailed reference

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

## Multiline parser and watermarks

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
