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
