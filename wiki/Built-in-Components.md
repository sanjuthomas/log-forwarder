# Built-in components

Built-in parsers, transformers, enrichers, filters, and sinks registered in the default binary.

## Built-in transformers

## `delimiter`

Splits each line on a configurable delimiter string. Defaults to tab (`\t`) when `delimiter` is omitted.

- If `columns` is set, values are mapped to those field names; extra columns become `field_N`.
- If `columns` is omitted, fields are named `field_0`, `field_1`, …

**Input:**

```
2024-01-01T00:00:00Z	INFO	service started
```

**Config:**

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

Pipe-delimited example:

```yaml
transform:
  type: delimiter
  delimiter: "|"
  columns:
    - timestamp
    - level
    - message
```

**Output record:**

```json
{
  "timestamp": "2024-01-01T00:00:00Z",
  "level": "INFO",
  "message": "service started",
  "_path": "/path/to/file.log"
}
```

## `tab`

Backward-compatible alias for `delimiter` with tab. Equivalent to `type: delimiter` and `delimiter: "\t"`.

## `regex`

Parses lines with a Go regular expression. Use **named capture groups** for field names.

**Input:**

```
ERROR connection refused
```

**Config:**

```yaml
transform:
  type: regex
  pattern: '^(?P<level>\S+)\s+(?P<message>.*)$'
  on_error: wrap
```

**Output record:**

```json
{
  "level": "ERROR",
  "message": "connection refused",
  "_path": "/path/to/file.log"
}
```

## Transform errors

When a line cannot be parsed:

- **`wrap`** — publish a record with `_raw`, `_path`, and `_error` fields
- **`skip`** — log at debug level and drop the line

Every successfully parsed record also includes `_path` (source file path).

## Built-in parsers

### `line`

Default. Each physical line from the watcher becomes one record for the transformer.

### `multiline`

Buffers lines until the next line matches `start_pattern`, then emits the joined record (newline-separated). The emitted event's offset is the byte position after the **last** line in that record. Continuation lines do not emit events or advance watermarks until the record is committed. Incomplete buffers are flushed on pipeline shutdown (graceful stop). See [[Watermarks-and-Restarts#Multiline-parser-and-watermarks|Multiline parser and watermarks]].

## Built-in enrichers

### `static`

Adds fixed key/value pairs from `fields` to every record.

### `host`

Adds `hostname` (from `os.Hostname()`, or `"unknown"` on failure).

## Built-in filters

Filtering runs on the structured record produced by the transformer. Built-in predicate types:

### `field`

Compares a field in the transformed record.

| `op` | Passes when |
|------|-------------|
| `eq` | Field equals `value` |
| `neq` | Field does not equal `value` |
| `in` | Field matches one of `values` |
| `not_in` | Field matches none of `values` |

When a referenced field is **missing**, the rule uses `on_missing` (`drop` by default). A dropped rule causes the record to be filtered out unless a compound `match: any` rule still passes.

### `compound`

Groups nested `rules` with `match: all` (AND) or `match: any` (OR).

Custom filters implement the `filter.Predicate` interface and register via `filter.Register` — see [[Custom-Extensions]].

## Built-in sinks

Every destination implements the same interface:

```go
type Sink interface {
    Publish(ctx context.Context, payload []byte) error
    Close() error
}
```

Sinks may also implement `sink.Checker` for a startup connectivity probe. Select the implementation with `sink.type` in config (see [[Choosing-a-Sink]]).

| Type | Description |
|------|-------------|
| `kafka` | Publish JSON records to a Kafka topic (default) |
| `file` | Append JSONL to a local file |
| `http-noauth` | POST JSON to an open HTTP endpoint (no credentials) |

Register custom types with `sink.Register` in a custom binary — for example BigQuery streaming or HTTP with OAuth2. See [[Custom-Extensions#Custom-sink]].
