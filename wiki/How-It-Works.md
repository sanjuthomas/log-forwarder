# How It Works

Think of log-forwarder as a small pipeline that runs on the machine where log files live (or where they are mounted).

```
Log files  →  read new lines  →  group into events  →  parse fields  →  filter  →  add metadata  →  publish JSON
```

## The stages (in order)

| Stage | What it does | You configure |
|-------|----------------|---------------|
| **Watcher** | Finds log files, tails new lines, handles rotation | `watch` |
| **Parser** | Groups physical lines into one logical event (important for stack traces) | `parser` |
| **Transform** | Turns each event into named fields (timestamp, level, message, …) | `transform` |
| **Filter** | Drops records that do not match predicate rules (optional; omit to pass all) | `filter` |
| **Enrich** | Adds extra fields (hostname, app name, environment) | `enrichers` |
| **Sink** | Sends one JSON object to your destination | `sink` |

Each successfully processed event is published as **one JSON object**. With the file sink, that is one **JSONL** line (JSON + newline).

Filtered records are **not** enriched or published, but watermarks still advance so tailing does not stall on noise you chose to drop. See [[Built-in-Components#Built-in filters|Built-in filters]], [[Configuration-Reference#filter|filter config]], and [[Filtering Vendor Noise]] for a cost-saving vendor-appliance walkthrough.

## One process, one sink

Each running forwarder process has exactly **one** `sink.type` in its config. Every record goes to that destination only.

| Goal | Approach |
|------|----------|
| Logs to Kafka only | One process, `sink.type: kafka` |
| Logs to a file for debugging | One process, `sink.type: file` |
| Same logs to Kafka **and** a file | **Two processes**, two configs, two watermark files |
| Prod Kafka + dev file copy | Two processes (recommended), not one config |

## What gets stored on disk (besides your logs)

| File | Purpose |
|------|---------|
| **Watermark file** (`watch.state.path`) | Remembers how far each source log file was read — see [[Watermarks and Restarts]] |
| **File sink output** | JSONL destination when using `sink.type: file` |
| **Forwarder log** (`logging.file`, optional) | The forwarder's own diagnostic log |

Watermarks track **source file progress for this process**. They do **not** record which sink type you use.

## What a published record looks like

After transform and enrich, a typical record might look like:

```json
{
  "timestamp": "2026-06-08 10:16:22.901",
  "level": "ERROR",
  "pid": "18432",
  "thread": "nio-8080-exec-5",
  "logger": "c.a.b.controller.PaymentController",
  "message": "Payment failed for invoiceId=inv_98765\norg.springframework.dao...",
  "_path": "/var/log/billing/application.log",
  "application_id": "billing-service",
  "environment": "prod",
  "hostname": "app-server-01"
}
```

- Parsed fields come from **transform**
- `_path` is the source log file (always added)
- `application_id`, `environment`, etc. come from **enrichers**

If a line cannot be parsed and `on_error: wrap` is set, you still get a record with `_raw`, `_path`, and `_error` instead of structured fields.

## Read next

- [[Configuration Guide]] — map each stage to YAML
- [[Built-in-Components]] — parsers, transforms, filters, enrichers, sinks
- [[Choosing a Sink]] — pick Kafka, file, or HTTP
- [[Spring Boot Logs]] — multiline stack traces
