# Spring Boot Logs

Spring Boot’s default **console** format (Logback) looks like this:

```
2026-06-08 10:16:22.901  ERROR 18432 --- [nio-8080-exec-5] c.a.b.controller.PaymentController       : Payment failed for invoiceId=inv_98765
org.springframework.dao.DataIntegrityViolationException: could not execute statement
        at org.springframework.orm.jpa.vendor.HibernateJpaDialect.convertHibernateAccessException(HibernateJpaDialect.java:290)
Caused by: org.postgresql.util.PSQLException: ERROR: ...
```

## The problem

Without a multiline parser, each physical line becomes a separate JSON record. Stack trace lines fail regex parsing and end up as wrapped `_raw` blobs.

## The solution

1. **`multiline` parser** — group the header line and all continuation lines into one event; idle-flush the trailing record after `flush_interval` (default `100ms`)
2. **`regex` transform** — extract timestamp, level, pid, thread, logger, message
3. **`(?s)` on the message group** — capture the full stack trace in `message`

## Ready-made configs

Same parser/transform/enrichers; only the sink differs:

| Sink | Config file |
|------|-------------|
| Kafka | [example-spring-boot-kafka.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-spring-boot-kafka.yaml) |
| File (JSONL) | [example-spring-boot-file.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-spring-boot-file.yaml) |
| HTTP (no auth) | [example-spring-boot-http-noauth.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-spring-boot-http-noauth.yaml) |

## Run

```bash
mkdir -p logs/spring-boot
./bin/log-forwarder -config configs/example-spring-boot-kafka.yaml
```

Point Spring Boot logging to a file under `logs/spring-boot/` (or adjust `watch.sources` to your real log path).

## Key config blocks

```yaml
watch:
  sources:
    - path: ./logs/spring-boot
      patterns: ["*.log", "*.log.*", "*.out"]

parser:
  type: multiline
  start_pattern: '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
  flush_interval: 100ms

transform:
  type: regex
  pattern: '^(?s)(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<level>\S+)\s+(?P<pid>\d+)\s+---\s+\[\s*(?P<thread>[^\]]+?)\s*\]\s+(?P<logger>\S+)\s+:\s+(?P<message>.*)$'
  on_error: wrap

enrichers:
  - type: static
    fields:
      application_id: billing-service
      environment: dev
  - type: host
```

## Example JSON output (error with stack trace)

```json
{
  "timestamp": "2026-06-08 10:16:22.901",
  "level": "ERROR",
  "pid": "18432",
  "thread": "nio-8080-exec-5",
  "logger": "c.a.b.controller.PaymentController",
  "message": "Payment failed for invoiceId=inv_98765\norg.springframework.dao.DataIntegrityViolationException: ...",
  "_path": "/var/log/billing/application.log",
  "application_id": "billing-service",
  "environment": "dev",
  "hostname": "app-server-01"
}
```

## Field reference (header line)

| Field | Example | Notes |
|-------|---------|-------|
| `timestamp` | `2026-06-08 10:16:22.901` | Date + time with milliseconds |
| `level` | `ERROR` | Log level |
| `pid` | `18432` | JVM process ID |
| `thread` | `nio-8080-exec-5` | Thread name (padding trimmed) |
| `logger` | `c.a.b.controller.PaymentController` | Logger name |
| `message` | `Payment failed...` + stack trace | Full multiline body after `:` |

## Alternative: JSON logging in Spring Boot

If you control the application, structured JSON logging (e.g. Logstash encoder) gives one JSON line per event in the file — no multiline parser needed. log-forwarder would need a JSON transform or pass-through (custom) for that format.

The configs above are for the **default text console layout** without changing the Spring Boot app.
