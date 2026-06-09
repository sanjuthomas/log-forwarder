# log-forwarder — User Guide

**log-forwarder** tails log files on a server, turns each log event into structured JSON, adds useful metadata, and sends the result to a destination you choose (Kafka, a local file, or an HTTP endpoint).

This wiki is written for **operators and integrators** — people who need to run and configure the forwarder, not extend its Go code. For build instructions, API details, and custom extensions, see the [project README](https://github.com/sanjuthomas/log-forwarder/blob/main/README.md).

## What you use it for

- Ship application logs to **Kafka** for streaming analytics or observability pipelines
- Write **JSONL** files for local debugging or batch loading
- POST logs to an **internal HTTP ingest** endpoint (no built-in OAuth — see [[Choosing a Sink]])
- Parse **Spring Boot** default console logs, including **multiline stack traces**, as one JSON record per event

## Quick start

```bash
git clone https://github.com/sanjuthomas/log-forwarder.git
cd log-forwarder
go build -o bin/log-forwarder ./cmd/log-forwarder
./bin/log-forwarder -config configs/example.yaml
```

See [[Installation and First Run]] for a full walkthrough.

## How to read this wiki

| Page | When to read it |
|------|-----------------|
| [[How It Works]] | You want a mental model before configuring anything |
| [[Configuration Guide]] | You are writing or editing a YAML config |
| [[Choosing a Sink]] | You need to pick Kafka, file, or HTTP |
| [[Spring Boot Logs]] | Your apps log in Spring Boot / Logback text format |
| [[Watermarks and Restarts]] | Restarts, log rotation, or re-sending old lines |
| [[Example Configs]] | Copy-paste starting points |
| [[Troubleshooting]] | Something is not tailing, parsing, or publishing |

## Important rules of thumb

1. **One process → one sink.** A single forwarder instance sends every record to exactly one destination. To fan out to Kafka *and* a file, run two forwarder processes with different configs and different watermark files.
2. **Watermarks are per process and per source file**, not per sink. If you change the sink and restart, tailing resumes where it left off; old lines are not automatically re-sent to the new destination.
3. **Config is YAML.** One file controls watch paths, parsing, enrichment, sink, and optional metrics.
