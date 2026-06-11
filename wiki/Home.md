# log-forwarder — User Guide

**log-forwarder** is a small, low-footprint alternative to Fluent Bit — written in Go, MIT-licensed, and built for teams that want a simple file → structured JSON → sink pipeline without a heavy agent.

It tails log files, optionally **filters** records with predicate rules, enriches them with metadata, and publishes through a **pluggable sink** (Kafka, file, HTTP, or custom). Extend behavior by registering parsers, filters, enrichers, and sinks in your own binary — no fork required. See [[Custom-Extensions]].

```mermaid
flowchart LR
    subgraph local["Local disk"]
        logs["Log files\n(watch paths)"]
    end

    subgraph host["Forwarder host"]
        watcher["Watcher\ntail · rotate"]
        parser["Parser\nline · multiline"]
        transform["Transform\ndelimiter / regex"]
        filter["Filter\nfield · compound · …"]
        enrich["Enrich\nhost · static · …"]
        sinkIf["Sink interface\nPublish · Close · Check"]
        watcher --> parser --> transform --> filter --> enrich --> sinkIf
    end

    subgraph sinks["Pluggable sinks (one per config)"]
        kafkaSink["kafka"]
        fileSink["file"]
        httpSink["http-noauth"]
        customSink["custom · bigquery · …"]
    end

    subgraph destinations["Destinations"]
        kafkaDest["Kafka topic"]
        fileDest["JSONL file"]
        httpDest["Open HTTP endpoint"]
        customDest["Your API / warehouse"]
    end

    logs -->|"read new lines"| watcher
    sinkIf -->|"sink.type"| kafkaSink
    sinkIf --> fileSink
    sinkIf --> httpSink
    sinkIf --> customSink
    kafkaSink --> kafkaDest
    fileSink --> fileDest
    httpSink --> httpDest
    customSink --> customDest
```

The pipeline always ends at the same `sink.Sink` interface. `sink.type` in config selects the implementation; only one sink is active at runtime. Register additional types (for example `http-oauth2` or BigQuery streaming) in a custom binary — see [[Custom-Extensions]].

The [repository README](https://github.com/sanjuthomas/log-forwarder/blob/main/README.md) is a short index; **this wiki** has the full documentation.

## Wiki index

| Topic | Page |
|-------|------|
| Install, build, run | [[Installation and First Run]] |
| Pipeline overview | [[How It Works]] |
| YAML config (overview) | [[Configuration Guide]] |
| YAML config (full reference) | [[Configuration-Reference]] |
| Config key catalog (what / when) | [[Config-Catalog]] |
| Kafka, file, HTTP sinks | [[Choosing a Sink]] |
| Spring Boot / multiline logs | [[Spring Boot Logs]] |
| Watermarks, rotation, restarts | [[Watermarks and Restarts]] |
| Built-in parsers, transforms, filters | [[Built-in-Components]] |
| Custom parsers, sinks, filters | [[Custom-Extensions]] |
| Metrics, health, alerts | [[Monitoring]] |
| Unit, integration, smoke tests | [[Testing]] |
| Docker images and smoke tests | [[Docker]] |
| Production deployment | [[Deployment]] |
| Repo layout (contributors) | [[Development]] |
| Example configs | [[Example Configs]] |
| Common problems | [[Troubleshooting]] |

## Important rules of thumb

1. **One process → one sink.** Run separate processes with separate watermark files to fan out to multiple destinations.
2. **Watermarks are per process and per source file**, not per sink.
3. **Default `pipeline.on_full: block`** in production; `drop` causes permanent log loss under overload.
4. **`copytruncate` log rotation is not supported** — use rename/create rotation.
