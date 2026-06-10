# Development

Repository layout for contributors. For running tests and smoke scripts, see [[Testing]].

## Project layout

```
cmd/log-forwarder/     Main entrypoint (built-in parsers/transformers/enrichers/filters only)
configs/               Example and local config files
docker/                Sample log data for docker compose
Dockerfile             Multi-stage image (Alpine runtime, non-root forwarder)
docker-compose.yaml    Local container smoke test (file sink)
docker-compose.smoke.yaml  Filter smoke test stack (file sink, ERROR-only)
docker-compose.kafka.yaml  Kafka round-trip smoke test stack
scripts/docker-smoke.sh    Automated file-sink filter verification
scripts/kafka-smoke.sh     Automated Kafka publish/consume verification
scripts/kafka-deadletter-smoke.sh  Kafka sink failure → dead letter spill + metrics
examples/custom/       Custom binary with registered extensions
internal/
  config/              YAML loading and validation
  metrics/             OpenTelemetry metrics and HTTP endpoints
  watcher/             File tailing and rotation detection
  parser/              Parser registry and built-ins (line, multiline)
  transform/           Transformer registry and built-ins (delimiter, regex, tab)
  filter/              Predicate registry and built-ins (field, compound)
  enrich/              Enricher registry and built-ins (static, host)
  pipeline/            Transform → filter → enrich → publish orchestration
  sink/                Pluggable sinks (kafka, file, http-noauth)
```
