# log-forwarder

A lightweight Go service that tails log files, parses and transforms lines into structured JSON, optionally filters records, enriches them with metadata, and publishes through a pluggable sink (Kafka, file, HTTP, or custom).

**Documentation:** [GitHub Wiki](https://github.com/sanjuthomas/log-forwarder/wiki) — install, configuration, sinks, watermarks, monitoring, deployment, and custom extensions.

## Quick start

```bash
git clone https://github.com/sanjuthomas/log-forwarder.git
cd log-forwarder
go build -o bin/log-forwarder ./cmd/log-forwarder
./bin/log-forwarder -config configs/example.yaml
```

See [Installation and First Run](https://github.com/sanjuthomas/log-forwarder/wiki/Installation-and-First-Run) on the wiki for requirements, defaults, and verification steps.

## Wiki index

| Topic | Wiki page |
|-------|-----------|
| Install, build, run | [Installation and First Run](https://github.com/sanjuthomas/log-forwarder/wiki/Installation-and-First-Run) |
| Pipeline overview | [How It Works](https://github.com/sanjuthomas/log-forwarder/wiki/How-It-Works) |
| YAML config (overview) | [Configuration Guide](https://github.com/sanjuthomas/log-forwarder/wiki/Configuration-Guide) |
| YAML config (full reference) | [Configuration-Reference](https://github.com/sanjuthomas/log-forwarder/wiki/Configuration-Reference) |
| Kafka, file, HTTP sinks | [Choosing a Sink](https://github.com/sanjuthomas/log-forwarder/wiki/Choosing-a-Sink) |
| Spring Boot / multiline logs | [Spring Boot Logs](https://github.com/sanjuthomas/log-forwarder/wiki/Spring-Boot-Logs) |
| Watermarks, rotation, restarts | [Watermarks and Restarts](https://github.com/sanjuthomas/log-forwarder/wiki/Watermarks-and-Restarts) |
| Built-in components | [Built-in-Components](https://github.com/sanjuthomas/log-forwarder/wiki/Built-in-Components) |
| Custom parsers, sinks, filters | [Custom-Extensions](https://github.com/sanjuthomas/log-forwarder/wiki/Custom-Extensions) |
| Metrics, health, alerts | [Monitoring](https://github.com/sanjuthomas/log-forwarder/wiki/Monitoring) |
| Tests and smoke scripts | [Testing](https://github.com/sanjuthomas/log-forwarder/wiki/Testing) |
| Docker images | [Docker](https://github.com/sanjuthomas/log-forwarder/wiki/Docker) |
| Production deployment | [Deployment](https://github.com/sanjuthomas/log-forwarder/wiki/Deployment) |
| Repo layout | [Development](https://github.com/sanjuthomas/log-forwarder/wiki/Development) |
| Example configs | [Example Configs](https://github.com/sanjuthomas/log-forwarder/wiki/Example-Configs) |
| Troubleshooting | [Troubleshooting](https://github.com/sanjuthomas/log-forwarder/wiki/Troubleshooting) |

## Repository resources

| Path | Contents |
|------|----------|
| [`configs/`](configs/) | Example YAML configs |
| [`examples/kafka/`](examples/kafka/) | Kafka security example configs |
| [`examples/custom/`](examples/custom/) | Custom binary with registered extensions |
| [`docs/integration-test-cases.txt`](docs/integration-test-cases.txt) | E2E test catalog |
| [`docs/production-battle-test.txt`](docs/production-battle-test.txt) | Staging / battle-test checklist |
| [`docs/docker.md`](docs/docker.md) | Container and sidecar notes |
| [`wiki/`](wiki/) | Wiki source (auto-synced to GitHub Wiki on merge to `main`; run `./scripts/sync-wiki.sh` locally to preview) |
| [`scripts/`](scripts/) | Docker and Kafka smoke tests |

## Docker

```bash
docker pull sanjuthomas/log-forwarder:latest
./scripts/docker-smoke.sh
./scripts/kafka-smoke.sh
```

See [Docker](https://github.com/sanjuthomas/log-forwarder/wiki/Docker) on the wiki for compose stacks and CI smoke tests.

## License

This project is licensed under the [MIT License](LICENSE).
