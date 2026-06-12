<p align="center">
  <img src="assets/logo.svg" alt="log-forwarder" width="340">
</p>

<p align="center">
  <a href="https://github.com/sanjuthomas/log-forwarder/actions/workflows/ci.yml"><img src="https://github.com/sanjuthomas/log-forwarder/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="MIT License"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white" alt="Go 1.22+"></a>
</p>

**log-forwarder** is a small, low-footprint alternative to Fluent Bit — written in Go, MIT-licensed, and built for teams that want a simple file → structured JSON → sink pipeline without a heavy agent.

Download the source, add your sink, and run. It tails log files, transforms and enriches records, and ships them to Kafka, files, HTTP, or your own integration via a **custom binary** — no fork required. Same operational ideas you expect (watermarks, rotation, dead letters, metrics), in a **~15 MB static binary**.

**Small agent. Your sink. Your rules.**

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
| Config key catalog (what / when) | [Config-Catalog](https://github.com/sanjuthomas/log-forwarder/wiki/Config-Catalog) |
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
| [`examples/config-catalog.yaml`](examples/config-catalog.yaml) | Master list of example configs and Prometheus `metrics_catalog` |
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

## Contributing

Contributions are welcome — bug reports, documentation, tests, and code. See **[CONTRIBUTING.md](CONTRIBUTING.md)** for:

- How to [open an issue](https://github.com/sanjuthomas/log-forwarder/issues/new/choose) (bug, feature, docs)
- How to submit a pull request (branch workflow, tests, CI checks)
- Coding guidelines and wiki sync notes

## License

This project is licensed under the [MIT License](LICENSE).
