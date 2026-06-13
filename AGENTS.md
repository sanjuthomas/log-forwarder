# AGENTS.md

Guidance for AI coding agents working in **log-forwarder** — a Go log tailing agent that parses, transforms, filters, enriches, and publishes JSON records to Kafka, file, HTTP, or custom sinks.

Human contributors: see [CONTRIBUTING.md](CONTRIBUTING.md). Full user docs live in the [GitHub Wiki](https://github.com/sanjuthomas/log-forwarder/wiki) (source in `wiki/`).

## Project summary

| Item | Detail |
|------|--------|
| Language | Go 1.26+ (`go.mod`, CI, Dockerfile) |
| License | MIT |
| Module | `github.com/sanjuthomas/log-forwarder` |
| Default branch | `main` (protected; PRs only) |
| Binary | `cmd/log-forwarder` — built-ins only |
| Extensions | `examples/custom/` — register types in `init()`, build a custom binary |

**Pipeline order:** watcher → parser → transform → filter → enrich → sink.

**Operational rules (do not break without explicit discussion):**

- One process → one sink. Fan-out = multiple processes with separate watermark files.
- Watermarks are per process and per source file, not per sink.
- Default `pipeline.on_full: block` in production; `drop` causes permanent loss under overload.
- `copytruncate` rotation is **not** supported — use rename/create rotation.

## Repository layout

```
cmd/log-forwarder/          Main entrypoint
configs/                    Example YAML configs (user-facing)
examples/custom/            Custom binary with registered extensions
examples/kafka/             Kafka security example configs
internal/
  config/                   YAML load, validation, type registries
  watcher/                  File tailing, rotation, line events
  parser/                   line, multiline parsers
  transform/                delimiter, regex, tab transformers
  filter/                   field, compound predicates
  enrich/                   static, host enrichers
  pipeline/                 Orchestration, publish batch, dead letter, hibernate
  sink/                     kafka, file, http-noauth (+ Register for custom)
  state/                    Watermark persistence
  metrics/                  OpenTelemetry → Prometheus HTTP
  deadletter/               Dead letter spill files
  runner/                   Concurrent watcher + pipeline lifecycle
  atc/                      Optional instance registration
  integration/              End-to-end tests
wiki/                       Wiki source (synced to GitHub Wiki on merge to main)
scripts/                    lint, copyright, smoke tests
docs/                       Test catalogs, docker notes, checklists
```

All production code is under `internal/`. There is no stable public Go API — extension authors import `internal/*` within this module (same as `examples/custom/`).

## Coding conventions

### Style and quality

- **Minimal, focused diffs** — one logical change; no drive-by refactors.
- **Match existing patterns** — error wrapping with `%w`, config validation in `internal/config/`, registry + `Register()` in `init()` for pluggable types.
- **Formatting/linting:** `./scripts/lint.sh` (gofumpt, goimports, staticcheck via golangci-lint). Config: `.golangci.yml`.
- **Comments:** godoc on exported types/functions in production packages; skip per-test commentary.
- **Tests:** add/update when behavior changes; integration tests in `internal/integration/` for E2E paths.
- **Coverage:** every `internal/*` package with production code must stay at **≥80%** statement coverage. Excluded from this bar: `cmd/`, `examples/`, and `internal/integration/` (E2E only, no in-package statements). When you change behavior in a package below 80%, add tests until it meets the bar before opening a PR.

### New Go files

Every `.go` file must start with:

```go
// Copyright (c) 2026 <Author Name>
// SPDX-License-Identifier: MIT
```

Keep existing copyright lines when editing files. Verify with `./scripts/check-copyright-header.sh`.

### Adding a config key

1. Add struct field + validation in `internal/config/`.
2. Wire behavior in the relevant package (`pipeline`, `sink`, etc.).
3. Document in `wiki/Configuration-Reference.md` and `wiki/Config-Catalog.md`.
4. Add or update an example under `configs/` if helpful.
5. Add config and/or integration tests.

### Adding a built-in component (parser, transform, filter, enricher, sink)

1. Implement the package interface (`parser.Parser`, `transform.Transformer`, etc.).
2. Register factory in package `init()` via `Register(name, factory)`.
3. Register type name for validation (`config.Register*Type` is called from `Register`).
4. Document in wiki Built-in-Components / Configuration Reference.
5. Add tests.

Custom types for end users belong in a **custom binary** (`examples/custom/`), not in `cmd/log-forwarder`.

### OpenTelemetry / metrics

Metrics use OpenTelemetry SDK with Prometheus export (`internal/metrics/`). When upgrading OTel dependencies:

- Keep **all** `go.opentelemetry.io/*` modules on compatible versions.
- Update `semconv` import to match the schema URL expected by the SDK (e.g. `go.opentelemetry.io/otel/semconv/v1.26.0` must align with contrib/sdk versions — mismatches cause `conflicting Schema URL` test failures).

## Commands

### Build and run

```bash
go build -o bin/log-forwarder ./cmd/log-forwarder
./bin/log-forwarder -config configs/example.yaml
go build -o bin/log-forwarder-custom ./examples/custom
```

### Required checks before proposing a PR

```bash
./scripts/lint.sh                    # or ./scripts/lint.sh --fix
./scripts/check-copyright-header.sh
./scripts/check-coverage.sh          # each internal/* package ≥80%
go mod tidy && git diff --exit-code go.mod go.sum
go test ./...
go test -race ./...
go build -o bin/log-forwarder ./cmd/log-forwarder
go build -o bin/log-forwarder-custom ./examples/custom
```

Install golangci-lint: `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`

### Optional (Kafka / Docker changes)

```bash
./scripts/docker-smoke.sh
./scripts/kafka-smoke.sh
./scripts/kafka-deadletter-smoke.sh
```

### Focused tests

```bash
go test ./internal/integration/ -v -run TestE2E_
go test ./internal/pipeline/ -v -run TestPipeline
go test ./internal/config/ -v
```

## CI (must pass on PRs to `main`)

| Job | Purpose |
|-----|---------|
| **build** | golangci-lint, copyright headers, `go mod tidy`, tests, `-race`, **coverage gate**, **govulncheck**, builds |
| **kafka-smoke** | Kafka round-trip + dead-letter Docker smoke |
| **maintainer-review** | External contributors need `@sanjuthomas` approval |
| **stale** | Inactive issues/PRs marked stale after 60 days |

## Documentation map

| Need | Location |
|------|----------|
| Pipeline behavior | `wiki/How-It-Works.md` |
| Config overview | `wiki/Configuration-Guide.md` |
| Config reference | `wiki/Configuration-Reference.md`, `wiki/Config-Catalog.md` |
| Custom extensions | `wiki/Custom-Extensions.md`, `examples/custom/main.go` |
| Testing | `wiki/Testing.md`, `docs/integration-test-cases.txt` |
| Docker | `wiki/Docker.md`, `docs/docker.md` |
| Contributor workflow | `CONTRIBUTING.md` |
| Security reporting | `SECURITY.md` (private advisories, not public issues) |

**Wiki rule:** edit files under `wiki/` in PRs — do **not** edit the GitHub Wiki UI (auto-sync overwrites it).

## Branch and commit conventions

- Branch prefixes: `fix/`, `feat/`, `docs/`, `test/`, `chore/`
- Commits: imperative mood; reference issues in PR description (`Closes #123`)
- PRs: one logical topic; fill `.github/PULL_REQUEST_TEMPLATE.md`
- Do **not** commit secrets, coverage artifacts, or local scratch files

## Common pitfalls for agents

| Pitfall | Instead |
|---------|---------|
| Editing GitHub Wiki directly | Edit `wiki/*.md` in the repo |
| Adding custom sinks to `cmd/log-forwarder` | Use `examples/custom/` pattern |
| Large unrelated refactors in a feature PR | Split or keep scope minimal |
| Breaking default config behavior silently | Discuss in an issue first; update wiki + examples |
| OpenTelemetry partial upgrades | Upgrade OTel modules together; fix `semconv` import |
| Ignoring watermark semantics in publish path | Watermarks advance only after successful publish (see wiki) |
| `pipeline.on_full: drop` as default | Default is `block`; drop loses data permanently |
| Merging with a package below 80% coverage | Run `./scripts/check-coverage.sh` and add tests |

## Dependabot notes

- Go groups: OpenTelemetry (`go.opentelemetry.io/*`) vs other modules (see `.github/dependabot.yml`).
- OTel bumps often require semconv alignment and may raise minimum Go version — update Dockerfile and CI together.
- GitHub Actions Dependabot PRs may show red on `maintainer-review` until a maintainer approves; that is expected.

## Questions

Open a [GitHub Discussion](https://github.com/sanjuthomas/log-forwarder/discussions) or issue when approach is unclear — prefer alignment before large changes.
