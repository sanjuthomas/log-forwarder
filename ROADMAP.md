# Roadmap

High-level direction for **log-forwarder**. This is not a commitment schedule —
priorities shift based on issues, PRs, and production feedback.

See [open issues](https://github.com/sanjuthomas/log-forwarder/issues) and
[Discussions](https://github.com/sanjuthomas/log-forwarder/discussions) for active work.

## Now (maintainer focus)

- Keep `main` green: CI, coverage ≥80% per `internal/*` package, Dependabot, smoke tests.
- Document and stabilize the custom-binary extension model (`examples/custom/`).
- Operational hardening: watermarks, dead letter, hibernate/wake, metrics/readiness.

## Next (likely near-term)

- Published GitHub Releases with notes from [CHANGELOG.md](CHANGELOG.md) (tags exist; release workflow TBD).
- Pre-built release binaries (GoReleaser or similar) for common platforms.
- Comparison and migration guides (Fluent Bit, Fluentd, Vector) on the wiki.
- Seed [`good first issue`](https://github.com/sanjuthomas/log-forwarder/labels/good%20first%20issue) tasks for new contributors.

## Later / under consideration

- Additional built-in sinks or enrichers driven by community demand.
- OpenSSF Scorecard improvements and optional badge.
- Helm chart or Kubernetes operator (today: sidecar/DaemonSet docs on wiki).

## Out of scope (for now)

- Multi-sink fan-out in a **single** process (by design: one process → one sink).
- `copytruncate` log rotation support.
- Stable public Go API outside this module (extensions compile against `internal/*` in-tree).

## How to influence the roadmap

1. Open a [Discussion](https://github.com/sanjuthomas/log-forwarder/discussions) to propose an idea.
2. If there is agreement, open a feature issue or PR.
3. For large changes, discuss in an issue **before** a large PR.
