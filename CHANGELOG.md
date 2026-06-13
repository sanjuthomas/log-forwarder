# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `./scripts/check-coverage.sh` — fails CI when any `internal/*` package drops below 80% statement coverage.
- `govulncheck` in CI for dependency vulnerability scanning.
- `CODEOWNERS`, `SUPPORT.md`, and `ROADMAP.md` for contributor and adopter onboarding.
- GitHub Actions stale-issue workflow for inactive issues and PRs.
- Community health files: Code of Conduct, security policy, Dependabot, and `AGENTS.md`.
- Unit tests raising sub-80% packages (`config`, `deadletter`, `metrics`, `sink`) to ≥80% coverage.

### Changed

- Minimum Go version raised to **1.26.4** (stdlib security fixes; enables clean `govulncheck` in CI).
- OpenTelemetry dependencies upgraded to v1.44 / contrib v0.69; semconv aligned to v1.41.0.
- Dependabot Go groups split (OpenTelemetry vs other modules) for easier review.
- `CONTRIBUTING.md`, PR template, and wiki Development page aligned with coverage and CI checks.
- README links to changelog, support channels, and positioning vs other log agents.

### Fixed

- OpenTelemetry resource schema mismatch after dependency upgrades (`semconv` v1.26 → v1.41.0).
- Docker and kafka-smoke CI failures when `go.mod` requires Go 1.25+ but the image used Go 1.22.
- Stale release wording in `SECURITY.md` (tags exist; GitHub Release notes are tracked separately).

---

## Release history

Git tags `v0.1.0` through `v0.5.0` mark development milestones on `main`. Published
[GitHub Releases](https://github.com/sanjuthomas/log-forwarder/releases) and release notes
will accompany future tagged versions. Until then, see the
[commit history](https://github.com/sanjuthomas/log-forwarder/commits/main) for recent changes.

| Tag | Notes |
|-----|-------|
| `v0.5.0` | Latest development tag (pre–GitHub Release workflow) |
| `v0.1.0`–`v0.4.0` | Earlier development milestones |

When maintainers cut the next release (e.g. `v0.6.0`), move the **Unreleased** section
above into a dated version heading and publish GitHub release notes from that entry.
