# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Community health files: Code of Conduct, security policy, and this changelog.
- Dependabot configuration for Go modules and GitHub Actions.
- CI checks for `gofmt`, `go vet`, `go mod tidy`, and copyright headers.

### Changed

- Minimum Go version raised to **1.25** (Dockerfile, CI, and `go.mod`).
- OpenTelemetry dependencies upgraded to v1.44 / contrib v0.69; semconv aligned to v1.41.0.
- Dependabot Go groups split (OpenTelemetry vs other modules) for easier review.

### Fixed

- OpenTelemetry resource schema mismatch after dependency upgrades (`semconv` v1.26 → v1.41.0).
- Docker and kafka-smoke CI failures when `go.mod` requires Go 1.25+ but the image used Go 1.22.

---

## Release history

There are no semver tags yet. When maintainers cut the first release (`v0.1.0` or
similar), move the **Unreleased** section above into a dated version heading and
publish GitHub release notes from that entry.

Recent work on `main` is visible in the
[commit history](https://github.com/sanjuthomas/log-forwarder/commits/main).
