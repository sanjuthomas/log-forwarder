# Contributing to log-forwarder

Thank you for your interest in contributing. This project tails log files, transforms them into structured JSON, and forwards them to a sink (Kafka, file, HTTP, or custom). All contributions — bug reports, docs, tests, and code — are welcome.

## Before you start

1. **Search existing issues** — someone may already be working on it.
2. **Open an issue first** for non-trivial changes (new features, behavior changes, refactors). Small fixes (typos, obvious bugs) can go straight to a PR with a clear description.
3. **Read the wiki** — especially [How It Works](https://github.com/sanjuthomas/log-forwarder/wiki/How-It-Works), [Development](https://github.com/sanjuthomas/log-forwarder/wiki/Development), and [Testing](https://github.com/sanjuthomas/log-forwarder/wiki/Testing).

## How to report an issue

Use [GitHub Issues](https://github.com/sanjuthomas/log-forwarder/issues/new/choose) and pick the closest template:

| Template | Use when |
|----------|----------|
| **Bug report** | Something breaks or behaves unexpectedly |
| **Feature request** | New capability or enhancement |
| **Documentation** | Wiki, README, config examples, or comments are wrong or missing |

Include as much of the following as you can:

- **What you expected** vs **what happened**
- **Steps to reproduce** (config snippet, log sample, commands run)
- **Environment** (OS, Go version, sink type, Docker image tag if relevant)
- **Version** (`git describe --tags` or Docker tag)

Do **not** paste secrets (Kafka passwords, TLS keys, API tokens). Redact hostnames if needed.

## How to submit a pull request

`main` is **protected** — changes land only through pull requests. Direct pushes to `main` are blocked.

### 1. Fork and branch

```bash
git clone https://github.com/<your-user>/log-forwarder.git
cd log-forwarder
git remote add upstream https://github.com/sanjuthomas/log-forwarder.git
git checkout -b fix/short-description   # or feat/, docs/, test/
```

Branch naming examples from this repo:

- `fix/issue-42-watermark-flush` — bug fix tied to an issue
- `feat/issue-15-custom-filter` — new feature
- `docs/kafka-delivery-semantics` — documentation only
- `test/improve-watcher-coverage` — tests only

### 2. Make your changes

- **Keep PRs focused** — one logical change per PR is easier to review.
- **Match existing style** — naming, error wrapping, config validation patterns, test layout.
- **Add or update tests** when behavior changes. CI runs `go test ./...` and `go test -race ./...`.
- **Update docs** when user-facing behavior or config changes (wiki pages under `wiki/`, example configs under `configs/`, or `docs/` as appropriate).

### 3. Run checks locally

```bash
go test ./...
go test -race ./...
go build -o bin/log-forwarder ./cmd/log-forwarder
```

Optional but recommended before Kafka-related changes:

```bash
./scripts/kafka-smoke.sh
./scripts/kafka-deadletter-smoke.sh
```

See [Testing](https://github.com/sanjuthomas/log-forwarder/wiki/Testing) for integration tests and Docker smoke scripts.

### 4. Commit

Write clear commit messages in the imperative mood:

```
Recover publish flusher after async flush errors (#84).

Fix stale flushErr gate that blocked subsequent flushes.
```

Reference issue numbers with `#123` or `Closes #123` in the PR description (not necessarily every commit).

### 5. Open the pull request

Push your branch and open a PR against `main`:

```bash
git push -u origin fix/short-description
```

Fill in the PR template. Link related issues (`Closes #123` or `Fixes #123`).

### 6. Review and CI

Every PR to `main` must pass:

| Check | What it runs |
|-------|----------------|
| **build** | `go test ./...`, `go test -race ./...`, build main and custom example |
| **kafka-smoke** | Kafka round-trip and dead-letter smoke scripts |
| **maintainer-review** | External contributors: `@sanjuthomas` must approve. Maintainer-authored PRs skip this check. |

**Review policy**

| PR author | Approval required |
|-----------|-------------------|
| **@sanjuthomas** (maintainer) | No — merge when CI is green |
| **Anyone else** | Yes — maintainer must approve before merge |

Direct pushes to `main` remain blocked; all changes go through a pull request.

Address review feedback with new commits on the same branch when offered; avoid force-pushing unless you need to rebase after `main` moves.

## Development notes

### Project layout

See [Development](https://github.com/sanjuthomas/log-forwarder/wiki/Development) on the wiki for package structure.

### Custom extensions

Built-in parsers, transformers, enrichers, filters, and sinks are registered via `init()`. User extensions belong in a **custom binary** — see [Custom Extensions](https://github.com/sanjuthomas/log-forwarder/wiki/Custom-Extensions) and `examples/custom/`.

### Wiki changes

Wiki source lives in `wiki/` and syncs to GitHub Wiki on merge to `main`. Edit files under `wiki/` in your PR; do not edit the GitHub Wiki UI directly (changes would be overwritten).

Preview locally:

```bash
./scripts/sync-wiki.sh   # if available; see .github/workflows/sync-wiki.yml
```

### Coding guidelines

- Prefer **minimal, focused diffs** over large refactors mixed with feature work.
- Reuse existing abstractions (`pipeline`, `sink`, `state`, config validation helpers).
- Comments only for non-obvious logic — code should mostly speak for itself.
- **Document exported APIs** — every exported type, function, and method in production packages should have a brief godoc comment. Test files do not need per-test comments.
- Config keys: add validation in `internal/config/`, document in wiki Config Catalog and Configuration Reference.
- Avoid breaking changes to default config behavior without discussion in an issue first.

## Copyright and licensing

This project is released under the [MIT License](LICENSE). By opening a pull request, you agree that your contributions are licensed under the same terms.

### File headers

Every Go source file should begin with a copyright and SPDX license identifier:

```go
// Copyright (c) 2026 Your Name
// SPDX-License-Identifier: MIT
```

| Situation | What to do |
|-----------|------------|
| **New file you create** | Use **your name** (or your legal entity) and the current year in the copyright line. |
| **Existing file you modify** | **Keep existing copyright lines.** Do not replace the original author. If your change is substantial, add a second line with your name and year. |
| **Minor edits** | Updating an existing copyright line is not required for small fixes. |

Example after a substantial contribution to an existing file:

```go
// Copyright (c) 2026 Sanju Thomas
// Copyright (c) 2026 Jane Contributor
// SPDX-License-Identifier: MIT
```

The SPDX line is always `MIT` — the same license as the project root `LICENSE` file.

### Helper script

To prepend the standard header to Go files that are missing it:

```bash
./scripts/add-copyright-header.sh
```

Pass your name when contributing new files, or edit the header manually. Maintainers use this script for bulk updates; contributors should set their own copyright on files they add.

### Pull request attestation

When you open a PR, confirm in the template that new files include your copyright and that all contributions are offered under the MIT License (see [License](#license) below).

## Release and Docker images

Maintainers tag releases (`v*`) on `main`; Docker images publish to `sanjuthomas/log-forwarder` on tag push. Contributors do not need to cut releases — merged PRs are included in the next maintainer release.

## Questions?

Open a [Discussion](https://github.com/sanjuthomas/log-forwarder/discussions) or comment on a related issue if you are unsure whether to proceed. We would rather align on approach early than review a large surprise PR.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
