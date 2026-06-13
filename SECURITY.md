# Security Policy

## Supported versions

Security fixes are applied to the default branch (`main`) and backported to tagged
releases when practical. There are no older release lines under active support
until the first semver tag is published.

| Version / branch | Supported |
|------------------|-----------|
| `main`           | Yes       |
| Latest release tag | Yes (when available) |
| Older tags       | No        |

## Reporting a vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Report them privately using [GitHub Security Advisories](https://github.com/sanjuthomas/log-forwarder/security/advisories/new):

1. Open **Report a vulnerability** on the repository Security tab.
2. Describe the issue, affected components (watcher, pipeline, sink, config, etc.),
   and steps to reproduce.
3. Include version information (`git describe --tags`, commit SHA, or Docker tag).

Redact secrets from configs, logs, and screenshots (Kafka credentials, TLS keys,
API tokens).

## What to expect

| Step | Target timeline |
|------|-----------------|
| Acknowledgement | Within 72 hours |
| Initial assessment | Within 7 days |
| Fix or mitigation plan | Depends on severity; we will keep you updated |

We may ask for additional details. When a fix is ready, we will coordinate
disclosure and credit you in the advisory unless you prefer to remain anonymous.

## Safe harbor

We consider good-faith security research conducted in line with this policy to be
authorized. Do not access data that is not yours, degrade service for other
users, or exfiltrate production logs containing personal information.

## Security-related configuration

When deploying log-forwarder, follow usual operational hygiene:

- Do not commit credentials in YAML configs; use environment variables or secret
  stores.
- Restrict network access to metrics and health endpoints.
- Keep Docker images and Go dependencies up to date (Dependabot PRs on this repo).

For non-security bugs, use the [bug report issue template](https://github.com/sanjuthomas/log-forwarder/issues/new/choose).
