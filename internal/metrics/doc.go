// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

// Package metrics exposes Prometheus metrics and HTTP health/readiness endpoints.
// When metrics are enabled, /metrics includes application counters, process_cpu_utilization,
// process_memory_usage, and OpenTelemetry host/runtime instrumentation.
// See examples/config-catalog.yaml (metrics_catalog) and wiki/Monitoring.md.
package metrics
