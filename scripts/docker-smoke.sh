#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f docker-compose.smoke.yaml)
FORWARDER_URL="${FORWARDER_URL:-http://127.0.0.1:18081}"
OUTPUT_FILE="${OUTPUT_FILE:-docker/output/smoke-filter/records.jsonl}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-60}"

cleanup() {
	"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

rm -rf docker/output/smoke-filter
mkdir -p docker/output/smoke-filter

echo "Starting docker filter smoke stack..."
"${COMPOSE[@]}" up -d --build --wait

echo "Waiting for forwarder readiness at ${FORWARDER_URL}/ready ..."
deadline=$((SECONDS + MAX_WAIT_SECONDS))
until curl -sf "${FORWARDER_URL}/ready" >/dev/null; do
	if (( SECONDS >= deadline )); then
		echo "timeout waiting for ${FORWARDER_URL}/ready" >&2
		"${COMPOSE[@]}" logs log-forwarder >&2 || true
		exit 1
	fi
	sleep 2
done
echo "Forwarder is ready."

if [[ ! -f "${OUTPUT_FILE}" ]]; then
	echo "expected output file missing: ${OUTPUT_FILE}" >&2
	"${COMPOSE[@]}" logs log-forwarder >&2 || true
	exit 1
fi

record_count="$(grep -c 'docker filter smoke error' "${OUTPUT_FILE}" || true)"
if [[ "${record_count}" -ne 1 ]]; then
	echo "expected exactly one ERROR record in ${OUTPUT_FILE}, got ${record_count}" >&2
	cat "${OUTPUT_FILE}" >&2 || true
	exit 1
fi

if grep -q 'docker filter smoke info' "${OUTPUT_FILE}"; then
	echo "INFO record should have been filtered out:" >&2
	cat "${OUTPUT_FILE}" >&2
	exit 1
fi

metrics="$(curl -sf "${FORWARDER_URL}/metrics")"
if ! grep -q 'log_forwarder_lines_filtered' <<<"${metrics}"; then
	echo "metrics missing lines_filtered counter" >&2
	echo "${metrics}" >&2
	exit 1
fi

echo "Docker filter smoke test passed."
echo "Published record:"
grep 'docker filter smoke error' "${OUTPUT_FILE}"
