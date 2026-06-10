#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f docker-compose.kafka.yaml)
FORWARDER_URL="${FORWARDER_URL:-http://127.0.0.1:18083}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-120}"

cleanup() {
	"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Starting Kafka smoke stack..."
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

echo "Waiting for at least one published record..."
publish_deadline=$((SECONDS + 30))
until curl -sf "${FORWARDER_URL}/metrics" | grep -qE 'log_forwarder_lines_published_total(\{[^}]*\})? [1-9]'; do
	if (( SECONDS >= publish_deadline )); then
		echo "timeout waiting for log_forwarder_lines_published" >&2
		curl -sf "${FORWARDER_URL}/metrics" >&2 || true
		"${COMPOSE[@]}" logs log-forwarder >&2 || true
		exit 1
	fi
	sleep 1
done

echo "Consuming from Kafka topic logs ..."
messages="$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server localhost:9092 \
	--topic logs \
	--from-beginning \
	--max-messages 2 \
	--timeout-ms 30000 2>/dev/null || true)"

if [[ -z "${messages}" ]]; then
	echo "no messages consumed from topic logs" >&2
	"${COMPOSE[@]}" logs log-forwarder >&2 || true
	exit 1
fi

message_count="$(grep -c 'kafka integration smoke' <<<"${messages}" || true)"
if [[ "${message_count}" -ne 1 ]]; then
	echo "expected exactly one kafka integration smoke message, got ${message_count}:" >&2
	echo "${messages}" >&2
	exit 1
fi

if grep -q 'second kafka smoke line' <<<"${messages}"; then
	echo "WARN line should have been filtered out before publish:" >&2
	echo "${messages}" >&2
	exit 1
fi

if ! grep -q '"application_id"' <<<"${messages}"; then
	echo "expected JSON enricher field not found in consumed messages:" >&2
	echo "${messages}" >&2
	exit 1
fi

metrics="$(curl -sf "${FORWARDER_URL}/metrics")"
if ! grep -q 'log_forwarder_lines_filtered' <<<"${metrics}"; then
	echo "metrics missing lines_filtered counter" >&2
	echo "${metrics}" >&2
	exit 1
fi

echo "Kafka smoke test passed."
echo "Sample message:"
head -1 <<<"${messages}"
