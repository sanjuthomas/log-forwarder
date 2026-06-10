#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f docker-compose.kafka-dlq.yaml)
FORWARDER_URL="${FORWARDER_URL:-http://127.0.0.1:18084}"
LOG_FILE="${ROOT}/docker/sample-data/kafka-dlq-smoke.log"
DLQ_LINE='2024-01-01T00:00:02Z	ERROR	kafka deadletter smoke'
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-120}"

cleanup() {
	: >"${LOG_FILE}"
	"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

: >"${LOG_FILE}"

echo "Starting Kafka dead letter smoke stack..."
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
echo "Forwarder is ready (Kafka sink check passed)."

echo "Stopping Kafka to force publish failures..."
"${COMPOSE[@]}" stop kafka >/dev/null

echo "Appending log line while Kafka is down..."
printf '%s\n' "${DLQ_LINE}" >>"${LOG_FILE}"

echo "Waiting for dead letter batch metric..."
dlq_deadline=$((SECONDS + 60))
until curl -sf "${FORWARDER_URL}/metrics" | grep -qE 'log_forwarder_publish_dead_letter_batches_total(\{[^}]*\})? [1-9]'; do
	if (( SECONDS >= dlq_deadline )); then
		echo "timeout waiting for log_forwarder_publish_dead_letter_batches" >&2
		curl -sf "${FORWARDER_URL}/metrics" >&2 || true
		"${COMPOSE[@]}" logs log-forwarder >&2 || true
		exit 1
	fi
	sleep 1
done

echo "Verifying dead letter file in forwarder container..."
dlq_files="$("${COMPOSE[@]}" exec -T log-forwarder sh -c 'ls -1 /dlq/*.jsonl 2>/dev/null || true')"
if [[ -z "${dlq_files}" ]]; then
	echo "no dead letter JSONL files found under /dlq" >&2
	"${COMPOSE[@]}" exec -T log-forwarder ls -la /dlq >&2 || true
	"${COMPOSE[@]}" logs log-forwarder >&2 || true
	exit 1
fi

dlq_content="$("${COMPOSE[@]}" exec -T log-forwarder sh -c 'cat /dlq/*.jsonl')"
if ! grep -q 'kafka deadletter smoke' <<<"${dlq_content}"; then
	echo "dead letter file missing expected message:" >&2
	echo "${dlq_content}" >&2
	exit 1
fi

metrics="$(curl -sf "${FORWARDER_URL}/metrics")"
if ! grep -qE 'log_forwarder_publish_batch_flushes_total\{[^}]*result="dead_letter"' <<<"${metrics}"; then
	echo "metrics missing publish batch flush with result=dead_letter" >&2
	echo "${metrics}" >&2
	exit 1
fi

if grep -qE 'log_forwarder_lines_published_total(\{[^}]*\})? [1-9]' <<<"${metrics}"; then
	echo "expected no successful sink publishes during dead letter smoke" >&2
	echo "${metrics}" >&2
	exit 1
fi

echo "Restarting Kafka and verifying dead-lettered line was not published..."
"${COMPOSE[@]}" start kafka >/dev/null
kafka_deadline=$((SECONDS + MAX_WAIT_SECONDS))
until "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do
	if (( SECONDS >= kafka_deadline )); then
		echo "timeout waiting for Kafka to restart" >&2
		exit 1
	fi
	sleep 2
done

sleep 5
messages="$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server localhost:9092 \
	--topic logs \
	--from-beginning \
	--max-messages 1 \
	--timeout-ms 10000 2>/dev/null || true)"
if grep -q 'kafka deadletter smoke' <<<"${messages}"; then
	echo "dead-lettered line should not appear in Kafka topic:" >&2
	echo "${messages}" >&2
	exit 1
fi

echo "Kafka dead letter smoke test passed."
echo "Dead letter file(s):"
echo "${dlq_files}"
