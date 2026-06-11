#!/usr/bin/env bash
# Append Spring Boot-style log lines to a file forever.
# Usage: ./scripts/generate-spring-boot-logs.sh <log-file> [sleep-seconds]
set -euo pipefail

log_file="${1:?usage: $0 <log-file> [sleep-seconds]}"
sleep_secs="${2:-1}"

pid="${PID:-$$}"
exec_num=1

levels=(INFO WARN ERROR DEBUG)
threads=(main nio-8080-exec-1 nio-8080-exec-5 scheduling-1)
loggers=(
  c.a.b.controller.PaymentController
  c.a.b.service.OrderService
  c.a.b.repository.InvoiceRepository
  com.example.Application
)
messages=(
  "Payment failed for invoiceId=inv_98765"
  "Processed order orderId=ord_12345 status=COMPLETED"
  "Saved invoice invoiceId=inv_98765 amount=129.99"
  "Starting Application v1.0.0"
  "Connection pool health check passed"
  "Retry attempt 2 for paymentId=pay_44210"
)

mkdir -p "$(dirname "$log_file")"

echo "Appending Spring Boot logs to $log_file (Ctrl+C to stop)" >&2

while true; do
  ts="$(date +"%Y-%m-%d %H:%M:%S").$(printf '%03d' $((RANDOM % 1000)))"
  level="${levels[RANDOM % ${#levels[@]}]}"
  thread="${threads[RANDOM % ${#threads[@]}]}"
  logger="${loggers[RANDOM % ${#loggers[@]}]}"
  message="${messages[RANDOM % ${#messages[@]}]}"

  if [[ "$thread" == nio-8080-exec-* ]]; then
    thread="nio-8080-exec-$exec_num"
    exec_num=$((exec_num % 10 + 1))
  fi

  printf '%s  %5s %5d --- [%s] %-40s : %s\n' \
    "$ts" "$level" "$pid" "$thread" "$logger" "$message" >> "$log_file"

  sleep "$sleep_secs"
done
