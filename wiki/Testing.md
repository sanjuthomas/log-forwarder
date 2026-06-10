# Testing

## Unit tests

```bash
go test ./...
```

Race detector (CI runs this on every PR to `main`):

```bash
go test -race ./...
```

Verbose output:

```bash
go test -v ./...
```

## Integration tests

Automated end-to-end tests (watcher + pipeline + sink) live in [`internal/integration/`](https://github.com/sanjuthomas/log-forwarder/blob/main/internal/integration/). Filter scenarios include ERROR-only forwarding, `match: any`, metrics counters, watermark advance on filtered lines, and `on_missing: drop`.

```bash
go test ./internal/integration/ -v
```

See [`docs/integration-test-cases.txt`](https://github.com/sanjuthomas/log-forwarder/blob/main/docs/integration-test-cases.txt) for the full catalog (E2E-1–E2E-14, CFG-1, KAFKA-1, SMOKE-1).

## Docker smoke tests

```bash
./scripts/docker-smoke.sh    # file sink + ERROR-only filter
./scripts/kafka-smoke.sh     # Kafka round-trip + ERROR-only filter
./scripts/kafka-deadletter-smoke.sh  # Kafka publish failure → dead letter JSONL
```

## End-to-end test with Kafka

**1. Start Kafka** (Docker example):

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka:latest
```

**2. Create a local config** (`configs/local.yaml`):

```yaml
watch:
  poll: 1s
  sources:
    - path: ./logs/app
      patterns:
        - "*.log"

sink:
  type: kafka
  kafka:
    brokers:
      - localhost:9092
    topic: logs

transform:
  type: delimiter
  delimiter: "\t"
  columns:
    - timestamp
    - level
    - message
  on_error: wrap

enrichers:
  - type: static
    fields:
      application_id: test-app
      environment: dev
  - type: host

pipeline:
  buffer_size: 1024
  on_full: block
```

**3. Run the forwarder:**

```bash
mkdir -p logs/app
./bin/log-forwarder -config configs/local.yaml
```

**4. Write a test log line:**

```bash
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)	info	hello from log-forwarder" >> logs/app/test.log
```

**5. Consume from Kafka:**

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic logs \
  --from-beginning
```

You should see JSON output with your transformed fields, enricher metadata, and `_path`.
