# Installation and First Run

## Requirements

- **Go 1.22+** (only if you build from source)
- Read access to the directories where your applications write logs
- A working **sink destination**:
  - Kafka broker and topic, or
  - A writable path for a JSONL file, or
  - An HTTP endpoint that accepts POST requests (open / no auth for the built-in `http-noauth` sink)

## Build

```bash
git clone https://github.com/sanjuthomas/log-forwarder.git
cd log-forwarder
go mod download
go build -o bin/log-forwarder ./cmd/log-forwarder
```

For a typical Linux server:

```bash
GOOS=linux GOARCH=amd64 go build -o bin/log-forwarder ./cmd/log-forwarder
```

Build a binary with custom parsers, transformers, enrichers, or sinks (see [[Custom-Extensions]]):

```bash
go build -o bin/log-forwarder-custom ./examples/custom
```

## Run with a config file

Always prefer an explicit config in production:

```bash
./bin/log-forwarder -config /etc/log-forwarder/config.yaml
```

Start the process from a directory where **relative paths** in the config resolve correctly, or use **absolute paths** in YAML for watch paths, watermark file, and file sink output.

The forwarder logs to **stderr** and stops cleanly on `SIGINT` / `SIGTERM`.

## Run with defaults (local try-out only)

Without `-config`, built-in defaults apply:

- Watches the **current directory** for `*.log*` files
- Kafka at `localhost:9092`, topic `logs`
- Tab-delimited parsing
- Hostname enrichment

```bash
./bin/log-forwarder
```

Defaults are convenient for a quick test; use a real config file for anything beyond local development.

## Verify it is working

1. **Startup logs** — look for:
   - `sink connectivity verified` — destination was reachable at startup
   - `log forwarder started` — includes watch sources and `sink_type`

2. **Write a test line** to a watched file (adjust path/pattern to match your config):

   ```bash
   mkdir -p logs/app
   echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)	info	test message" >> logs/app/test.log
   ```

3. **Check the sink:**
   - **Kafka** — consume the topic with your usual Kafka tools
   - **File** — `tail -f` the JSONL output path from config
   - **HTTP** — confirm your ingest service received the POST

4. **Optional metrics** — if `metrics.enabled: true`, scrape `http://<host>:<port>/metrics` and check `log_forwarder_lines_published`.

## Startup failure: sink unavailable

If the sink cannot be reached at startup (for example Kafka is down), the forwarder **refuses to start**:

```
sink unavailable at startup; refusing to start forwarder
```

Fix connectivity or config, then restart. This avoids silently running without a working destination.

## Next steps

- [[How It Works]] — pipeline overview
- [[Configuration Guide]] — YAML sections explained
- [[Example Configs]] — ready-made configs to copy
