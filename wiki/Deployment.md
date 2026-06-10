# Deployment

Binary install on the host is still supported. A typical production setup:

1. Cross-compile the binary for the target OS/arch.
2. Install the binary and config on the host (e.g. `/opt/log-forwarder/`).
3. Ensure the service user can read configured log paths.
4. Point `sink.kafka.brokers` at your production cluster (or switch `sink.type` to `file` / `http-noauth`).
5. Run under a process supervisor such as systemd:

```ini
[Unit]
Description=Log Forwarder
After=network.target

[Service]
Type=simple
User=logforwarder
WorkingDirectory=/opt/log-forwarder
ExecStart=/opt/log-forwarder/log-forwarder -config /opt/log-forwarder/config.yaml
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

For custom transformers or enrichers, deploy the binary built from your own entrypoint (e.g. `./examples/custom`) instead of `./cmd/log-forwarder`.
