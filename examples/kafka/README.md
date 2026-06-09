# Kafka security examples

Example configs for every supported Kafka client security mode. Each file is a complete log-forwarder config — copy one, adjust paths and credentials, and run:

```bash
go build -o bin/log-forwarder ./cmd/log-forwarder
./bin/log-forwarder -config examples/kafka/sasl-ssl-scram-sha-512.yaml
```

## Files

| File | Protocol | Auth | Notes |
|------|----------|------|-------|
| [`plaintext.yaml`](plaintext.yaml) | `PLAINTEXT` | None | Local dev only |
| [`ssl-server-auth.yaml`](ssl-server-auth.yaml) | `SSL` | TLS (broker cert) | Encrypt + verify broker |
| [`ssl-mtls.yaml`](ssl-mtls.yaml) | `SSL` | mTLS | Client + broker certificates |
| [`sasl-plaintext-plain.yaml`](sasl-plaintext-plain.yaml) | `SASL_PLAINTEXT` | PLAIN | Dev only — no encryption |
| [`sasl-ssl-plain.yaml`](sasl-ssl-plain.yaml) | `SASL_SSL` | PLAIN | Prefer SCRAM when available |
| [`sasl-ssl-scram-sha-256.yaml`](sasl-ssl-scram-sha-256.yaml) | `SASL_SSL` | SCRAM-SHA-256 | Common production default |
| [`sasl-ssl-scram-sha-512.yaml`](sasl-ssl-scram-sha-512.yaml) | `SASL_SSL` | SCRAM-SHA-512 | Common production default |
| [`sasl-ssl-gssapi.yaml`](sasl-ssl-gssapi.yaml) | `SASL_SSL` | GSSAPI (Kerberos) | Config reference; sink not yet implemented |
| [`sasl-ssl-oauthbearer.yaml`](sasl-ssl-oauthbearer.yaml) | `SASL_SSL` | OAUTHBEARER | Bearer token from your IdP |

## Config shape

All secured configs use the same `sink.kafka.security` block:

```yaml
sink:
  type: kafka
  kafka:
    brokers:
      - kafka.example.com:9093
    topic: logs
    security:
      protocol: SASL_SSL          # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
      tls:
        ca_file: /etc/kafka/ca.crt
        cert_file: /etc/kafka/client.crt   # optional — mTLS
        key_file: /etc/kafka/client.key    # optional — mTLS
        insecure_skip_verify: false        # dev only
      sasl:
        mechanism: SCRAM-SHA-512           # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | GSSAPI | OAUTHBEARER
        username: log-forwarder
        password: secret
        kerberos:                          # GSSAPI only
          service_name: kafka
          realm: EXAMPLE.COM
          principal: log-forwarder/host@EXAMPLE.COM
          keytab: /etc/kafka/log-forwarder.keytab
          config_path: /etc/krb5.conf
        oauth:                             # OAUTHBEARER only
          token: eyJhbG...
```

Omit `sink.kafka.security` entirely to use plaintext (same as `protocol: PLAINTEXT`).

## Implementation status

| Mechanism | Supported |
|-----------|-----------|
| PLAINTEXT | Yes |
| SSL / mTLS | Yes |
| SASL PLAIN | Yes |
| SASL SCRAM-SHA-256 / SHA-512 | Yes |
| SASL OAUTHBEARER | Yes (static token) |
| SASL GSSAPI (Kerberos) | Config only — not yet implemented |

Replace `${KAFKA_PASSWORD}` and `${KAFKA_OAUTH_TOKEN}` with real values in your deployment (env var expansion is not built in yet).
