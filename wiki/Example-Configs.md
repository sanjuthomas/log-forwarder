# Example Configs

Copy these from the [configs/](https://github.com/sanjuthomas/log-forwarder/tree/main/configs) directory in the repository.

## General purpose

| Config | Sink | Log format | Notes |
|--------|------|------------|-------|
| [example.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example.yaml) | Kafka | Tab-delimited | Full-featured starter |
| [example-regex.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-regex.yaml) | Kafka | Regex | Generic regex parsing |
| [example-file.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-file.yaml) | File | Tab-delimited | JSONL to `./output/` |
| [example-http-noauth.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-http-noauth.yaml) | HTTP | Tab-delimited | POST to localhost ingest |

## Vendor noise / ingest cost

| Config | Sink | Filter | Notes |
|--------|------|--------|-------|
| [example-vendor-filter-kafka.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-vendor-filter-kafka.yaml) | Kafka | INFO, WARN, ERROR | Third-party DEBUG-heavy appliance — see [[Filtering Vendor Noise]] |
| [example-filter.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-filter.yaml) | File | WARN, ERROR | Integration-tested; local file sink |

## Spring Boot (default Logback console)

| Config | Sink |
|--------|------|
| [example-spring-boot-kafka.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-spring-boot-kafka.yaml) | Kafka |
| [example-spring-boot-file.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-spring-boot-file.yaml) | File |
| [example-spring-boot-http-noauth.yaml](https://github.com/sanjuthomas/log-forwarder/blob/main/configs/example-spring-boot-http-noauth.yaml) | HTTP |

All three share multiline parser + Spring Boot regex. See [[Spring Boot Logs]].

## Kafka security modes

Under [examples/kafka/](https://github.com/sanjuthomas/log-forwarder/tree/main/examples/kafka):

| File | Protocol |
|------|----------|
| plaintext.yaml | PLAINTEXT (dev only) |
| ssl-server-auth.yaml | SSL |
| ssl-mtls.yaml | mTLS |
| sasl-plaintext-plain.yaml | SASL_PLAINTEXT + PLAIN |
| sasl-ssl-plain.yaml | SASL_SSL + PLAIN |
| sasl-ssl-scram-sha-256.yaml | SASL_SSL + SCRAM-SHA-256 |
| sasl-ssl-scram-sha-512.yaml | SASL_SSL + SCRAM-SHA-512 |
| sasl-ssl-oauthbearer.yaml | SASL_SSL + OAUTHBEARER |
| sasl-ssl-gssapi.yaml | SASL_SSL + GSSAPI (reference; not fully implemented) |

## Typical customisation workflow

1. Copy the closest example to `/etc/log-forwarder/config.yaml`
2. Update `watch.sources` paths and patterns to your log directories
3. Choose `sink` type and destination settings
4. Set `enrichers` static fields (`application_id`, `environment`, …)
5. Set a dedicated `watch.state.path` for this process
6. Run: `./bin/log-forwarder -config /etc/log-forwarder/config.yaml`
