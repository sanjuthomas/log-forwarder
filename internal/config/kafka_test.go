package config

import "testing"

func TestKafkaConfigDefaultPlaintext(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "logs",
	}

	if cfg.SecurityProtocol() != KafkaProtocolPlaintext {
		t.Fatalf("SecurityProtocol() = %q, want PLAINTEXT", cfg.SecurityProtocol())
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestKafkaConfigSASLSSLSCRAM(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{
		Brokers: []string{"kafka.example.com:9093"},
		Topic:   "logs",
		Security: &KafkaSecurityConfig{
			Protocol: KafkaProtocolSASLSSL,
			TLS: &KafkaTLSConfig{
				CAFile: "/etc/kafka/ca.crt",
			},
			SASL: &KafkaSASLConfig{
				Mechanism: KafkaSASLSCRAMSHA512,
				Username:  "log-forwarder",
				Password:  "secret",
			},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestKafkaConfigSSLRequiresTLS(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{
		Brokers: []string{"kafka.example.com:9093"},
		Topic:   "logs",
		Security: &KafkaSecurityConfig{
			Protocol: KafkaProtocolSSL,
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when tls block is missing for SSL")
	}
}

func TestKafkaConfigGSSAPIRequiresKerberos(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{
		Brokers: []string{"kafka.example.com:9093"},
		Topic:   "logs",
		Security: &KafkaSecurityConfig{
			Protocol: KafkaProtocolSASLSSL,
			TLS: &KafkaTLSConfig{
				CAFile: "/etc/kafka/ca.crt",
			},
			SASL: &KafkaSASLConfig{
				Mechanism: KafkaSASLGSSAPI,
			},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when kerberos block is missing")
	}
}

func TestKafkaConfigOAuthRequiresToken(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{
		Brokers: []string{"kafka.example.com:9093"},
		Topic:   "logs",
		Security: &KafkaSecurityConfig{
			Protocol: KafkaProtocolSASLSSL,
			TLS: &KafkaTLSConfig{
				CAFile: "/etc/kafka/ca.crt",
			},
			SASL: &KafkaSASLConfig{
				Mechanism: KafkaSASLOAuthBearer,
				OAuth:     &KafkaOAuthConfig{},
			},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when oauth token is missing")
	}
}

func TestLoadKafkaSecurityExample(t *testing.T) {
	t.Parallel()

	cfg, err := Load("../../examples/kafka/sasl-ssl-scram-sha-512.yaml")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Kafka.SecurityProtocol() != KafkaProtocolSASLSSL {
		t.Fatalf("protocol = %q", cfg.Kafka.SecurityProtocol())
	}
	if cfg.Kafka.Security.SASL.Mechanism != KafkaSASLSCRAMSHA512 {
		t.Fatalf("mechanism = %q", cfg.Kafka.Security.SASL.Mechanism)
	}
}
