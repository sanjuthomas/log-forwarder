// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"testing"
	"time"
)

func TestKafkaConfigConnectTimeoutDuration(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{}
	if got := cfg.ConnectTimeoutDuration(); got != 10*time.Second {
		t.Fatalf("ConnectTimeoutDuration() = %v, want 10s default", got)
	}

	cfg.ConnectTimeout = "5s"
	if got := cfg.ConnectTimeoutDuration(); got != 5*time.Second {
		t.Fatalf("ConnectTimeoutDuration() = %v, want 5s", got)
	}

	cfg.ConnectTimeout = "invalid"
	if got := cfg.ConnectTimeoutDuration(); got != 10*time.Second {
		t.Fatalf("ConnectTimeoutDuration() = %v, want 10s fallback", got)
	}
}

func TestKafkaConfigValidateSASLPlainRequiresCredentials(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "logs",
		Security: &KafkaSecurityConfig{
			Protocol: KafkaProtocolSASLPlaintext,
			SASL:     &KafkaSASLConfig{Mechanism: KafkaSASLPlain},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when PLAIN credentials missing")
	}
}

func TestKafkaConfigValidateSASLMechanismRequired(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "logs",
		Security: &KafkaSecurityConfig{
			Protocol: KafkaProtocolSASLPlaintext,
			SASL:     &KafkaSASLConfig{},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when sasl mechanism missing")
	}
}

func TestKafkaConfigValidateTLSRequiresCertAndKeyPair(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{
		Brokers: []string{"localhost:9093"},
		Topic:   "logs",
		Security: &KafkaSecurityConfig{
			Protocol: KafkaProtocolSSL,
			TLS:      &KafkaTLSConfig{CertFile: "/tmp/client.crt"},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when only cert_file is set")
	}
}

func TestKafkaConfigValidateSASLSCRAMRequiresPassword(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "logs",
		Security: &KafkaSecurityConfig{
			Protocol: KafkaProtocolSASLPlaintext,
			SASL: &KafkaSASLConfig{
				Mechanism: KafkaSASLSCRAMSHA256,
				Username:  "user",
			},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when SCRAM password missing")
	}
}

func TestKafkaConfigValidateOAuthRequiresToken(t *testing.T) {
	t.Parallel()

	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "logs",
		Security: &KafkaSecurityConfig{
			Protocol: KafkaProtocolSASLSSL,
			TLS:      &KafkaTLSConfig{InsecureSkipVerify: true},
			SASL:     &KafkaSASLConfig{Mechanism: KafkaSASLOAuthBearer},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when oauth token missing")
	}
}

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

	if cfg.Sink.Kafka.SecurityProtocol() != KafkaProtocolSASLSSL {
		t.Fatalf("protocol = %q", cfg.Sink.Kafka.SecurityProtocol())
	}
	if cfg.Sink.Kafka.Security.SASL.Mechanism != KafkaSASLSCRAMSHA512 {
		t.Fatalf("mechanism = %q", cfg.Sink.Kafka.Security.SASL.Mechanism)
	}
}
