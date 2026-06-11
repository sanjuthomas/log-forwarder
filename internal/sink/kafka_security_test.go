// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestBuildSASLMechanismPlain(t *testing.T) {
	t.Parallel()

	mechanism, err := buildSASLMechanism(&config.KafkaSASLConfig{
		Mechanism: config.KafkaSASLPlain,
		Username:  "user",
		Password:  "pass",
	})
	if err != nil {
		t.Fatal(err)
	}
	if mechanism.Name() != config.KafkaSASLPlain {
		t.Fatalf("Name() = %q", mechanism.Name())
	}
}

func TestBuildSASLMechanismUnsupported(t *testing.T) {
	t.Parallel()

	_, err := buildSASLMechanism(&config.KafkaSASLConfig{Mechanism: "UNKNOWN"})
	if err == nil {
		t.Fatal("expected error for unsupported mechanism")
	}
}

func TestBuildSASLMechanismRequiresConfig(t *testing.T) {
	t.Parallel()

	_, err := buildSASLMechanism(nil)
	if err == nil {
		t.Fatal("expected error when sasl config is nil")
	}
}

func TestBuildTLSConfigInsecureSkipVerify(t *testing.T) {
	t.Parallel()

	tlsConfig, err := buildTLSConfig(&config.KafkaTLSConfig{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	if !tlsConfig.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify to be set")
	}
}

func TestBuildTLSConfigRequiresConfig(t *testing.T) {
	t.Parallel()

	_, err := buildTLSConfig(nil)
	if err == nil {
		t.Fatal("expected error when tls config is nil")
	}
}

func TestBuildDialerSASLSSL(t *testing.T) {
	t.Parallel()

	dialer, err := buildDialer(config.KafkaConfig{
		Brokers: []string{"kafka.example.com:9093"},
		Topic:   "logs",
		Security: &config.KafkaSecurityConfig{
			Protocol: config.KafkaProtocolSASLSSL,
			TLS:      &config.KafkaTLSConfig{InsecureSkipVerify: true},
			SASL: &config.KafkaSASLConfig{
				Mechanism: config.KafkaSASLSCRAMSHA512,
				Username:  "user",
				Password:  "pass",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if dialer.TLS == nil || dialer.SASLMechanism == nil {
		t.Fatal("expected TLS and SASL on SASL_SSL dialer")
	}
}

func TestOAuthBearerMechanismStart(t *testing.T) {
	t.Parallel()

	m := oauthBearerMechanism{token: "abc123"}
	if m.Name() != config.KafkaSASLOAuthBearer {
		t.Fatalf("Name() = %q", m.Name())
	}
	_, payload, err := m.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), "abc123") {
		t.Fatalf("payload = %q, want token", payload)
	}
	done, _, err := m.Next(context.Background(), nil)
	if err != nil || !done {
		t.Fatalf("Next() done = %v err = %v, want true nil", done, err)
	}
}

func TestBuildTLSConfigMissingCAFile(t *testing.T) {
	t.Parallel()

	_, err := buildTLSConfig(&config.KafkaTLSConfig{CAFile: "/no/such/ca.pem"})
	if err == nil {
		t.Fatal("expected error when ca_file is missing")
	}
}

func TestBuildTLSConfigInvalidCAFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("not-a-certificate"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := buildTLSConfig(&config.KafkaTLSConfig{CAFile: caPath})
	if err == nil {
		t.Fatal("expected error when ca_file has no certificates")
	}
}

func TestBuildDialerPlaintext(t *testing.T) {
	t.Parallel()

	dialer, err := buildDialer(config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "logs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if dialer.TLS != nil {
		t.Fatal("expected no TLS for PLAINTEXT")
	}
	if dialer.SASLMechanism != nil {
		t.Fatal("expected no SASL for PLAINTEXT")
	}
}

func TestBuildSASLMechanismSCRAM(t *testing.T) {
	t.Parallel()

	mechanism, err := buildSASLMechanism(&config.KafkaSASLConfig{
		Mechanism: config.KafkaSASLSCRAMSHA256,
		Username:  "user",
		Password:  "pass",
	})
	if err != nil {
		t.Fatal(err)
	}
	if mechanism.Name() != config.KafkaSASLSCRAMSHA256 {
		t.Fatalf("Name() = %q", mechanism.Name())
	}
}

func TestBuildSASLMechanismOAuth(t *testing.T) {
	t.Parallel()

	mechanism, err := buildSASLMechanism(&config.KafkaSASLConfig{
		Mechanism: config.KafkaSASLOAuthBearer,
		OAuth:     &config.KafkaOAuthConfig{Token: "test-token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mechanism.Name() != config.KafkaSASLOAuthBearer {
		t.Fatalf("Name() = %q", mechanism.Name())
	}
}

func TestBuildSASLMechanismGSSAPINotImplemented(t *testing.T) {
	t.Parallel()

	_, err := buildSASLMechanism(&config.KafkaSASLConfig{
		Mechanism: config.KafkaSASLGSSAPI,
		Kerberos: &config.KafkaKerberosConfig{
			Keytab:    "/etc/kafka/log-forwarder.keytab",
			Principal: "log-forwarder/host@EXAMPLE.COM",
		},
	})
	if err == nil {
		t.Fatal("expected error for GSSAPI")
	}
}

func TestNewKafkaPlaintext(t *testing.T) {
	t.Parallel()

	s, err := New(config.SinkConfig{
		Type: "kafka",
		Kafka: &config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "logs",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewKafkaGSSAPIFailsAtSink(t *testing.T) {
	t.Parallel()

	_, err := New(config.SinkConfig{
		Type: "kafka",
		Kafka: &config.KafkaConfig{
			Brokers: []string{"kafka.example.com:9093"},
			Topic:   "logs",
			Security: &config.KafkaSecurityConfig{
				Protocol: config.KafkaProtocolSASLSSL,
				TLS: &config.KafkaTLSConfig{
					InsecureSkipVerify: true,
				},
				SASL: &config.KafkaSASLConfig{
					Mechanism: config.KafkaSASLGSSAPI,
					Kerberos: &config.KafkaKerberosConfig{
						Keytab:    "/etc/kafka/log-forwarder.keytab",
						Principal: "log-forwarder/host@EXAMPLE.COM",
					},
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected error when creating GSSAPI sink")
	}
}
