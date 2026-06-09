package sink

import (
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

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
