package sink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func buildDialer(cfg config.KafkaConfig) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout: kafka.DefaultDialer.Timeout,
	}

	protocol := cfg.SecurityProtocol()
	switch protocol {
	case config.KafkaProtocolPlaintext:
		return dialer, nil
	case config.KafkaProtocolSSL:
		tlsConfig, err := buildTLSConfig(cfg.Security.TLS)
		if err != nil {
			return nil, err
		}
		dialer.TLS = tlsConfig
		return dialer, nil
	case config.KafkaProtocolSASLPlaintext:
		mechanism, err := buildSASLMechanism(cfg.Security.SASL)
		if err != nil {
			return nil, err
		}
		dialer.SASLMechanism = mechanism
		return dialer, nil
	case config.KafkaProtocolSASLSSL:
		tlsConfig, err := buildTLSConfig(cfg.Security.TLS)
		if err != nil {
			return nil, err
		}
		mechanism, err := buildSASLMechanism(cfg.Security.SASL)
		if err != nil {
			return nil, err
		}
		dialer.TLS = tlsConfig
		dialer.SASLMechanism = mechanism
		return dialer, nil
	default:
		return nil, fmt.Errorf("unsupported kafka security protocol %q", protocol)
	}
}

func buildTLSConfig(tlsCfg *config.KafkaTLSConfig) (*tls.Config, error) {
	if tlsCfg == nil {
		return nil, fmt.Errorf("tls config is required")
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: tlsCfg.InsecureSkipVerify,
	}

	if tlsCfg.CAFile != "" {
		caPEM, err := os.ReadFile(tlsCfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read kafka tls ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("kafka tls ca_file contains no certificates")
		}
		tlsConfig.RootCAs = pool
	}

	if tlsCfg.CertFile != "" || tlsCfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load kafka tls client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func buildSASLMechanism(saslCfg *config.KafkaSASLConfig) (sasl.Mechanism, error) {
	if saslCfg == nil {
		return nil, fmt.Errorf("sasl config is required")
	}

	switch strings.ToUpper(saslCfg.Mechanism) {
	case config.KafkaSASLPlain:
		return plain.Mechanism{
			Username: saslCfg.Username,
			Password: saslCfg.Password,
		}, nil
	case config.KafkaSASLSCRAMSHA256:
		return scram.Mechanism(scram.SHA256, saslCfg.Username, saslCfg.Password)
	case config.KafkaSASLSCRAMSHA512:
		return scram.Mechanism(scram.SHA512, saslCfg.Username, saslCfg.Password)
	case config.KafkaSASLOAuthBearer:
		return oauthBearerMechanism{token: saslCfg.OAuth.Token}, nil
	case config.KafkaSASLGSSAPI:
		return nil, fmt.Errorf("kafka GSSAPI (Kerberos) is not yet supported by log-forwarder")
	default:
		return nil, fmt.Errorf("unsupported kafka sasl mechanism %q", saslCfg.Mechanism)
	}
}

type oauthBearerMechanism struct {
	token string
}

func (oauthBearerMechanism) Name() string {
	return config.KafkaSASLOAuthBearer
}

func (m oauthBearerMechanism) Start(_ context.Context) (sasl.StateMachine, []byte, error) {
	payload := fmt.Sprintf("n,,\x01auth=Bearer %s\x01\x01", m.token)
	return m, []byte(payload), nil
}

func (m oauthBearerMechanism) Next(_ context.Context, _ []byte) (bool, []byte, error) {
	return true, nil, nil
}
