// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"fmt"
	"strings"
	"time"
)

const (
	KafkaProtocolPlaintext     = "PLAINTEXT"
	KafkaProtocolSSL           = "SSL"
	KafkaProtocolSASLPlaintext = "SASL_PLAINTEXT"
	KafkaProtocolSASLSSL       = "SASL_SSL"
)

const (
	KafkaSASLPlain       = "PLAIN"
	KafkaSASLSCRAMSHA256 = "SCRAM-SHA-256"
	KafkaSASLSCRAMSHA512 = "SCRAM-SHA-512"
	KafkaSASLGSSAPI      = "GSSAPI"
	KafkaSASLOAuthBearer = "OAUTHBEARER"
)

type KafkaConfig struct {
	Brokers        []string             `yaml:"brokers"`
	Topic          string               `yaml:"topic"`
	ConnectTimeout string               `yaml:"connect_timeout"`
	Security       *KafkaSecurityConfig `yaml:"security,omitempty"`
}

type KafkaSecurityConfig struct {
	Protocol string           `yaml:"protocol"`
	TLS      *KafkaTLSConfig  `yaml:"tls,omitempty"`
	SASL     *KafkaSASLConfig `yaml:"sasl,omitempty"`
}

type KafkaTLSConfig struct {
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

type KafkaSASLConfig struct {
	Mechanism string               `yaml:"mechanism"`
	Username  string               `yaml:"username"`
	Password  string               `yaml:"password"`
	Kerberos  *KafkaKerberosConfig `yaml:"kerberos,omitempty"`
	OAuth     *KafkaOAuthConfig    `yaml:"oauth,omitempty"`
}

type KafkaKerberosConfig struct {
	ServiceName string `yaml:"service_name"`
	Realm       string `yaml:"realm"`
	Keytab      string `yaml:"keytab"`
	Principal   string `yaml:"principal"`
	ConfigPath  string `yaml:"config_path"`
}

type KafkaOAuthConfig struct {
	Token string `yaml:"token"`
}

func (c KafkaConfig) SecurityProtocol() string {
	if c.Security == nil || c.Security.Protocol == "" {
		return KafkaProtocolPlaintext
	}
	return strings.ToUpper(c.Security.Protocol)
}

func (c KafkaConfig) ConnectTimeoutDuration() time.Duration {
	if c.ConnectTimeout == "" {
		return 10 * time.Second
	}
	d, err := time.ParseDuration(c.ConnectTimeout)
	if err != nil {
		return 10 * time.Second
	}
	return d
}

func (c KafkaConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must not be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("kafka.topic must not be empty")
	}
	if c.ConnectTimeout != "" {
		if _, err := time.ParseDuration(c.ConnectTimeout); err != nil {
			return fmt.Errorf("kafka.connect_timeout: %w", err)
		}
	}

	protocol := c.SecurityProtocol()
	switch protocol {
	case KafkaProtocolPlaintext, KafkaProtocolSSL, KafkaProtocolSASLPlaintext, KafkaProtocolSASLSSL:
	default:
		return fmt.Errorf("kafka.security.protocol must be PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL")
	}

	if c.Security == nil {
		return nil
	}

	if err := c.validateTLS(protocol); err != nil {
		return err
	}
	return c.validateSASL(protocol)
}

func (c KafkaConfig) validateTLS(protocol string) error {
	if protocol != KafkaProtocolSSL && protocol != KafkaProtocolSASLSSL {
		return nil
	}

	tlsCfg := c.Security.TLS
	if tlsCfg == nil {
		return fmt.Errorf("kafka.security.tls is required when protocol is %s", protocol)
	}

	hasCert := tlsCfg.CertFile != "" || tlsCfg.KeyFile != ""
	if hasCert && (tlsCfg.CertFile == "" || tlsCfg.KeyFile == "") {
		return fmt.Errorf("kafka.security.tls.cert_file and key_file must both be set for mTLS")
	}

	if tlsCfg.CAFile == "" && !tlsCfg.InsecureSkipVerify && !hasCert {
		return fmt.Errorf("kafka.security.tls.ca_file is required when protocol is %s (or set insecure_skip_verify for dev only)", protocol)
	}

	return nil
}

func (c KafkaConfig) validateSASL(protocol string) error {
	if protocol != KafkaProtocolSASLPlaintext && protocol != KafkaProtocolSASLSSL {
		return nil
	}

	saslCfg := c.Security.SASL
	if saslCfg == nil {
		return fmt.Errorf("kafka.security.sasl is required when protocol is %s", protocol)
	}

	mechanism := strings.ToUpper(saslCfg.Mechanism)
	switch mechanism {
	case KafkaSASLPlain:
		if saslCfg.Username == "" {
			return fmt.Errorf("kafka.security.sasl.username is required for PLAIN")
		}
		if saslCfg.Password == "" {
			return fmt.Errorf("kafka.security.sasl.password is required for PLAIN")
		}
	case KafkaSASLSCRAMSHA256, KafkaSASLSCRAMSHA512:
		if saslCfg.Username == "" {
			return fmt.Errorf("kafka.security.sasl.username is required for %s", mechanism)
		}
		if saslCfg.Password == "" {
			return fmt.Errorf("kafka.security.sasl.password is required for %s", mechanism)
		}
	case KafkaSASLGSSAPI:
		krb := saslCfg.Kerberos
		if krb == nil {
			return fmt.Errorf("kafka.security.sasl.kerberos is required for GSSAPI")
		}
		if krb.Keytab == "" {
			return fmt.Errorf("kafka.security.sasl.kerberos.keytab is required for GSSAPI")
		}
		if krb.Principal == "" {
			return fmt.Errorf("kafka.security.sasl.kerberos.principal is required for GSSAPI")
		}
	case KafkaSASLOAuthBearer:
		if saslCfg.OAuth == nil || saslCfg.OAuth.Token == "" {
			return fmt.Errorf("kafka.security.sasl.oauth.token is required for OAUTHBEARER")
		}
	default:
		if mechanism == "" {
			return fmt.Errorf("kafka.security.sasl.mechanism is required when protocol is %s", protocol)
		}
		return fmt.Errorf("kafka.security.sasl.mechanism %q is not supported", saslCfg.Mechanism)
	}

	return nil
}
