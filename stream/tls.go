package stream

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	Enabled            bool
	CertFile           string
	KeyFile            string
	CAFile             string
	InsecureSkipVerify bool
	ServerName         string
}

func (c TLSConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.InsecureSkipVerify {
		return fmt.Errorf("tls: InsecureSkipVerify is not allowed")
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return fmt.Errorf("tls: cert and key files must be configured together")
	}
	return nil
}

func (c TLSConfig) Build() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.InsecureSkipVerify,
		ServerName:         c.ServerName,
	}
	if c.CAFile != "" {
		caCert, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls: read CA file: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("tls: failed to parse CA certificate")
		}
		cfg.RootCAs = caPool
	}
	if c.CertFile != "" && c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: load key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
