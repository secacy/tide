package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func ClientOption(caFile, certFile, keyFile string) (grpc.DialOption, error) {
	if caFile == "" {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	roots, err := loadPool(caFile)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	if certFile != "" || keyFile != "" {
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(config)), nil
}

func ServerOption(caFile, certFile, keyFile string) (grpc.ServerOption, error) {
	if certFile == "" && keyFile == "" {
		return grpc.Creds(insecure.NewCredentials()), nil
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	}
	if caFile != "" {
		pool, err := loadPool(caFile)
		if err != nil {
			return nil, err
		}
		config.ClientCAs = pool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return grpc.Creds(credentials.NewTLS(config)), nil
}

func loadPool(filename string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA file %s has no certificates", filename)
	}
	return pool, nil
}
