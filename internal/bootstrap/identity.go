package bootstrap

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ancyloce/anvilkit-agent-control/internal/config"
	grpctransport "github.com/ancyloce/anvilkit-agent-control/internal/transport/grpc"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity"
)

// serverIdentity builds the listener identity of grpc.identity: the
// watched material and the allowlist under the trust domain, or nil for the
// DEVELOPMENT_ONLY plaintext listener (admitted by the loader only with
// development.enabled).
func serverIdentity(cfg config.Config, log *slog.Logger) (*grpctransport.Identity, *identity.Reloader, error) {
	id := cfg.GRPC.Identity
	if id.Mode == "development" {
		log.Warn("DEVELOPMENT_ONLY listener identity: plaintext gRPC, no caller is authenticated or authorized; qualifies no production identity")
		return nil, nil, nil
	}
	r, err := identity.New(identity.Files{CertFile: id.CertFile, KeyFile: id.KeyFile, CAFile: id.CAFile}, id.ReloadInterval, log)
	if err != nil {
		return nil, nil, fmt.Errorf("grpc.identity: %w", err)
	}
	return &grpctransport.Identity{Reloader: r, TrustDomain: cfg.TrustDomain(), Policy: grpctransport.Policy(), MaxConnectionAge: id.MaxConnectionAge}, r, nil
}

// clientCredentials is the gRPC transport credential of an outbound
// connection (Temporal, OTLP) under a ClientTLS section. The mtls form
// rotates with its own files; tls verifies against the bundle read once.
func clientCredentials(name string, t config.ClientTLS, development bool, log *slog.Logger) (credentials.TransportCredentials, error) {
	switch t.Mode {
	case "development":
		if !development {
			return nil, fmt.Errorf("%s: development mode without development.enabled", name)
		}
		log.Warn("DEVELOPMENT_ONLY plaintext transport", "connection", name)
		return insecure.NewCredentials(), nil
	case "tls":
		pool, err := caPool(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		return credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: t.ServerName, MinVersion: tls.VersionTLS13}), nil
	case "mtls":
		r, err := identity.New(identity.Files{CertFile: t.CertFile, KeyFile: t.KeyFile, CAFile: t.CAFile}, 0, log)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		r.Start()
		creds, err := identity.NewClientCredentials(r, t.ServerName)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		return creds, nil
	}
	return nil, fmt.Errorf("%s: unknown mode %q", name, t.Mode)
}

// clientTransport is clientCredentials as a dial option (Temporal).
func clientTransport(name string, t config.ClientTLS, development bool, log *slog.Logger) (grpc.DialOption, error) {
	c, err := clientCredentials(name, t, development, log)
	if err != nil {
		return nil, err
	}
	return grpc.WithTransportCredentials(c), nil
}

func caPool(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("ca bundle holds no certificate")
	}
	return pool, nil
}
