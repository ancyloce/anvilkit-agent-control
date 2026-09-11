package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"os"
	"slices"
)

// ConfigureTLS adds the selected local API workload identity to the existing
// method-scoped credentials. It does not grant a tenant or an action by itself.
// Empty inputs preserve the existing loopback-only h2c test profile.
func ConfigureTLS(server *http.Server, caFile, certificateFile, keyFile string) error {
	if caFile == "" && certificateFile == "" && keyFile == "" {
		return nil
	}
	certificate, err := tls.LoadX509KeyPair(certificateFile, keyFile)
	if err != nil {
		return errors.New("Control cannot load its TLS certificate and key")
	}
	ca, err := os.ReadFile(caFile)
	roots := x509.NewCertPool()
	if err != nil || !roots.AppendCertsFromPEM(ca) {
		return errors.New("Control cannot load its client CA")
	}
	server.TLSConfig = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    roots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 || !slices.Contains(state.PeerCertificates[0].DNSNames, "anvilkit-agent-api") {
				return errors.New("Control requires the anvilkit-agent-api workload identity")
			}
			return nil
		},
	}
	server.Protocols = new(http.Protocols)
	server.Protocols.SetHTTP2(true)
	return nil
}
