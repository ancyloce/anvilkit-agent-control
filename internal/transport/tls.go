package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"os"
	"slices"
)

// Workload identities Control's listener admits: the API for every public
// forwarding, and since S2 the Workflow Worker for the preparation round
// methods. The request layer binds each credential class to its workload.
const (
	APIWorkload      = "anvilkit-agent-api"
	WorkflowWorkload = "anvilkit-agent-workflow"
)

// ConfigureTLS adds the selected local workload identities to the existing
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
			if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 || (!slices.Contains(state.PeerCertificates[0].DNSNames, APIWorkload) && !slices.Contains(state.PeerCertificates[0].DNSNames, WorkflowWorkload)) {
				return errors.New("Control requires the anvilkit-agent-api or anvilkit-agent-workflow workload identity")
			}
			return nil
		},
	}
	server.Protocols = new(http.Protocols)
	server.Protocols.SetHTTP2(true)
	return nil
}
