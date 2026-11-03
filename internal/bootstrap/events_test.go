package bootstrap

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/outbox"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity/identitytest"
)

// fakeNATS speaks just enough of the NATS client protocol over TLS for a
// connection to establish: INFO with tls_required, the TLS upgrade under the
// current server certificate, PONG for every PING.
type fakeNATS struct {
	ln          net.Listener
	certificate atomic.Pointer[tls.Certificate]
	handshakes  atomic.Int32
}

func newFakeNATS(t *testing.T, leaf identitytest.Leaf) *fakeNATS {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &fakeNATS{ln: ln}
	f.serve(t, leaf)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.handle(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *fakeNATS) serve(t *testing.T, leaf identitytest.Leaf) {
	t.Helper()
	cert, err := tls.X509KeyPair(leaf.CertPEM, leaf.KeyPEM)
	require.NoError(t, err)
	f.certificate.Store(&cert)
}

func (f *fakeNATS) handle(c net.Conn) {
	defer c.Close()
	if _, err := io.WriteString(c, `INFO {"server_id":"fake","version":"2.11.0","proto":1,"max_payload":1048576,"headers":true,"tls_required":true}`+"\r\n"); err != nil {
		return
	}
	tc := tls.Server(c, &tls.Config{
		MinVersion:     tls.VersionTLS13,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return f.certificate.Load(), nil },
	})
	if err := tc.Handshake(); err != nil {
		return
	}
	f.handshakes.Add(1)
	r := bufio.NewReader(tc)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if strings.HasPrefix(line, "PING") {
			if _, err := io.WriteString(tc, "PONG\r\n"); err != nil {
				return
			}
		}
	}
}

// A rotation that retires the root which verified the NATS server
// reconnects the connection, and the reconnect verifies the server under
// the current bundle (the roots were a snapshot of the first bundle, so the
// reconnect to a server certificate of the new CA failed). A transition
// that keeps the root keeps the connection.
func TestNATSFollowsTheCurrentTrust(t *testing.T) {
	ca1, ca2 := identitytest.NewCA(t, "ca1"), identitytest.NewCA(t, "ca2")
	proxyID := identitytest.SPIFFE("anvilkit.local", "anvilkit-apps", "anvilkit-agent-control")
	server := newFakeNATS(t, ca1.Issue(t, "nats", nil, "nats"))

	dir := t.TempDir()
	identitytest.Mount(t, dir, ca1.Issue(t, "control", []string{proxyID}), ca1.PEM)
	cert, key, ca := identitytest.Files(dir)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r, err := identity.New(identity.Files{CertFile: cert, KeyFile: key, CAFile: ca}, time.Hour, log)
	require.NoError(t, err)

	nc, err := outbox.Connect(outbox.NATSConfig{
		URL: "nats://" + server.ln.Addr().String(), Name: "control-test", PublishTimeout: 5 * time.Second,
		TLS: identity.ClientTLSConfig(r, "nats"), RootCAs: func() (*x509.CertPool, error) { return r.Current().Pool, nil },
	})
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	reconnected := make(chan struct{}, 1)
	nc.SetReconnectHandler(func(*nats.Conn) { reconnected <- struct{}{} })
	followNATSTrust(r, nc, log)
	require.Equal(t, int32(1), server.handshakes.Load())

	// The transition bundle still trusts ca1: the connection is kept.
	identitytest.Mount(t, dir, ca1.Issue(t, "control", []string{proxyID}), append(append([]byte{}, ca1.PEM...), ca2.PEM...))
	require.True(t, r.Reload())
	require.Equal(t, nats.CONNECTED, nc.Status())
	require.Equal(t, uint64(0), nc.Stats().Reconnects)

	// ca1 retired: the server moves to a ca2 certificate, the connection
	// verified under ca1 reconnects and verifies it under {ca2}.
	server.serve(t, ca2.Issue(t, "nats", nil, "nats"))
	identitytest.Mount(t, dir, ca2.Issue(t, "control", []string{proxyID}), ca2.PEM)
	require.True(t, r.Reload())
	select {
	case <-reconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection of retired trust did not reconnect under the current bundle")
	}
	require.Equal(t, int32(2), server.handshakes.Load())
	state, err := nc.TLSConnectionState()
	require.NoError(t, err)
	chain := state.VerifiedChains[0]
	require.Equal(t, ca2.Cert.Raw, chain[len(chain)-1].Raw)
}
