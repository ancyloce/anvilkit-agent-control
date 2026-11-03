package bootstrap

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx/fxtest"

	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity/identitytest"
)

// A client Reloader polls from the lifecycle's start to its stop: a
// rotation is followed while the process runs and no longer after it
// stopped (Control's client Reloaders were started and never stopped).
func TestClientReloaderFollowsTheLifecycle(t *testing.T) {
	ca := identitytest.NewCA(t, "ca")
	dir := t.TempDir()
	identitytest.Mount(t, dir, ca.Issue(t, "control", nil), ca.PEM)
	cert, key, caFile := identitytest.Files(dir)
	r, err := identity.New(identity.Files{CertFile: cert, KeyFile: key, CAFile: caFile}, 10*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	lc := fxtest.NewLifecycle(t)
	followLifecycle(lc, r)

	lc.RequireStart()
	before := r.Current().Digest
	identitytest.Mount(t, dir, ca.Issue(t, "control", nil), ca.PEM)
	require.Eventually(t, func() bool { return r.Current().Digest != before }, 5*time.Second, 10*time.Millisecond, "polled while started")

	lc.RequireStop()
	stopped := r.Current().Digest
	identitytest.Mount(t, dir, ca.Issue(t, "control", nil), ca.PEM)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, stopped, r.Current().Digest, "no polling after the stop")
}
