package application_test

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	artifactstore "github.com/ancyloce/anvilkit-agent-control/internal/adapters/artifacts"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// TestFinalizeMemoryIsBounded is P1.5 AC8 (CTL-INF-07): three concurrent
// finalizations of 64 MiB source archives, which are the class that must
// be read whole for the archive check, stay under the 256 MiB container
// limit. The finalize slots (two) bound the bytes held at once and the
// object read allocates the declared size once; the heap is sampled while
// the three run.
func TestFinalizeMemoryIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("uploads 192 MiB to the development store")
	}
	const size = 64 << 20
	const limitBytes = 256 << 20
	inst := testdb.Start(t)
	objects := startArtifactStore(t)
	fsInv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	p := newProcess(t, inst, fsInv)
	limits := application.ArtifactLimits{MaxObjectBytes: size + (1 << 20), MaxWindow: time.Hour, CapabilityTTL: 5 * time.Minute, FinalizeConcurrency: 2,
		Archive: domain.ArchiveLimits{MaxEntries: 100, MaxUncompressed: 2 * size}}
	a := application.NewArtifacts(postgres.NewStore(p.pool), objects, limits, domain.SystemClock{}, testLog)
	ctx := context.Background()

	type staged struct{ id, version string }
	var stagedAll []staged
	for i := range 3 {
		data := randomZip(t, uint64(i), size-(64<<10))
		req := domain.TransferRequest{Class: domain.ArtifactSource, MediaType: "application/zip", ExpectedDigest: digestOf(data), ExpectedSize: int64(len(data)), Deadline: time.Now().Add(10 * time.Minute)}
		tr, _, cap, err := a.Begin(ctx, transferCmd("tenant_a", fmt.Sprintf("mem_%d", i)), scopeA, req)
		require.NoError(t, err)
		version, err := artifactstore.Upload(ctx, *cap, data)
		require.NoError(t, err)
		stagedAll = append(stagedAll, staged{tr.ID, version})
	}
	// The uploads' bytes are not part of the measurement. The soft limit is
	// the chart's GOMEMLIMIT (goMemLimit: 200MiB).
	const softLimit = 200 << 20
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(softLimit))
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	// peak is the heap in use; resident approximates what the cgroup counts:
	// everything the runtime holds from the OS minus the heap it returned.
	var peak, resident atomic.Uint64
	raise := func(v *atomic.Uint64, x uint64) {
		for cur := v.Load(); x > cur && !v.CompareAndSwap(cur, x); cur = v.Load() {
		}
	}
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			raise(&peak, m.HeapInuse)
			raise(&resident, m.Sys-m.HeapReleased)
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
	var wg sync.WaitGroup
	errs := make([]error, len(stagedAll))
	for i, s := range stagedAll {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, errs[i] = a.Finalize(ctx, transferCmd("tenant_a", fmt.Sprintf("mem_%d:fin", i)), s.id, "", s.version, "")
		}()
	}
	wg.Wait()
	close(stop)
	<-sampled
	for i, err := range errs {
		require.NoError(t, err, "finalization %d", i)
		got, err := a.Get(ctx, scopeA, stagedAll[i].id, "")
		require.NoError(t, err)
		require.Equal(t, domain.TransferFinalized, got.State)
	}
	growth := int64(peak.Load()) - int64(base.HeapInuse)
	t.Logf("P1.5 AC8: base heap in use %d MiB, peak heap in use %d MiB (growth %d MiB), peak runtime resident %d MiB over three concurrent 64 MiB archive finalizations (container limit %d MiB, GOMEMLIMIT %d MiB)",
		base.HeapInuse>>20, peak.Load()>>20, growth>>20, resident.Load()>>20, limitBytes>>20, softLimit>>20)
	require.Less(t, peak.Load(), uint64(limitBytes), "the heap stays under the container limit")
	require.Less(t, resident.Load(), uint64(limitBytes), "what the runtime holds from the OS stays under the container limit")
}

// randomZip is a stored (uncompressed) zip with one entry of n random bytes.
func randomZip(t *testing.T, seed uint64, n int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.CreateHeader(&zip.FileHeader{Name: "src/blob.bin", Method: zip.Store})
	require.NoError(t, err)
	r := rand.NewChaCha8([32]byte{byte(seed)})
	chunk := make([]byte, 1<<20)
	for written := 0; written < n; written += len(chunk) {
		if n-written < len(chunk) {
			chunk = chunk[:n-written]
		}
		_, _ = r.Read(chunk)
		_, err := f.Write(chunk)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}
