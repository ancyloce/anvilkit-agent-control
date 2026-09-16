package inventory_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

func TestPutIdempotentByKeyAndBody(t *testing.T) {
	root := t.TempDir()
	inv, err := inventory.NewFilesystem(root)
	require.NoError(t, err)
	ctx := context.Background()
	v1, err := inv.Put(ctx, "intake/op_1", []byte(`{"class":"intake","operationId":"op_1"}`))
	require.NoError(t, err)
	v2, err := inv.Put(ctx, "intake/op_1", []byte(`{"class":"intake","operationId":"op_1"}`))
	require.NoError(t, err, "a retry after a lost acknowledgement succeeds")
	require.Equal(t, v1, v2, "same body, same immutable version")
	_, err = inv.Put(ctx, "intake/op_1", []byte(`{"class":"intake","operationId":"op_1","extra":true}`))
	require.ErrorIs(t, err, domain.ErrIdempotencyConflict)
	body, err := os.ReadFile(filepath.Join(root, "intake", "op_1"))
	require.NoError(t, err)
	require.JSONEq(t, `{"class":"intake","operationId":"op_1"}`, string(body), "the published object is never overwritten")
	entries, err := os.ReadDir(filepath.Join(root, "intake"))
	require.NoError(t, err)
	require.Len(t, entries, 1, "no temporary file is left behind")
}

// A write interrupted before publication leaves only a temporary file; the
// key is absent, and a retry publishes the full body under the key.
func TestPutRecoversAfterInterruptedWrite(t *testing.T) {
	root := t.TempDir()
	inv, err := inventory.NewFilesystem(root)
	require.NoError(t, err)
	dir := filepath.Join(root, "job-launch")
	require.NoError(t, os.MkdirAll(dir, 0o750))
	// What a crashed replica leaves behind: a partial temporary file.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lch_1.partial-deadbeefdeadbeef"), []byte(`{"class":"job-la`), 0o640))
	_, err = os.Stat(filepath.Join(dir, "lch_1"))
	require.True(t, errors.Is(err, os.ErrNotExist), "an interrupted write never appears under the key")

	body := []byte(`{"class":"job-launch","launchId":"lch_1"}`)
	v, err := inv.Put(context.Background(), "job-launch/lch_1", body)
	require.NoError(t, err)
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, v)
	got, err := os.ReadFile(filepath.Join(dir, "lch_1"))
	require.NoError(t, err)
	require.Equal(t, body, got)
	again, err := inv.Put(context.Background(), "job-launch/lch_1", body)
	require.NoError(t, err)
	require.Equal(t, v, again)
}

// Concurrent publications of one key: identical bodies all succeed with one
// version; different bodies leave exactly one winner and the winner's
// bytes, never a torn or mixed object.
func TestPutConcurrentPublications(t *testing.T) {
	inv, err := inventory.NewFilesystem(t.TempDir())
	require.NoError(t, err)
	ctx := context.Background()
	body := []byte(strings.Repeat(`{"class":"intake","operationId":"op_c"}`, 64))
	var wg sync.WaitGroup
	versions := make([]string, 32)
	errs := make([]error, 32)
	for i := range versions {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			versions[i], errs[i] = inv.Put(ctx, "intake/op_c", body)
		}(i)
	}
	wg.Wait()
	for i := range versions {
		require.NoError(t, errs[i])
		require.Equal(t, versions[0], versions[i])
	}

	wins, conflicts := 0, 0
	bodies := make([][]byte, 32)
	for i := range bodies {
		bodies[i] = []byte(`{"class":"intake","operationId":"op_d","writer":` + strings.Repeat("9", i+1) + `}`)
	}
	results := make([]error, 32)
	for i := range bodies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = inv.Put(ctx, "intake/op_d", bodies[i])
		}(i)
	}
	wg.Wait()
	var winner []byte
	for i, err := range results {
		switch {
		case err == nil:
			wins++
			winner = bodies[i]
		case errors.Is(err, domain.ErrIdempotencyConflict):
			conflicts++
		default:
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	require.Equal(t, 1, wins)
	require.Equal(t, 31, conflicts)
	got, err := os.ReadFile(filepath.Join(inv.Root(), "intake", "op_d"))
	require.NoError(t, err)
	require.Equal(t, winner, got)
}

func TestPutRejectsUnsafeKeys(t *testing.T) {
	inv, err := inventory.NewFilesystem(t.TempDir())
	require.NoError(t, err)
	for _, key := range []string{"../escape", "/abs", "intake/x.partial", "intake/x.partial-00"} {
		_, err := inv.Put(context.Background(), key, []byte("{}"))
		require.ErrorIs(t, err, domain.ErrInvalid, key)
	}
}

// List enumerates published objects in key order with a resumable cursor,
// never a temporary file, and a missing prefix is a complete empty page.
func TestListIsOrderedResumableAndSkipsTemporaries(t *testing.T) {
	root := t.TempDir()
	inv, err := inventory.NewFilesystem(root)
	require.NoError(t, err)
	ctx := context.Background()
	for _, id := range []string{"c", "a", "b"} {
		_, err := inv.Put(ctx, "intake/op_"+id, []byte(`{"class":"intake","operationId":"op_`+id+`"}`))
		require.NoError(t, err)
	}
	_, err = inv.Put(ctx, "job-launch/lch_1", []byte(`{"class":"job-launch"}`))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "intake", "op_d.partial-deadbeefdeadbeef"), []byte(`{`), 0o640))

	p1, err := inv.List(ctx, "intake/", "", 2)
	require.NoError(t, err)
	require.False(t, p1.Complete)
	require.Equal(t, []string{"intake/op_a", "intake/op_b"}, keys(p1))
	require.Equal(t, "intake/op_b", p1.NextCursor)
	p2, err := inv.List(ctx, "intake/", p1.NextCursor, 2)
	require.NoError(t, err)
	require.True(t, p2.Complete)
	require.Equal(t, []string{"intake/op_c"}, keys(p2), "the temporary file is never an object")
	for _, o := range append(p1.Objects, p2.Objects...) {
		require.Regexp(t, `^sha256:[0-9a-f]{64}$`, o.Version)
		require.False(t, o.ModifiedAt.IsZero())
	}
	empty, err := inv.List(ctx, "business-write/", "", 10)
	require.NoError(t, err)
	require.True(t, empty.Complete)
	require.Empty(t, empty.Objects)
	all, err := inv.List(ctx, "", "", 10)
	require.NoError(t, err)
	require.Len(t, all.Objects, 4)
}

func keys(p application.InventoryPage) []string {
	out := make([]string, 0, len(p.Objects))
	for _, o := range p.Objects {
		out = append(out, o.Key)
	}
	return out
}

// An absent class directory under an available root is an empty, complete
// listing; an absent root (an unmounted volume) is unavailability, never an
// empty inventory or a missing object, and no error names the path.
func TestUnavailableRootIsNotAnEmptyInventory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "inventory")
	inv, err := inventory.NewFilesystem(root)
	require.NoError(t, err)
	ctx := context.Background()
	_, err = inv.Put(ctx, "intake/op_1", []byte(`{"class":"intake","operationId":"op_1"}`))
	require.NoError(t, err)
	page, err := inv.List(ctx, "business-write/", "", 10)
	require.NoError(t, err)
	require.True(t, page.Complete)
	require.Empty(t, page.Objects, "no object of the class was ever published")

	require.NoError(t, os.RemoveAll(root))
	_, err = inv.List(ctx, "intake/", "", 10)
	require.ErrorIs(t, err, inventory.ErrUnavailable, "an absent root is not an empty listing")
	require.NotContains(t, err.Error(), root, "no path in the diagnostic")
	_, _, err = inv.Get(ctx, "intake/op_1")
	require.ErrorIs(t, err, inventory.ErrUnavailable)
	require.NotErrorIs(t, err, domain.ErrNotFound, "unavailability is not absence")
	require.NotContains(t, err.Error(), root)
	_, err = inv.Put(ctx, "intake/op_2", []byte(`{}`))
	require.ErrorIs(t, err, inventory.ErrUnavailable, "the root is not silently recreated")
	require.NotContains(t, err.Error(), root)
	_, err = os.Stat(root)
	require.True(t, errors.Is(err, os.ErrNotExist))
}
