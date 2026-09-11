package intake

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

const testKey = "obligations/workflow02/2026/09/10/12/fixture-tenant/intake/op-one"

func TestConditionalPersistenceAndRestart(t *testing.T) {
	directory := t.TempDir()
	f, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"fixtureId":"plain-v1","operationId":"op-one"}`)
	var wg sync.WaitGroup
	versions := make(chan string, 12)
	for range 12 {
		wg.Go(func() {
			version, err := f.Persist(testKey, body)
			if err != nil {
				t.Error(err)
			}
			versions <- version
		})
	}
	wg.Wait()
	close(versions)
	var original string
	for version := range versions {
		if original != "" && version != original {
			t.Fatal("identical persistence returned a different immutable version")
		}
		original = version
	}
	if _, err := f.Persist(testKey, []byte(`{"fixtureId":"newline-v1"}`)); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting body: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	actual, err := f.Read(testKey)
	if err != nil || !bytes.Equal(actual, body) {
		t.Fatal("restart lost immutable content", err)
	}
	// An abandoned file before atomic publication is not an intake object.
	if err := os.WriteFile(filepath.Join(directory, filepath.Dir(testKey), ".pending-crash"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	keys, err := f.List("obligations/workflow02")
	if err != nil || len(keys) != 1 || keys[0] != testKey {
		t.Fatalf("incomplete discovery: %v %v", keys, err)
	}
	if _, err := f.Persist(testKey, body); err != nil {
		t.Fatal("readback replay failed", err)
	}
}

func TestPersistenceFailsClosed(t *testing.T) {
	directory := t.TempDir()
	f, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, key := range []string{"../escape", "/absolute", testKey + "/child", "obligations/local/2026/09/10/12/../intake/op-one"} {
		if _, err := f.Persist(key, []byte("body")); err == nil {
			t.Fatalf("accepted invalid key %q", key)
		}
	}
	if _, err := f.Persist(testKey, make([]byte, maxBytes+1)); err == nil {
		t.Fatal("accepted oversized object")
	}
	if _, err := f.List("obligations/missing"); err == nil {
		t.Fatal("reported missing prefix as a complete empty inventory")
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(directory, "obligations")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Persist(testKey, []byte("body")); err == nil {
		t.Fatal("followed link outside intake root")
	}
}
