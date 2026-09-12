package artifacts

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func digestOf(body []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(body)) }

func TestImmutableObjectsAndReferenceVerification(t *testing.T) {
	directory := t.TempDir()
	s, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	body := []byte(`{"fixture":"S1-T05 artifact","bytes":"controlled"}`)
	key, err := Key("tenant-a", "op-1", "source", "src-1")
	if err != nil {
		t.Fatal(err)
	}
	stored, err := s.Write(key, body, ByteCeiling("source"))
	if err != nil {
		t.Fatal(err)
	}
	if stored.ContentDigest != digestOf(body) || stored.ObjectVersion != digestOf(body) || stored.SizeBytes != uint64(len(body)) {
		t.Fatal("stored identity is not derived from the bytes", stored)
	}
	// Identical replay returns the same object; different bytes under the key conflict; nothing is replaced.
	if again, err := s.Write(key, body, ByteCeiling("source")); err != nil || again != stored {
		t.Fatal("identical replay changed the object", err, again)
	}
	if _, err := s.Write(key, []byte(`{"fixture":"altered"}`), ByteCeiling("source")); !errors.Is(err, ErrConflict) {
		t.Fatalf("different bytes under an immutable key: %v", err)
	}
	if actual, err := os.ReadFile(filepath.Join(directory, key)); err != nil || !bytes.Equal(actual, body) {
		t.Fatal("conflicting write altered the stored bytes")
	}
	// The ceiling is enforced before and after the bytes exist.
	if _, err := s.Write(key+"-big", make([]byte, 3), 2); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("ceiling: %v", err)
	}
	if _, err := s.Write(key+"-abs", []byte("x"), AbsoluteByteCeiling+1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("absolute ceiling: %v", err)
	}
	valid := Reference{Kind: "source", RefID: "src-1", SubjectDigest: digestOf([]byte("subject")), ContentDigest: stored.ContentDigest, SizeBytes: stored.SizeBytes, ObjectVersion: stored.ObjectVersion}
	if got, err := s.Verify("tenant-a", "op-1", valid); err != nil || got != stored {
		t.Fatal("a faithful reference must verify", err)
	}
	cases := []struct {
		name       string
		tenant, op string
		ref        Reference
		want       error
	}{
		{"wrong tenant", "tenant-b", "op-1", valid, ErrNotFound},
		{"wrong operation", "tenant-a", "op-2", valid, ErrNotFound},
		{"altered content digest", "tenant-a", "op-1", func() Reference { r := valid; r.ContentDigest = digestOf([]byte("tampered")); return r }(), ErrContent},
		{"altered size", "tenant-a", "op-1", func() Reference { r := valid; r.SizeBytes++; return r }(), ErrContent},
		{"altered object version", "tenant-a", "op-1", func() Reference { r := valid; r.ObjectVersion = "v-not-issued"; return r }(), ErrVersion},
		{"other kind", "tenant-a", "op-1", func() Reference { r := valid; r.Kind = "bundle"; return r }(), ErrNotFound},
	}
	for _, c := range cases {
		if _, err := s.Verify(c.tenant, c.op, c.ref); !errors.Is(err, c.want) {
			t.Fatalf("%s: wanted %v, got %v", c.name, c.want, err)
		}
		if got, _, err := s.Read(c.tenant, c.op, c.ref); err == nil || got != nil {
			t.Fatalf("%s: a reference that does not verify must return no bytes", c.name)
		}
	}
	if _, err := s.Verify("tenant-a", "op-1", Reference{Kind: "source", RefID: "src-1"}); err == nil {
		t.Fatal("a malformed reference must be rejected before any read")
	}
	got, _, err := s.Read("tenant-a", "op-1", valid)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatal("verified read", err)
	}
	// An interrupted temporary file is never an object, and listing is complete or fails.
	if err := os.WriteFile(filepath.Join(directory, filepath.Dir(key), ".pending-crash"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	keys, err := s.List("artifacts/tenant-a")
	if err != nil || len(keys) != 1 || keys[0] != key {
		t.Fatal("listing", keys, err)
	}
	if _, err := s.List("obligations/x"); err == nil {
		t.Fatal("the artifact store lists only artifact keys")
	}
	// Bytes altered on disk behind the store are detected by every verification.
	if err := os.Chmod(filepath.Join(directory, key), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, key), []byte(`{"fixture":"S1-T05 artifact","bytes":"REPLACED"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Verify("tenant-a", "op-1", valid); !errors.Is(err, ErrContent) {
		t.Fatalf("tampered stored bytes: %v", err)
	}
}

func TestKeysAndCeilings(t *testing.T) {
	if _, err := Key("tenant/../x", "op", "source", "r"); err == nil {
		t.Fatal("path characters in a tenant id")
	}
	if _, err := Key("tenant", "op", "not-a-kind", "r"); err == nil {
		t.Fatal("unknown artifact kind")
	}
	if ByteCeiling("bundle") != 32<<20 || ByteCeiling("source") != 64<<20 || ByteCeiling("preview-artifact") > AbsoluteByteCeiling {
		t.Fatal("development ceilings")
	}
	if ValidReference(Reference{Kind: "source", RefID: "r", SubjectDigest: digestOf(nil), ContentDigest: digestOf(nil), SizeBytes: 0, ObjectVersion: "v"}) {
		t.Fatal("a zero-size reference is not valid")
	}
}
