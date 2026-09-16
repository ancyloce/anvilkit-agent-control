// Package inventory holds InventoryPort adapters (DD-02 §5).
//
// Filesystem is DEVELOPMENT_ONLY: it gives the application the conditional
// immutable-create semantics of the qualified RGW backend (same key+body
// idempotent, changed body conflicts, versions immutable) on a local
// directory. It is not an independent failure domain and proves nothing
// about DR. S3 is the AWS SDK for Go v2 adapter for an S3-compatible
// backend (Ceph RGW as the C09 primary); its Qualify probe verifies the
// conditional-create and enumeration behavior of the actual backend, and
// nothing in this package claims the independent failure domain of ENV-02.
package inventory

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

type Filesystem struct{ root string }

// ErrUnavailable marks a backend that could not answer: the inventory root
// is absent or unreadable, a request could not be made, or the backend
// answered with a failure. It is never absence of an object or an empty
// listing.
var ErrUnavailable = errors.New("inventory backend unavailable")

// failure wraps a backend error into a controlled diagnostic: the
// operation, the object's controlled identity (application.DescribeKey)
// and a classification of the failure. Paths, prefixes, buckets, URLs and
// the backend's own message never propagate (security.md data
// classification, "full object keys and presigned URLs never enter logs").
func failure(op, key string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrIdempotencyConflict) || errors.Is(err, domain.ErrInvalid) || errors.Is(err, ErrUnavailable) {
		return err
	}
	where := "inventory"
	if key != "" {
		where = "inventory " + application.DescribeKey(key)
	}
	return fmt.Errorf("%w: %s %s: %s", ErrUnavailable, op, where, classify(err))
}

// classify names the failure without its locators.
func classify(err error) string {
	var errno syscall.Errno
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, os.ErrNotExist):
		return "path absent"
	case errors.Is(err, os.ErrPermission):
		return "permission denied"
	case errors.As(err, &errno):
		return "errno " + errno.Error()
	}
	return "backend error"
}

// available verifies that the inventory root still exists as a directory:
// an unmounted or removed root is unavailability, never an empty
// inventory.
func (f *Filesystem) available() error {
	info, err := os.Stat(f.root)
	if err != nil {
		return failure("stat root", "", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: root is not a directory", ErrUnavailable)
	}
	return nil
}

// Root is the directory holding the objects (tests and diagnostics).
func (f *Filesystem) Root() string { return f.root }

func NewFilesystem(root string) (*Filesystem, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	return &Filesystem{root: root}, nil
}

// tempSuffix marks in-progress objects; a key never ends with it, so an
// interrupted write can never be read back as the object.
const tempSuffix = ".partial"

// Put publishes the object atomically and durably: the body is written to a
// private temporary file in the key's directory, fsynced, then linked into
// place under the key (link fails when the key exists, which is the
// conditional create), and the directory entry is fsynced. The key therefore
// either holds the complete body or nothing; a process that dies mid-write
// leaves only a temporary file, and a retry of the same key and body
// succeeds with the same version. A different body for an existing key is
// domain.ErrIdempotencyConflict. The version is the content digest, which
// is what an immutable object store returns for identical bodies.
//
// A same-key retry or a concurrent writer that finds the object already
// linked may be observing another process's write before that process
// fsynced the directory entry (or after it died before doing so). Every
// success path therefore fsyncs the file and its directory itself before
// returning the version, so no caller commits a durable record against an
// object that could still vanish.
func (f *Filesystem) Put(ctx context.Context, key string, body []byte) (string, error) {
	version, err := f.put(ctx, key, body)
	return version, failure("put", key, err)
}

func (f *Filesystem) put(ctx context.Context, key string, body []byte) (string, error) {
	if strings.Contains(key, "..") || strings.HasPrefix(key, "/") || strings.Contains(key, tempSuffix) {
		return "", fmt.Errorf("%w: inventory key %s", domain.ErrInvalid, application.DescribeKey(key))
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := f.available(); err != nil {
		return "", err
	}
	path := filepath.Join(f.root, filepath.FromSlash(key))
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	version := fmt.Sprintf("sha256:%x", sha256.Sum256(body))
	if existing, err := os.ReadFile(path); err == nil {
		if err := compare(key, existing, body); err != nil {
			return "", err
		}
		return version, syncPublished(path, dir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	tmp := path + tempSuffix + "-" + hex.EncodeToString(nonce[:])
	if err := writeSynced(tmp, body); err != nil {
		os.Remove(tmp)
		return "", err
	}
	defer os.Remove(tmp)
	if err := os.Link(tmp, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return "", err
		}
		// Another replica (or an earlier attempt whose acknowledgement was
		// lost) published the key first: same body is idempotent, and this
		// writer makes the winner durable before reporting success.
		existing, rerr := os.ReadFile(path)
		if rerr != nil {
			return "", rerr
		}
		if err := compare(key, existing, body); err != nil {
			return "", err
		}
		return version, syncPublished(path, dir)
	}
	if err := syncDir(dir); err != nil {
		return "", err
	}
	return version, nil
}

// syncPublished fsyncs an already linked object and its directory entry.
func syncPublished(path, dir string) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	return syncDir(dir)
}

func compare(key string, existing, body []byte) error {
	if !bytes.Equal(existing, body) {
		return fmt.Errorf("%w: inventory %s holds a different body", domain.ErrIdempotencyConflict, application.DescribeKey(key))
	}
	return nil
}

// writeSynced writes body to a new private file and fsyncs it before close.
func writeSynced(path string, body []byte) error {
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return err
	}
	if _, err := fh.Write(body); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	return fh.Close()
}

// syncDir makes the new directory entry durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Get reads a published object and its version (the content digest). A
// missing key under an available root is domain.ErrNotFound; an
// unavailable root is ErrUnavailable, never absence; an object is never
// read through its temporary name.
func (f *Filesystem) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.Contains(key, "..") || strings.HasPrefix(key, "/") || strings.Contains(key, tempSuffix) {
		return nil, "", fmt.Errorf("%w: inventory key %s", domain.ErrInvalid, application.DescribeKey(key))
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := f.available(); err != nil {
		return nil, "", err
	}
	body, err := os.ReadFile(filepath.Join(f.root, filepath.FromSlash(key)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", fmt.Errorf("%w: inventory %s", domain.ErrNotFound, application.DescribeKey(key))
	}
	if err != nil {
		return nil, "", failure("get", key, err)
	}
	return body, fmt.Sprintf("sha256:%x", sha256.Sum256(body)), nil
}

// List enumerates the published objects under prefix in key order, one
// page per call. cursor is the last key of the previous page (StartAfter
// semantics). The inventory root must exist: a root that is absent or
// unreadable (an unmounted volume) is ErrUnavailable, never an empty
// listing; a class directory that does not exist under an available root
// is an empty, complete listing, because no object of that class was ever
// published. Any other unreadable directory is an error. Temporary files
// of interrupted writes are never listed.
func (f *Filesystem) List(ctx context.Context, prefix, cursor string, limit int) (application.InventoryPage, error) {
	if strings.Contains(prefix, "..") || strings.HasPrefix(prefix, "/") {
		return application.InventoryPage{}, fmt.Errorf("%w: inventory prefix %q", domain.ErrInvalid, prefix)
	}
	if limit <= 0 {
		limit = 1000
	}
	if err := ctx.Err(); err != nil {
		return application.InventoryPage{}, err
	}
	if err := f.available(); err != nil {
		return application.InventoryPage{}, err
	}
	var keys []string
	start := filepath.Join(f.root, filepath.FromSlash(prefix))
	// A prefix that names a directory lists its tree; any other prefix is
	// matched against the keys of its parent directory's tree.
	walkRoot := start
	if info, err := os.Stat(start); err != nil || !info.IsDir() {
		walkRoot = filepath.Dir(start)
	}
	if _, err := os.Stat(walkRoot); errors.Is(err, os.ErrNotExist) {
		return application.InventoryPage{Complete: true}, nil
	} else if err != nil {
		return application.InventoryPage{}, failure("list", prefix, err)
	}
	err := filepath.WalkDir(walkRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.Contains(d.Name(), tempSuffix) {
			return nil
		}
		rel, err := filepath.Rel(f.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) && key > cursor {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return application.InventoryPage{}, failure("list", prefix, err)
	}
	sort.Strings(keys)
	page := application.InventoryPage{Complete: true}
	if len(keys) > limit {
		keys, page.Complete = keys[:limit], false
	}
	for _, key := range keys {
		body, version, err := f.Get(ctx, key)
		if err != nil {
			return application.InventoryPage{}, err
		}
		info, err := os.Stat(filepath.Join(f.root, filepath.FromSlash(key)))
		if err != nil {
			return application.InventoryPage{}, failure("list", key, err)
		}
		page.Objects = append(page.Objects, application.InventoryObject{Key: key, Version: version, ModifiedAt: info.ModTime().UTC(), Size: int64(len(body))})
	}
	if !page.Complete {
		page.NextCursor = keys[len(keys)-1]
	}
	return page, nil
}
