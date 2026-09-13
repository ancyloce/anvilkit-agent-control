// Package artifacts is Control's artifact-access adapter (DD-02
// #artifact-adapter-mode; development plan S1-T05, 2026-09-12): an immutable
// object store for artifact bytes, reference verification against the bytes
// actually stored, and the scoped, expiring transfer capabilities that jobs
// obtain through the sidecar. The store is an operator-provisioned directory
// confined by os.Root, the same mechanics as the obligation inventory; it is the
// compatible local backend, not the external artifact API of DD-06.
//
// Nothing here deletes an object. Active drafts, review subjects, publication
// intents and saved-page locks hold exact versions, so automatic cleanup stays
// disabled and any removal is a reviewed operator action outside this package.
package artifacts

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

var (
	// ErrConflict: a different body already exists under an immutable key.
	ErrConflict = errors.New("artifact object conflicts with the stored bytes")
	// ErrTooLarge: the body exceeds the capability's byte ceiling.
	ErrTooLarge = errors.New("RESOURCE_LIMIT: artifact exceeds its byte ceiling")
	// ErrNotFound: no object is stored for the tenant and reference.
	ErrNotFound = errors.New("NOT_FOUND: artifact object")
	// ErrContent: the reference's content digest or size disagree with the stored bytes.
	ErrContent = errors.New("INVALID_ARGUMENT: artifact reference does not match the stored content")
	// ErrVersion: the reference names an object version the store never issued for those bytes.
	ErrVersion = errors.New("INVALID_ARGUMENT: artifact reference does not match the stored object version")
)

// Kinds mirrors urn:anvilkit:values:v1#/$defs/artifactRef/properties/kind.
var Kinds = []string{"generation-request", "release-request", "source", "plan", "build-support", "validation", "bundle", "preview-artifact", "model-response", "transcript", "evidence", "preparation-input"}

var (
	id        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	digest    = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	objectKey = regexp.MustCompile(`^artifacts/[A-Za-z0-9][A-Za-z0-9._:-]*/[A-Za-z0-9][A-Za-z0-9._:-]*/[a-z-]+/[A-Za-z0-9][A-Za-z0-9._:-]*$`)
)

// AbsoluteByteCeiling is the DD-02 development default for a single object.
// It bounds every capability; the per-kind ceilings below never exceed it.
const AbsoluteByteCeiling uint64 = 256 << 20

// ByteCeiling is the DD-02 development default per artifact kind (64 MiB for
// source snapshots, preview artifacts and evidence classes, 32 MiB for the
// delivery bundle). These are local-work defaults, not measured capacity; the
// qualified values are activation inputs.
func ByteCeiling(kind string) uint64 {
	if kind == "bundle" {
		return 32 << 20
	}
	return 64 << 20
}

// Object describes bytes the store actually holds. ObjectVersion identifies the
// immutable stored content: a local object is never replaced, so its version is
// the digest of its bytes, exactly as the intake inventory reports it. A remote
// backend substitutes its server-issued immutable version.
type Object struct {
	Key, ContentDigest, ObjectVersion string
	SizeBytes                         uint64
}

// Reference mirrors the artifactRef value: an immutable reference verified
// against the stored bytes before anything accepts it.
type Reference struct {
	Kind, RefID, SubjectDigest, ContentDigest, ObjectVersion string
	SizeBytes                                                uint64
}

// Key is the deterministic object key of one artifact: the owning tenant and
// operation are part of the path, so a reference presented under another
// tenant resolves to nothing.
func Key(tenantID, operationID, kind, refID string) (string, error) {
	if !id.MatchString(tenantID) || !id.MatchString(operationID) || !id.MatchString(refID) || !slices.Contains(Kinds, kind) {
		return "", errors.New("INVALID_ARGUMENT: artifact key")
	}
	return fmt.Sprintf("artifacts/%s/%s/%s/%s", tenantID, operationID, kind, refID), nil
}

// ValidReference reports whether every field of the reference has the shape the
// common values contract requires. Shape is not verification; see Verify.
func ValidReference(ref Reference) bool {
	return slices.Contains(Kinds, ref.Kind) && id.MatchString(ref.RefID) && digest.MatchString(ref.SubjectDigest) && digest.MatchString(ref.ContentDigest) && id.MatchString(ref.ObjectVersion) && ref.SizeBytes > 0
}

type Store struct{ root *os.Root }

// Open requires an operator-provisioned directory outside database storage.
func Open(directory string) (*Store, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, errors.New("artifact directory is unavailable")
	}
	return &Store{root}, nil
}

func (s *Store) Close() error { return s.root.Close() }

// Write publishes an object under an immutable key: a fully synced temporary
// file linked into place without replacement. The outcomes are created,
// identical-body replay (the same Object again) or ErrConflict for different
// bytes under the key; an interrupted temporary file is never an object.
func (s *Store) Write(key string, body []byte, ceiling uint64) (Object, error) {
	if !objectKey.MatchString(key) || len(body) == 0 {
		return Object{}, errors.New("INVALID_ARGUMENT: artifact object")
	}
	if ceiling == 0 || ceiling > AbsoluteByteCeiling || uint64(len(body)) > ceiling {
		return Object{}, ErrTooLarge
	}
	directory := path.Dir(key)
	if err := s.root.MkdirAll(directory, 0700); err != nil {
		return Object{}, err
	}
	temporary := path.Join(directory, ".pending-"+rand.Text())
	file, err := s.root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return Object{}, err
	}
	defer s.root.Remove(temporary)
	_, writeErr := file.Write(body)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return Object{}, err
	}
	if err := s.root.Link(temporary, key); err != nil && !errors.Is(err, fs.ErrExist) {
		return Object{}, err
	}
	stored, actual, err := s.read(key, ceiling)
	if err != nil {
		return Object{}, err
	}
	if !bytes.Equal(actual, body) {
		return Object{}, ErrConflict
	}
	for dir := directory; ; dir = path.Dir(dir) {
		d, err := s.root.Open(dir)
		if err != nil {
			return Object{}, err
		}
		if err := errors.Join(d.Sync(), d.Close()); err != nil {
			return Object{}, err
		}
		if dir == "." {
			break
		}
	}
	return stored, nil
}

// Stat reads the stored bytes and reports their actual identity. It never
// trusts a caller-supplied hash: the digest, size and version come from the
// bytes on disk.
func (s *Store) Stat(key string, ceiling uint64) (Object, error) {
	stored, _, err := s.read(key, ceiling)
	return stored, err
}

func (s *Store) read(key string, ceiling uint64) (Object, []byte, error) {
	if !objectKey.MatchString(key) {
		return Object{}, nil, errors.New("INVALID_ARGUMENT: artifact key")
	}
	if ceiling == 0 || ceiling > AbsoluteByteCeiling {
		ceiling = AbsoluteByteCeiling
	}
	file, err := s.root.Open(key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Object{}, nil, ErrNotFound
		}
		return Object{}, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || uint64(info.Size()) > ceiling {
		return Object{}, nil, errors.New("invalid artifact file")
	}
	body, err := io.ReadAll(io.LimitReader(file, int64(ceiling)+1))
	if err != nil {
		return Object{}, nil, err
	}
	if uint64(len(body)) > ceiling {
		return Object{}, nil, ErrTooLarge
	}
	sum := fmt.Sprintf("sha256:%x", sha256.Sum256(body))
	return Object{Key: key, ContentDigest: sum, ObjectVersion: sum, SizeBytes: uint64(len(body))}, body, nil
}

// Verify resolves the reference under the tenant and operation that own it and
// compares it with the bytes actually stored: a wrong tenant or operation finds
// no object, altered bytes fail the digest or size, and a version the store
// never issued fails the object version. Only a reference that passes all of
// them is accepted anywhere in Control.
func (s *Store) Verify(tenantID, operationID string, ref Reference) (Object, error) {
	stored, _, err := s.verify(tenantID, operationID, ref)
	return stored, err
}

func (s *Store) verify(tenantID, operationID string, ref Reference) (Object, []byte, error) {
	if !ValidReference(ref) {
		return Object{}, nil, errors.New("INVALID_ARGUMENT: artifact reference")
	}
	key, err := Key(tenantID, operationID, ref.Kind, ref.RefID)
	if err != nil {
		return Object{}, nil, err
	}
	stored, body, err := s.read(key, ByteCeiling(ref.Kind))
	if err != nil {
		return Object{}, nil, err
	}
	if stored.ContentDigest != ref.ContentDigest || stored.SizeBytes != ref.SizeBytes {
		return Object{}, nil, ErrContent
	}
	if stored.ObjectVersion != ref.ObjectVersion {
		return Object{}, nil, ErrVersion
	}
	return stored, body, nil
}

// Read returns the bytes of a verified reference; a reference that does not
// verify returns no bytes at all.
func (s *Store) Read(tenantID, operationID string, ref Reference) ([]byte, Object, error) {
	stored, body, err := s.verify(tenantID, operationID, ref)
	if err != nil {
		return nil, Object{}, err
	}
	return body, stored, nil
}

// List returns every object key below a tenant or operation prefix. Traversal
// errors are returned, never reported as a complete empty listing.
func (s *Store) List(prefix string) ([]string, error) {
	if !fs.ValidPath(prefix) || !strings.HasPrefix(prefix+"/", "artifacts/") {
		return nil, errors.New("INVALID_ARGUMENT: artifact prefix")
	}
	var keys []string
	err := fs.WalkDir(s.root.FS(), prefix, func(key string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && objectKey.MatchString(key) {
			if entry.Type()&os.ModeSymlink != 0 {
				return errors.New("artifact store contains a symbolic link")
			}
			keys = append(keys, key)
		}
		return nil
	})
	return keys, err
}

// SizeString renders a byte count as the bounded decimal string the JSON
// contracts carry for 64-bit counters.
func SizeString(n uint64) string { return strconv.FormatUint(n, 10) }

// Digest is the content digest the store issues for bytes: sha256 over them.
func Digest(body []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(body)) }
