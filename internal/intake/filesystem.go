// Package intake persists the immutable obligation inventory independently of
// SQL: the intake objects of local checks and, since S1-T05 (2026-09-12), the
// job-launch, model-dispatch and business-write records of the DD-02 recovery
// discovery boundaries. Objects are never deleted here; automatic cleanup stays
// disabled and any removal is a reviewed operator action.
package intake

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
	"strings"
)

var ErrConflict = errors.New("intake object conflicts with retained identity")
var objectKey = regexp.MustCompile(`^obligations/[a-z0-9-]+/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/[A-Za-z0-9][A-Za-z0-9._:-]*/(intake|job-launch|model-dispatch|business-write)/[A-Za-z0-9][A-Za-z0-9._:-]*$`)

const maxBytes = 4096

type Filesystem struct{ root *os.Root }

// Open requires an operator-provisioned directory outside database storage.
// os.Root confines every lookup, including any symlink, to this directory.
func Open(directory string) (*Filesystem, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, errors.New("intake directory is unavailable")
	}
	return &Filesystem{root}, nil
}

func (f *Filesystem) Close() error { return f.root.Close() }

// Persist publishes a fully synced file with an atomic, no-replace hard link.
// An interrupted temporary file is never a discoverable obligation. Every
// ancestor is synced even on replay, including concurrent directory creation.
func (f *Filesystem) Persist(key string, body []byte) (string, error) {
	if !objectKey.MatchString(key) || len(body) == 0 || len(body) > maxBytes {
		return "", errors.New("invalid intake object")
	}
	directory := path.Dir(key)
	if err := f.root.MkdirAll(directory, 0700); err != nil {
		return "", err
	}
	temporary := path.Join(directory, ".pending-"+rand.Text())
	file, err := f.root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	defer f.root.Remove(temporary)
	_, writeErr := file.Write(body)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return "", err
	}
	if err := f.root.Link(temporary, key); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", err
	}
	actual, err := f.Read(key)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(actual, body) {
		return "", ErrConflict
	}
	for dir := directory; ; dir = path.Dir(dir) {
		d, err := f.root.Open(dir)
		if err != nil {
			return "", err
		}
		err = errors.Join(d.Sync(), d.Close())
		if err != nil {
			return "", err
		}
		if dir == "." {
			break
		}
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(actual)), nil
}

func (f *Filesystem) Read(key string) ([]byte, error) {
	if !objectKey.MatchString(key) {
		return nil, errors.New("invalid intake key")
	}
	file, err := f.root.Open(key)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxBytes {
		return nil, errors.New("invalid intake file")
	}
	return io.ReadAll(io.LimitReader(file, maxBytes+1))
}

// List returns every complete key below the requested environment/date prefix.
// Traversal errors are returned, never reported as a complete empty inventory.
func (f *Filesystem) List(prefix string) ([]string, error) {
	if !fs.ValidPath(prefix) || !strings.HasPrefix(prefix+"/", "obligations/") {
		return nil, errors.New("invalid intake prefix")
	}
	var keys []string
	err := fs.WalkDir(f.root.FS(), prefix, func(key string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && objectKey.MatchString(key) {
			if entry.Type()&os.ModeSymlink != 0 {
				return errors.New("intake inventory contains a symbolic link")
			}
			keys = append(keys, key)
		}
		return nil
	})
	return keys, err
}
