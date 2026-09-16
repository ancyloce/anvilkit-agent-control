package domain

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

// ArchiveLimits bound the inspection of an archive-typed artifact.
type ArchiveLimits struct {
	MaxEntries      int
	MaxUncompressed int64
}

// Archive formats Control inspects at the transfer boundary (DD-04 §1):
// the source contract's package forms.
const (
	archiveZip = "zip"
	archiveTar = "tar"
	archiveTgz = "tgz"
)

// ArchiveMediaTypes maps the media types a caller may declare for an
// archive to the format the bytes must then have. The declaration never
// decides whether an archive is inspected: DetectArchive does, from the
// bytes themselves.
var ArchiveMediaTypes = map[string]string{
	"application/zip":        archiveZip,
	"application/x-tar":      archiveTar,
	"application/gzip":       archiveTgz,
	"application/x-gzip":     archiveTgz,
	"application/x-tar+gzip": archiveTgz,
}

// DetectArchive names the archive format of the bytes from their own
// signature (zip local-file, end-of-central-directory or spanned marker;
// gzip member header; a tar header block with a POSIX/GNU magic or a
// verifying v7 checksum), or "" when the bytes are not an archive Control
// knows. A caller's media type plays no part.
func DetectArchive(data []byte) string {
	switch {
	case len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b:
		return archiveTgz
	case len(data) >= 4 && data[0] == 'P' && data[1] == 'K' && ((data[2] == 3 && data[3] == 4) || (data[2] == 5 && data[3] == 6) || (data[2] == 7 && data[3] == 8)):
		return archiveZip
	case len(data) >= 512 && (string(data[257:262]) == "ustar" || tarHeaderVerifies(data[:512])):
		return archiveTar
	}
	return ""
}

// tarHeaderVerifies reports whether a 512-byte block carries a tar header
// whose checksum field matches the block (the pre-POSIX v7 layout has no
// magic, only the checksum). An all-zero block is not a header.
func tarHeaderVerifies(blk []byte) bool {
	if bytes.Equal(blk, make([]byte, 512)) {
		return false
	}
	_, err := tar.NewReader(bytes.NewReader(blk)).Next()
	return err == nil
}

// CheckArchive inspects the listing of an archive artifact without
// extracting or executing anything (DD-04 §1). Whether the bytes are
// inspected is decided from the bytes: an artifact of class source must be
// an archive Control knows whatever media type was declared; bytes that
// carry an archive signature are inspected under every class; a media type
// that declares an archive the bytes are not, or another format than the
// bytes carry, is refused. Every entry must be a relative, normalized path
// inside the archive root (no absolute path, no ".." segment before or
// after normalization, no backslash or NUL); symbolic and hard links,
// device, socket and FIFO entries are refused; two paths, files or
// directories at any depth, that differ only by case are refused (case
// aliases), as are a path that is both a file and a directory and a file
// entered twice; the declared uncompressed size and entry count stay
// within the limits (decompression bombs). Bytes that are no archive and
// were not declared as one are not inspected.
func CheckArchive(class ArtifactClass, mediaType string, data []byte, limits ArchiveLimits) error {
	declared := ArchiveMediaTypes[strings.ToLower(strings.TrimSpace(strings.Split(mediaType, ";")[0]))]
	detected := DetectArchive(data)
	switch {
	case class == ArtifactSource && detected == "":
		return fmt.Errorf("%w: %s: a source artifact must be a zip, tar or gzip-compressed tar archive; the bytes are none", ErrInvalid, ReasonArchiveInvalid)
	case declared != "" && detected == "":
		return fmt.Errorf("%w: %s: declared as a %s archive, the bytes are no archive", ErrInvalid, ReasonArchiveInvalid, declared)
	case declared != "" && detected != declared:
		return fmt.Errorf("%w: %s: declared as a %s archive, the bytes are a %s archive", ErrInvalid, ReasonArchiveInvalid, declared, detected)
	case detected == "":
		return nil
	}
	switch detected {
	case archiveZip:
		return checkZip(data, limits)
	case archiveTar:
		return checkTar(bytes.NewReader(data), limits)
	case archiveTgz:
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("%w: %s: gzip stream is not readable", ErrInvalid, ReasonArchiveInvalid)
		}
		defer gz.Close()
		return checkTar(gz, limits)
	}
	return nil
}

// containedPath validates one entry path and returns its normalized form.
// An upward segment is refused on the raw name, before any normalization
// could fold it away ("src/../outside.ts" escapes as much as "../x").
func containedPath(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: %s: empty entry name", ErrInvalid, ReasonPathEscape)
	}
	if strings.ContainsRune(name, '\\') || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("%w: %s: entry name contains a forbidden separator", ErrInvalid, ReasonPathEscape)
	}
	if strings.HasPrefix(name, "/") || (len(name) > 1 && name[1] == ':') {
		return "", fmt.Errorf("%w: %s: absolute entry path", ErrInvalid, ReasonPathEscape)
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == ".." {
			return "", fmt.Errorf("%w: %s: entry %q carries an upward segment", ErrInvalid, ReasonPathEscape, name)
		}
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("%w: %s: entry escapes the archive root", ErrInvalid, ReasonPathEscape)
	}
	return clean, nil
}

// pathSet is the case-folded index of every path an archive names,
// explicitly or as the parent of an entry, with the spelling first seen
// and whether it is a directory.
type pathSet map[string]pathEntry

type pathEntry struct {
	spelling string
	dir      bool
}

// add indexes an entry and each of its ancestors. A second spelling of a
// folded path is a case alias; a path that is a directory for one entry
// and a file for another is a collision; a file entered twice is a
// duplicate. A directory named by several entries (explicitly or as a
// parent) is ordinary.
func (s pathSet) add(clean string, isDir bool) error {
	segs := strings.Split(clean, "/")
	for i := range segs {
		p := strings.Join(segs[:i+1], "/")
		dir := i < len(segs)-1 || isDir
		folded := strings.ToLower(p)
		prev, seen := s[folded]
		if !seen {
			s[folded] = pathEntry{spelling: p, dir: dir}
			continue
		}
		switch {
		case prev.spelling != p:
			return fmt.Errorf("%w: %s: entries %q and %q differ only by case", ErrInvalid, ReasonArchiveInvalid, prev.spelling, p)
		case prev.dir != dir:
			return fmt.Errorf("%w: %s: %q is both a file and a directory", ErrInvalid, ReasonArchiveInvalid, p)
		case !dir:
			return fmt.Errorf("%w: %s: file %q entered twice", ErrInvalid, ReasonArchiveInvalid, p)
		}
	}
	return nil
}

func checkZip(data []byte, limits ArchiveLimits) error {
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("%w: %s: zip archive is not readable", ErrInvalid, ReasonArchiveInvalid)
	}
	if limits.MaxEntries > 0 && len(r.File) > limits.MaxEntries {
		return fmt.Errorf("%w: %s: %d entries exceed the bound %d", ErrInvalid, ReasonArchiveInvalid, len(r.File), limits.MaxEntries)
	}
	seen := pathSet{}
	var total uint64
	for _, f := range r.File {
		clean, err := containedPath(f.Name)
		if err != nil {
			return err
		}
		mode := f.Mode()
		switch {
		case mode&os.ModeSymlink != 0:
			return fmt.Errorf("%w: %s: symbolic link entry", ErrInvalid, ReasonPathEscape)
		case mode&(os.ModeDevice|os.ModeCharDevice|os.ModeNamedPipe|os.ModeSocket) != 0:
			return fmt.Errorf("%w: %s: special file entry", ErrInvalid, ReasonArchiveInvalid)
		}
		if err := seen.add(clean, f.FileInfo().IsDir()); err != nil {
			return err
		}
		total += f.UncompressedSize64
		if limits.MaxUncompressed > 0 && total > uint64(limits.MaxUncompressed) {
			return fmt.Errorf("%w: %s: declared uncompressed size exceeds the bound %d", ErrInvalid, ReasonArchiveInvalid, limits.MaxUncompressed)
		}
	}
	return nil
}

func checkTar(r io.Reader, limits ArchiveLimits) error {
	tr := tar.NewReader(r)
	seen := pathSet{}
	var total int64
	entries := 0
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%w: %s: tar archive is not readable", ErrInvalid, ReasonArchiveInvalid)
		}
		entries++
		if limits.MaxEntries > 0 && entries > limits.MaxEntries {
			return fmt.Errorf("%w: %s: entries exceed the bound %d", ErrInvalid, ReasonArchiveInvalid, limits.MaxEntries)
		}
		clean, err := containedPath(h.Name)
		if err != nil {
			return err
		}
		switch h.Typeflag {
		case tar.TypeSymlink, tar.TypeLink:
			return fmt.Errorf("%w: %s: link entry", ErrInvalid, ReasonPathEscape)
		case tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
			return fmt.Errorf("%w: %s: special file entry", ErrInvalid, ReasonArchiveInvalid)
		}
		if err := seen.add(clean, h.Typeflag == tar.TypeDir); err != nil {
			return err
		}
		if h.Typeflag == tar.TypeDir {
			continue
		}
		total += h.Size
		if limits.MaxUncompressed > 0 && total > limits.MaxUncompressed {
			return fmt.Errorf("%w: %s: declared uncompressed size exceeds the bound %d", ErrInvalid, ReasonArchiveInvalid, limits.MaxUncompressed)
		}
		// The entry body is skipped, never extracted; a body longer than
		// its header claims is a malformed archive.
		if _, err := io.Copy(io.Discard, io.LimitReader(tr, h.Size)); err != nil {
			return fmt.Errorf("%w: %s: tar entry is not readable", ErrInvalid, ReasonArchiveInvalid)
		}
	}
}
