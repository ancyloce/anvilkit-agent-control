package domain_test

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

type entry struct {
	name string
	body string
	dir  bool
	mode os.FileMode
}

func zipOf(t *testing.T, entries ...entry) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for _, e := range entries {
		h := &zip.FileHeader{Name: e.name}
		if e.mode != 0 {
			h.SetMode(e.mode)
		} else if e.dir {
			h.SetMode(os.ModeDir | 0o755)
		}
		f, err := w.CreateHeader(h)
		require.NoError(t, err)
		_, err = f.Write([]byte(e.body))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func tarOf(t *testing.T, gz bool, entries ...entry) []byte {
	t.Helper()
	var buf bytes.Buffer
	var out interface {
		Write([]byte) (int, error)
		Close() error
	}
	if gz {
		out = gzip.NewWriter(&buf)
	} else {
		out = &nopCloser{&buf}
	}
	w := tar.NewWriter(out)
	for _, e := range entries {
		h := &tar.Header{Name: e.name, Mode: 0o644, Size: int64(len(e.body))}
		if e.dir {
			h.Typeflag, h.Mode, h.Size = tar.TypeDir, 0o755, 0
		}
		require.NoError(t, w.WriteHeader(h))
		if !e.dir {
			_, err := w.Write([]byte(e.body))
			require.NoError(t, err)
		}
	}
	require.NoError(t, w.Close())
	require.NoError(t, out.Close())
	return buf.Bytes()
}

type nopCloser struct{ *bytes.Buffer }

func (nopCloser) Close() error { return nil }

var limits = domain.ArchiveLimits{MaxEntries: 100, MaxUncompressed: 1 << 20}

// The archive checks run on the bytes, not on the declared media type; an
// upward segment is refused before normalization; case aliases are found
// among directories and parents as well as files.
func TestCheckArchive(t *testing.T) {
	escaping := zipOf(t, entry{name: "src/index.tsx", body: "x"}, entry{name: "../escape.sh", body: "rm"})
	contained := zipOf(t, entry{name: "src/", dir: true}, entry{name: "src/index.tsx", body: "export const x = 1;"}, entry{name: "package.json", body: "{}"})
	cases := []struct {
		name      string
		class     domain.ArtifactClass
		mediaType string
		data      []byte
		reason    string // "" for accepted
	}{
		{"contained zip under the source class", domain.ArtifactSource, "application/zip", contained, ""},
		{"contained zip declared as octet-stream under the source class", domain.ArtifactSource, "application/octet-stream", contained, ""},
		{"escaping zip declared as zip", domain.ArtifactSource, "application/zip", escaping, domain.ReasonPathEscape},
		{"escaping zip declared as octet-stream under the source class", domain.ArtifactSource, "application/octet-stream", escaping, domain.ReasonPathEscape},
		{"escaping zip declared as text under the evidence class", domain.ArtifactEvidence, "text/plain", escaping, domain.ReasonPathEscape},
		{"plain text under the source class", domain.ArtifactSource, "text/plain", []byte("not an archive\n"), domain.ReasonArchiveInvalid},
		{"plain text declared as zip", domain.ArtifactEvidence, "application/zip", []byte("not an archive\n"), domain.ReasonArchiveInvalid},
		{"a tar declared as zip", domain.ArtifactSource, "application/zip", tarOf(t, false, entry{name: "a.ts", body: "x"}), domain.ReasonArchiveInvalid},
		{"plain text under another class is not inspected", domain.ArtifactPrompt, "text/plain", []byte("hello\n"), ""},
		{"upward segment inside the path", domain.ArtifactSource, "application/zip", zipOf(t, entry{name: "src/../outside.ts", body: "x"}), domain.ReasonPathEscape},
		{"upward segment inside a tar path", domain.ArtifactSource, "application/x-tar", tarOf(t, false, entry{name: "src/../outside.ts", body: "x"}), domain.ReasonPathEscape},
		{"absolute path", domain.ArtifactSource, "application/zip", zipOf(t, entry{name: "/etc/passwd", body: "x"}), domain.ReasonPathEscape},
		{"backslash separator", domain.ArtifactSource, "application/zip", zipOf(t, entry{name: `src\..\x.ts`, body: "x"}), domain.ReasonPathEscape},
		{"file case alias", domain.ArtifactSource, "application/zip", zipOf(t, entry{name: "src/Index.tsx", body: "a"}, entry{name: "src/index.tsx", body: "b"}), domain.ReasonArchiveInvalid},
		{"directory entry aliasing a parent", domain.ArtifactSource, "application/zip", zipOf(t, entry{name: "SRC/", dir: true}, entry{name: "src/a.ts", body: "a"}), domain.ReasonArchiveInvalid},
		{"parents of two files aliasing each other", domain.ArtifactSource, "application/zip", zipOf(t, entry{name: "Src/a.ts", body: "a"}, entry{name: "src/b.ts", body: "b"}), domain.ReasonArchiveInvalid},
		{"tar directory aliasing a parent", domain.ArtifactSource, "application/x-tar", tarOf(t, false, entry{name: "SRC/", dir: true}, entry{name: "src/a.ts", body: "a"}), domain.ReasonArchiveInvalid},
		{"tgz directory aliasing a parent", domain.ArtifactSource, "application/gzip", tarOf(t, true, entry{name: "src/a.ts", body: "a"}, entry{name: "SRC", dir: true}), domain.ReasonArchiveInvalid},
		{"a path that is both a file and a directory", domain.ArtifactSource, "application/zip", zipOf(t, entry{name: "src", body: "a"}, entry{name: "src/a.ts", body: "b"}), domain.ReasonArchiveInvalid},
		{"a file entered twice", domain.ArtifactSource, "application/zip", zipOf(t, entry{name: "a.ts", body: "a"}, entry{name: "a.ts", body: "b"}), domain.ReasonArchiveInvalid},
		{"the same directory named by several entries", domain.ArtifactSource, "application/zip", zipOf(t, entry{name: "src/", dir: true}, entry{name: "src/a.ts", body: "a"}, entry{name: "src/lib/", dir: true}, entry{name: "src/lib/b.ts", body: "b"}), ""},
		{"tgz with directories and files", domain.ArtifactSource, "application/gzip", tarOf(t, true, entry{name: "src/", dir: true}, entry{name: "src/a.ts", body: "a"}, entry{name: "src/b.ts", body: "b"}), ""},
		{"tgz declared as octet-stream under the source class", domain.ArtifactSource, "application/octet-stream", tarOf(t, true, entry{name: "src/a.ts", body: "a"}), ""},
		{"symbolic link", domain.ArtifactSource, "application/zip", zipOf(t, entry{name: "src/link", body: "../../etc/passwd", mode: os.ModeSymlink | 0o777}), domain.ReasonPathEscape},
		{"entries beyond the bound", domain.ArtifactSource, "application/zip", func() []byte {
			var es []entry
			for i := 0; i <= limits.MaxEntries; i++ {
				es = append(es, entry{name: "f" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".ts", body: "x"})
			}
			return zipOf(t, es...)
		}(), domain.ReasonArchiveInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := domain.CheckArchive(c.class, c.mediaType, c.data, limits)
			if c.reason == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, domain.ErrInvalid)
			require.Contains(t, err.Error(), c.reason)
		})
	}
}

// A tar link entry and the declared uncompressed bound are still refused.
func TestCheckArchiveTarLimits(t *testing.T) {
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)
	require.NoError(t, w.WriteHeader(&tar.Header{Name: "src/link", Typeflag: tar.TypeSymlink, Linkname: "../../etc/passwd", Mode: 0o777}))
	require.NoError(t, w.Close())
	err := domain.CheckArchive(domain.ArtifactSource, "application/x-tar", buf.Bytes(), limits)
	require.ErrorIs(t, err, domain.ErrInvalid)
	require.Contains(t, err.Error(), domain.ReasonPathEscape)

	big := tarOf(t, false, entry{name: "a.bin", body: string(make([]byte, 2048))})
	err = domain.CheckArchive(domain.ArtifactSource, "application/x-tar", big, domain.ArchiveLimits{MaxEntries: 10, MaxUncompressed: 1024})
	require.ErrorIs(t, err, domain.ErrInvalid)
	require.Contains(t, err.Error(), domain.ReasonArchiveInvalid)
	require.Equal(t, "tar", domain.DetectArchive(big))
	require.Equal(t, "zip", domain.DetectArchive(zipOf(t)))
	require.Equal(t, "", domain.DetectArchive(make([]byte, 1024)), "zero blocks are no archive")
}
