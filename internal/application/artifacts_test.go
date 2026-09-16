package application_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	artifactstore "github.com/ancyloce/anvilkit-agent-control/internal/adapters/artifacts"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// minioImage is the S3-compatible development store (DEVELOPMENT_ONLY)
// these tests run the artifact adapter against: a real S3 API with a
// versioned bucket. It is not the production object backend and passing
// here qualifies no placement, retention or failure domain.
// The MinIO image of the foundation (deploy/dev), by the registry that still
// serves this release publicly: Docker Hub denies the pull of this tag on a
// fresh machine (GitHub-hosted runners), quay.io serves the same digest
// (sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e).
const minioImage = "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"

// startArtifactStore starts MinIO, creates a versioned artifact bucket and
// returns the qualified adapter.
func startArtifactStore(t *testing.T) *artifactstore.S3 {
	t.Helper()
	if os.Getenv("ANVILKIT_SKIP_DOCKER_TESTS") != "" {
		t.Skip("ANVILKIT_SKIP_DOCKER_TESTS set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: minioImage, Cmd: []string{"server", "/data"}, ExposedPorts: []string{"9000/tcp"},
			Env:        map[string]string{"MINIO_ROOT_USER": "artifacts", "MINIO_ROOT_PASSWORD": "artifacts-secret"},
			WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start minio: %v", err)
	}
	testcontainers.CleanupContainer(t, c)
	endpoint, err := c.PortEndpoint(ctx, "9000/tcp", "http")
	require.NoError(t, err)
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("artifacts", "artifacts-secret", "")),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired))
	require.NoError(t, err)
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("anvilkit-artifacts")})
	require.NoError(t, err)
	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String("anvilkit-artifacts"), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
	require.NoError(t, err)
	store, err := artifactstore.NewS3(ctx, artifactstore.S3Config{Endpoint: endpoint, Region: "us-east-1", Bucket: "anvilkit-artifacts", Prefix: "artifacts", PathStyle: true, AccessKeyID: "artifacts", SecretAccessKey: "artifacts-secret"})
	require.NoError(t, err)
	require.NoError(t, store.Qualify(ctx), "the development backend passes the qualification probe")
	return store
}

func digestOf(b []byte) domain.Digest {
	return domain.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(b)))
}

func transferCmd(tenant, id string) domain.CommandIdentity {
	return domain.CommandIdentity{TenantID: tenant, CommandID: "xfer_" + id, ActorID: "user_" + tenant, RequestDigest: application.DigestOf([]byte("xfer_" + id))}
}

// zipOf writes the entries in name order; a name ending in "/" is a
// directory entry.
func zipOf(t *testing.T, entries map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		h := &zip.FileHeader{Name: name}
		if strings.HasSuffix(name, "/") {
			h.SetMode(os.ModeDir | 0o755)
		}
		f, err := w.CreateHeader(h)
		require.NoError(t, err)
		_, err = f.Write([]byte(entries[name]))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// TestArtifacts runs the scoped transfer on two Control replicas over one
// database and one versioned S3-compatible store: begin, upload through
// the capability, finalize on the verified bytes, read in scope, and every
// refusal the design requires.
func TestArtifacts(t *testing.T) {
	inst := testdb.Start(t)
	objects := startArtifactStore(t)
	fsInv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	inv := &flakyInventory{inner: fsInv}
	p1, p2 := newProcess(t, inst, inv), newProcess(t, inst, inv)
	limits := application.ArtifactLimits{MaxObjectBytes: 1 << 20, MaxWindow: time.Hour, CapabilityTTL: 5 * time.Minute, Archive: domain.ArchiveLimits{MaxEntries: 100, MaxUncompressed: 1 << 20}}
	a1 := application.NewArtifacts(postgres.NewStore(p1.pool), objects, limits, domain.SystemClock{}, testLog)
	a2 := application.NewArtifacts(postgres.NewStore(p2.pool), objects, limits, domain.SystemClock{}, testLog)
	ctx := context.Background()
	body := []byte("hello, prompt\n")
	req := domain.TransferRequest{Class: domain.ArtifactPrompt, MediaType: "text/plain", ExpectedDigest: digestOf(body), ExpectedSize: int64(len(body)), Deadline: time.Now().Add(10 * time.Minute)}

	t.Run("begin, upload through the capability, finalize on the verified bytes; repeats return the original", func(t *testing.T) {
		c := transferCmd("tenant_a", "p1")
		results := make(chan *domain.Transfer, 2)
		var wg sync.WaitGroup
		for _, a := range []*application.Artifacts{a1, a2} {
			wg.Add(1)
			go func(a *application.Artifacts) {
				defer wg.Done()
				tr, _, cap, err := a.Begin(ctx, c, scopeA, req)
				require.NoError(t, err)
				require.NotNil(t, cap)
				results <- tr
			}(a)
		}
		wg.Wait()
		close(results)
		ids := map[string]bool{}
		for tr := range results {
			ids[tr.ID] = true
		}
		require.Len(t, ids, 1, "the same command on two replicas is one transfer")
		tr, existing, cap, err := a1.Begin(ctx, c, scopeA, req)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, domain.TransferBegun, tr.State)
		require.Equal(t, "PUT", cap.Method)
		require.Equal(t, "text/plain", cap.Headers["Content-Type"])
		require.NotContains(t, tr.Handle, tr.ObjectKey)
		changed := req
		changed.ExpectedSize++
		_, _, _, err = a2.Begin(ctx, c, scopeA, changed)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "a changed request under the same command conflicts")
		_, _, _, err = a2.Begin(ctx, domain.CommandIdentity{TenantID: "tenant_b", CommandID: c.CommandID, ActorID: "user_b", RequestDigest: c.RequestDigest}, scopeA, req)
		require.ErrorIs(t, err, domain.ErrInvalid, "the command tenant is the scope")

		// Nothing uploaded yet: the object version does not exist and the
		// transfer stays begun (the upload is incomplete).
		_, _, err = a1.Finalize(ctx, transferCmd("tenant_a", "p1:fin"), tr.ID, "", "no-such-version", "")
		require.ErrorIs(t, err, domain.ErrInvalid)
		still, err := a2.Get(ctx, scopeA, tr.ID, "")
		require.NoError(t, err)
		require.Equal(t, domain.TransferBegun, still.State)

		version, err := artifactstore.Upload(ctx, *cap, body)
		require.NoError(t, err)
		require.NotEmpty(t, version, "the versioned bucket assigns an object version")
		_, err = artifactstore.Upload(ctx, *cap, append(body, '!'))
		require.Error(t, err, "the capability admits only the declared length")

		finalized := make(chan *domain.Transfer, 2)
		fin := transferCmd("tenant_a", "p1:fin")
		for _, a := range []*application.Artifacts{a1, a2} {
			wg.Add(1)
			go func(a *application.Artifacts) {
				defer wg.Done()
				got, _, err := a.Finalize(ctx, fin, "", tr.Handle, version, "")
				require.NoError(t, err)
				finalized <- got
			}(a)
		}
		wg.Wait()
		close(finalized)
		for got := range finalized {
			require.Equal(t, domain.TransferFinalized, got.State)
			require.Equal(t, version, got.ObjectVersion)
			require.Equal(t, digestOf(body), got.ActualDigest)
			require.Equal(t, int64(len(body)), got.ActualSize)
		}
		again, existing, err := a1.Finalize(ctx, fin, tr.ID, "", version, "")
		require.NoError(t, err)
		require.True(t, existing, "a repeated finalize returns the original outcome")
		require.Equal(t, version, again.ObjectVersion)
		_, _, err = a1.Finalize(ctx, transferCmd("tenant_a", "p1:fin2"), tr.ID, "", version, "")
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "another command cannot claim the finalization")
		other, err := artifactstore.Upload(ctx, *cap, body)
		require.NoError(t, err)
		require.NotEqual(t, version, other)
		_, _, err = a2.Finalize(ctx, fin, tr.ID, "", other, "")
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "a conflicting object version never replaces the finalized one")
		got, err := a2.Get(ctx, scopeA, "", tr.Handle)
		require.NoError(t, err)
		require.Equal(t, version, got.ObjectVersion, "restarts and other replicas read the original verified version")
		_, err = a2.Get(ctx, scopeB, tr.ID, "")
		require.ErrorIs(t, err, domain.ErrNotFound, "another tenant sees nothing")
		_, _, err = a2.Finalize(ctx, domain.CommandIdentity{TenantID: "tenant_b", CommandID: "steal", ActorID: "user_b", RequestDigest: fin.RequestDigest}, "", tr.Handle, version, "")
		require.ErrorIs(t, err, domain.ErrNotFound)
		_, _, cap2, err := a1.Begin(ctx, c, scopeA, req)
		require.NoError(t, err)
		require.Nil(t, cap2, "a finalized transfer issues no capability")
	})

	t.Run("a partial upload, a wrong digest and an oversized object never finalize", func(t *testing.T) {
		c := transferCmd("tenant_a", "partial")
		tr, _, cap, err := a1.Begin(ctx, c, scopeA, req)
		require.NoError(t, err)
		// The uploader writes fewer bytes than declared under its own
		// request (the capability's length is what the backend signs, so
		// the partial write needs a second capability of that length: the
		// finalize still verifies the bytes, never the capability).
		partialCap, err := objects.UploadCapability(ctx, tr.ObjectKey, "text/plain", 5, time.Now().Add(time.Minute))
		require.NoError(t, err)
		version, err := artifactstore.Upload(ctx, partialCap, body[:5])
		require.NoError(t, err)
		_, _, err = a2.Finalize(ctx, transferCmd("tenant_a", "partial:fin"), "", tr.Handle, version, "")
		require.ErrorIs(t, err, domain.ErrInvalid)
		rejected, err := a1.Get(ctx, scopeA, tr.ID, "")
		require.NoError(t, err)
		require.Equal(t, domain.TransferRejected, rejected.State)
		require.Equal(t, domain.ReasonSizeMismatch, rejected.ReasonCode)
		full, err := artifactstore.Upload(ctx, *cap, body)
		require.NoError(t, err)
		_, _, err = a1.Finalize(ctx, transferCmd("tenant_a", "partial:fin2"), tr.ID, "", full, "")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a rejected transfer is never finalized by a later upload")

		wrong := []byte("other bytes\n")
		c2 := transferCmd("tenant_a", "digest")
		tr2, _, cap2, err := a2.Begin(ctx, c2, scopeA, domain.TransferRequest{Class: domain.ArtifactPrompt, MediaType: "text/plain", ExpectedDigest: digestOf(body), ExpectedSize: int64(len(wrong)), Deadline: time.Now().Add(10 * time.Minute)})
		require.NoError(t, err)
		v2, err := artifactstore.Upload(ctx, *cap2, wrong)
		require.NoError(t, err)
		_, _, err = a1.Finalize(ctx, transferCmd("tenant_a", "digest:fin"), tr2.ID, "", v2, "")
		require.ErrorIs(t, err, domain.ErrInvalid)
		got, err := a2.Get(ctx, scopeA, tr2.ID, "")
		require.NoError(t, err)
		require.Equal(t, domain.ReasonDigestMismatch, got.ReasonCode, "the backend's ETag or the caller's word never substitute for the bytes")

		_, _, _, err = a1.Begin(ctx, transferCmd("tenant_a", "huge"), scopeA, domain.TransferRequest{Class: domain.ArtifactSource, MediaType: "application/zip", ExpectedDigest: digestOf(body), ExpectedSize: limits.MaxObjectBytes + 1, Deadline: time.Now().Add(time.Minute)})
		require.ErrorIs(t, err, domain.ErrInvalid, "an object beyond the bound is refused at begin")
	})

	t.Run("deadlines and scope are checked as of the decision", func(t *testing.T) {
		short := req
		short.Deadline = time.Now().Add(1200 * time.Millisecond)
		tr, _, cap, err := a1.Begin(ctx, transferCmd("tenant_a", "late"), scopeA, short)
		require.NoError(t, err)
		require.False(t, cap.ExpiresAt.After(tr.Deadline), "a capability never outlives the transfer deadline")
		version, err := artifactstore.Upload(ctx, *cap, body)
		require.NoError(t, err)
		time.Sleep(time.Until(tr.Deadline) + 100*time.Millisecond)
		_, _, err = a2.Finalize(ctx, transferCmd("tenant_a", "late:fin"), tr.ID, "", version, "")
		require.ErrorIs(t, err, domain.ErrStaleExecution)
		expired, err := a1.Get(ctx, scopeA, tr.ID, "")
		require.NoError(t, err)
		require.Equal(t, domain.TransferExpired, expired.State)
		past := req
		past.Deadline = time.Now().Add(-time.Second)
		_, _, _, err = a1.Begin(ctx, transferCmd("tenant_a", "past"), scopeA, past)
		require.ErrorIs(t, err, domain.ErrStaleExecution)
		far := req
		far.Deadline = time.Now().Add(2 * limits.MaxWindow)
		_, _, _, err = a1.Begin(ctx, transferCmd("tenant_a", "far"), scopeA, far)
		require.ErrorIs(t, err, domain.ErrInvalid)

		// A bound transfer: the operation and attempt bound the deadline,
		// and a fence or a moved epoch between the upload and the finalize
		// rejects it.
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "xfer_op", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		at, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", "xfer_op_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		bound := req
		bound.OperationID, bound.AttemptID, bound.Deadline = op.ID, at.ID, time.Now().Add(30*time.Minute)
		_, _, _, err = a1.Begin(ctx, transferCmd("tenant_b", "bound_b"), scopeB, bound)
		require.ErrorIs(t, err, domain.ErrNotFound, "another tenant cannot bind to the operation")
		tb, _, capB, err := a1.Begin(ctx, transferCmd("tenant_a", "bound"), scopeA, bound)
		require.NoError(t, err)
		require.False(t, tb.Deadline.After(at.Deadline), "the transfer never outlives the attempt")
		require.Equal(t, op.ExecutionEpoch, tb.ExecutionEpoch)
		again, existing, capAgain, err := a2.Begin(ctx, transferCmd("tenant_a", "bound"), scopeA, bound)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, tb.ID, again.ID)
		require.NotNil(t, capAgain, "a valid, uncanceled request recovers its capability idempotently")
		vb, err := artifactstore.Upload(ctx, *capB, body)
		require.NoError(t, err)
		current, err := p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		_, _, err = p1.ops.SubmitCommand(ctx, cmd("tenant_a", "xfer_cancel", "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
		require.NoError(t, err)
		// The original begin, repeated after the cancel: the transfer is
		// answered as it is, without fresh upload authority.
		fenced, existing, capFenced, err := a2.Begin(ctx, transferCmd("tenant_a", "bound"), scopeA, bound)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, tb.ID, fenced.ID)
		require.Equal(t, domain.TransferBegun, fenced.State, "the original transfer state is returned")
		require.Nil(t, capFenced, "no capability is reissued under a cancel fence")
		_, _, err = a2.Finalize(ctx, transferCmd("tenant_a", "bound:fin"), tb.ID, "", vb, "")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a fenced operation finalizes nothing")
		rejected, err := a1.Get(ctx, scopeA, tb.ID, "")
		require.NoError(t, err)
		require.Equal(t, domain.TransferRejected, rejected.State)
		require.Equal(t, domain.ReasonStaleExecution, rejected.ReasonCode)
		_, _, _, err = a1.Begin(ctx, transferCmd("tenant_a", "bound2"), scopeA, bound)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "no transfer begins under a fence")
		_, _, capFenced, err = a1.Begin(ctx, transferCmd("tenant_a", "bound"), scopeA, bound)
		require.NoError(t, err)
		require.Nil(t, capFenced, "nor is one reissued for the rejected transfer")
	})

	t.Run("an archive that escapes its root or aliases paths is rejected without being extracted, whatever media type was declared", func(t *testing.T) {
		for _, c := range []struct {
			name, mediaType string
			class           domain.ArtifactClass
			entries         map[string]string
			reason          string
		}{
			{"escape", "application/zip", domain.ArtifactSource, map[string]string{"src/index.tsx": "x", "../evil.sh": "rm"}, domain.ReasonPathEscape},
			{"escape_octet", "application/octet-stream", domain.ArtifactSource, map[string]string{"src/index.tsx": "x", "../escape.sh": "rm"}, domain.ReasonPathEscape},
			{"escape_evidence", "application/octet-stream", domain.ArtifactEvidence, map[string]string{"../escape.sh": "rm"}, domain.ReasonPathEscape},
			{"inner_upward", "application/zip", domain.ArtifactSource, map[string]string{"src/../outside.ts": "x"}, domain.ReasonPathEscape},
			{"absolute", "application/zip", domain.ArtifactSource, map[string]string{"/etc/passwd": "x"}, domain.ReasonPathEscape},
			{"alias", "application/zip", domain.ArtifactSource, map[string]string{"src/Index.tsx": "a", "src/index.tsx": "b"}, domain.ReasonArchiveInvalid},
			{"dir_alias", "application/zip", domain.ArtifactSource, map[string]string{"SRC/": "", "src/a.ts": "a"}, domain.ReasonArchiveInvalid},
		} {
			data := zipOf(t, c.entries)
			tr, _, cap, err := a1.Begin(ctx, transferCmd("tenant_a", "zip_"+c.name), scopeA, domain.TransferRequest{Class: c.class, MediaType: c.mediaType, ExpectedDigest: digestOf(data), ExpectedSize: int64(len(data)), Deadline: time.Now().Add(time.Minute)})
			require.NoError(t, err)
			version, err := artifactstore.Upload(ctx, *cap, data)
			require.NoError(t, err)
			_, _, err = a2.Finalize(ctx, transferCmd("tenant_a", "zip_"+c.name+":fin"), tr.ID, "", version, "")
			require.ErrorIs(t, err, domain.ErrInvalid, c.name)
			got, err := a1.Get(ctx, scopeA, tr.ID, "")
			require.NoError(t, err)
			require.Equal(t, domain.TransferRejected, got.State, c.name)
			require.Equal(t, c.reason, got.ReasonCode, c.name)
		}
		// Bytes that are no archive at all never pass as a source artifact.
		text := []byte("export const x = 1;\n")
		tr, _, cap, err := a1.Begin(ctx, transferCmd("tenant_a", "src_text"), scopeA, domain.TransferRequest{Class: domain.ArtifactSource, MediaType: "text/plain", ExpectedDigest: digestOf(text), ExpectedSize: int64(len(text)), Deadline: time.Now().Add(time.Minute)})
		require.NoError(t, err)
		version, err := artifactstore.Upload(ctx, *cap, text)
		require.NoError(t, err)
		_, _, err = a2.Finalize(ctx, transferCmd("tenant_a", "src_text:fin"), tr.ID, "", version, "")
		require.ErrorIs(t, err, domain.ErrInvalid)
		got, err := a1.Get(ctx, scopeA, tr.ID, "")
		require.NoError(t, err)
		require.Equal(t, domain.ReasonArchiveInvalid, got.ReasonCode)
		// A symbolic link entry is refused too.
		var buf bytes.Buffer
		w := zip.NewWriter(&buf)
		h := &zip.FileHeader{Name: "src/link"}
		h.SetMode(os.ModeSymlink | 0o777)
		f, err := w.CreateHeader(h)
		require.NoError(t, err)
		_, _ = f.Write([]byte("../../etc/passwd"))
		require.NoError(t, w.Close())
		data := buf.Bytes()
		tr, _, cap, err = a1.Begin(ctx, transferCmd("tenant_a", "zip_symlink"), scopeA, domain.TransferRequest{Class: domain.ArtifactSource, MediaType: "application/zip", ExpectedDigest: digestOf(data), ExpectedSize: int64(len(data)), Deadline: time.Now().Add(time.Minute)})
		require.NoError(t, err)
		version, err = artifactstore.Upload(ctx, *cap, data)
		require.NoError(t, err)
		_, _, err = a2.Finalize(ctx, transferCmd("tenant_a", "zip_symlink:fin"), tr.ID, "", version, "")
		require.ErrorIs(t, err, domain.ErrInvalid)
		require.Contains(t, err.Error(), domain.ReasonPathEscape)
		// A contained archive finalizes, declared as a zip or as plain
		// octet-stream: the inspection ran on the bytes either way.
		good := zipOf(t, map[string]string{"src/": "", "src/index.tsx": "export const x = 1;", "src/lib/": "", "src/lib/util.ts": "export {};", "package.json": "{}"})
		for name, mediaType := range map[string]string{"zip_good": "application/zip", "zip_good_octet": "application/octet-stream"} {
			tr, _, cap, err = a1.Begin(ctx, transferCmd("tenant_a", name), scopeA, domain.TransferRequest{Class: domain.ArtifactSource, MediaType: mediaType, ExpectedDigest: digestOf(good), ExpectedSize: int64(len(good)), Deadline: time.Now().Add(time.Minute)})
			require.NoError(t, err)
			version, err = artifactstore.Upload(ctx, *cap, good)
			require.NoError(t, err)
			got, _, err := a2.Finalize(ctx, transferCmd("tenant_a", name+":fin"), tr.ID, "", version, "")
			require.NoError(t, err, name)
			require.Equal(t, domain.TransferFinalized, got.State)
		}
	})

	t.Run("a zero-byte transfer is bound to its empty body", func(t *testing.T) {
		empty := []byte{}
		tr, _, cap, err := a1.Begin(ctx, transferCmd("tenant_a", "empty"), scopeA, domain.TransferRequest{Class: domain.ArtifactEvidence, MediaType: "text/plain", ExpectedDigest: digestOf(empty), ExpectedSize: 0, Deadline: time.Now().Add(time.Minute)})
		require.NoError(t, err)
		require.Equal(t, "0", cap.Headers["Content-Length"])
		_, err = artifactstore.Upload(ctx, *cap, []byte("not empty"))
		require.Error(t, err, "the backend refuses a non-empty body under the zero-byte capability")
		_, err = artifactstore.Upload(ctx, *cap, []byte("!"))
		require.Error(t, err, "a single byte is refused too")
		version, err := artifactstore.Upload(ctx, *cap, nil)
		require.NoError(t, err, "the empty body is accepted")
		require.NotEmpty(t, version)
		got, _, err := a2.Finalize(ctx, transferCmd("tenant_a", "empty:fin"), tr.ID, "", version, "")
		require.NoError(t, err)
		require.Equal(t, domain.TransferFinalized, got.State)
		require.Zero(t, got.ActualSize)
		require.Equal(t, digestOf(empty), got.ActualDigest)
		// Finalize still verifies the bytes it reads: an object of another
		// size under the key (written under a capability of that size)
		// rejects a zero-byte transfer.
		tr2, _, _, err := a1.Begin(ctx, transferCmd("tenant_a", "empty2"), scopeA, domain.TransferRequest{Class: domain.ArtifactEvidence, MediaType: "text/plain", ExpectedDigest: digestOf(empty), ExpectedSize: 0, Deadline: time.Now().Add(time.Minute)})
		require.NoError(t, err)
		oneByte, err := objects.UploadCapability(ctx, tr2.ObjectKey, "text/plain", 1, time.Now().Add(time.Minute))
		require.NoError(t, err)
		v1, err := artifactstore.Upload(ctx, oneByte, []byte("!"))
		require.NoError(t, err)
		_, _, err = a2.Finalize(ctx, transferCmd("tenant_a", "empty2:fin"), tr2.ID, "", v1, "")
		require.ErrorIs(t, err, domain.ErrInvalid)
		rejected, err := a1.Get(ctx, scopeA, tr2.ID, "")
		require.NoError(t, err)
		require.Equal(t, domain.ReasonSizeMismatch, rejected.ReasonCode)
	})

	t.Run("a handle resolves only for the current instance of its attempt; results bind only verified artifacts", func(t *testing.T) {
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "res_op", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		at, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", "res_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		launch, _, err := p1.exec.PrepareLaunch(ctx, cmd("tenant_a", "res_launch", "l"), at.ID, "lc-res", "kind", imageDig, time.Now().Add(time.Minute))
		require.NoError(t, err)
		owner, _, err := p1.exec.RegisterInstance(ctx, cmd("tenant_a", "res_reg_1", "r"), at.ID, launch.LaunchKey, "kind", "job-res", "pod-res-1", imageDig)
		require.NoError(t, err)
		dup, _, err := p1.exec.RegisterInstance(ctx, cmd("tenant_a", "res_reg_2", "r"), at.ID, launch.LaunchKey, "kind", "job-res", "pod-res-2", imageDig)
		require.NoError(t, err)
		require.True(t, owner.Current)
		require.False(t, dup.Current)
		evidence := []byte("observer log\n")
		bound := domain.TransferRequest{Class: domain.ArtifactEvidence, MediaType: "text/plain", ExpectedDigest: digestOf(evidence), ExpectedSize: int64(len(evidence)), OperationID: op.ID, AttemptID: at.ID, Deadline: time.Now().Add(time.Minute)}
		tr, _, _, err := a1.Begin(ctx, transferCmd("tenant_a", "res_ev"), scopeA, bound)
		require.NoError(t, err)
		_, _, err = a2.Resolve(ctx, tr.Handle, dup.ID)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a duplicate Pod is not the current owner")
		_, _, err = a2.Resolve(ctx, "hdl_forged", owner.ID)
		require.ErrorIs(t, err, domain.ErrNotFound)
		unbound, _, _, err := a1.Begin(ctx, transferCmd("tenant_a", "res_unbound"), scopeA, req)
		require.NoError(t, err)
		_, _, err = a2.Resolve(ctx, unbound.Handle, owner.ID)
		require.ErrorIs(t, err, domain.ErrForbidden, "an unbound transfer resolves for no instance")
		resolved, cap, err := a2.Resolve(ctx, tr.Handle, owner.ID)
		require.NoError(t, err)
		require.Equal(t, tr.ID, resolved.ID)
		require.NotEmpty(t, cap.URL)
		version, err := artifactstore.Upload(ctx, *cap, evidence)
		require.NoError(t, err)
		_, _, err = a1.Finalize(ctx, transferCmd("tenant_a", "res_ev:fin"), "", tr.Handle, version, dup.ID)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a duplicate Pod cannot finalize")
		fin, _, err := a1.Finalize(ctx, transferCmd("tenant_a", "res_ev:fin"), "", tr.Handle, version, owner.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TransferFinalized, fin.State)
		_, _, err = a2.Resolve(ctx, tr.Handle, owner.ID)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a finalized transfer issues no capability")

		// A certified LocalCheck manifest names the profile's fixed result
		// (embedded: the reviewed digest and size, no object) and then the
		// outputs the case under test adds.
		outputsOf := func(outputs string) string {
			fixed := `{"class":"result","digest":"` + fixedResult + `","sizeBytes":"24"}`
			if outputs == "" {
				return fixed
			}
			return fixed + "," + outputs
		}
		manifestOf := func(outputs string) []byte {
			return []byte(`{"schemaVersion":1,"launchId":"` + launch.ID + `","attemptId":"` + at.ID + `","jobKind":"validator","profileId":"local-check-v1","verdict":"certified","outputs":[` + outputs + `],"completedAt":"2026-09-16T00:00:00Z"}`)
		}
		manifest := func(outputs string) []byte { return manifestOf(outputsOf(outputs)) }
		boundEvidence := `{"class":"evidence","digest":"` + string(digestOf(evidence)) + `","sizeBytes":"` + fmt.Sprint(len(evidence)) + `","handle":"` + tr.Handle + `"}`
		good := manifest(boundEvidence)
		epoch := op.ExecutionEpoch
		staleEpoch := epoch + 1
		// The prescribed fixed result is checked from the whole outputs list
		// before any output is bound: an empty list, a real finalized object
		// in its place, or the reviewed digest with another size refuses the
		// result, and the refusal writes nothing (no stage, no stage
		// artifact, the attempt and operation where they were).
		eventsBefore, _, _ := eventCount(t, p1.pool, op.ID)
		for name, m := range map[string][]byte{
			"empty":      manifestOf(``),
			"substitute": manifestOf(boundEvidence),
			"size":       manifestOf(`{"class":"result","digest":"` + fixedResult + `","sizeBytes":"25"}`),
		} {
			_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "res_acc_"+name, "a"), at.ID, owner.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(m), m, "observer", &epoch)
			require.ErrorIs(t, err, domain.ErrInvalid, "%s: a certified result without its fixed result is refused", name)
		}
		var attemptState, opPhase string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT a.state, o.phase FROM attempts a JOIN operations o ON o.operation_id = a.operation_id WHERE a.attempt_id = $1", at.ID).Scan(&attemptState, &opPhase))
		require.Equal(t, string(domain.AttemptRunning), attemptState, "no refusal moved the attempt")
		require.Equal(t, "running", opPhase, "no refusal moved the operation")
		eventsAfter, _, _ := eventCount(t, p1.pool, op.ID)
		require.Equal(t, eventsBefore, eventsAfter, "no refusal committed an event")
		_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "res_acc_stale", "a"), at.ID, owner.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(good), good, "observer", &staleEpoch)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a result observed under another epoch is refused")
		forgedHandle := manifest(`{"class":"evidence","digest":"` + string(digestOf(evidence)) + `","sizeBytes":"` + fmt.Sprint(len(evidence)) + `","handle":"hdl_forged"}`)
		_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "res_acc_forged", "a"), at.ID, owner.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(forgedHandle), forgedHandle, "observer", &epoch)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "an output naming no transfer of this scope is refused")
		wrongDigest := manifest(`{"class":"evidence","digest":"` + string(digestOf(body)) + `","sizeBytes":"` + fmt.Sprint(len(evidence)) + `","handle":"` + tr.Handle + `"}`)
		_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "res_acc_digest", "a"), at.ID, owner.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(wrongDigest), wrongDigest, "observer", &epoch)
		require.ErrorIs(t, err, domain.ErrInvalid, "an output whose digest differs from the verified object is refused")
		wrongClass := manifest(`{"class":"source","digest":"` + string(digestOf(evidence)) + `","sizeBytes":"` + fmt.Sprint(len(evidence)) + `","handle":"` + tr.Handle + `"}`)
		_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "res_acc_class", "a"), at.ID, owner.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(wrongClass), wrongClass, "observer", &epoch)
		require.ErrorIs(t, err, domain.ErrInvalid, "an output of another class than the transfer is refused")
		unfinalized := manifest(`{"class":"prompt","digest":"` + string(digestOf(body)) + `","sizeBytes":"` + fmt.Sprint(len(body)) + `","handle":"` + unbound.Handle + `"}`)
		_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "res_acc_unfin", "a"), at.ID, owner.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(unfinalized), unfinalized, "observer", &epoch)
		require.ErrorIs(t, err, domain.ErrInvalid, "an output class without an artifact binding on this surface is refused")
		_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "res_acc_dup", "a"), at.ID, dup.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(good), good, "observer", &epoch)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a duplicate Pod owns no result")
		var stages int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM stage_manifests WHERE attempt_id = $1", at.ID).Scan(&stages))
		require.Zero(t, stages, "no refused acceptance left a stage behind")

		st, existing, err := p2.exec.AcceptResult(ctx, cmd("tenant_a", "res_acc", "a"), at.ID, owner.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(good), good, "observer", &epoch)
		require.NoError(t, err)
		require.False(t, existing)
		require.Len(t, st.Artifacts, 1, "the embedded fixed result binds no object; the evidence does")
		require.Equal(t, tr.ID, st.Artifacts[0].TransferID)
		require.Equal(t, version, st.Artifacts[0].ObjectVersion, "the stage binds the exact verified object version")
		require.Equal(t, op.ExecutionEpoch, st.ExecutionEpoch)
		same, existing, err := p1.exec.AcceptResult(ctx, cmd("tenant_a", "res_acc", "a"), at.ID, owner.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(good), good, "observer", &epoch)
		require.NoError(t, err)
		require.True(t, existing, "a repeated acceptance returns the original stage")
		require.Equal(t, st.ID, same.ID)
		require.Len(t, same.Artifacts, 1)
		conflicting := manifest(``)
		_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "res_acc_conflict", "a"), at.ID, owner.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(conflicting), conflicting, "observer", &epoch)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "conflicting content never overwrites the accepted result")
		read, err := p2.exec.GetAcceptedStage(ctx, at.ID, "tenant_a")
		require.NoError(t, err)
		require.Equal(t, good, read.ResultManifest)
		require.Len(t, read.Artifacts, 1)
		require.Equal(t, version, read.Artifacts[0].ObjectVersion)
		_, err = p2.exec.GetAcceptedStage(ctx, at.ID, "tenant_b")
		require.ErrorIs(t, err, domain.ErrNotFound, "the accepted stage is read in scope")
		var artifactRows int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM stage_artifacts WHERE stage_id = $1", st.ID).Scan(&artifactRows))
		require.Equal(t, 1, artifactRows)
	})

	t.Run("a disabled store answers unavailable and decides nothing", func(t *testing.T) {
		none := application.NewArtifacts(postgres.NewStore(p1.pool), nil, limits, domain.SystemClock{}, testLog)
		_, _, _, err := none.Begin(ctx, transferCmd("tenant_a", "none"), scopeA, req)
		require.ErrorIs(t, err, domain.ErrUnavailable)
		var n int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM artifact_transfers WHERE command_id = 'xfer_none'").Scan(&n))
		require.Zero(t, n)
	})
	_ = json.Marshal
}
