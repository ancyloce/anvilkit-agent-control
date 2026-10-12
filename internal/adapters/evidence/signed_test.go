package evidence_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/evidence"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

type objects map[string][]byte

func (o objects) Get(_ context.Context, key string) ([]byte, string, error) {
	b, ok := o[key]
	if !ok {
		return nil, "", domain.ErrNotFound
	}
	return b, "v1", nil
}

func (o objects) put(key string, body []byte) domain.Digest {
	o[key] = body
	return domain.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(body)))
}

func keyFile(t *testing.T, pub ed25519.PublicKey) string {
	der, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "issuer.pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0o600))
	return path
}

// TestSignedEvidence: only an envelope signed by a configured issuer key
// and naming exactly the tenant, dispatch identity and epoch (or the
// obligation, decision and recovery epoch) is evidence; unsigned JSON, a
// foreign key, a tampered payload or another binding are refused (P2.1 AC4).
func TestSignedEvidence(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, foreign, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	issuers, err := evidence.LoadIssuers([]evidence.IssuerSpec{{Name: "anvilkit-model-proxy", KeyID: "mp-1", PublicKeyFile: keyFile(t, pub)}})
	require.NoError(t, err)
	store := objects{}
	v := evidence.NewVerifier(store, issuers)
	ctx := context.Background()
	d := &domain.Dispatch{ID: "dsp_1", TenantID: "tenant_a", CallID: "call_1", Owner: "anvilkit-agent-model-proxy", ExecutionEpoch: 3}
	sealed := time.Now().UTC().Format(time.RFC3339)
	good := evidence.NotSent{Class: "not-sent", TenantID: "tenant_a", DispatchID: "dsp_1", CallID: "call_1", Owner: "anvilkit-agent-model-proxy", ExecutionEpoch: "3", Issuer: "anvilkit-model-proxy", SealedAt: sealed}
	ref := evidence.NotSentKey(d.ID)

	sign := func(key ed25519.PrivateKey, keyID string, payload any) []byte {
		b, err := evidence.Sign(key, keyID, payload)
		require.NoError(t, err)
		return b
	}
	cases := map[string][]byte{
		"unsigned JSON":   mustJSON(good),
		"foreign key":     sign(foreign, "mp-1", good),
		"unknown key id":  sign(priv, "mp-9", good),
		"other tenant":    sign(priv, "mp-1", with(good, func(p *evidence.NotSent) { p.TenantID = "tenant_b" })),
		"other epoch":     sign(priv, "mp-1", with(good, func(p *evidence.NotSent) { p.ExecutionEpoch = "2" })),
		"other call":      sign(priv, "mp-1", with(good, func(p *evidence.NotSent) { p.CallID = "call_2" })),
		"issuer mismatch": sign(priv, "mp-1", with(good, func(p *evidence.NotSent) { p.Issuer = "someone" })),
	}
	for name, body := range cases {
		digest := store.put(ref, body)
		require.ErrorIs(t, v.VerifyNotSent(ctx, d, ref, digest), domain.ErrEvidenceInsufficient, name)
	}
	tampered := sign(priv, "mp-1", good)
	var env evidence.Envelope
	require.NoError(t, json.Unmarshal(tampered, &env))
	env.Payload = env.Payload[:len(env.Payload)-4] + "AAAA"
	require.ErrorIs(t, v.VerifyNotSent(ctx, d, ref, store.put(ref, mustJSON(env))), domain.ErrEvidenceInsufficient, "a tampered payload")

	digest := store.put(ref, sign(priv, "mp-1", good))
	require.NoError(t, v.VerifyNotSent(ctx, d, ref, digest))
	require.ErrorIs(t, v.VerifyNotSent(ctx, d, ref, domain.Digest("sha256:"+fmt.Sprintf("%064d", 0))), domain.ErrEvidenceInsufficient, "the offered digest binds the object")
	require.ErrorIs(t, v.VerifyNotSent(ctx, d, "not-sent/dsp_2", digest), domain.ErrEvidenceInsufficient, "the reference is the dispatch's own")

	disp := evidence.Disposition{Class: "disposition", TenantID: "tenant_a", ObligationClass: "business-write", ObligationID: "eff_1", Decision: "resolve_succeeded", RecoveryEpoch: "4", Issuer: "anvilkit-model-proxy", SealedAt: sealed}
	dref := evidence.DispositionKey("business-write", "eff_1")
	ddigest := store.put(dref, sign(priv, "mp-1", disp))
	require.NoError(t, v.VerifyDisposition(ctx, "tenant_a", 4, "business-write", "eff_1", domain.DisposeResolveSucceeded, dref, ddigest))
	require.ErrorIs(t, v.VerifyDisposition(ctx, "tenant_a", 5, "business-write", "eff_1", domain.DisposeResolveSucceeded, dref, ddigest), domain.ErrEvidenceInsufficient, "another recovery epoch")
	require.ErrorIs(t, v.VerifyDisposition(ctx, "tenant_b", 4, "business-write", "eff_1", domain.DisposeResolveSucceeded, dref, ddigest), domain.ErrEvidenceInsufficient, "another tenant")
	require.ErrorIs(t, v.VerifyDisposition(ctx, "tenant_a", 4, "business-write", "eff_1", domain.DisposeResolveFailed, dref, ddigest), domain.ErrEvidenceInsufficient, "another decision")
	require.ErrorIs(t, v.VerifyDisposition(ctx, "tenant_a", 4, "business-write", "eff_1", domain.DisposeResolveSucceeded, dref, store.put(dref, mustJSON(disp))), domain.ErrEvidenceInsufficient, "unsigned")
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func with(p evidence.NotSent, change func(*evidence.NotSent)) evidence.NotSent {
	change(&p)
	return p
}
