// Package evidence verifies signed evidence objects (P2.1, DD-02 §4–§5):
// the not-sent attestation that lets a replacement send be admitted and
// the evidence behind an operator disposition. An object is an envelope
// holding the payload bytes, the issuer's key id and an Ed25519 signature
// over exactly those bytes; it is read from the obligation inventory by
// its reference, must hash to the digest the caller offered, must be signed
// by a configured issuer key (issuer rotation is a new key id) and must name
// the tenant, the obligation, the epoch and the decision it is offered
// for. Unsigned JSON is never evidence here: the DEVELOPMENT_ONLY unsigned
// attestations live in internal/adapters/development.
package evidence

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// ObjectReader reads published inventory objects.
type ObjectReader interface {
	Get(ctx context.Context, key string) (body []byte, version string, err error)
}

// Envelope is the stored evidence object.
type Envelope struct {
	SchemaVersion int    `json:"schemaVersion"`
	KeyID         string `json:"keyId"`
	Payload       string `json:"payload"`
	Signature     string `json:"signature"`
}

// NotSent is the payload of a not-sent attestation: the issuer terminated
// the original sender's ability to send the call of this dispatch under
// this execution epoch.
type NotSent struct {
	Class          string `json:"class"`
	TenantID       string `json:"tenantId"`
	DispatchID     string `json:"dispatchId"`
	CallID         string `json:"callId"`
	Owner          string `json:"owner"`
	ExecutionEpoch string `json:"executionEpoch"`
	Issuer         string `json:"issuer"`
	SealedAt       string `json:"sealedAt"`
}

// Disposition is the payload backing an operator disposition.
type Disposition struct {
	Class           string `json:"class"`
	TenantID        string `json:"tenantId"`
	ObligationClass string `json:"obligationClass"`
	ObligationID    string `json:"obligationId"`
	Decision        string `json:"decision"`
	RecoveryEpoch   string `json:"recoveryEpoch"`
	Issuer          string `json:"issuer"`
	SealedAt        string `json:"sealedAt"`
}

// Issuer is one trusted signing key.
type Issuer struct {
	Name  string
	KeyID string
	Key   ed25519.PublicKey
}

// IssuerSpec names an issuer and its PEM (PKIX) public key file.
type IssuerSpec struct {
	Name          string
	KeyID         string
	PublicKeyFile string
}

// LoadIssuers reads the configured issuer keys.
func LoadIssuers(specs []IssuerSpec) ([]Issuer, error) {
	out := make([]Issuer, 0, len(specs))
	seen := map[string]bool{}
	for _, s := range specs {
		if s.Name == "" || s.KeyID == "" || s.PublicKeyFile == "" {
			return nil, errors.New("evidence: an issuer needs a name, a key id and a public key file")
		}
		if seen[s.KeyID] {
			return nil, fmt.Errorf("evidence: key id %q is configured twice", s.KeyID)
		}
		seen[s.KeyID] = true
		raw, err := os.ReadFile(s.PublicKeyFile)
		if err != nil {
			return nil, fmt.Errorf("evidence: issuer %s key %s: %w", s.Name, s.KeyID, err)
		}
		block, _ := pem.Decode(raw)
		if block == nil {
			return nil, fmt.Errorf("evidence: issuer %s key %s is not PEM", s.Name, s.KeyID)
		}
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("evidence: issuer %s key %s: %w", s.Name, s.KeyID, err)
		}
		key, ok := pub.(ed25519.PublicKey)
		if !ok {
			return nil, fmt.Errorf("evidence: issuer %s key %s is not an Ed25519 key", s.Name, s.KeyID)
		}
		out = append(out, Issuer{Name: s.Name, KeyID: s.KeyID, Key: key})
	}
	return out, nil
}

// Verifier verifies signed evidence from the inventory.
type Verifier struct {
	objects ObjectReader
	keys    map[string]Issuer
}

func NewVerifier(objects ObjectReader, issuers []Issuer) *Verifier {
	v := &Verifier{objects: objects, keys: map[string]Issuer{}}
	for _, i := range issuers {
		v.keys[i.KeyID] = i
	}
	return v
}

// NotSentKey and DispositionKey are the inventory keys of the objects.
func NotSentKey(dispatchID string) string { return "not-sent/" + dispatchID }

func DispositionKey(class, obligationID string) string {
	return "disposition/" + class + "/" + obligationID
}

func insufficient(format string, args ...any) error {
	return fmt.Errorf("%w: %s", domain.ErrEvidenceInsufficient, fmt.Sprintf(format, args...))
}

// open reads, hashes and verifies the envelope at ref and returns the
// signed payload bytes and the signing issuer.
func (v *Verifier) open(ctx context.Context, ref string, digest domain.Digest) ([]byte, Issuer, error) {
	body, _, err := v.objects.Get(ctx, ref)
	if err != nil {
		return nil, Issuer{}, insufficient("evidence %s cannot be read: %v", ref, err)
	}
	if actual := domain.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(body))); actual != digest {
		return nil, Issuer{}, insufficient("evidence %s hashes to %s, not the offered %s", ref, actual, digest)
	}
	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil || env.SchemaVersion != 1 || env.Payload == "" || env.Signature == "" {
		return nil, Issuer{}, insufficient("evidence %s is not a signed envelope", ref)
	}
	issuer, ok := v.keys[env.KeyID]
	if !ok {
		return nil, Issuer{}, insufficient("evidence %s is signed by unknown key %q", ref, env.KeyID)
	}
	payload, err := base64.StdEncoding.DecodeString(env.Payload)
	if err != nil {
		return nil, Issuer{}, insufficient("evidence %s payload is not base64", ref)
	}
	sig, err := base64.StdEncoding.DecodeString(env.Signature)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return nil, Issuer{}, insufficient("evidence %s signature is malformed", ref)
	}
	if !ed25519.Verify(issuer.Key, payload, sig) {
		return nil, Issuer{}, insufficient("evidence %s signature does not verify under key %s", ref, env.KeyID)
	}
	return payload, issuer, nil
}

func strictDecode(payload []byte, into any) error {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	return dec.Decode(into)
}

// VerifyNotSent implements application.NotSentEvidence.
func (v *Verifier) VerifyNotSent(ctx context.Context, d *domain.Dispatch, ref string, digest domain.Digest) error {
	if ref != NotSentKey(d.ID) {
		return insufficient("evidence %q is not the attestation of dispatch %s", ref, d.ID)
	}
	payload, issuer, err := v.open(ctx, ref, digest)
	if err != nil {
		return err
	}
	var p NotSent
	if err := strictDecode(payload, &p); err != nil {
		return insufficient("attestation %s payload is not the declared shape", ref)
	}
	if p.Class != "not-sent" || p.TenantID != d.TenantID || p.DispatchID != d.ID || p.CallID != d.CallID || p.Owner != d.Owner ||
		p.ExecutionEpoch != strconv.FormatUint(d.ExecutionEpoch, 10) {
		return insufficient("attestation %s does not name dispatch %s (call %s of %s, tenant %s, epoch %d)", ref, d.ID, d.CallID, d.Owner, d.TenantID, d.ExecutionEpoch)
	}
	if p.Issuer != issuer.Name {
		return insufficient("attestation %s names issuer %q but is signed by %s", ref, p.Issuer, issuer.Name)
	}
	if _, err := time.Parse(time.RFC3339, p.SealedAt); err != nil {
		return insufficient("attestation %s has no valid sealing time", ref)
	}
	return nil
}

// VerifyDisposition implements application.DispositionEvidence.
func (v *Verifier) VerifyDisposition(ctx context.Context, tenantID string, epoch uint64, class, obligationID string, decision domain.DispositionDecision, ref string, digest domain.Digest) error {
	if ref != DispositionKey(class, obligationID) {
		return insufficient("evidence %q is not the disposition evidence of %s/%s", ref, class, obligationID)
	}
	payload, issuer, err := v.open(ctx, ref, digest)
	if err != nil {
		return err
	}
	var p Disposition
	if err := strictDecode(payload, &p); err != nil {
		return insufficient("disposition evidence %s payload is not the declared shape", ref)
	}
	if p.Class != "disposition" || p.TenantID != tenantID || p.ObligationClass != class || p.ObligationID != obligationID ||
		p.Decision != string(decision) || p.RecoveryEpoch != strconv.FormatUint(epoch, 10) {
		return insufficient("disposition evidence %s does not name %s/%s of tenant %s with decision %s at epoch %d", ref, class, obligationID, tenantID, decision, epoch)
	}
	if p.Issuer != issuer.Name {
		return insufficient("disposition evidence %s names issuer %q but is signed by %s", ref, p.Issuer, issuer.Name)
	}
	if _, err := time.Parse(time.RFC3339, p.SealedAt); err != nil {
		return insufficient("disposition evidence %s has no valid sealing time", ref)
	}
	return nil
}

// Sign builds the envelope of a payload: what a trusted issuer publishes
// (and what tests and drills use with a generated key pair).
func Sign(key ed25519.PrivateKey, keyID string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{SchemaVersion: 1, KeyID: keyID, Payload: base64.StdEncoding.EncodeToString(raw), Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(key, raw))})
}
