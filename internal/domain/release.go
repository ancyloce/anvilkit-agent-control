package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"
)

// Releases (DD-04 §4, DD-06 §4, P21): the committed projection of a
// release operation. The Workflow records each transition under the
// expected projection revision; Control checks every binding it records
// and decides nothing:
//
//   - the subject is recorded once and never changes; its digest is the
//     RFC 8785 digest of its fields, its revision and version are the
//     operation's, and its certification and artifact digests are those of
//     the operation's accepted certified stage;
//   - no target is published without an approval of exactly that digest;
//   - a succeeded target names the subject digest, its version and the
//     subject's destination; a definite target outcome never changes;
//   - published needs both targets succeeded, activated additionally the
//     activation; an unknown target keeps the release reconciling.

type ReleaseState string

const (
	ReleaseCertifying         ReleaseState = "certifying"
	ReleaseAwaitingApproval   ReleaseState = "awaiting_approval"
	ReleasePublishing         ReleaseState = "publishing"
	ReleasePublished          ReleaseState = "published"
	ReleaseActivated          ReleaseState = "activated"
	ReleasePartiallyPublished ReleaseState = "partially_published"
	ReleaseReconciling        ReleaseState = "reconciling"
	ReleaseRejected           ReleaseState = "rejected"
	ReleaseFailed             ReleaseState = "failed"
)

type ApprovalState string

const (
	ApprovalPending     ApprovalState = "pending"
	ApprovalApproved    ApprovalState = "approved"
	ApprovalRejected    ApprovalState = "rejected"
	ApprovalInvalidated ApprovalState = "invalidated"
)

type TargetState string

const (
	TargetPending   TargetState = "pending"
	TargetSucceeded TargetState = "succeeded"
	TargetFailed    TargetState = "failed"
	TargetUnknown   TargetState = "unknown"
)

// ArtifactDigestRef is one certified artifact's digest and size.
type ArtifactDigestRef struct {
	Digest    Digest `json:"digest"`
	SizeBytes string `json:"sizeBytes"`
}

// ReleaseDestinations are the release profile's destinations.
type ReleaseDestinations struct {
	NpmRegistry   string `json:"npmRegistry"`
	BrowserOrigin string `json:"browserOrigin"`
}

// ReleaseSubject is components/component.schema.json#/$defs/releaseSubject.
type ReleaseSubject struct {
	SchemaVersion               int                 `json:"schemaVersion"`
	ComponentID                 string              `json:"componentId"`
	PuckType                    string              `json:"puckType"`
	SourceRevision              string              `json:"sourceRevision"`
	SourceDigest                Digest              `json:"sourceDigest"`
	PackageName                 string              `json:"packageName"`
	Version                     string              `json:"version"`
	Npm                         ArtifactDigestRef   `json:"npm"`
	Browser                     ArtifactDigestRef   `json:"browser"`
	CSS                         []ArtifactDigestRef `json:"css"`
	BuildProfileID              string              `json:"buildProfileId"`
	BuildProfileDigest          Digest              `json:"buildProfileDigest"`
	ValidatorProfileID          string              `json:"validatorProfileId"`
	ValidatorProfileDigest      Digest              `json:"validatorProfileDigest"`
	HostAbi                     string              `json:"hostAbi"`
	HostAbiDigest               Digest              `json:"hostAbiDigest"`
	Destinations                ReleaseDestinations `json:"destinations"`
	CertificationEvidenceDigest Digest              `json:"certificationEvidenceDigest"`
	SubjectDigest               Digest              `json:"subjectDigest"`
}

func (s *ReleaseSubject) equal(o *ReleaseSubject) bool {
	a, _ := json.Marshal(s)
	b, _ := json.Marshal(o)
	return bytes.Equal(a, b)
}

// ComputeSubjectDigest is the contract's subject digest: sha256 over the
// RFC 8785 canonical JSON of the subject without subjectDigest. The values
// are strings, one small integer and nested objects/arrays, so a generic
// map encoded without HTML escaping (encoding/json sorts map keys) is the
// canonical form.
func ComputeSubjectDigest(s ReleaseSubject) (Digest, error) {
	s.SchemaVersion = 1
	raw, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return "", err
	}
	delete(m, "subjectDigest")
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return "", err
	}
	sum := sha256.Sum256(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
	return Digest(fmt.Sprintf("sha256:%x", sum)), nil
}

// Approval is the maintainer's decision, bound to the digest it was made for.
type Approval struct {
	State         ApprovalState `json:"state"`
	SubjectDigest Digest        `json:"subjectDigest"`
	ApproverID    string        `json:"approverId,omitempty"`
	DecidedAt     *time.Time    `json:"decidedAt,omitempty"`
	ReasonCode    string        `json:"reasonCode,omitempty"`
}

// ReleaseTarget is one guarded mutation of the release and its receipt.
type ReleaseTarget struct {
	State          TargetState `json:"state"`
	EffectID       string      `json:"effectId,omitempty"`
	ReceiptID      string      `json:"receiptId,omitempty"`
	ReceiptDigest  string      `json:"receiptDigest,omitempty"`
	SubjectDigest  Digest      `json:"subjectDigest,omitempty"`
	Destination    string      `json:"destination,omitempty"`
	Version        string      `json:"version,omitempty"`
	ManifestDigest string      `json:"manifestDigest,omitempty"`
	FailureCode    string      `json:"failureCode,omitempty"`
}

func (t ReleaseTarget) definite() bool { return t.State == TargetSucceeded || t.State == TargetFailed }

type Release struct {
	OperationID      string
	TenantID         string
	Lineage          Digest
	SourceRevision   string
	State            ReleaseState
	Subject          *ReleaseSubject
	ReleaseID        string
	ReviewEffectID   string
	Approval         *Approval
	ApprovalDeadline *time.Time
	Npm              ReleaseTarget
	Browser          ReleaseTarget
	Activation       ReleaseTarget
	CatalogRevision  string
	FailureCode      string
	Revision         uint64
	UpdatedAt        time.Time
}

// Approved reports whether the recorded approval is of exactly the
// recorded subject.
func (r *Release) Approved() bool {
	return r != nil && r.Subject != nil && r.Approval != nil && r.Approval.State == ApprovalApproved && r.Approval.SubjectDigest == r.Subject.SubjectDigest
}

// Published reports whether both targets succeeded with receipts that
// bind the approved subject.
func (r *Release) Published() bool {
	return r.Approved() && r.Npm.State == TargetSucceeded && r.Browser.State == TargetSucceeded
}

// ReleaseRecord is one transition the Workflow asks to record.
type ReleaseRecord struct {
	ExpectedRevision uint64
	State            ReleaseState
	Subject          *ReleaseSubject
	ReleaseID        string
	ReviewEffectID   string
	Approval         *Approval
	ApprovalDeadline *time.Time
	Npm              ReleaseTarget
	Browser          ReleaseTarget
	Activation       ReleaseTarget
	CatalogRevision  string
	FailureCode      string
}

// ReleaseCertification is the operation's accepted certified stage as the
// subject must bind it: the evidence artifact and the certified artifacts.
type ReleaseCertification struct {
	EvidenceDigest Digest
	Npm            ArtifactDigestRef
	Browser        ArtifactDigestRef
	CSS            []ArtifactDigestRef
}

// CertificationOf reads the certification bindings from a certified
// stage's artifacts (nil when a class is missing).
func CertificationOf(artifacts []StageArtifact) *ReleaseCertification {
	c := &ReleaseCertification{}
	ref := func(a StageArtifact) ArtifactDigestRef {
		return ArtifactDigestRef{Digest: a.Digest, SizeBytes: strconv.FormatInt(a.SizeBytes, 10)}
	}
	for _, a := range artifacts {
		switch a.Class {
		case ArtifactEvidence:
			c.EvidenceDigest = a.Digest
		case ArtifactNpm:
			c.Npm = ref(a)
		case ArtifactBrowser:
			c.Browser = ref(a)
		case ArtifactCSS:
			c.CSS = append(c.CSS, ref(a))
		}
	}
	if c.EvidenceDigest == "" || c.Npm.Digest == "" || c.Browser.Digest == "" || len(c.CSS) == 0 {
		return nil
	}
	return c
}

var releaseNext = map[ReleaseState][]ReleaseState{
	"":                      {ReleaseCertifying, ReleaseFailed},
	ReleaseCertifying:       {ReleaseAwaitingApproval, ReleaseFailed},
	ReleaseAwaitingApproval: {ReleasePublishing, ReleaseRejected, ReleaseFailed},
	ReleasePublishing:       {ReleasePublishing, ReleasePublished, ReleasePartiallyPublished, ReleaseReconciling, ReleaseFailed},
	ReleaseReconciling:      {ReleasePublishing, ReleasePublished, ReleasePartiallyPublished, ReleaseReconciling, ReleaseActivated, ReleaseFailed},
	ReleasePublished:        {ReleaseActivated, ReleaseReconciling, ReleaseFailed},
}

func (r ReleaseRecord) same(p *Release) bool {
	if r.State != p.State || r.ReleaseID != p.ReleaseID || r.ReviewEffectID != p.ReviewEffectID || r.CatalogRevision != p.CatalogRevision || r.FailureCode != p.FailureCode ||
		r.Npm != p.Npm || r.Browser != p.Browser || r.Activation != p.Activation {
		return false
	}
	if (r.Subject == nil) != (p.Subject == nil) || (r.Subject != nil && !r.Subject.equal(p.Subject)) {
		return false
	}
	a, _ := json.Marshal(r.Approval)
	b, _ := json.Marshal(p.Approval)
	if !bytes.Equal(a, b) {
		return false
	}
	return timesEqual(r.ApprovalDeadline, p.ApprovalDeadline)
}

func timesEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// checkTarget verifies a target record against the current one and the
// subject: a definite outcome never changes, an effect identity never
// changes, and a succeeded target binds the subject digest, its version
// and the destination.
func checkTarget(name string, cur, next ReleaseTarget, s *ReleaseSubject, destination string) error {
	if cur.definite() && next != cur {
		return fmt.Errorf("%w: the %s target is %s; a definite outcome never changes", ErrInvalid, name, cur.State)
	}
	if cur.EffectID != "" && next.EffectID != cur.EffectID {
		return fmt.Errorf("%w: the %s target's effect %s never changes", ErrInvalid, name, cur.EffectID)
	}
	switch next.State {
	case TargetPending, TargetUnknown, TargetFailed:
		if next.State != TargetPending && next.EffectID == "" {
			return fmt.Errorf("%w: a sent %s target names its effect", ErrInvalid, name)
		}
		if next.State == TargetFailed && next.FailureCode == "" {
			return fmt.Errorf("%w: a failed %s target names its failure", ErrInvalid, name)
		}
		return nil
	case TargetSucceeded:
	default:
		return fmt.Errorf("%w: %s target state %q", ErrInvalid, name, next.State)
	}
	switch {
	case s == nil:
		return fmt.Errorf("%w: a %s receipt without a subject", ErrInvalid, name)
	case next.EffectID == "" || next.ReceiptID == "" || next.ReceiptDigest == "":
		return fmt.Errorf("%w: a succeeded %s target names its effect and its receipt", ErrInvalid, name)
	case next.SubjectDigest != s.SubjectDigest:
		return fmt.Errorf("%w: the %s receipt binds subject %s, the release %s", ErrInvalid, name, next.SubjectDigest, s.SubjectDigest)
	case destination != "" && next.Destination != destination:
		return fmt.Errorf("%w: the %s receipt names destination %q, the subject %q", ErrInvalid, name, next.Destination, destination)
	case destination != "" && next.Version != s.Version:
		return fmt.Errorf("%w: the %s receipt names version %q, the subject %q", ErrInvalid, name, next.Version, s.Version)
	case name == "browser" && next.ManifestDigest == "":
		return fmt.Errorf("%w: the browser receipt names its manifest", ErrInvalid)
	}
	return nil
}

// ReleaseEffects are the effect rows a release names (its review and its
// targets' effects) by effect id, as Control's ledger holds them; an id
// without a row is absent.
type ReleaseEffects map[string]*Effect

// checkTargetEffect binds a succeeded target to the effect ledger (B-38):
// the effect it names is a succeeded effect of this release operation, of
// the target's kind and canonical subject, whose observed outcome is the
// digest of the target's receipt. A recorder's assertion alone never makes
// a target succeeded.
func checkTargetEffect(op *Operation, name string, t ReleaseTarget, s *ReleaseSubject, effects ReleaseEffects) error {
	kind := EffectPublication
	if name == "activation" {
		kind = EffectActivation
	}
	e := effects[t.EffectID]
	switch {
	case e == nil || e.OperationID != op.ID:
		return fmt.Errorf("%w: the %s target names effect %s, which is no effect of release %s", ErrIntegrity, name, t.EffectID, op.ID)
	case e.Kind != kind || e.CanonicalSubject != ReleaseEffectSubject(s.SubjectDigest, name):
		return fmt.Errorf("%w: effect %s is a %s of %q, not the %s target of subject %s", ErrIntegrity, e.ID, e.Kind, e.CanonicalSubject, name, s.SubjectDigest)
	case e.State != EffectSucceeded:
		return fmt.Errorf("%w: effect %s of the %s target is %s; a succeeded target needs a succeeded effect", ErrIntegrity, e.ID, name, e.State)
	case e.OutcomeRef != t.ReceiptDigest:
		return fmt.Errorf("%w: effect %s observed receipt %q, the %s target names receipt %s", ErrIntegrity, e.ID, e.OutcomeRef, name, t.ReceiptDigest)
	}
	return nil
}

// checkReviewEffect binds an approval to the registered review: the
// release's review effect is a succeeded review of this operation whose
// observed outcome is the subject the approval decides.
func checkReviewEffect(op *Operation, subject Digest, reviewEffectID string, effects ReleaseEffects) error {
	e := effects[reviewEffectID]
	switch {
	case e == nil || e.OperationID != op.ID || e.Kind != EffectReview:
		return fmt.Errorf("%w: the approval names review effect %s, which is no review of release %s", ErrIntegrity, reviewEffectID, op.ID)
	case e.State != EffectSucceeded || e.OutcomeRef != string(subject):
		return fmt.Errorf("%w: review effect %s is %s with outcome %q; an approval needs the registered review of subject %s", ErrIntegrity, e.ID, e.State, e.OutcomeRef, subject)
	}
	return nil
}

// ApplyRelease decides a record against the current projection (nil for
// none): the next projection, or existing=true when the same transition
// was already recorded. cert is the operation's accepted certified stage
// (nil when it has none); effects are the ledger rows the record names.
func ApplyRelease(op *Operation, cur *Release, r ReleaseRecord, cert *ReleaseCertification, effects ReleaseEffects, now time.Time) (next *Release, existing bool, err error) {
	if op.Kind != KindRelease {
		return nil, false, fmt.Errorf("%w: operation %s is not a release", ErrInvalid, op.ID)
	}
	var state ReleaseState
	var rev uint64
	base := &Release{Npm: ReleaseTarget{State: TargetPending}, Browser: ReleaseTarget{State: TargetPending}, Activation: ReleaseTarget{State: TargetPending}}
	if cur != nil {
		state, rev, base = cur.State, cur.Revision, cur
		if r.ExpectedRevision+1 == rev && r.same(cur) {
			return cur, true, nil
		}
	}
	if r.ExpectedRevision != rev {
		return nil, false, fmt.Errorf("%w: release of %s is at revision %d, the record expects %d", ErrRevisionConflict, op.ID, rev, r.ExpectedRevision)
	}
	if !slices.Contains(releaseNext[state], r.State) {
		return nil, false, fmt.Errorf("%w: release of %s cannot move from %q to %q", ErrInvalid, op.ID, state, r.State)
	}
	// The subject: recorded once, never changed, bound to the operation
	// and its certification.
	switch {
	case base.Subject != nil && (r.Subject == nil || !r.Subject.equal(base.Subject)):
		return nil, false, fmt.Errorf("%w: the subject of a release never changes; a changed subject is a new release", ErrInvalid)
	case base.Subject == nil && r.Subject != nil:
		if err := checkSubject(op, r.Subject, cert); err != nil {
			return nil, false, err
		}
	}
	if base.ReleaseID != "" && r.ReleaseID != base.ReleaseID {
		return nil, false, fmt.Errorf("%w: the release id %s never changes", ErrInvalid, base.ReleaseID)
	}
	if base.ReviewEffectID != "" && r.ReviewEffectID != base.ReviewEffectID {
		return nil, false, fmt.Errorf("%w: the review effect %s never changes", ErrInvalid, base.ReviewEffectID)
	}
	if base.ApprovalDeadline != nil && !timesEqual(r.ApprovalDeadline, base.ApprovalDeadline) {
		return nil, false, fmt.Errorf("%w: the approval deadline is set once", ErrInvalid)
	}
	if r.Approval != nil && r.Subject == nil {
		return nil, false, fmt.Errorf("%w: an approval without a subject", ErrInvalid)
	}
	s := r.Subject
	var npmDest, browserDest string
	if s != nil {
		npmDest, browserDest = s.Destinations.NpmRegistry, s.Destinations.BrowserOrigin
	}
	if err := checkTarget("npm", base.Npm, r.Npm, s, npmDest); err != nil {
		return nil, false, err
	}
	if err := checkTarget("browser", base.Browser, r.Browser, s, browserDest); err != nil {
		return nil, false, err
	}
	if err := checkTarget("activation", base.Activation, r.Activation, s, ""); err != nil {
		return nil, false, err
	}
	// The ledger: an approval is decided under the registered review, and
	// every succeeded target is a succeeded effect with the target's receipt.
	if r.Approval != nil && r.Approval.State == ApprovalApproved {
		if err := checkReviewEffect(op, s.SubjectDigest, r.ReviewEffectID, effects); err != nil {
			return nil, false, err
		}
	}
	for _, t := range []struct {
		name   string
		target ReleaseTarget
	}{{"npm", r.Npm}, {"browser", r.Browser}, {"activation", r.Activation}} {
		if t.target.State == TargetSucceeded {
			if err := checkTargetEffect(op, t.name, t.target, s, effects); err != nil {
				return nil, false, err
			}
		}
	}
	probe := &Release{Subject: s, Approval: r.Approval, Npm: r.Npm, Browser: r.Browser, Activation: r.Activation}
	sent := r.Npm.State != TargetPending || r.Browser.State != TargetPending || r.Activation.State != TargetPending
	switch {
	case r.State == ReleaseAwaitingApproval && (s == nil || r.ReleaseID == "" || r.ReviewEffectID == "" || r.ApprovalDeadline == nil):
		return nil, false, fmt.Errorf("%w: a release awaiting approval names its subject, its review and the approval deadline", ErrInvalid)
	case sent && !probe.Approved():
		return nil, false, fmt.Errorf("%w: no target is sent without an approval of exactly subject %v", ErrInvalid, subjectDigestOf(s))
	case (r.State == ReleasePublishing || r.State == ReleasePublished || r.State == ReleaseActivated) && !probe.Approved():
		return nil, false, fmt.Errorf("%w: a %s release needs an approval of exactly its subject", ErrInvalid, r.State)
	case r.Activation.State != TargetPending && !probe.Published():
		return nil, false, fmt.Errorf("%w: activation needs both verified receipts and the approval", ErrInvalid)
	case r.State == ReleasePublished && !probe.Published():
		return nil, false, fmt.Errorf("%w: a published release needs both targets succeeded", ErrInvalid)
	case r.State == ReleaseActivated && (!probe.Published() || r.Activation.State != TargetSucceeded || r.CatalogRevision == ""):
		return nil, false, fmt.Errorf("%w: an activated release needs both receipts, the approval and the activation receipt", ErrInvalid)
	case r.State == ReleasePartiallyPublished && !((r.Npm.State == TargetSucceeded && r.Browser.State == TargetFailed) || (r.Npm.State == TargetFailed && r.Browser.State == TargetSucceeded)):
		return nil, false, fmt.Errorf("%w: a partially published release has exactly one succeeded and one failed target", ErrInvalid)
	case r.State == ReleaseReconciling && r.Npm.State != TargetUnknown && r.Browser.State != TargetUnknown && r.Activation.State != TargetUnknown:
		return nil, false, fmt.Errorf("%w: a reconciling release has an unknown target", ErrInvalid)
	case (r.State == ReleaseFailed || r.State == ReleaseRejected) && r.FailureCode == "":
		return nil, false, fmt.Errorf("%w: a %s release names its failure", ErrInvalid, r.State)
	}
	return &Release{
		OperationID: op.ID, TenantID: op.TenantID, Lineage: op.Subject.SubjectDigest, SourceRevision: op.Subject.SourceRevision, State: r.State,
		Subject: r.Subject, ReleaseID: r.ReleaseID, ReviewEffectID: r.ReviewEffectID, Approval: r.Approval, ApprovalDeadline: r.ApprovalDeadline,
		Npm: r.Npm, Browser: r.Browser, Activation: r.Activation, CatalogRevision: r.CatalogRevision, FailureCode: r.FailureCode,
		Revision: rev + 1, UpdatedAt: now,
	}, false, nil
}

func subjectDigestOf(s *ReleaseSubject) Digest {
	if s == nil {
		return ""
	}
	return s.SubjectDigest
}

func checkSubject(op *Operation, s *ReleaseSubject, cert *ReleaseCertification) error {
	d, err := ComputeSubjectDigest(*s)
	if err != nil {
		return fmt.Errorf("%w: subject: %v", ErrInvalid, err)
	}
	switch {
	case s.SchemaVersion != 1:
		return fmt.Errorf("%w: subject schema version %d", ErrInvalid, s.SchemaVersion)
	case d != s.SubjectDigest:
		return fmt.Errorf("%w: the subject's fields digest to %s, it names %s", ErrInvalid, d, s.SubjectDigest)
	case s.SourceRevision != op.Subject.SourceRevision:
		return fmt.Errorf("%w: the subject names revision %s, the release was accepted for %s", ErrInvalid, s.SourceRevision, op.Subject.SourceRevision)
	case s.Version != op.Subject.PackageVersion:
		return fmt.Errorf("%w: the subject names version %s, the release was accepted for %s", ErrInvalid, s.Version, op.Subject.PackageVersion)
	case cert == nil:
		return fmt.Errorf("%w: release %s has no accepted certified stage", ErrInvalid, op.ID)
	case s.CertificationEvidenceDigest != cert.EvidenceDigest || s.Npm != cert.Npm || s.Browser != cert.Browser || !slices.Equal(sortedRefs(s.CSS), sortedRefs(cert.CSS)):
		return fmt.Errorf("%w: the subject's certification and artifact digests are not the operation's accepted certification", ErrInvalid)
	case s.Destinations.NpmRegistry == "" || s.Destinations.BrowserOrigin == "":
		return fmt.Errorf("%w: the subject names both destinations", ErrInvalid)
	}
	return nil
}

func sortedRefs(in []ArtifactDigestRef) []ArtifactDigestRef {
	out := slices.Clone(in)
	slices.SortFunc(out, func(a, b ArtifactDigestRef) int {
		if a.Digest < b.Digest {
			return -1
		}
		if a.Digest > b.Digest {
			return 1
		}
		return 0
	})
	return out
}

// Canonical subjects of the release's guarded mutations: the effect's
// canonical subject names the subject digest and the target, so a permit
// can be checked against the recorded release.
func ReleaseEffectSubject(subjectDigest Digest, target string) string {
	return "release:" + string(subjectDigest) + ":" + target
}

// CheckReleaseEffect refuses a release mutation the recorded projection
// does not authorize (DD-06 §4): a publication without an approval of
// exactly the subject it names, decided under the registered review; an
// activation without both verified receipts, each a succeeded publication
// effect of the ledger (B-38). Review registration needs only a release
// operation.
func CheckReleaseEffect(req EffectRequest, op *Operation, rel *Release, effects ReleaseEffects) error {
	if op.Kind != KindRelease {
		if req.Kind == EffectPublication || req.Kind == EffectActivation || req.Kind == EffectReview {
			return deny(DenyInvalidArgument, "a %s effect belongs to a release operation", req.Kind)
		}
		return nil
	}
	switch req.Kind {
	case EffectPublication:
		if !rel.Approved() {
			return deny(DenyApprovalRequired, "release %s has no approval of its subject", op.ID)
		}
		if err := checkReviewEffect(op, rel.Subject.SubjectDigest, rel.ReviewEffectID, effects); err != nil {
			return deny(DenyApprovalRequired, "%v", err)
		}
		if req.CanonicalSubject != ReleaseEffectSubject(rel.Subject.SubjectDigest, "npm") && req.CanonicalSubject != ReleaseEffectSubject(rel.Subject.SubjectDigest, "browser") {
			return deny(DenyApprovalRequired, "publication subject %q is not the approved subject %s", req.CanonicalSubject, rel.Subject.SubjectDigest)
		}
	case EffectActivation:
		if rel == nil || !rel.Published() {
			return deny(DenyReceiptsRequired, "release %s has not both verified receipts under its approval", op.ID)
		}
		for _, t := range []struct {
			name   string
			target ReleaseTarget
		}{{"npm", rel.Npm}, {"browser", rel.Browser}} {
			if err := checkTargetEffect(op, t.name, t.target, rel.Subject, effects); err != nil {
				return deny(DenyReceiptsRequired, "%v", err)
			}
		}
		if req.CanonicalSubject != ReleaseEffectSubject(rel.Subject.SubjectDigest, "activation") {
			return deny(DenyReceiptsRequired, "activation subject %q is not the published subject %s", req.CanonicalSubject, rel.Subject.SubjectDigest)
		}
	}
	return nil
}

const (
	// DenyApprovalRequired refuses a publication without an approval of
	// exactly the subject; DenyReceiptsRequired an activation without
	// both verified receipts. Both are FAILED_PRECONDITION-like refusals
	// recorded as denied effects.
	DenyApprovalRequired = "APPROVAL_REQUIRED"
	DenyReceiptsRequired = "RECEIPTS_REQUIRED"
)
