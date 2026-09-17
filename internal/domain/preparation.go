package domain

import (
	"fmt"
	"time"
)

// Preparation records (DD-01 §3): a grouped question round with its
// immutable absolute expiry, one accepted answer per question set revision
// committed with its Update relay intent, and the frozen brief that binds
// the exact inputs a generation may start from.

type QuestionSetState string

const (
	QuestionSetOpen       QuestionSetState = "open"
	QuestionSetAnswered   QuestionSetState = "answered"
	QuestionSetExpired    QuestionSetState = "expired"
	QuestionSetSuperseded QuestionSetState = "superseded"
)

// Question is one clarification question.
type Question struct {
	ID   string `json:"questionId"`
	Text string `json:"text"`
}

// QuestionSet is one grouped clarification round.
type QuestionSet struct {
	ID            string
	OperationID   string
	TenantID      string
	Revision      uint64
	Round         uint64
	Questions     []Question
	State         QuestionSetState
	CommandID     string
	RequestDigest Digest
	AskedAt       time.Time
	ExpiresAt     time.Time
}

// NewQuestionSet validates a round against the profile's bounds and the
// operation's state and returns the open set with its immutable expiry
// (asked at now plus the profile's wait). A fenced or terminal operation,
// a round beyond the bound, more questions than the bound, a round while
// another is open, or a round that would end after the operation's own
// deadline is refused.
func NewQuestionSet(op *Operation, profile Profile, cmd CommandIdentity, round uint64, questions []Question, openExists bool, now time.Time) (*QuestionSet, error) {
	if op.Kind != KindPreparation {
		return nil, fmt.Errorf("%w: operation %s is a %s, not a preparation", ErrInvalid, op.ID, op.Kind)
	}
	if op.FencedForNewDispatch() {
		return nil, fmt.Errorf("%w: operation %s is fenced (%s/%s)", ErrStaleExecution, op.ID, op.Lifecycle, op.Control)
	}
	if op.Intake != IntakeConfirmed {
		return nil, fmt.Errorf("%w: intake of %s is not confirmed", ErrStaleExecution, op.ID)
	}
	b := profile.Clarification
	if round == 0 || round > b.MaxRounds {
		return nil, fmt.Errorf("%w: round %d exceeds the profile's %d rounds", ErrInvalid, round, b.MaxRounds)
	}
	if len(questions) == 0 || uint64(len(questions)) > b.MaxQuestions {
		return nil, fmt.Errorf("%w: %d questions exceed the profile's %d per round", ErrInvalid, len(questions), b.MaxQuestions)
	}
	seen := map[string]bool{}
	for _, q := range questions {
		if q.ID == "" || q.Text == "" || seen[q.ID] {
			return nil, fmt.Errorf("%w: question %q is not a distinct question with text", ErrInvalid, q.ID)
		}
		seen[q.ID] = true
	}
	if openExists {
		return nil, fmt.Errorf("%w: operation %s already has an open question set", ErrStaleExecution, op.ID)
	}
	if b.Wait <= 0 {
		return nil, fmt.Errorf("%w: the profile has no clarification wait", ErrProfileUnqualified)
	}
	// The stored precision is the identity: the Workflow's timer and the
	// public projection read the same instant back.
	asked := now.UTC().Truncate(time.Microsecond)
	expires := asked.Add(b.Wait)
	if !expires.Before(op.Deadline) {
		return nil, fmt.Errorf("%w: a round asked now would wait past the operation deadline %s", ErrStaleExecution, op.Deadline.UTC().Format(time.RFC3339))
	}
	return &QuestionSet{
		ID: NewID("qs"), OperationID: op.ID, TenantID: op.TenantID, Revision: 1, Round: round, Questions: questions,
		State: QuestionSetOpen, CommandID: cmd.CommandID, RequestDigest: cmd.RequestDigest, AskedAt: asked, ExpiresAt: expires,
	}, nil
}

// Expired reports whether the wait of the round ended at now.
func (q *QuestionSet) Expired(now time.Time) bool { return !now.Before(q.ExpiresAt) }

// AnswerRelayState is the Update relay intent of an accepted answer.
type AnswerRelayState string

const (
	AnswerRelayPending  AnswerRelayState = "pending"
	AnswerRelaySent     AnswerRelayState = "sent"
	AnswerRelayApplied  AnswerRelayState = "applied"
	AnswerRelayRejected AnswerRelayState = "rejected"
)

// Answer is one accepted clarification answer.
type Answer struct {
	ID                  string
	OperationID         string
	TenantID            string
	ActorID             string
	QuestionSetID       string
	QuestionSetRevision uint64
	Answer              ArtifactBinding
	Handle              string
	UpdateID            string
	CommandID           string
	RequestDigest       Digest
	Relay               AnswerRelayState
	AcceptedAt          time.Time
	RelayedAt           *time.Time
}

// NewAnswer accepts an answer for the open question set: the set must be
// the operation's, open, at the stated revision and not expired at now; the
// transfer must be a finalized answer artifact of the same tenant holding
// the declared digest. The Update identity is the answer identity, so a
// re-issued relay reaches the same Update.
func NewAnswer(op *Operation, qs *QuestionSet, t *Transfer, cmd CommandIdentity, revision uint64, binding ArtifactBinding, now time.Time) (*Answer, error) {
	if qs.OperationID != op.ID || qs.TenantID != op.TenantID || op.TenantID != cmd.TenantID {
		return nil, ErrNotFound
	}
	if op.Lifecycle.Terminal() || op.Control != ControlNone {
		return nil, fmt.Errorf("%w: operation %s is %s (%s)", ErrStaleExecution, op.ID, op.Lifecycle, op.Control)
	}
	if qs.Revision != revision {
		return nil, fmt.Errorf("%w: question set %s is at revision %d, the answer names %d", ErrRevisionConflict, qs.ID, qs.Revision, revision)
	}
	if qs.State != QuestionSetOpen {
		return nil, fmt.Errorf("%w: question set %s is %s", ErrStaleExecution, qs.ID, qs.State)
	}
	if qs.Expired(now) {
		return nil, fmt.Errorf("%w: question set %s expired at %s", ErrStaleExecution, qs.ID, qs.ExpiresAt.UTC().Format(time.RFC3339))
	}
	if t == nil || t.TenantID != op.TenantID {
		return nil, fmt.Errorf("%w: answer transfer %s is not of this scope", ErrInvalid, binding.TransferID)
	}
	if t.State != TransferFinalized || t.Class != ArtifactAnswer {
		return nil, fmt.Errorf("%w: answer transfer %s is not a finalized answer artifact (%s/%s)", ErrInvalid, t.ID, t.State, t.Class)
	}
	if t.ExpectedDigest != binding.Digest || (t.ActualDigest != "" && t.ActualDigest != binding.Digest) {
		return nil, fmt.Errorf("%w: answer transfer %s holds %s, the answer names %s", ErrInvalid, t.ID, t.ExpectedDigest, binding.Digest)
	}
	if t.OperationID != "" && t.OperationID != op.ID {
		return nil, fmt.Errorf("%w: answer transfer %s is bound to another operation", ErrInvalid, t.ID)
	}
	id := NewID("ans")
	return &Answer{
		ID: id, OperationID: op.ID, TenantID: op.TenantID, ActorID: cmd.ActorID, QuestionSetID: qs.ID, QuestionSetRevision: qs.Revision,
		Answer: binding, Handle: t.Handle, UpdateID: id, CommandID: cmd.CommandID, RequestDigest: cmd.RequestDigest,
		Relay: AnswerRelayPending, AcceptedAt: now,
	}, nil
}

type BriefState string

const (
	BriefCurrent    BriefState = "current"
	BriefSuperseded BriefState = "superseded"
)

// ContentDigest freezes the exact content of a referenced source revision.
type ContentDigest struct {
	SourceID string `json:"sourceId"`
	Revision uint64 `json:"revision,string"`
	Digest   Digest `json:"digest"`
}

// Brief is the frozen brief of a preparation (DD-01 §3): it binds the
// brief artifact, the requirements digest, the source revisions and the
// brand/asset content digests to the preparation operation and its
// revision. A superseded brief starts no generation.
type Brief struct {
	ID                 string
	OperationID        string
	TenantID           string
	Revision           uint64
	Brief              ArtifactBinding
	Handle             string
	RequirementsDigest Digest
	SourceRevisions    []SourceReference
	BrandDigests       []ContentDigest
	AssetDigests       []ContentDigest
	State              BriefState
	CommandID          string
	RequestDigest      Digest
	FrozenAt           time.Time
}

// NewBrief freezes a brief for the preparation: the transfer must be a
// finalized brief artifact bound to the operation holding the declared
// digest; every brand/asset reference of the intake must be frozen with a
// content digest, none may be invented; the operation must not be fenced.
func NewBrief(op *Operation, t *Transfer, cmd CommandIdentity, revision uint64, binding ArtifactBinding, requirements Digest, sources []SourceReference, brands, assets []ContentDigest, now time.Time) (*Brief, error) {
	if op.Kind != KindPreparation || op.Preparation == nil {
		return nil, fmt.Errorf("%w: operation %s is not a preparation with an intake", ErrInvalid, op.ID)
	}
	if op.FencedForNewDispatch() {
		return nil, fmt.Errorf("%w: operation %s is fenced (%s/%s)", ErrStaleExecution, op.ID, op.Lifecycle, op.Control)
	}
	if t == nil || t.TenantID != op.TenantID || t.OperationID != op.ID {
		return nil, fmt.Errorf("%w: brief transfer %s is not bound to operation %s", ErrInvalid, binding.TransferID, op.ID)
	}
	if t.State != TransferFinalized || t.Class != ArtifactBrief {
		return nil, fmt.Errorf("%w: brief transfer %s is not a finalized brief artifact (%s/%s)", ErrInvalid, t.ID, t.State, t.Class)
	}
	if t.ExpectedDigest != binding.Digest || (t.ActualDigest != "" && t.ActualDigest != binding.Digest) {
		return nil, fmt.Errorf("%w: brief transfer %s holds %s, the brief names %s", ErrInvalid, t.ID, t.ExpectedDigest, binding.Digest)
	}
	if requirements == "" {
		return nil, fmt.Errorf("%w: a brief needs the requirements digest", ErrInvalid)
	}
	if err := frozenReferences("brand", op.Preparation.BrandReferences, brands); err != nil {
		return nil, err
	}
	if err := frozenReferences("asset", op.Preparation.AssetReferences, assets); err != nil {
		return nil, err
	}
	return &Brief{
		ID: NewID("brf"), OperationID: op.ID, TenantID: op.TenantID, Revision: revision, Brief: binding, Handle: t.Handle,
		RequirementsDigest: requirements, SourceRevisions: sources, BrandDigests: brands, AssetDigests: assets, State: BriefCurrent,
		CommandID: cmd.CommandID, RequestDigest: cmd.RequestDigest, FrozenAt: now,
	}, nil
}

// frozenReferences checks that the frozen digests are exactly the intake's
// references: each reference once, at its exact revision, and nothing else.
func frozenReferences(kind string, refs []SourceReference, digests []ContentDigest) error {
	if len(refs) != len(digests) {
		return fmt.Errorf("%w: the intake names %d %s references, the brief freezes %d", ErrInvalid, len(refs), kind, len(digests))
	}
	want := map[string]uint64{}
	for _, r := range refs {
		want[r.SourceID] = r.Revision
	}
	seen := map[string]bool{}
	for _, d := range digests {
		rev, ok := want[d.SourceID]
		if !ok || rev != d.Revision || seen[d.SourceID] || d.Digest == "" {
			return fmt.Errorf("%w: %s reference %s@%d is not an intake reference frozen once with a digest", ErrInvalid, kind, d.SourceID, d.Revision)
		}
		seen[d.SourceID] = true
	}
	return nil
}

// StartsGeneration reports whether a generation may be accepted against
// this brief now: the brief is current and belongs to the caller's tenant.
func (b *Brief) StartsGeneration(tenantID string) error {
	if b.TenantID != tenantID {
		return ErrNotFound
	}
	if b.State != BriefCurrent {
		return fmt.Errorf("%w: brief %s is %s", ErrStaleExecution, b.ID, b.State)
	}
	return nil
}
