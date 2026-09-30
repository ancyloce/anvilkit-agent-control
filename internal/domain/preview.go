package domain

import (
	"fmt"
	"slices"
	"time"
)

// Preview builds (DD-04 §5, DD-05 §1): the committed projection of a
// preview_build operation. The Workflow records each transition under the
// expected projection revision: saving, then the conditional save's
// outcome (conflict with the current revision, or building the saved
// revision, or failed), then the build's outcome (ready with the exact
// module and stylesheets, stale when a newer revision was saved meanwhile,
// or failed). A stale preview stays readable as a diagnostic; it never
// becomes the current one.

type PreviewState string

const (
	PreviewSaving   PreviewState = "saving"
	PreviewConflict PreviewState = "conflict"
	PreviewBuilding PreviewState = "building"
	PreviewReady    PreviewState = "ready"
	PreviewStale    PreviewState = "stale"
	PreviewFailed   PreviewState = "failed"
)

// PreviewArtifact is one accepted output of the preview's build.
type PreviewArtifact struct {
	Handle        string `json:"handle"`
	Class         string `json:"class"`
	Digest        Digest `json:"digest"`
	SizeBytes     int64  `json:"sizeBytes"`
	TransferID    string `json:"transferId"`
	ObjectVersion string `json:"objectVersion"`
}

type Preview struct {
	OperationID     string
	TenantID        string
	SubjectDigest   Digest
	BaseRevision    string
	State           PreviewState
	SourceRevision  string
	CurrentRevision string
	SourceDigest    Digest
	Module          *PreviewArtifact
	Styles          []PreviewArtifact
	BuildProfileID  string
	HostProfileID   string
	FailureCode     string
	Revision        uint64
	UpdatedAt       time.Time
}

// PreviewRecord is one transition the Workflow asks to record.
type PreviewRecord struct {
	ExpectedRevision uint64
	State            PreviewState
	SourceRevision   string
	CurrentRevision  string
	SourceDigest     Digest
	Module           *PreviewArtifact
	Styles           []PreviewArtifact
	BuildProfileID   string
	HostProfileID    string
	FailureCode      string
}

var previewNext = map[PreviewState][]PreviewState{
	"":              {PreviewSaving},
	PreviewSaving:   {PreviewConflict, PreviewBuilding, PreviewFailed},
	PreviewBuilding: {PreviewReady, PreviewStale, PreviewFailed},
}

// same reports whether the record states exactly what p holds.
func (r PreviewRecord) same(p *Preview) bool {
	if r.State != p.State || r.SourceRevision != p.SourceRevision || r.CurrentRevision != p.CurrentRevision || r.SourceDigest != p.SourceDigest ||
		r.BuildProfileID != p.BuildProfileID || r.HostProfileID != p.HostProfileID || r.FailureCode != p.FailureCode || len(r.Styles) != len(p.Styles) {
		return false
	}
	if (r.Module == nil) != (p.Module == nil) || (r.Module != nil && *r.Module != *p.Module) {
		return false
	}
	return slices.Equal(r.Styles, p.Styles)
}

// ApplyPreview decides a record against the current projection (nil for
// none): the next projection, or existing=true when the same transition was
// already recorded; a record under another revision or an illegal
// transition is refused.
func ApplyPreview(op *Operation, cur *Preview, r PreviewRecord, now time.Time) (next *Preview, existing bool, err error) {
	if op.Kind != KindPreviewBuild {
		return nil, false, fmt.Errorf("%w: operation %s is not a preview build", ErrInvalid, op.ID)
	}
	var state PreviewState
	var rev uint64
	if cur != nil {
		state, rev = cur.State, cur.Revision
		if r.ExpectedRevision+1 == rev && r.same(cur) {
			return cur, true, nil
		}
	}
	if r.ExpectedRevision != rev {
		return nil, false, fmt.Errorf("%w: preview of %s is at revision %d, the record expects %d", ErrRevisionConflict, op.ID, rev, r.ExpectedRevision)
	}
	if !slices.Contains(previewNext[state], r.State) {
		return nil, false, fmt.Errorf("%w: preview of %s cannot move from %q to %q", ErrInvalid, op.ID, state, r.State)
	}
	switch {
	case r.State == PreviewConflict && r.CurrentRevision == "":
		return nil, false, fmt.Errorf("%w: a conflict names the current revision", ErrInvalid)
	case (r.State == PreviewBuilding || r.State == PreviewReady || r.State == PreviewStale) && r.SourceRevision == "":
		return nil, false, fmt.Errorf("%w: a saved preview names its revision", ErrInvalid)
	case (r.State == PreviewReady || r.State == PreviewStale) && r.Module == nil:
		return nil, false, fmt.Errorf("%w: a built preview names its module", ErrInvalid)
	case r.State == PreviewFailed && r.FailureCode == "":
		return nil, false, fmt.Errorf("%w: a failed preview names its failure", ErrInvalid)
	case cur != nil && cur.SourceRevision != "" && r.SourceRevision != cur.SourceRevision:
		return nil, false, fmt.Errorf("%w: the saved revision of a preview never changes", ErrInvalid)
	}
	return &Preview{
		OperationID: op.ID, TenantID: op.TenantID, SubjectDigest: op.Subject.SubjectDigest, BaseRevision: op.Subject.SourceRevision,
		State: r.State, SourceRevision: r.SourceRevision, CurrentRevision: r.CurrentRevision, SourceDigest: r.SourceDigest, Module: r.Module,
		Styles: r.Styles, BuildProfileID: r.BuildProfileID, HostProfileID: r.HostProfileID, FailureCode: r.FailureCode, Revision: rev + 1, UpdatedAt: now,
	}, false, nil
}
