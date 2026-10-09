package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Previews keeps the committed projection of preview_build operations
// (P20): the Workflow records each transition under the expected revision
// (the operation row locked first, then the preview), the API reads it.
// Control records; it never decides the next step.
type Previews struct {
	store Store
	clock domain.Clock
}

func NewPreviews(store Store, clock domain.Clock) *Previews {
	return &Previews{store: store, clock: clock}
}

// Record applies one transition of the command's tenant (an operation of
// another tenant is not found); the same transition repeated answers the
// recorded projection (existing).
func (s *Previews) Record(ctx context.Context, cmd domain.CommandIdentity, operationID string, rec domain.PreviewRecord) (p *domain.Preview, existing bool, err error) {
	err = s.store.Tx(ctx, func(r Repo) error {
		op, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if op.TenantID != cmd.TenantID {
			return domain.ErrNotFound
		}
		if op.Kind == domain.KindPreviewBuild {
			// The recorded source digest is the edited artifact's actual one.
			t, err := r.GetTransferByHandle(ctx, op.Subject.SourceHandle)
			if err != nil {
				return err
			}
			if t.ActualDigest != rec.SourceDigest {
				return fmt.Errorf("%w: the preview's source digest %s is not the edited artifact's %s", domain.ErrInvalid, rec.SourceDigest, t.ActualDigest)
			}
		}
		cur, err := r.LockPreview(ctx, operationID)
		if errors.Is(err, domain.ErrNotFound) {
			cur, err = nil, nil
		}
		if err != nil {
			return err
		}
		next, same, err := domain.ApplyPreview(op, cur, rec, s.clock.Now())
		if err != nil {
			return err
		}
		p, existing = next, same
		if same {
			return nil
		}
		if cur == nil {
			return r.InsertPreview(ctx, next)
		}
		ok, err := r.UpdatePreview(ctx, next, cur.Revision)
		if err == nil && !ok {
			err = fmt.Errorf("%w: preview of %s changed concurrently", domain.ErrRevisionConflict, operationID)
		}
		return err
	})
	if errors.Is(err, ErrDuplicateKey) {
		return nil, false, fmt.Errorf("%w: preview of %s was recorded concurrently", domain.ErrRevisionConflict, operationID)
	}
	return p, existing, err
}

// Get answers the preview of an operation of the tenant.
func (s *Previews) Get(ctx context.Context, tenantID, operationID string) (*domain.Preview, error) {
	var p *domain.Preview
	err := s.store.Read(ctx, func(r Repo) error {
		var err error
		p, err = r.GetPreview(ctx, operationID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if p.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return p, nil
}

// OperationSource is the source a workbench edits and the revision these
// bytes are, when known.
type OperationSource struct {
	Source   domain.StageArtifact
	Lineage  domain.Digest
	Revision string
}

// Source answers the source of a generation (its latest certified source
// and the revision its candidate registration answered) or of a preview
// build (its edited source and the revision its save created, else its
// base), for an operation of the tenant.
func (s *Previews) Source(ctx context.Context, tenantID, operationID string) (OperationSource, error) {
	var out OperationSource
	err := s.store.Read(ctx, func(r Repo) error {
		op, err := r.GetOperationScoped(ctx, operationID, tenantID)
		if err != nil {
			return err
		}
		out, _, err = resolveSource(ctx, r, op)
		return err
	})
	return out, err
}

// resolveSource answers an operation's source and whether the revision is
// one the source authority named for these bytes (saved): a preview
// build's edited source with the revision its save created (else its
// base, not saved), or a generation's latest certified source with the
// revision its candidate registration answered.
func resolveSource(ctx context.Context, r Repo, op *domain.Operation) (out OperationSource, saved bool, err error) {
	out.Lineage = op.Subject.SubjectDigest
	switch op.Kind {
	case domain.KindPreviewBuild:
		t, err := r.GetTransferByHandle(ctx, op.Subject.SourceHandle)
		if err != nil {
			return out, false, err
		}
		out.Source = domain.StageArtifact{TransferID: t.ID, Handle: t.Handle, Class: t.Class, Digest: t.ActualDigest, SizeBytes: t.ActualSize, ObjectVersion: t.ObjectVersion}
		out.Revision = op.Subject.SourceRevision
		if p, err := r.GetPreview(ctx, op.ID); err == nil && p.SourceRevision != "" {
			out.Revision, saved = p.SourceRevision, true
		} else if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return out, false, err
		}
	case domain.KindGeneration, domain.KindRefinement:
		a, err := r.GetCertifiedSourceArtifact(ctx, op.ID)
		if err != nil {
			return out, false, err
		}
		out.Source = *a
		if op.CandidateEffectID != "" {
			// The candidate registration's receipt is the revision the
			// source authority registered (the Workflow observes it so).
			for _, src := range []string{"workflow", "workflow-query"} {
				if o, err := r.GetEffectObservation(ctx, op.CandidateEffectID, src, 1); err == nil {
					out.Revision, saved = o.ReceiptDigest, true
					break
				}
			}
		}
	default:
		return out, false, fmt.Errorf("%w: operation %s has no source", domain.ErrNotFound, op.ID)
	}
	return out, saved, nil
}
