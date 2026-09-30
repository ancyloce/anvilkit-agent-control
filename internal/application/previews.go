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

// Record applies one transition; the same transition repeated answers the
// recorded projection (existing).
func (s *Previews) Record(ctx context.Context, operationID string, rec domain.PreviewRecord) (p *domain.Preview, existing bool, err error) {
	err = s.store.Tx(ctx, func(r Repo) error {
		op, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
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
		out.Lineage = op.Subject.SubjectDigest
		switch op.Kind {
		case domain.KindPreviewBuild:
			t, err := r.GetTransferByHandle(ctx, op.Subject.SourceHandle)
			if err != nil {
				return err
			}
			out.Source = domain.StageArtifact{TransferID: t.ID, Handle: t.Handle, Class: t.Class, Digest: t.ActualDigest, SizeBytes: t.ActualSize, ObjectVersion: t.ObjectVersion}
			out.Revision = op.Subject.SourceRevision
			if p, err := r.GetPreview(ctx, operationID); err == nil && p.SourceRevision != "" {
				out.Revision = p.SourceRevision
			} else if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return err
			}
		case domain.KindGeneration, domain.KindRefinement:
			a, err := r.GetCertifiedSourceArtifact(ctx, operationID)
			if err != nil {
				return err
			}
			out.Source = *a
			if op.CandidateEffectID != "" {
				// The candidate registration's receipt is the revision the
				// source authority registered (the Workflow observes it so).
				for _, src := range []string{"workflow", "workflow-query"} {
					if o, err := r.GetEffectObservation(ctx, op.CandidateEffectID, src, 1); err == nil {
						out.Revision = o.ReceiptDigest
						break
					}
				}
			}
		default:
			return fmt.Errorf("%w: operation %s has no source", domain.ErrNotFound, operationID)
		}
		return nil
	})
	return out, err
}
