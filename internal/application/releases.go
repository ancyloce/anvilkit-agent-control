package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Releases keeps the committed projection of release operations (P21):
// the Workflow records each transition under the expected revision (the
// operation row locked first, then the release), Control checks every
// binding (subject digest and certification, approval digest, receipts,
// monotonic target outcomes) and the API reads it. Control records; it
// never decides the next step.
type Releases struct {
	store Store
	clock domain.Clock
}

func NewReleases(store Store, clock domain.Clock) *Releases {
	return &Releases{store: store, clock: clock}
}

// micro normalizes a recorded instant to the stored precision, so the
// same transition repeated compares equal to what was stored.
func micro(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC().Truncate(time.Microsecond)
	return &u
}

// Record applies one transition of the command's tenant (an operation of
// another tenant is not found); the same transition repeated answers the
// recorded projection (existing). The effect rows the record names are
// read in the same transaction: an approval and every succeeded target
// must be backed by them (B-38).
func (s *Releases) Record(ctx context.Context, cmd domain.CommandIdentity, operationID string, rec domain.ReleaseRecord) (p *domain.Release, existing bool, err error) {
	rec.ApprovalDeadline = micro(rec.ApprovalDeadline)
	if rec.Approval != nil {
		a := *rec.Approval
		a.DecidedAt = micro(a.DecidedAt)
		rec.Approval = &a
	}
	err = s.store.Tx(ctx, func(r Repo) error {
		op, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if op.TenantID != cmd.TenantID {
			return domain.ErrNotFound
		}
		cur, err := r.LockRelease(ctx, operationID)
		if errors.Is(err, domain.ErrNotFound) {
			cur, err = nil, nil
		}
		if err != nil {
			return err
		}
		var cert *domain.ReleaseCertification
		if rec.Subject != nil && (cur == nil || cur.Subject == nil) {
			if cert, err = certificationOf(ctx, r, operationID); err != nil {
				return err
			}
			// P0.8: the subject names the identity allocated to the lineage,
			// whatever the certified source declared for itself.
			if cert != nil {
				switch l, err := r.GetLineageIdentity(ctx, op.TenantID, op.Subject.SubjectDigest); {
				case err == nil:
					cert.Allocated = &l.Identity
				case !errors.Is(err, domain.ErrNotFound):
					return err
				}
			}
		}
		effects, err := releaseEffects(ctx, r, rec.ReviewEffectID, rec.Npm.EffectID, rec.Browser.EffectID, rec.Activation.EffectID)
		if err != nil {
			return err
		}
		next, same, err := domain.ApplyRelease(op, cur, rec, cert, effects, s.clock.Now())
		if err != nil {
			return err
		}
		p, existing = next, same
		if same {
			return nil
		}
		if cur == nil {
			return r.InsertRelease(ctx, next)
		}
		ok, err := r.UpdateRelease(ctx, next, cur.Revision)
		if err == nil && !ok {
			err = fmt.Errorf("%w: release of %s changed concurrently", domain.ErrRevisionConflict, operationID)
		}
		return err
	})
	if errors.Is(err, ErrDuplicateKey) {
		return nil, false, fmt.Errorf("%w: release of %s was recorded concurrently", domain.ErrRevisionConflict, operationID)
	}
	return p, existing, err
}

// releaseEffects reads the ledger rows of the named effects (empty ids and
// ids without a row are left out); effects rank after the release, and a
// read takes no lock.
func releaseEffects(ctx context.Context, r Repo, ids ...string) (domain.ReleaseEffects, error) {
	out := domain.ReleaseEffects{}
	for _, id := range ids {
		if id == "" {
			continue
		}
		e, err := r.GetEffect(ctx, id)
		if errors.Is(err, domain.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[id] = e
	}
	return out, nil
}

// certificationOf reads the operation's latest accepted certified stage.
func certificationOf(ctx context.Context, r Repo, operationID string) (*domain.ReleaseCertification, error) {
	stages, err := r.ListStagesByOperation(ctx, operationID)
	if err != nil {
		return nil, err
	}
	for i := len(stages) - 1; i >= 0; i-- {
		if stages[i].Verdict != domain.VerdictCertified {
			continue
		}
		artifacts, err := r.ListStageArtifacts(ctx, stages[i].ID)
		if err != nil {
			return nil, err
		}
		return domain.CertificationOf(artifacts), nil
	}
	return nil, nil
}

// Get answers the release of an operation of the tenant.
func (s *Releases) Get(ctx context.Context, tenantID, operationID string) (*domain.Release, error) {
	var p *domain.Release
	err := s.store.Read(ctx, func(r Repo) error {
		var err error
		p, err = r.GetRelease(ctx, operationID)
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
