package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

func toRelease(m sqlc.Release) *domain.Release {
	p := &domain.Release{
		OperationID: m.OperationID, TenantID: m.TenantID, Lineage: domain.Digest(m.Lineage), SourceRevision: m.SourceRevision,
		State: domain.ReleaseState(m.State), ReleaseID: deref(m.ReleaseID), ReviewEffectID: deref(m.ReviewEffectID),
		ApprovalDeadline: fromTsPtr(m.ApprovalDeadline), CatalogRevision: deref(m.CatalogRevision), FailureCode: deref(m.FailureCode),
		Revision: uint64(m.Revision), UpdatedAt: m.UpdatedAt.Time.UTC(),
	}
	if len(m.Subject) > 0 && string(m.Subject) != "null" {
		var s domain.ReleaseSubject
		if json.Unmarshal(m.Subject, &s) == nil {
			p.Subject = &s
		}
	}
	if len(m.Approval) > 0 && string(m.Approval) != "null" {
		var a domain.Approval
		if json.Unmarshal(m.Approval, &a) == nil {
			if a.DecidedAt != nil {
				t := a.DecidedAt.UTC()
				a.DecidedAt = &t
			}
			p.Approval = &a
		}
	}
	_ = json.Unmarshal(m.Npm, &p.Npm)
	_ = json.Unmarshal(m.Browser, &p.Browser)
	_ = json.Unmarshal(m.Activation, &p.Activation)
	return p
}

type releaseColumns struct {
	subject, approval, npm, browser, activation []byte
	subjectDigest                               *string
	deadline                                    pgtype.Timestamptz
}

func releaseJSON(p *domain.Release) releaseColumns {
	var c releaseColumns
	if p.Subject != nil {
		c.subject, _ = json.Marshal(p.Subject)
		c.subjectDigest = strPtr(string(p.Subject.SubjectDigest))
	}
	if p.Approval != nil {
		c.approval, _ = json.Marshal(p.Approval)
	}
	c.npm, _ = json.Marshal(p.Npm)
	c.browser, _ = json.Marshal(p.Browser)
	c.activation, _ = json.Marshal(p.Activation)
	if p.ApprovalDeadline != nil {
		c.deadline = ts(p.ApprovalDeadline.Truncate(time.Microsecond))
	}
	return c
}

func (r *repo) GetRelease(ctx context.Context, operationID string) (*domain.Release, error) {
	m, err := r.q.GetRelease(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toRelease(m), nil
}

func (r *repo) LockRelease(ctx context.Context, operationID string) (*domain.Release, error) {
	m, err := r.q.LockRelease(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toRelease(m), nil
}

func (r *repo) InsertRelease(ctx context.Context, p *domain.Release) error {
	c := releaseJSON(p)
	return r.q.InsertRelease(ctx, sqlc.InsertReleaseParams{
		OperationID: p.OperationID, TenantID: p.TenantID, Lineage: string(p.Lineage), SourceRevision: p.SourceRevision, State: string(p.State),
		Subject: c.subject, SubjectDigest: c.subjectDigest, ReleaseID: strPtr(p.ReleaseID), ReviewEffectID: strPtr(p.ReviewEffectID), Approval: c.approval,
		ApprovalDeadline: c.deadline, Npm: c.npm, Browser: c.browser, Activation: c.activation, CatalogRevision: strPtr(p.CatalogRevision),
		FailureCode: strPtr(p.FailureCode), Revision: int64(p.Revision), UpdatedAt: ts(p.UpdatedAt.Truncate(time.Microsecond)),
	})
}

func (r *repo) UpdateRelease(ctx context.Context, p *domain.Release, expected uint64) (bool, error) {
	c := releaseJSON(p)
	n, err := r.q.UpdateRelease(ctx, sqlc.UpdateReleaseParams{
		OperationID: p.OperationID, State: string(p.State), Subject: c.subject, SubjectDigest: c.subjectDigest, ReleaseID: strPtr(p.ReleaseID),
		ReviewEffectID: strPtr(p.ReviewEffectID), Approval: c.approval, ApprovalDeadline: c.deadline, Npm: c.npm, Browser: c.browser,
		Activation: c.activation, CatalogRevision: strPtr(p.CatalogRevision), FailureCode: strPtr(p.FailureCode), Revision: int64(p.Revision),
		UpdatedAt: ts(p.UpdatedAt.Truncate(time.Microsecond)), ExpectedRevision: int64(expected),
	})
	return n == 1, err
}
