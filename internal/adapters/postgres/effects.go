package postgres

import (
	"context"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// ---- effects ----

func toEffect(m sqlc.EffectIntent) *domain.Effect {
	e := &domain.Effect{
		ID: m.EffectID, TenantID: m.TenantID, OperationID: m.OperationID, AttemptID: deref(m.AttemptID), Kind: domain.EffectKind(m.Kind), Occurrence: uint64(m.Occurrence),
		CommandID: m.CommandID, Owner: m.Owner, CanonicalSubject: m.CanonicalSubject, RequestDigest: domain.Digest(m.RequestDigest), ExpectedRevision: deref(m.ExpectedRevision),
		ExecutionEpoch: uint64(m.ExecutionEpoch), RecoveryEpoch: uint64(m.RecoveryEpoch), LeaseID: deref(m.LeaseID), State: domain.EffectState(m.State),
		Outcome: domain.EffectOutcome(deref(m.Outcome)), DenialCode: deref(m.DenialCode), OutcomeRef: deref(m.OutcomeRef), QueryRef: deref(m.QueryRef),
		Inventory: domain.InventoryState(m.InventoryState), InventoryVersion: deref(m.InventoryVersion), Deadline: fromTs(m.Deadline),
		CreatedAt: fromTs(m.CreatedAt), UpdatedAt: fromTs(m.UpdatedAt), ObservedAt: fromTsPtr(m.ObservedAt),
	}
	if m.LeaseFence != nil {
		e.LeaseFence = uint64(*m.LeaseFence)
	}
	if m.LeaseExpiresAt.Valid {
		e.LeaseExpiresAt = fromTs(m.LeaseExpiresAt)
	}
	return e
}

func (r *repo) GetEffectByCommand(ctx context.Context, tenantID, commandID string) (*domain.Effect, error) {
	m, err := r.q.GetEffectByCommand(ctx, sqlc.GetEffectByCommandParams{TenantID: tenantID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toEffect(m), nil
}

func (r *repo) GetEffectByOccurrence(ctx context.Context, operationID string, kind domain.EffectKind, occurrence uint64) (*domain.Effect, error) {
	m, err := r.q.GetEffectByOccurrence(ctx, sqlc.GetEffectByOccurrenceParams{OperationID: operationID, Kind: string(kind), Occurrence: int64(occurrence)})
	if err != nil {
		return nil, mapErr(err)
	}
	return toEffect(m), nil
}

func (r *repo) GetEffect(ctx context.Context, effectID string) (*domain.Effect, error) {
	m, err := r.q.GetEffect(ctx, effectID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toEffect(m), nil
}

func (r *repo) LockEffect(ctx context.Context, effectID string) (*domain.Effect, error) {
	m, err := r.q.LockEffect(ctx, effectID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toEffect(m), nil
}

func (r *repo) InsertEffect(ctx context.Context, e *domain.Effect) error {
	p := sqlc.InsertEffectParams{
		EffectID: e.ID, OperationID: e.OperationID, Kind: string(e.Kind), Occurrence: int64(e.Occurrence), CanonicalSubject: e.CanonicalSubject,
		RequestDigest: string(e.RequestDigest), ExpectedRevision: strPtr(e.ExpectedRevision), State: string(e.State), OutcomeRef: strPtr(e.OutcomeRef), QueryRef: strPtr(e.QueryRef),
		InventoryState: string(e.Inventory), InventoryVersion: strPtr(e.InventoryVersion), CreatedAt: ts(e.CreatedAt), UpdatedAt: ts(e.UpdatedAt), TenantID: e.TenantID,
		AttemptID: strPtr(e.AttemptID), CommandID: e.CommandID, Owner: e.Owner, ExecutionEpoch: int64(e.ExecutionEpoch), RecoveryEpoch: int64(e.RecoveryEpoch),
		LeaseID: strPtr(e.LeaseID), LeaseFence: int64Ptr(e.LeaseFence, e.LeaseID != ""), Outcome: strPtr(string(e.Outcome)), DenialCode: strPtr(e.DenialCode),
		Deadline: ts(e.Deadline), ObservedAt: tsPtr(e.ObservedAt),
	}
	if !e.LeaseExpiresAt.IsZero() {
		p.LeaseExpiresAt = ts(e.LeaseExpiresAt)
	}
	return r.q.InsertEffect(ctx, p)
}

func (r *repo) UpdateEffect(ctx context.Context, e *domain.Effect) error {
	return r.q.UpdateEffect(ctx, sqlc.UpdateEffectParams{
		EffectID: e.ID, State: string(e.State), Outcome: strPtr(string(e.Outcome)), DenialCode: strPtr(e.DenialCode), OutcomeRef: strPtr(e.OutcomeRef), QueryRef: strPtr(e.QueryRef),
		InventoryState: string(e.Inventory), InventoryVersion: strPtr(e.InventoryVersion), RecoveryEpoch: int64(e.RecoveryEpoch), ObservedAt: tsPtr(e.ObservedAt), UpdatedAt: ts(e.UpdatedAt),
	})
}

func (r *repo) HasUnresolvedEffect(ctx context.Context, operationID string) (bool, error) {
	ok, err := r.q.HasUnresolvedEffect(ctx, operationID)
	return ok, mapErr(err)
}

func (r *repo) GetEffectObservation(ctx context.Context, effectID, source string, sequence uint64) (*domain.EffectObservation, error) {
	m, err := r.q.GetEffectObservation(ctx, sqlc.GetEffectObservationParams{EffectID: effectID, Source: source, Sequence: int64(sequence)})
	if err != nil {
		return nil, mapErr(err)
	}
	return &domain.EffectObservation{ID: m.ObservationID, EffectID: m.EffectID, Source: m.Source, Sequence: uint64(m.Sequence), Outcome: domain.EffectOutcome(m.Outcome), ReceiptDigest: m.ReceiptDigest, NativeReference: m.NativeReference, ObservedAt: fromTs(m.ObservedAt)}, nil
}

func (r *repo) InsertEffectObservation(ctx context.Context, o *domain.EffectObservation) error {
	return r.q.InsertEffectObservation(ctx, sqlc.InsertEffectObservationParams{
		ObservationID: o.ID, EffectID: o.EffectID, Source: o.Source, Sequence: int64(o.Sequence), Outcome: string(o.Outcome), ReceiptDigest: o.ReceiptDigest,
		NativeReference: o.NativeReference, ObservedAt: ts(o.ObservedAt),
	})
}

// ---- dispositions ----

func toDisposition(m sqlc.ObligationDisposition) *domain.Disposition {
	return &domain.Disposition{
		ID: m.DispositionID, Class: m.Class, ObligationID: m.ObligationID, TenantID: m.TenantID, RunID: deref(m.RunID), RecoveryEpoch: uint64(m.RecoveryEpoch),
		CommandID: m.CommandID, ActorID: m.ActorID, RequestDigest: domain.Digest(m.RequestDigest), Decision: domain.DispositionDecision(m.Decision),
		EvidenceRef: m.EvidenceRef, EvidenceDigest: domain.Digest(m.EvidenceDigest), Reason: m.Reason, DecidedAt: fromTs(m.DecidedAt),
	}
}

func (r *repo) GetDisposition(ctx context.Context, class, obligationID string) (*domain.Disposition, error) {
	m, err := r.q.GetDisposition(ctx, sqlc.GetDispositionParams{Class: class, ObligationID: obligationID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toDisposition(m), nil
}

func (r *repo) GetDispositionByCommand(ctx context.Context, tenantID, commandID string) (*domain.Disposition, error) {
	m, err := r.q.GetDispositionByCommand(ctx, sqlc.GetDispositionByCommandParams{TenantID: tenantID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toDisposition(m), nil
}

func (r *repo) InsertDisposition(ctx context.Context, d *domain.Disposition) error {
	return r.q.InsertDisposition(ctx, sqlc.InsertDispositionParams{
		DispositionID: d.ID, Class: d.Class, ObligationID: d.ObligationID, TenantID: d.TenantID, RunID: strPtr(d.RunID), RecoveryEpoch: int64(d.RecoveryEpoch),
		CommandID: d.CommandID, ActorID: d.ActorID, RequestDigest: string(d.RequestDigest), Decision: string(d.Decision), EvidenceRef: d.EvidenceRef,
		EvidenceDigest: string(d.EvidenceDigest), Reason: d.Reason, DecidedAt: ts(d.DecidedAt),
	})
}
