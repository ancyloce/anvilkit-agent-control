package postgres

import (
	"context"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// ---- artifact transfers ----

func toTransfer(m sqlc.ArtifactTransfer) *domain.Transfer {
	t := &domain.Transfer{
		ID: m.TransferID, TenantID: m.TenantID, ActorID: m.ActorID, OperationID: deref(m.OperationID), AttemptID: deref(m.AttemptID), Class: domain.ArtifactClass(m.Class),
		MediaType: m.MediaType, ExpectedDigest: domain.Digest(m.ExpectedDigest), ExpectedSize: m.ExpectedSize, Handle: m.Handle, State: domain.TransferState(m.State),
		ObjectKey: m.ObjectKey, ObjectVersion: deref(m.ObjectVersion), ActualDigest: domain.Digest(deref(m.ActualDigest)), ReasonCode: deref(m.ReasonCode),
		CommandID: m.CommandID, RequestDigest: domain.Digest(m.RequestDigest), FinalizeCommandID: deref(m.FinalizeCommandID), FinalizeRequestDigest: domain.Digest(deref(m.FinalizeRequestDigest)),
		ExecutionEpoch: uint64(m.ExecutionEpoch), RecoveryEpoch: uint64(m.RecoveryEpoch), Deadline: fromTs(m.Deadline), CreatedAt: fromTs(m.CreatedAt), UpdatedAt: fromTs(m.UpdatedAt), FinalizedAt: fromTsPtr(m.FinalizedAt),
	}
	if m.ActualSize != nil {
		t.ActualSize = *m.ActualSize
	}
	return t
}

func (r *repo) GetTransferByCommand(ctx context.Context, tenantID, commandID string) (*domain.Transfer, error) {
	m, err := r.q.GetTransferByCommand(ctx, sqlc.GetTransferByCommandParams{TenantID: tenantID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toTransfer(m), nil
}

func (r *repo) GetTransfer(ctx context.Context, transferID string) (*domain.Transfer, error) {
	m, err := r.q.GetTransfer(ctx, transferID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toTransfer(m), nil
}

func (r *repo) GetTransferByHandle(ctx context.Context, handle string) (*domain.Transfer, error) {
	m, err := r.q.GetTransferByHandle(ctx, handle)
	if err != nil {
		return nil, mapErr(err)
	}
	return toTransfer(m), nil
}

func (r *repo) LockTransfer(ctx context.Context, transferID string) (*domain.Transfer, error) {
	m, err := r.q.LockTransfer(ctx, transferID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toTransfer(m), nil
}

func (r *repo) InsertTransfer(ctx context.Context, t *domain.Transfer) error {
	var actualSize *int64
	if t.State == domain.TransferFinalized {
		size := t.ActualSize
		actualSize = &size
	}
	return r.q.InsertTransfer(ctx, sqlc.InsertTransferParams{
		TransferID: t.ID, TenantID: t.TenantID, OperationID: strPtr(t.OperationID), AttemptID: strPtr(t.AttemptID), Class: string(t.Class), MediaType: t.MediaType,
		ExpectedDigest: string(t.ExpectedDigest), ExpectedSize: t.ExpectedSize, Handle: t.Handle, State: string(t.State), ObjectVersion: strPtr(t.ObjectVersion),
		ReasonCode: strPtr(t.ReasonCode), CommandID: t.CommandID, RequestDigest: string(t.RequestDigest), Deadline: ts(t.Deadline), CreatedAt: ts(t.CreatedAt), FinalizedAt: tsPtr(t.FinalizedAt),
		ActorID: t.ActorID, ExecutionEpoch: int64(t.ExecutionEpoch), RecoveryEpoch: int64(t.RecoveryEpoch), ObjectKey: t.ObjectKey, ActualSize: actualSize,
		ActualDigest: strPtr(string(t.ActualDigest)), FinalizeCommandID: strPtr(t.FinalizeCommandID), FinalizeRequestDigest: strPtr(string(t.FinalizeRequestDigest)), UpdatedAt: ts(t.UpdatedAt),
	})
}

func (r *repo) UpdateTransfer(ctx context.Context, t *domain.Transfer) error {
	var actualSize *int64
	if t.State == domain.TransferFinalized {
		size := t.ActualSize
		actualSize = &size
	}
	return r.q.UpdateTransfer(ctx, sqlc.UpdateTransferParams{
		TransferID: t.ID, State: string(t.State), ObjectVersion: strPtr(t.ObjectVersion), ReasonCode: strPtr(t.ReasonCode), FinalizedAt: tsPtr(t.FinalizedAt),
		ActualSize: actualSize, ActualDigest: strPtr(string(t.ActualDigest)), FinalizeCommandID: strPtr(t.FinalizeCommandID), FinalizeRequestDigest: strPtr(string(t.FinalizeRequestDigest)), UpdatedAt: ts(t.UpdatedAt),
	})
}

// ---- stage artifacts ----

func (r *repo) InsertStageArtifact(ctx context.Context, a *domain.StageArtifact) error {
	return r.q.InsertStageArtifact(ctx, sqlc.InsertStageArtifactParams{
		StageID: a.StageID, TransferID: a.TransferID, Handle: a.Handle, Class: string(a.Class), Digest: string(a.Digest), SizeBytes: a.SizeBytes, ObjectVersion: a.ObjectVersion,
	})
}

func (r *repo) ListStageArtifacts(ctx context.Context, stageID string) ([]domain.StageArtifact, error) {
	rows, err := r.q.ListStageArtifacts(ctx, stageID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]domain.StageArtifact, 0, len(rows))
	for _, m := range rows {
		out = append(out, domain.StageArtifact{StageID: m.StageID, TransferID: m.TransferID, Handle: m.Handle, Class: domain.ArtifactClass(m.Class), Digest: domain.Digest(m.Digest), SizeBytes: m.SizeBytes, ObjectVersion: m.ObjectVersion})
	}
	return out, nil
}
