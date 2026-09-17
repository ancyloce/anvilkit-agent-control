package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// ---- question sets, answers and briefs (P13) ----

func toQuestionSet(m sqlc.QuestionSet) *domain.QuestionSet {
	qs := &domain.QuestionSet{
		ID: m.QuestionSetID, OperationID: m.OperationID, TenantID: m.TenantID, Revision: uint64(m.Revision), Round: uint64(m.Round),
		State: domain.QuestionSetState(m.State), CommandID: m.CommandID, RequestDigest: domain.Digest(m.RequestDigest),
		AskedAt: fromTs(m.AskedAt), ExpiresAt: fromTs(m.ExpiresAt),
	}
	_ = json.Unmarshal(m.Questions, &qs.Questions)
	return qs
}

func (r *repo) InsertQuestionSet(ctx context.Context, qs *domain.QuestionSet) error {
	questions, err := json.Marshal(qs.Questions)
	if err != nil {
		return err
	}
	return r.q.InsertQuestionSet(ctx, sqlc.InsertQuestionSetParams{
		QuestionSetID: qs.ID, OperationID: qs.OperationID, TenantID: qs.TenantID, Revision: int64(qs.Revision), Round: int64(qs.Round),
		Questions: questions, State: string(qs.State), CommandID: qs.CommandID, RequestDigest: string(qs.RequestDigest),
		AskedAt: ts(qs.AskedAt), ExpiresAt: ts(qs.ExpiresAt),
	})
}

func (r *repo) GetQuestionSet(ctx context.Context, id string) (*domain.QuestionSet, error) {
	m, err := r.q.GetQuestionSet(ctx, id)
	if err != nil {
		return nil, mapErr(err)
	}
	return toQuestionSet(m), nil
}

func (r *repo) LockQuestionSet(ctx context.Context, id string) (*domain.QuestionSet, error) {
	m, err := r.q.LockQuestionSet(ctx, id)
	if err != nil {
		return nil, mapErr(err)
	}
	return toQuestionSet(m), nil
}

func (r *repo) GetQuestionSetByCommand(ctx context.Context, operationID, commandID string) (*domain.QuestionSet, error) {
	m, err := r.q.GetQuestionSetByCommand(ctx, sqlc.GetQuestionSetByCommandParams{OperationID: operationID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toQuestionSet(m), nil
}

func (r *repo) GetOpenQuestionSet(ctx context.Context, operationID string) (*domain.QuestionSet, error) {
	m, err := r.q.GetOpenQuestionSet(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toQuestionSet(m), nil
}

func (r *repo) ListQuestionSets(ctx context.Context, operationID string) ([]*domain.QuestionSet, error) {
	rows, err := r.q.ListQuestionSets(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.QuestionSet, 0, len(rows))
	for _, m := range rows {
		out = append(out, toQuestionSet(m))
	}
	return out, nil
}

func (r *repo) UpdateQuestionSetState(ctx context.Context, id string, state domain.QuestionSetState) error {
	return r.q.UpdateQuestionSetState(ctx, sqlc.UpdateQuestionSetStateParams{QuestionSetID: id, State: string(state)})
}

func toAnswer(m sqlc.Answer) *domain.Answer {
	return &domain.Answer{
		ID: m.AnswerID, OperationID: m.OperationID, TenantID: m.TenantID, ActorID: m.ActorID, QuestionSetID: m.QuestionSetID,
		QuestionSetRevision: uint64(m.QuestionSetRevision), Answer: domain.ArtifactBinding{TransferID: m.TransferID, Digest: domain.Digest(m.Digest)},
		Handle: m.Handle, UpdateID: m.UpdateID, CommandID: m.CommandID, RequestDigest: domain.Digest(m.RequestDigest),
		Relay: domain.AnswerRelayState(m.RelayState), AcceptedAt: fromTs(m.AcceptedAt), RelayedAt: fromTsPtr(m.RelayedAt),
	}
}

func (r *repo) InsertAnswer(ctx context.Context, a *domain.Answer) error {
	return r.q.InsertAnswer(ctx, sqlc.InsertAnswerParams{
		AnswerID: a.ID, OperationID: a.OperationID, TenantID: a.TenantID, ActorID: a.ActorID, QuestionSetID: a.QuestionSetID,
		QuestionSetRevision: int64(a.QuestionSetRevision), TransferID: a.Answer.TransferID, Digest: string(a.Answer.Digest), Handle: a.Handle,
		UpdateID: a.UpdateID, CommandID: a.CommandID, RequestDigest: string(a.RequestDigest), RelayState: string(a.Relay),
		AcceptedAt: ts(a.AcceptedAt), RelayedAt: tsPtr(a.RelayedAt),
	})
}

func (r *repo) GetAnswerByCommand(ctx context.Context, tenantID, commandID string) (*domain.Answer, error) {
	m, err := r.q.GetAnswerByCommand(ctx, sqlc.GetAnswerByCommandParams{TenantID: tenantID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toAnswer(m), nil
}

func (r *repo) GetAnswer(ctx context.Context, answerID string) (*domain.Answer, error) {
	m, err := r.q.GetAnswer(ctx, answerID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toAnswer(m), nil
}

func (r *repo) LockAnswer(ctx context.Context, answerID string) (*domain.Answer, error) {
	m, err := r.q.LockAnswer(ctx, answerID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toAnswer(m), nil
}

func (r *repo) GetAnswerByQuestionSet(ctx context.Context, questionSetID string, revision uint64) (*domain.Answer, error) {
	m, err := r.q.GetAnswerByQuestionSet(ctx, sqlc.GetAnswerByQuestionSetParams{QuestionSetID: questionSetID, QuestionSetRevision: int64(revision)})
	if err != nil {
		return nil, mapErr(err)
	}
	return toAnswer(m), nil
}

func (r *repo) ListAnswers(ctx context.Context, operationID string) ([]*domain.Answer, error) {
	rows, err := r.q.ListAnswers(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Answer, 0, len(rows))
	for _, m := range rows {
		out = append(out, toAnswer(m))
	}
	return out, nil
}

func (r *repo) ListAnswerRelayPending(ctx context.Context, limit int) ([]*domain.Answer, error) {
	rows, err := r.q.ListAnswerRelayPending(ctx, int32(limit))
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Answer, 0, len(rows))
	for _, m := range rows {
		out = append(out, toAnswer(m))
	}
	return out, nil
}

func (r *repo) UpdateAnswerRelay(ctx context.Context, answerID string, state domain.AnswerRelayState, at *time.Time) error {
	return r.q.UpdateAnswerRelay(ctx, sqlc.UpdateAnswerRelayParams{AnswerID: answerID, RelayState: string(state), RelayedAt: tsPtr(at)})
}

func toBrief(m sqlc.Brief) *domain.Brief {
	b := &domain.Brief{
		ID: m.BriefID, OperationID: m.OperationID, TenantID: m.TenantID, Revision: uint64(m.Revision),
		Brief: domain.ArtifactBinding{TransferID: m.TransferID, Digest: domain.Digest(m.Digest)}, Handle: m.Handle,
		RequirementsDigest: domain.Digest(m.RequirementsDigest), State: domain.BriefState(m.State), CommandID: m.CommandID,
		RequestDigest: domain.Digest(m.RequestDigest), FrozenAt: fromTs(m.FrozenAt),
	}
	_ = json.Unmarshal(m.SourceRevisions, &b.SourceRevisions)
	_ = json.Unmarshal(m.BrandDigests, &b.BrandDigests)
	_ = json.Unmarshal(m.AssetDigests, &b.AssetDigests)
	return b
}

func (r *repo) InsertBrief(ctx context.Context, b *domain.Brief) error {
	return r.q.InsertBrief(ctx, sqlc.InsertBriefParams{
		BriefID: b.ID, OperationID: b.OperationID, TenantID: b.TenantID, Revision: int64(b.Revision), TransferID: b.Brief.TransferID,
		Digest: string(b.Brief.Digest), Handle: b.Handle, RequirementsDigest: string(b.RequirementsDigest),
		SourceRevisions: jsonOrEmptyList(b.SourceRevisions), BrandDigests: jsonOrEmptyList(b.BrandDigests), AssetDigests: jsonOrEmptyList(b.AssetDigests),
		State: string(b.State), CommandID: b.CommandID, RequestDigest: string(b.RequestDigest), FrozenAt: ts(b.FrozenAt),
	})
}

func (r *repo) GetBrief(ctx context.Context, briefID string) (*domain.Brief, error) {
	m, err := r.q.GetBrief(ctx, briefID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toBrief(m), nil
}

func (r *repo) GetBriefByCommand(ctx context.Context, operationID, commandID string) (*domain.Brief, error) {
	m, err := r.q.GetBriefByCommand(ctx, sqlc.GetBriefByCommandParams{OperationID: operationID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toBrief(m), nil
}

func (r *repo) GetCurrentBrief(ctx context.Context, operationID string) (*domain.Brief, error) {
	m, err := r.q.GetCurrentBrief(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toBrief(m), nil
}

func (r *repo) CountBriefs(ctx context.Context, operationID string) (uint64, error) {
	n, err := r.q.CountBriefs(ctx, operationID)
	return uint64(n), mapErr(err)
}

func (r *repo) SupersedeBriefs(ctx context.Context, operationID string) error {
	return mapErr(r.q.SupersedeBriefs(ctx, operationID))
}

// ---- execution permits, funding and operation-scoped stage reads (P13) ----

func toPermit(m sqlc.Permit) *domain.Permit {
	return &domain.Permit{
		ID: m.PermitID, PoolID: m.PoolID, OwnerKind: m.OwnerKind, OwnerID: m.OwnerID, FenceEpoch: uint64(m.FenceEpoch),
		State: domain.PermitState(m.State), GrantedAt: fromTs(m.GrantedAt), ReleasedAt: fromTsPtr(m.ReleasedAt), ReleaseEvidence: deref(m.ReleaseEvidence),
	}
}

func (r *repo) LockResourcePool(ctx context.Context, poolID string) (*domain.ResourcePool, error) {
	m, err := r.q.LockResourcePool(ctx, poolID)
	if err != nil {
		return nil, mapErr(err)
	}
	return &domain.ResourcePool{ID: m.PoolID, Class: m.Class, Capacity: m.Capacity, ReservedControl: m.ReservedControl}, nil
}

func (r *repo) UpsertResourcePool(ctx context.Context, p *domain.ResourcePool) error {
	return mapErr(r.q.UpsertResourcePool(ctx, sqlc.UpsertResourcePoolParams{PoolID: p.ID, Class: p.Class, Capacity: p.Capacity, ReservedControl: p.ReservedControl}))
}

func (r *repo) CountActivePermits(ctx context.Context, poolID string) (int64, error) {
	n, err := r.q.CountActivePermits(ctx, poolID)
	return n, mapErr(err)
}

func (r *repo) GetActivePermit(ctx context.Context, poolID, ownerKind, ownerID string) (*domain.Permit, error) {
	m, err := r.q.GetActivePermit(ctx, sqlc.GetActivePermitParams{PoolID: poolID, OwnerKind: ownerKind, OwnerID: ownerID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toPermit(m), nil
}

func (r *repo) GetPermitByOwner(ctx context.Context, ownerKind, ownerID string) (*domain.Permit, error) {
	m, err := r.q.GetPermitByOwner(ctx, sqlc.GetPermitByOwnerParams{OwnerKind: ownerKind, OwnerID: ownerID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toPermit(m), nil
}

func (r *repo) InsertPermit(ctx context.Context, p *domain.Permit) error {
	return mapErr(r.q.InsertPermit(ctx, sqlc.InsertPermitParams{
		PermitID: p.ID, PoolID: p.PoolID, OwnerKind: p.OwnerKind, OwnerID: p.OwnerID, FenceEpoch: int64(p.FenceEpoch), State: string(p.State),
		GrantedAt: ts(p.GrantedAt), ReleasedAt: tsPtr(p.ReleasedAt), ReleaseEvidence: strPtr(p.ReleaseEvidence),
	}))
}

func (r *repo) ReleasePermits(ctx context.Context, ownerKind, ownerID string, at time.Time, evidence string) error {
	return mapErr(r.q.ReleasePermits(ctx, sqlc.ReleasePermitsParams{OwnerKind: ownerKind, OwnerID: ownerID, ReleasedAt: ts(at), ReleaseEvidence: strPtr(evidence)}))
}

func (r *repo) GetFunding(ctx context.Context, operationID string) (*domain.Funding, error) {
	m, err := r.q.GetFunding(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	return &domain.Funding{OperationID: m.OperationID, CommandID: m.CommandID, RequestDigest: domain.Digest(m.RequestDigest), Amount: domain.Money{Currency: m.Currency, Amount: m.Amount}, FundedAt: fromTs(m.FundedAt)}, nil
}

func (r *repo) InsertFunding(ctx context.Context, f *domain.Funding) error {
	return mapErr(r.q.InsertFunding(ctx, sqlc.InsertFundingParams{OperationID: f.OperationID, CommandID: f.CommandID, RequestDigest: string(f.RequestDigest), Currency: f.Amount.Currency, Amount: f.Amount.Amount, FundedAt: ts(f.FundedAt)}))
}

func (r *repo) ListStagesByOperation(ctx context.Context, operationID string) ([]*domain.Stage, error) {
	rows, err := r.q.ListStagesByOperation(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Stage, 0, len(rows))
	for _, m := range rows {
		out = append(out, toStage(m))
	}
	return out, nil
}

func (r *repo) GetStageArtifactByTransfer(ctx context.Context, transferID, operationID string) (*domain.StageArtifact, error) {
	m, err := r.q.GetStageArtifactByTransfer(ctx, sqlc.GetStageArtifactByTransferParams{TransferID: transferID, OperationID: operationID})
	if err != nil {
		return nil, mapErr(err)
	}
	return &domain.StageArtifact{StageID: m.StageID, TransferID: m.TransferID, Handle: m.Handle, Class: domain.ArtifactClass(m.Class), Digest: domain.Digest(m.Digest), SizeBytes: m.SizeBytes, ObjectVersion: m.ObjectVersion}, nil
}
