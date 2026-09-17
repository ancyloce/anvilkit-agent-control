// Package postgres implements the application Store/Repo ports with pgx/v5
// and sqlc-generated queries (A05/A06). One pgx.Tx backs one application
// transaction; nothing here performs network, storage or Temporal I/O.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Tx(ctx context.Context, fn func(application.Repo) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return mapErr(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op
	if err := fn(&repo{q: sqlc.New(tx)}); err != nil {
		return mapErr(err)
	}
	return mapErr(tx.Commit(ctx))
}

func (s *Store) Read(ctx context.Context, fn func(application.Repo) error) error {
	return mapErr(fn(&repo{q: sqlc.New(s.pool)}))
}

// Ping is used by readiness.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

type repo struct{ q *sqlc.Queries }

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return fmt.Errorf("%w: %s", application.ErrDuplicateKey, pgErr.ConstraintName)
	}
	return err
}

func ts(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t.UTC(), Valid: true} }

func tsPtr(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return ts(*t)
}

func fromTs(t pgtype.Timestamptz) time.Time { return t.Time.UTC() }

func fromTsPtr(t pgtype.Timestamptz) *time.Time {
	if !t.Valid {
		return nil
	}
	v := t.Time.UTC()
	return &v
}

func int64Ptr(v uint64, present bool) *int64 {
	if !present {
		return nil
	}
	i := int64(v)
	return &i
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ---- operations ----

func toOperation(m sqlc.Operation) *domain.Operation {
	return &domain.Operation{
		ID: m.OperationID, TenantID: m.TenantID, ProjectID: m.ProjectID, ActorID: m.ActorID, CommandID: m.CommandID,
		Kind:           domain.OperationKind(m.Kind),
		Subject:        domain.Subject{ProfileID: m.ProfileID, SubjectDigest: domain.Digest(m.SubjectDigest), BriefID: deref(m.BriefID), SourceRevision: deref(m.SourceRevision)},
		SemanticDigest: domain.Digest(m.SemanticDigest), Lifecycle: domain.Lifecycle(m.Lifecycle), Phase: m.Phase,
		Control: domain.ControlState(m.ControlState), Cleanup: domain.CleanupState(m.CleanupState), Finance: domain.FinanceState(m.FinanceState),
		FailureCode: deref(m.FailureCode), Revision: domain.Revision(m.Revision), NextEventSeq: uint64(m.NextEventSeq),
		ExecutionEpoch: uint64(m.ExecutionEpoch), RecoveryEpoch: uint64(m.RecoveryEpoch), Deadline: fromTs(m.Deadline),
		Intake: domain.IntakeState(m.IntakeState), IntakeVersion: deref(m.IntakeVersion), Relay: domain.RelayState(m.RelayState),
		RelayRunID: deref(m.RelayRunID), CreatedAt: fromTs(m.CreatedAt), UpdatedAt: fromTs(m.UpdatedAt),
		ActiveDeadline:       fromTsPtr(m.ActiveDeadline),
		Lease:                domain.LeaseRecord{State: domain.LeaseState(m.LeaseState), LeaseID: deref(m.LeaseID), Fence: uint64(derefInt(m.LeaseFence)), ExpiresAt: fromTsPtr(m.LeaseExpiresAt), Occurrence: uint64(m.LeaseOccurrence)},
		DefinitionActivation: m.DefinitionActivation, CandidateEffectID: deref(m.CandidateEffectID),
		Preparation: toIntake(m),
	}
}

func derefInt(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// toIntake reads the preparation intake columns; a row without a prompt is
// no preparation.
func toIntake(m sqlc.Operation) *domain.PreparationIntake {
	if m.PromptTransferID == nil {
		return nil
	}
	in := &domain.PreparationIntake{Prompt: domain.ArtifactBinding{TransferID: *m.PromptTransferID, Digest: domain.Digest(deref(m.PromptDigest))}}
	_ = json.Unmarshal(m.BrandReferences, &in.BrandReferences)
	_ = json.Unmarshal(m.AssetReferences, &in.AssetReferences)
	return in
}

func jsonOrEmptyList(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil || string(b) == "null" {
		return []byte("[]")
	}
	return b
}

func (r *repo) InsertOperation(ctx context.Context, o *domain.Operation) error {
	return r.q.InsertOperation(ctx, sqlc.InsertOperationParams{
		OperationID: o.ID, TenantID: o.TenantID, ProjectID: o.ProjectID, ActorID: o.ActorID, CommandID: o.CommandID,
		Kind: string(o.Kind), ProfileID: o.Subject.ProfileID, SubjectDigest: string(o.Subject.SubjectDigest),
		BriefID: strPtr(o.Subject.BriefID), SourceRevision: strPtr(o.Subject.SourceRevision), SemanticDigest: string(o.SemanticDigest),
		Lifecycle: string(o.Lifecycle), Phase: o.Phase, ControlState: string(o.Control), CleanupState: string(o.Cleanup),
		FinanceState: string(o.Finance), FailureCode: strPtr(o.FailureCode), Revision: int64(o.Revision), NextEventSeq: int64(o.NextEventSeq),
		ExecutionEpoch: int64(o.ExecutionEpoch), RecoveryEpoch: int64(o.RecoveryEpoch), Deadline: ts(o.Deadline),
		IntakeState: string(o.Intake), IntakeVersion: strPtr(o.IntakeVersion), RelayState: string(o.Relay), RelayRunID: strPtr(o.RelayRunID),
		CreatedAt: ts(o.CreatedAt), UpdatedAt: ts(o.UpdatedAt),
		DefinitionActivation: o.DefinitionActivation, PromptTransferID: promptTransfer(o), PromptDigest: promptDigest(o),
		BrandReferences: jsonOrEmptyList(brandRefs(o)), AssetReferences: jsonOrEmptyList(assetRefs(o)),
	})
}

func promptTransfer(o *domain.Operation) *string {
	if o.Preparation == nil {
		return nil
	}
	return strPtr(o.Preparation.Prompt.TransferID)
}

func promptDigest(o *domain.Operation) *string {
	if o.Preparation == nil {
		return nil
	}
	return strPtr(string(o.Preparation.Prompt.Digest))
}

func brandRefs(o *domain.Operation) []domain.SourceReference {
	if o.Preparation == nil {
		return nil
	}
	return o.Preparation.BrandReferences
}

func assetRefs(o *domain.Operation) []domain.SourceReference {
	if o.Preparation == nil {
		return nil
	}
	return o.Preparation.AssetReferences
}

func (r *repo) GetOperationByCommand(ctx context.Context, tenantID, commandID string) (*domain.Operation, error) {
	m, err := r.q.GetOperationByCommand(ctx, sqlc.GetOperationByCommandParams{TenantID: tenantID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toOperation(m), nil
}

func (r *repo) GetOperationScoped(ctx context.Context, operationID, tenantID string) (*domain.Operation, error) {
	m, err := r.q.GetOperationScoped(ctx, sqlc.GetOperationScopedParams{OperationID: operationID, TenantID: tenantID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toOperation(m), nil
}

func (r *repo) LockOperation(ctx context.Context, operationID string) (*domain.Operation, error) {
	m, err := r.q.LockOperation(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toOperation(m), nil
}

func (r *repo) UpdateOperation(ctx context.Context, o *domain.Operation) error {
	return r.q.UpdateOperation(ctx, sqlc.UpdateOperationParams{
		OperationID: o.ID, Lifecycle: string(o.Lifecycle), Phase: o.Phase, ControlState: string(o.Control), CleanupState: string(o.Cleanup),
		FinanceState: string(o.Finance), FailureCode: strPtr(o.FailureCode), Revision: int64(o.Revision), NextEventSeq: int64(o.NextEventSeq),
		ExecutionEpoch: int64(o.ExecutionEpoch), RecoveryEpoch: int64(o.RecoveryEpoch), IntakeState: string(o.Intake),
		IntakeVersion: strPtr(o.IntakeVersion), RelayState: string(o.Relay), RelayRunID: strPtr(o.RelayRunID), UpdatedAt: ts(o.UpdatedAt),
		ActiveDeadline: tsPtr(o.ActiveDeadline), LeaseState: string(o.Lease.State), LeaseID: strPtr(o.Lease.LeaseID),
		LeaseFence: int64Ptr(o.Lease.Fence, o.Lease.State != domain.LeaseNone), LeaseExpiresAt: tsPtr(o.Lease.ExpiresAt), LeaseOccurrence: int64(o.Lease.Occurrence),
		DefinitionActivation: o.DefinitionActivation, BriefID: strPtr(o.Subject.BriefID), CandidateEffectID: strPtr(o.CandidateEffectID),
	})
}

func (r *repo) InsertEvent(ctx context.Context, ev domain.Event) error {
	payload, err := json.Marshal(ev.Payload)
	if err != nil {
		return err
	}
	return r.q.InsertOperationEvent(ctx, sqlc.InsertOperationEventParams{
		OperationID: ev.OperationID, EventSeq: int64(ev.EventSeq), TransitionID: ev.TransitionID, EventType: ev.EventType,
		Revision: int64(ev.Revision), Payload: payload, OccurredAt: ts(ev.OccurredAt),
	})
}

func (r *repo) ListEvents(ctx context.Context, operationID string, afterSeq uint64, limit int) ([]domain.Event, error) {
	rows, err := r.q.ListOperationEvents(ctx, sqlc.ListOperationEventsParams{OperationID: operationID, EventSeq: int64(afterSeq), Limit: int32(limit)})
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]domain.Event, 0, len(rows))
	for _, m := range rows {
		ev := domain.Event{OperationID: m.OperationID, EventSeq: uint64(m.EventSeq), TransitionID: m.TransitionID, EventType: m.EventType, Revision: domain.Revision(m.Revision), OccurredAt: fromTs(m.OccurredAt)}
		if err := json.Unmarshal(m.Payload, &ev.Payload); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}

func (r *repo) ListIntakePending(ctx context.Context, olderThan time.Time, limit int) ([]*domain.Operation, error) {
	rows, err := r.q.ListIntakePending(ctx, sqlc.ListIntakePendingParams{CreatedAt: ts(olderThan), Limit: int32(limit)})
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Operation, 0, len(rows))
	for _, m := range rows {
		out = append(out, toOperation(m))
	}
	return out, nil
}

func (r *repo) ListRelayPending(ctx context.Context, limit int) ([]*domain.Operation, error) {
	rows, err := r.q.ListRelayPending(ctx, int32(limit))
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Operation, 0, len(rows))
	for _, m := range rows {
		out = append(out, toOperation(m))
	}
	return out, nil
}

// ---- commands ----

func toCommand(m sqlc.OperationCommand) *domain.Command {
	return &domain.Command{
		TenantID: m.TenantID, CommandID: m.CommandID, OperationID: m.OperationID, ActorID: m.ActorID, Kind: domain.CommandKind(m.Kind),
		ExpectedRevision: domain.Revision(m.ExpectedRevision), RequestDigest: domain.Digest(m.RequestDigest),
		TargetDefinitionActivation: deref(m.TargetDefinitionActivation), Outcome: domain.CommandOutcome(m.Outcome), ReasonCode: deref(m.ReasonCode),
		OperationRevision: domain.Revision(m.OperationRevision), AcceptedAt: fromTs(m.AcceptedAt), SettledAt: fromTsPtr(m.SettledAt),
		Relay: domain.CommandRelayState(m.RelayState),
	}
}

func (r *repo) GetCommand(ctx context.Context, tenantID, commandID string) (*domain.Command, error) {
	m, err := r.q.GetCommand(ctx, sqlc.GetCommandParams{TenantID: tenantID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toCommand(m), nil
}

func (r *repo) InsertCommand(ctx context.Context, c *domain.Command) error {
	return r.q.InsertCommand(ctx, sqlc.InsertCommandParams{
		TenantID: c.TenantID, CommandID: c.CommandID, OperationID: c.OperationID, ActorID: c.ActorID, Kind: string(c.Kind),
		ExpectedRevision: int64(c.ExpectedRevision), RequestDigest: string(c.RequestDigest), TargetDefinitionActivation: strPtr(c.TargetDefinitionActivation),
		Outcome: string(c.Outcome), ReasonCode: strPtr(c.ReasonCode), OperationRevision: int64(c.OperationRevision), AcceptedAt: ts(c.AcceptedAt), SettledAt: tsPtr(c.SettledAt),
		RelayState: relayOrNone(c.Relay),
	})
}

func relayOrNone(s domain.CommandRelayState) string {
	if s == "" {
		return string(domain.CommandRelayNone)
	}
	return string(s)
}

func (r *repo) LockCommand(ctx context.Context, tenantID, commandID string) (*domain.Command, error) {
	m, err := r.q.LockCommand(ctx, sqlc.LockCommandParams{TenantID: tenantID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toCommand(m), nil
}

func (r *repo) UpdateCommand(ctx context.Context, c *domain.Command) error {
	return r.q.UpdateCommand(ctx, sqlc.UpdateCommandParams{
		TenantID: c.TenantID, CommandID: c.CommandID, Outcome: string(c.Outcome), ReasonCode: strPtr(c.ReasonCode),
		OperationRevision: int64(c.OperationRevision), SettledAt: tsPtr(c.SettledAt), RelayState: relayOrNone(c.Relay),
	})
}

func (r *repo) ListCommandRelayPending(ctx context.Context, limit int) ([]*domain.Command, error) {
	rows, err := r.q.ListCommandRelayPending(ctx, int32(limit))
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Command, 0, len(rows))
	for _, m := range rows {
		out = append(out, toCommand(m))
	}
	return out, nil
}

func (r *repo) ListOperationCommands(ctx context.Context, operationID string) ([]*domain.Command, error) {
	rows, err := r.q.ListOperationCommands(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Command, 0, len(rows))
	for _, m := range rows {
		out = append(out, toCommand(m))
	}
	return out, nil
}

func (r *repo) SettlePendingCommands(ctx context.Context, operationID string, kind domain.CommandKind, outcome domain.CommandOutcome, rev domain.Revision, now time.Time) error {
	return r.q.SettlePendingCommands(ctx, sqlc.SettlePendingCommandsParams{OperationID: operationID, Kind: string(kind), Outcome: string(outcome), OperationRevision: int64(rev), SettledAt: ts(now)})
}

// ---- attempts ----

func toAttempt(m sqlc.Attempt) *domain.Attempt {
	return &domain.Attempt{
		ID: m.AttemptID, OperationID: m.OperationID, TenantID: m.TenantID, StepID: m.StepID, VisitOrdinal: uint64(m.VisitOrdinal),
		AttemptOrdinal: uint64(m.AttemptOrdinal), ProfileID: m.ProfileID, ExecutionEpoch: uint64(m.ExecutionEpoch), CommandID: m.CommandID,
		RequestDigest: domain.Digest(m.RequestDigest), State: domain.AttemptState(m.State), Outcome: domain.AttemptOutcome(deref(m.Outcome)),
		Cleanup: domain.CleanupState(deref(m.CleanupState)), FailureCode: deref(m.FailureCode), AcceptedStageID: deref(m.AcceptedStageID),
		Deadline: fromTs(m.Deadline), CreatedAt: fromTs(m.CreatedAt), UpdatedAt: fromTs(m.UpdatedAt),
	}
}

func (r *repo) GetAttemptByCommand(ctx context.Context, operationID, commandID string) (*domain.Attempt, error) {
	m, err := r.q.GetAttemptByCommand(ctx, sqlc.GetAttemptByCommandParams{OperationID: operationID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toAttempt(m), nil
}

func (r *repo) GetAttempt(ctx context.Context, attemptID string) (*domain.Attempt, error) {
	m, err := r.q.GetAttempt(ctx, attemptID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toAttempt(m), nil
}

func (r *repo) LockAttempt(ctx context.Context, attemptID string) (*domain.Attempt, error) {
	m, err := r.q.LockAttempt(ctx, attemptID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toAttempt(m), nil
}

func (r *repo) CountAttempts(ctx context.Context, operationID, stepID string, visit uint64) (uint64, error) {
	n, err := r.q.CountAttempts(ctx, sqlc.CountAttemptsParams{OperationID: operationID, StepID: stepID, VisitOrdinal: int64(visit)})
	return uint64(n), mapErr(err)
}

func (r *repo) HasOpenAttempt(ctx context.Context, operationID string) (bool, error) {
	ok, err := r.q.HasOpenAttempt(ctx, operationID)
	return ok, mapErr(err)
}

func (r *repo) InsertAttempt(ctx context.Context, a *domain.Attempt) error {
	return r.q.InsertAttempt(ctx, sqlc.InsertAttemptParams{
		AttemptID: a.ID, OperationID: a.OperationID, TenantID: a.TenantID, StepID: a.StepID, VisitOrdinal: int64(a.VisitOrdinal),
		AttemptOrdinal: int64(a.AttemptOrdinal), ProfileID: a.ProfileID, ExecutionEpoch: int64(a.ExecutionEpoch), CommandID: a.CommandID,
		RequestDigest: string(a.RequestDigest), State: string(a.State), Outcome: strPtr(string(a.Outcome)), CleanupState: strPtr(string(a.Cleanup)),
		FailureCode: strPtr(a.FailureCode), AcceptedStageID: strPtr(a.AcceptedStageID), Deadline: ts(a.Deadline), CreatedAt: ts(a.CreatedAt), UpdatedAt: ts(a.UpdatedAt),
	})
}

func (r *repo) UpdateAttempt(ctx context.Context, a *domain.Attempt) error {
	return r.q.UpdateAttempt(ctx, sqlc.UpdateAttemptParams{
		AttemptID: a.ID, State: string(a.State), Outcome: strPtr(string(a.Outcome)), CleanupState: strPtr(string(a.Cleanup)),
		FailureCode: strPtr(a.FailureCode), AcceptedStageID: strPtr(a.AcceptedStageID), UpdatedAt: ts(a.UpdatedAt),
	})
}

// ---- launches ----

func toLaunch(m sqlc.Launch) *domain.Launch {
	return &domain.Launch{
		ID: m.LaunchID, AttemptID: m.AttemptID, OperationID: m.OperationID, LaunchKey: m.LaunchKey, Backend: m.Backend, ProfileID: m.ProfileID,
		ImageDigest: domain.Digest(m.ImageDigest), ExecutionEpoch: uint64(m.ExecutionEpoch), LaunchEpoch: uint64(m.LaunchEpoch), Deadline: fromTs(m.Deadline),
		CommandID: m.CommandID, RequestDigest: domain.Digest(m.RequestDigest), Inventory: domain.InventoryState(m.InventoryState),
		InventoryVersion: deref(m.InventoryVersion), CreatedAt: fromTs(m.CreatedAt), UpdatedAt: fromTs(m.UpdatedAt),
	}
}

func (r *repo) GetLaunchByCommand(ctx context.Context, attemptID, commandID string) (*domain.Launch, error) {
	m, err := r.q.GetLaunchByCommand(ctx, sqlc.GetLaunchByCommandParams{AttemptID: attemptID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toLaunch(m), nil
}

func (r *repo) GetLaunchByKey(ctx context.Context, backend, launchKey string) (*domain.Launch, error) {
	m, err := r.q.GetLaunchByKey(ctx, sqlc.GetLaunchByKeyParams{Backend: backend, LaunchKey: launchKey})
	if err != nil {
		return nil, mapErr(err)
	}
	return toLaunch(m), nil
}

func (r *repo) LockLaunch(ctx context.Context, launchID string) (*domain.Launch, error) {
	m, err := r.q.LockLaunch(ctx, launchID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toLaunch(m), nil
}

func (r *repo) InsertLaunch(ctx context.Context, l *domain.Launch) error {
	return r.q.InsertLaunch(ctx, sqlc.InsertLaunchParams{
		LaunchID: l.ID, AttemptID: l.AttemptID, OperationID: l.OperationID, LaunchKey: l.LaunchKey, Backend: l.Backend, ProfileID: l.ProfileID,
		ImageDigest: string(l.ImageDigest), ExecutionEpoch: int64(l.ExecutionEpoch), LaunchEpoch: int64(l.LaunchEpoch), Deadline: ts(l.Deadline),
		CommandID: l.CommandID, RequestDigest: string(l.RequestDigest), InventoryState: string(l.Inventory), InventoryVersion: strPtr(l.InventoryVersion),
		CreatedAt: ts(l.CreatedAt), UpdatedAt: ts(l.UpdatedAt),
	})
}

func (r *repo) UpdateLaunchInventory(ctx context.Context, l *domain.Launch) error {
	return r.q.UpdateLaunchInventory(ctx, sqlc.UpdateLaunchInventoryParams{LaunchID: l.ID, InventoryState: string(l.Inventory), InventoryVersion: strPtr(l.InventoryVersion), UpdatedAt: ts(l.UpdatedAt)})
}

// ---- instances ----

func toInstance(m sqlc.PhysicalInstance) *domain.Instance {
	return &domain.Instance{
		ID: m.InstanceID, AttemptID: m.AttemptID, LaunchID: m.LaunchID, LaunchKey: m.LaunchKey, Backend: m.Backend, JobUID: m.JobUid, PodUID: m.PodUid,
		ImageDigest: domain.Digest(m.ImageDigest), LaunchEpoch: uint64(m.LaunchEpoch), Phase: domain.InstancePhase(m.Phase), ExitCode: m.ExitCode,
		Current: m.IsCurrent, RegisteredAt: fromTs(m.RegisteredAt), ObservedAt: fromTsPtr(m.ObservedAt),
	}
}

func (r *repo) GetInstanceByPod(ctx context.Context, backend, podUID string) (*domain.Instance, error) {
	m, err := r.q.GetInstanceByPod(ctx, sqlc.GetInstanceByPodParams{Backend: backend, PodUid: podUID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toInstance(m), nil
}

func (r *repo) LockInstance(ctx context.Context, instanceID string) (*domain.Instance, error) {
	m, err := r.q.LockInstance(ctx, instanceID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toInstance(m), nil
}

func (r *repo) HasCurrentInstance(ctx context.Context, attemptID string) (bool, error) {
	ok, err := r.q.HasCurrentInstance(ctx, attemptID)
	return ok, mapErr(err)
}

func (r *repo) InsertInstance(ctx context.Context, i *domain.Instance) error {
	return r.q.InsertInstance(ctx, sqlc.InsertInstanceParams{
		InstanceID: i.ID, AttemptID: i.AttemptID, LaunchID: i.LaunchID, LaunchKey: i.LaunchKey, Backend: i.Backend, JobUid: i.JobUID, PodUid: i.PodUID,
		ImageDigest: string(i.ImageDigest), LaunchEpoch: int64(i.LaunchEpoch), Phase: string(i.Phase), ExitCode: i.ExitCode, IsCurrent: i.Current,
		RegisteredAt: ts(i.RegisteredAt), ObservedAt: tsPtr(i.ObservedAt),
	})
}

func (r *repo) UpdateInstanceObservation(ctx context.Context, i *domain.Instance) error {
	return r.q.UpdateInstanceObservation(ctx, sqlc.UpdateInstanceObservationParams{InstanceID: i.ID, Phase: string(i.Phase), ExitCode: i.ExitCode, ObservedAt: tsPtr(i.ObservedAt)})
}

// ---- stages ----

func toStage(m sqlc.StageManifest) *domain.Stage {
	return &domain.Stage{
		ID: m.StageID, AttemptID: m.AttemptID, InstanceID: m.InstanceID, OperationID: m.OperationID, PhaseOrdinal: uint64(m.PhaseOrdinal), ProfileID: m.ProfileID,
		Verdict: domain.Verdict(m.Verdict), FailureCode: deref(m.FailureCode), ResultDigest: domain.Digest(m.ResultDigest), ResultManifest: m.ResultManifestBytes,
		ObserverIdentity: m.ObserverIdentity, CommandID: m.CommandID, RequestDigest: domain.Digest(m.RequestDigest), AcceptedAt: fromTs(m.AcceptedAt),
		ExecutionEpoch: uint64(m.ExecutionEpoch), RecoveryEpoch: uint64(m.RecoveryEpoch),
	}
}

func (r *repo) GetStageByAttempt(ctx context.Context, attemptID string) (*domain.Stage, error) {
	m, err := r.q.GetStageByAttempt(ctx, attemptID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toStage(m), nil
}

func (r *repo) InsertStage(ctx context.Context, s *domain.Stage) error {
	return r.q.InsertStage(ctx, sqlc.InsertStageParams{
		StageID: s.ID, AttemptID: s.AttemptID, InstanceID: s.InstanceID, OperationID: s.OperationID, PhaseOrdinal: int64(s.PhaseOrdinal), ProfileID: s.ProfileID,
		Verdict: string(s.Verdict), FailureCode: strPtr(s.FailureCode), ResultDigest: string(s.ResultDigest), ResultManifest: s.ResultManifest, ResultManifestBytes: s.ResultManifest,
		ObserverIdentity: s.ObserverIdentity, CommandID: s.CommandID, RequestDigest: string(s.RequestDigest), AcceptedAt: ts(s.AcceptedAt),
		ExecutionEpoch: int64(s.ExecutionEpoch), RecoveryEpoch: int64(s.RecoveryEpoch),
	})
}

// ---- budgets ----

func toPool(m sqlc.BudgetPool) *domain.BudgetPool {
	return &domain.BudgetPool{
		ID: m.PoolID, Level: domain.BudgetLevel(m.Level), TenantID: deref(m.TenantID), ActorID: deref(m.ActorID), PeriodStart: fromTs(m.PeriodStart),
		PeriodEnd: fromTs(m.PeriodEnd), Currency: m.Currency, Cap: m.CapAmount, Allocated: m.Allocated, Revision: uint64(m.Revision),
	}
}

func (r *repo) ListCurrentPools(ctx context.Context, currency string, tenantID, actorID string, at time.Time) ([]*domain.BudgetPool, error) {
	rows, err := r.q.ListCurrentPools(ctx, sqlc.ListCurrentPoolsParams{Currency: currency, PeriodStart: ts(at), TenantID: strPtr(tenantID), ActorID: strPtr(actorID)})
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.BudgetPool, 0, len(rows))
	for _, m := range rows {
		out = append(out, toPool(m))
	}
	return out, nil
}

func (r *repo) LockPool(ctx context.Context, poolID string) (*domain.BudgetPool, error) {
	m, err := r.q.LockPool(ctx, poolID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toPool(m), nil
}

func (r *repo) UpdatePool(ctx context.Context, p *domain.BudgetPool) error {
	return r.q.UpdatePool(ctx, sqlc.UpdatePoolParams{PoolID: p.ID, Allocated: p.Allocated, Revision: int64(p.Revision)})
}

func toAllocation(m sqlc.Allocation) *domain.Allocation {
	return &domain.Allocation{
		ID: m.AllocationID, PoolID: m.PoolID, OperationID: m.OperationID, Currency: m.Currency, Amount: m.Amount, Reserved: m.Reserved,
		Consumed: m.Consumed, Revision: uint64(m.Revision), CreatedAt: fromTs(m.CreatedAt),
	}
}

func (r *repo) LockAllocations(ctx context.Context, operationID string) ([]*domain.Allocation, error) {
	rows, err := r.q.LockAllocations(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Allocation, 0, len(rows))
	for _, m := range rows {
		out = append(out, toAllocation(m))
	}
	return out, nil
}

func (r *repo) GetAllocationByPool(ctx context.Context, poolID, operationID string) (*domain.Allocation, error) {
	m, err := r.q.GetAllocationByPool(ctx, sqlc.GetAllocationByPoolParams{PoolID: poolID, OperationID: operationID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toAllocation(m), nil
}

func (r *repo) InsertAllocation(ctx context.Context, a *domain.Allocation) error {
	return r.q.InsertAllocation(ctx, sqlc.InsertAllocationParams{
		AllocationID: a.ID, PoolID: a.PoolID, OperationID: a.OperationID, Currency: a.Currency, Amount: a.Amount, Reserved: a.Reserved,
		Consumed: a.Consumed, Revision: int64(a.Revision), CreatedAt: ts(a.CreatedAt),
	})
}

func (r *repo) UpdateAllocation(ctx context.Context, a *domain.Allocation) error {
	return r.q.UpdateAllocation(ctx, sqlc.UpdateAllocationParams{AllocationID: a.ID, Reserved: a.Reserved, Consumed: a.Consumed, Revision: int64(a.Revision)})
}

// ---- dispatches ----

func toDispatch(m sqlc.Dispatch) *domain.Dispatch {
	d := &domain.Dispatch{
		ID: m.DispatchID, Kind: domain.DispatchKind(m.Kind), TenantID: m.TenantID, OperationID: m.OperationID, AttemptID: m.AttemptID, InstanceID: deref(m.InstanceID),
		CallID: m.CallID, Owner: m.Owner, RequestDigest: domain.Digest(m.RequestDigest), RouteID: deref(m.RouteID), GrantID: deref(m.GrantID),
		ExecutionEpoch: uint64(m.ExecutionEpoch), State: domain.DispatchState(m.State), Outcome: domain.DispatchOutcome(deref(m.Outcome)), DenialCode: deref(m.DenialCode),
		Reserved: domain.Money{Currency: m.ReservedCurrency, Amount: m.ReservedAmount}, MeterRevision: m.MeterRevision, SupersedesCallID: deref(m.SupersedesCallID),
		EvidenceRef: deref(m.EvidenceRef), Inventory: domain.InventoryState(m.InventoryState), InventoryVersion: deref(m.InventoryVersion),
		Deadline: fromTs(m.Deadline), AdmittedAt: fromTs(m.AdmittedAt), ObservedAt: fromTsPtr(m.ObservedAt),
	}
	if m.GrantRevision != nil {
		d.GrantRevision = uint64(*m.GrantRevision)
	}
	return d
}

func (r *repo) GetDispatchByCall(ctx context.Context, tenantID, owner, callID string) (*domain.Dispatch, error) {
	m, err := r.q.GetDispatchByCall(ctx, sqlc.GetDispatchByCallParams{TenantID: tenantID, Owner: owner, CallID: callID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toDispatch(m), nil
}

// FindDispatchByOwnerCall resolves an owner/call pair without a tenant; an
// owner shared across tenants makes the pair ambiguous and is not found.
func (r *repo) FindDispatchByOwnerCall(ctx context.Context, owner, callID string) (*domain.Dispatch, error) {
	rows, err := r.q.FindDispatchesByOwnerCall(ctx, sqlc.FindDispatchesByOwnerCallParams{Owner: owner, CallID: callID})
	if err != nil {
		return nil, mapErr(err)
	}
	if len(rows) != 1 {
		return nil, domain.ErrNotFound
	}
	return toDispatch(rows[0]), nil
}

func (r *repo) GetDispatch(ctx context.Context, dispatchID string) (*domain.Dispatch, error) {
	m, err := r.q.GetDispatch(ctx, dispatchID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toDispatch(m), nil
}

func (r *repo) LockDispatch(ctx context.Context, dispatchID string) (*domain.Dispatch, error) {
	m, err := r.q.LockDispatch(ctx, dispatchID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toDispatch(m), nil
}

func (r *repo) InsertDispatch(ctx context.Context, d *domain.Dispatch) error {
	return r.q.InsertDispatch(ctx, sqlc.InsertDispatchParams{
		DispatchID: d.ID, Kind: string(d.Kind), TenantID: d.TenantID, OperationID: d.OperationID, AttemptID: d.AttemptID, InstanceID: strPtr(d.InstanceID),
		CallID: d.CallID, Owner: d.Owner, RequestDigest: string(d.RequestDigest), RouteID: strPtr(d.RouteID), GrantID: strPtr(d.GrantID),
		GrantRevision: int64Ptr(d.GrantRevision, d.GrantID != ""), ExecutionEpoch: int64(d.ExecutionEpoch), State: string(d.State), Outcome: strPtr(string(d.Outcome)),
		DenialCode: strPtr(d.DenialCode), ReservedCurrency: d.Reserved.Currency, ReservedAmount: d.Reserved.Amount, MeterRevision: d.MeterRevision,
		SupersedesCallID: strPtr(d.SupersedesCallID), EvidenceRef: strPtr(d.EvidenceRef), InventoryState: string(d.Inventory), InventoryVersion: strPtr(d.InventoryVersion),
		Deadline: ts(d.Deadline), AdmittedAt: ts(d.AdmittedAt), ObservedAt: tsPtr(d.ObservedAt),
	})
}

func (r *repo) UpdateDispatch(ctx context.Context, d *domain.Dispatch) error {
	return r.q.UpdateDispatch(ctx, sqlc.UpdateDispatchParams{
		DispatchID: d.ID, State: string(d.State), Outcome: strPtr(string(d.Outcome)), DenialCode: strPtr(d.DenialCode), EvidenceRef: strPtr(d.EvidenceRef),
		InventoryState: string(d.Inventory), InventoryVersion: strPtr(d.InventoryVersion), ObservedAt: tsPtr(d.ObservedAt),
	})
}

func (r *repo) HasOverspend(ctx context.Context, operationID string) (bool, error) {
	ok, err := r.q.HasOverspend(ctx, operationID)
	return ok, mapErr(err)
}

func (r *repo) HasUnknownDispatch(ctx context.Context, operationID string) (bool, error) {
	ok, err := r.q.HasUnknownDispatch(ctx, operationID)
	return ok, mapErr(err)
}

func (r *repo) GetUsageObservation(ctx context.Context, dispatchID, source string, sequence uint64) (*domain.UsageObservation, error) {
	m, err := r.q.GetUsageObservation(ctx, sqlc.GetUsageObservationParams{DispatchID: dispatchID, Source: source, Sequence: int64(sequence)})
	if err != nil {
		return nil, mapErr(err)
	}
	o := &domain.UsageObservation{
		ID: m.ObservationID, DispatchID: m.DispatchID, Source: m.Source, Sequence: uint64(m.Sequence), NativeReference: m.NativeReference,
		CostRevision: m.CostRevision, ObservedAt: fromTs(m.ObservedAt),
	}
	if m.UsageReported {
		o.Usage = &domain.Usage{Input: uint64(m.InputUnits), Output: uint64(m.OutputUnits), Reasoning: uint64(m.ReasoningUnits), CachedInput: uint64(m.CachedInputUnits)}
	}
	return o, nil
}

// InsertUsageObservation keeps a report without usage as a row whose
// counters are placeholders under usage_reported = false; only explicitly
// reported counters (zero included) are metered.
func (r *repo) InsertUsageObservation(ctx context.Context, o *domain.UsageObservation) error {
	var usage domain.Usage
	if o.Usage != nil {
		usage = *o.Usage
	}
	return r.q.InsertUsageObservation(ctx, sqlc.InsertUsageObservationParams{
		ObservationID: o.ID, DispatchID: o.DispatchID, Source: o.Source, Sequence: int64(o.Sequence), NativeReference: o.NativeReference,
		InputUnits: int64(usage.Input), OutputUnits: int64(usage.Output), ReasoningUnits: int64(usage.Reasoning), CachedInputUnits: int64(usage.CachedInput),
		CostRevision: o.CostRevision, ObservedAt: ts(o.ObservedAt), UsageReported: o.Usage != nil,
	})
}

func (r *repo) HasReportedUsage(ctx context.Context, dispatchID string) (bool, error) {
	ok, err := r.q.HasReportedUsage(ctx, dispatchID)
	return ok, mapErr(err)
}

func (r *repo) MaxUsage(ctx context.Context, dispatchID string) (domain.Usage, error) {
	m, err := r.q.MaxUsage(ctx, dispatchID)
	if err != nil {
		return domain.Usage{}, mapErr(err)
	}
	return domain.Usage{Input: uint64(m.InputUnits), Output: uint64(m.OutputUnits), Reasoning: uint64(m.ReasoningUnits), CachedInput: uint64(m.CachedInputUnits)}, nil
}

func (r *repo) ChargedCost(ctx context.Context, dispatchID string) (int64, string, error) {
	m, err := r.q.ChargedCost(ctx, &dispatchID)
	if err != nil {
		return 0, "", mapErr(err)
	}
	return m.Amount, m.ActualEntryID, nil
}

func (r *repo) InsertCostEntry(ctx context.Context, e *domain.CostEntry) error {
	return r.q.InsertCostEntry(ctx, sqlc.InsertCostEntryParams{
		EntryID: e.ID, OperationID: e.OperationID, DispatchID: strPtr(e.DispatchID), ObservationID: strPtr(e.ObservationID), Kind: string(e.Kind),
		Currency: e.Amount.Currency, Amount: e.Amount.Amount, CorrectionOf: strPtr(e.CorrectionOf), CreatedAt: ts(e.CreatedAt),
	})
}

// ---- grant policy projection ----

func (r *repo) GetGrantPolicy(ctx context.Context, grantID string, revision uint64) (*domain.GrantPolicy, error) {
	m, err := r.q.GetGrantPolicy(ctx, sqlc.GetGrantPolicyParams{GrantID: grantID, GrantRevision: int64(revision)})
	if err != nil {
		return nil, mapErr(err)
	}
	g := &domain.GrantPolicy{
		GrantID: m.GrantID, GrantRevision: uint64(m.GrantRevision), TenantID: m.TenantID, PolicyDigest: domain.Digest(m.PolicyDigest), ServerID: m.ServerID,
		Methods: m.Methods, ExpiresAt: fromTsPtr(m.ExpiresAt), PolicyEpoch: uint64(m.PolicyEpoch), ReceiptID: m.ReceiptID, RevocationState: m.RevocationState,
	}
	if m.CostCapCurrency != nil && m.CostCapAmount != nil {
		g.CostCap = &domain.Money{Currency: *m.CostCapCurrency, Amount: *m.CostCapAmount}
	}
	return g, nil
}
