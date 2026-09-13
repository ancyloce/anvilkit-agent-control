package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/jackc/pgx/v5"
)

// Preparation persistence (development plan S2, 2026-09-13): the `preparation`
// kind's intake, its typed execution record (`preparations`, the local_checks
// shape), the rounds Control accepts from the Workflow and the actor
// (`preparation_rounds`), and the lifecycle projection. Every mutation locks
// rank 2 then rank 6 (`preparations`, `preparation_rounds`, the intake
// obligation) and appends at rank 7; callers do filesystem, artifact and
// Temporal work between transactions, never under these locks.

// ErrPreparationTerminal reports an answer or round against an operation that
// has already ended (expired clock, cancellation, brief ready or terminal).
var ErrPreparationTerminal = errors.New("OPERATION_TERMINAL")

// Ref is the public JSON form of an artifactRef; stored as jsonb exactly.
type Ref = contracts.ArtifactRefV1

// PreparationRound is one row of preparation_rounds.
type PreparationRound struct {
	Ordinal                         int64
	QuestionSetRef                  *Ref
	QuestionSetRevision             int64
	AskedAt, ExpiresAt              *time.Time
	AnswerSetRef                    *Ref
	AnswerCommandID, AnswerDigest   string
	AcceptedAt                      *time.Time
	DeliveryIntentID, DeliveryState string
	DeliveredAt                     *time.Time
	BriefRef                        *Ref
	BriefRevision                   int64
	ContentRejections               int64
}

// Preparation is the durable admission, execution and round snapshot of one
// preparation operation. All mutations reload it under the operation lock; a
// snapshot alone conveys no authority.
type Preparation struct {
	Scope
	ID, CommandID, IntakeSource, RequestDigest                            string
	AcceptedAt, QueueExpiresAt                                            time.Time
	FundingAuthority, AuthorizedFundingRef, FundingPolicyRevision         string
	QuotedCredits                                                         int64
	InputRef                                                              Ref
	Input                                                                 contracts.PreparationWorkflowInputV1
	InputDigest, WorkflowID, Namespace, RunID, StartState                 string
	ExecutionGeneration, RecoveryGeneration, Revision, NextSequence       int64
	Status, Stage, ControlState, CleanupState, ExpiryReason               string
	CancelRequested, Resolved, Recorded                                   bool
	ObjectKey, ObjectDigest, ObjectVersion, CancelCommandID, CancelDigest string
	CancelAcknowledgedAt                                                  *time.Time
	TerminalType, TerminalDigest                                          string
	TerminalEventID                                                       int64
	Existing                                                              bool
	Rounds                                                                []PreparationRound
}

// Current is the highest round, or nil before the first one.
func (p Preparation) Current() *PreparationRound {
	if len(p.Rounds) == 0 {
		return nil
	}
	return &p.Rounds[len(p.Rounds)-1]
}

// refMap renders a reference as the plain object the schema validator reads.
func refMap(ref Ref) map[string]any {
	return map[string]any{"kind": ref.Kind, "refId": ref.RefID, "subjectDigest": ref.SubjectDigest, "contentDigest": ref.ContentDigest, "sizeBytes": ref.SizeBytes, "objectVersion": ref.ObjectVersion}
}

// Projection is the PreparationProjection of the view and the lifecycle
// payload: references only, never text.
func (p Preparation) Projection() map[string]any {
	projection := map[string]any{"round": "0"}
	rejections := int64(0)
	for i := range p.Rounds {
		r := &p.Rounds[i]
		rejections += r.ContentRejections
		if r.AnswerSetRef != nil {
			projection["acceptedAnswerSetRef"] = refMap(*r.AnswerSetRef)
		}
		if r.BriefRef != nil {
			projection["briefRef"] = refMap(*r.BriefRef)
			projection["briefRevision"] = strconv.FormatInt(r.BriefRevision, 10)
		}
	}
	if current := p.Current(); current != nil {
		projection["round"] = strconv.FormatInt(current.Ordinal, 10)
		if current.QuestionSetRef != nil {
			projection["questionSetRef"] = refMap(*current.QuestionSetRef)
			projection["questionSetRevision"] = strconv.FormatInt(current.QuestionSetRevision, 10)
			projection["questionSetExpiresAt"] = current.ExpiresAt.UTC().Format(time.RFC3339Nano)
		}
	}
	projection["contentRejections"] = strconv.FormatInt(rejections, 10)
	return projection
}

// PreparationPublicStatus composes the public status of a preparation stage
// (DD-02 §4.4.1 Preparation rows).
func PreparationPublicStatus(stage string) string {
	switch stage {
	case "analyzing":
		return "running"
	case "brief_ready":
		return "succeeded"
	case "expired", "failed", "canceled":
		return stage
	}
	return "pending"
}

func scanRef(raw []byte) (*Ref, error) {
	if raw == nil {
		return nil, nil
	}
	var ref Ref
	if err := json.Unmarshal(raw, &ref); err != nil {
		return nil, ErrInvalid
	}
	return &ref, nil
}

func readPreparationRounds(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, id string) ([]PreparationRound, error) {
	rows, err := q.Query(ctx, `SELECT round_ordinal,question_set_ref,coalesce(question_set_revision,0),asked_at,expires_at,answer_set_ref,coalesce(answer_command_id,''),
		coalesce(answer_request_digest,''),accepted_at,coalesce(delivery_intent_id,''),coalesce(delivery_state,''),delivered_at,brief_ref,coalesce(brief_revision,0),content_rejections
		FROM agent_control.preparation_rounds WHERE operation_id=$1 ORDER BY round_ordinal`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rounds []PreparationRound
	for rows.Next() {
		var r PreparationRound
		var question, answer, brief []byte
		if err := rows.Scan(&r.Ordinal, &question, &r.QuestionSetRevision, &r.AskedAt, &r.ExpiresAt, &answer, &r.AnswerCommandID, &r.AnswerDigest, &r.AcceptedAt, &r.DeliveryIntentID, &r.DeliveryState, &r.DeliveredAt, &brief, &r.BriefRevision, &r.ContentRejections); err != nil {
			return nil, err
		}
		if r.QuestionSetRef, err = scanRef(question); err != nil {
			return nil, err
		}
		if r.AnswerSetRef, err = scanRef(answer); err != nil {
			return nil, err
		}
		if r.BriefRef, err = scanRef(brief); err != nil {
			return nil, err
		}
		rounds = append(rounds, r)
	}
	return rounds, rows.Err()
}

type preparationQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func readPreparation(ctx context.Context, q preparationQuerier, id string) (Preparation, error) {
	var p Preparation
	var inputRef, input []byte
	err := q.QueryRow(ctx, `SELECT o.operation_id,o.tenant_id,o.actor_id,o.intake_source,o.command_id,o.request_digest,o.accepted_at,o.queue_expires_at,
		coalesce(o.funding_authority,''),coalesce(o.authorized_funding_ref,''),coalesce(o.funding_policy_revision,''),coalesce(o.quoted_credits,0),
		p.input_ref,p.input_digest,p.workflow_input,p.workflow_id,p.temporal_namespace,coalesce(p.run_id,''),p.start_state,
		o.execution_generation,o.recovery_generation,o.operation_revision,o.next_event_seq,
		o.public_status,o.business_stage,o.control_state,o.cleanup_state,coalesce(o.expiry_reason,''),o.cancel_requested,p.resolved,
		b.state='obligation_recorded',b.object_key,b.body_digest,coalesce(b.object_version,''),
		coalesce(p.cancel_command_id,''),coalesce(p.cancel_request_digest,''),p.cancel_acknowledged_at,
		coalesce(p.accepted_terminal_type,''),coalesce(p.accepted_terminal_digest,''),coalesce(p.accepted_terminal_event_id,0)
		FROM agent_control.operations o JOIN agent_control.preparations p USING(operation_id)
		JOIN agent_control.obligations b ON b.operation_id=o.operation_id AND b.class='intake'
		WHERE o.operation_id=$1 AND o.kind='preparation'`, id).Scan(
		&p.ID, &p.TenantID, &p.ActorID, &p.IntakeSource, &p.CommandID, &p.RequestDigest, &p.AcceptedAt, &p.QueueExpiresAt,
		&p.FundingAuthority, &p.AuthorizedFundingRef, &p.FundingPolicyRevision, &p.QuotedCredits,
		&inputRef, &p.InputDigest, &input, &p.WorkflowID, &p.Namespace, &p.RunID, &p.StartState,
		&p.ExecutionGeneration, &p.RecoveryGeneration, &p.Revision, &p.NextSequence,
		&p.Status, &p.Stage, &p.ControlState, &p.CleanupState, &p.ExpiryReason, &p.CancelRequested, &p.Resolved,
		&p.Recorded, &p.ObjectKey, &p.ObjectDigest, &p.ObjectVersion, &p.CancelCommandID, &p.CancelDigest, &p.CancelAcknowledgedAt,
		&p.TerminalType, &p.TerminalDigest, &p.TerminalEventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, ErrNotFound
	}
	if err != nil {
		return p, err
	}
	if err := json.Unmarshal(inputRef, &p.InputRef); err != nil {
		return p, ErrInvalid
	}
	if err := json.Unmarshal(input, &p.Input); err != nil {
		return p, ErrInvalid
	}
	canonicalInput, err := canonical(p.Input)
	if err != nil || hash(canonicalInput) != p.InputDigest {
		return p, ErrConflict
	}
	p.Rounds, err = readPreparationRounds(ctx, q, id)
	return p, err
}

func lockPreparation(ctx context.Context, tx pgx.Tx, id string) (Preparation, error) {
	// Rank 2, then rank 6 preparations, preparation_rounds, then the intake
	// obligation. Separate statements keep the planner from reordering locks.
	for _, query := range []string{
		`SELECT operation_id FROM agent_control.operations WHERE operation_id=$1 AND kind='preparation' FOR UPDATE`,
		`SELECT operation_id FROM agent_control.preparations WHERE operation_id=$1 FOR UPDATE`,
		`SELECT operation_id FROM agent_control.obligations WHERE operation_id=$1 AND class='intake' FOR UPDATE`,
	} {
		var found string
		if err := tx.QueryRow(ctx, query, id).Scan(&found); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return Preparation{}, ErrNotFound
			}
			return Preparation{}, err
		}
	}
	if _, err := tx.Exec(ctx, `SELECT operation_id FROM agent_control.preparation_rounds WHERE operation_id=$1 FOR UPDATE`, id); err != nil {
		return Preparation{}, err
	}
	return readPreparation(ctx, tx, id)
}

func (s *Store) ReadPreparation(ctx context.Context, id string) (Preparation, error) {
	return readPreparation(ctx, s.pool, id)
}

// UnresolvedPreparations lists the executions the relay still owns, oldest first.
func (s *Store) UnresolvedPreparations(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT p.operation_id FROM agent_control.preparations p JOIN agent_control.operations o USING(operation_id) WHERE NOT p.resolved ORDER BY o.accepted_at, p.operation_id LIMIT 64`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// FindPreparationCommand resolves an already accepted command before the caller
// stores a new input artifact; a different digest is reported by the insert.
func (s *Store) FindPreparationCommand(ctx context.Context, scope Scope, commandID string) (Preparation, error) {
	var id string
	err := s.pool.QueryRow(ctx, `SELECT operation_id FROM agent_control.operations WHERE tenant_id=$1 AND actor_id=$2 AND kind='preparation' AND command_id=$3`, scope.TenantID, scope.ActorID, commandID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Preparation{}, ErrNotFound
	}
	if err != nil {
		return Preparation{}, err
	}
	p, err := readPreparation(ctx, s.pool, id)
	p.Existing = true
	return p, err
}

// PreparationIntake carries what the caller verified before the transaction.
type PreparationIntake struct {
	Scope
	OperationID, CommandID, IntakeSource, Namespace, Environment string
	InputRef                                                     Ref
	Profile                                                      contracts.PreparationProfile
	Limits                                                       contracts.PreparationLimits
	Billing                                                      contracts.TestBillingPolicy
}

// PreparationSemanticDigest is the intake semantic digest: the stored input
// content, so the same command with the same Prompt and references replays.
func PreparationSemanticDigest(ref Ref) (string, error) {
	semantic, err := canonical(map[string]any{"schemaVersion": 1, "kind": "preparation", "inputKind": ref.Kind, "contentDigest": ref.ContentDigest, "sizeBytes": ref.SizeBytes})
	if err != nil {
		return "", err
	}
	return hash(semantic), nil
}

func (p Preparation) IntakeBody() ([]byte, error) {
	body, err := canonical(map[string]any{"schemaVersion": 1, "class": "intake", "operationId": p.ID, "tenantId": p.TenantID,
		"kind": "preparation", "commandId": p.CommandID, "requestDigest": p.RequestDigest, "acceptedAt": p.AcceptedAt.UTC().Format(time.RFC3339Nano),
		"inputRef": p.InputRef, "fundingAuthority": p.FundingAuthority, "authorizedFundingRef": p.AuthorizedFundingRef, "workflowId": p.WorkflowID})
	if err != nil {
		return nil, ErrInvalid
	}
	return body, nil
}

// PreparePreparation reserves the operation identity, binds the zero-credit test
// quote and records the execution binding. It does not acknowledge admission or
// call the filesystem/Temporal; the input artifact was stored by the caller.
func (s *Store) PreparePreparation(ctx context.Context, in PreparationIntake, authorizedUntil time.Time) (Preparation, error) {
	if !s.validIDs(in.TenantID, in.ActorID, in.OperationID, in.CommandID) || in.Namespace == "" || !localEnvironment.MatchString(in.Environment) || (in.IntakeSource != "ui" && in.IntakeSource != "api") || in.InputRef.Kind != "preparation-input" || in.Billing.PolicyRevision == "" || in.Billing.PreparationPrice != 0 {
		return Preparation{}, ErrInvalid
	}
	digest, err := PreparationSemanticDigest(in.InputRef)
	if err != nil {
		return Preparation{}, err
	}
	id := in.OperationID
	var result Preparation
	err = s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if !now.Add(2 * time.Second).Before(authorizedUntil) {
			return ErrLeaseLost
		}
		queueExpiresAt := now.Add(time.Duration(in.Profile.StartWindowSeconds) * time.Second)
		quote := "tq-" + id
		var inserted string
		err := tx.QueryRow(ctx, `INSERT INTO agent_control.operations
			(operation_id,tenant_id,actor_id,kind,intake_source,command_id,request_digest,funding_authority,authorized_funding_ref,funding_policy_revision,quoted_credits,funding_state,public_status,business_stage,control_state,cleanup_state,accepted_at,queue_expires_at)
			VALUES($1,$2,$3,'preparation',$4,$5,$6,'test',$7,$8,0,NULL,'pending','admission_pending','running','not_required',$9,$10)
			ON CONFLICT(tenant_id,actor_id,kind,command_id) DO NOTHING RETURNING operation_id`, id, in.TenantID, in.ActorID, in.IntakeSource, in.CommandID, digest, quote, in.Billing.PolicyRevision, now, queueExpiresAt).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			var originalID, originalDigest string
			if err := tx.QueryRow(ctx, `SELECT operation_id,request_digest FROM agent_control.operations WHERE tenant_id=$1 AND actor_id=$2 AND kind='preparation' AND command_id=$3`, in.TenantID, in.ActorID, in.CommandID).Scan(&originalID, &originalDigest); err != nil {
				return err
			}
			if originalDigest != digest {
				return ErrConflict
			}
			result, err = lockPreparation(ctx, tx, originalID)
			result.Existing = true
			return err
		}
		if err != nil {
			return err
		}
		p := Preparation{Scope: in.Scope, ID: id, CommandID: in.CommandID, IntakeSource: in.IntakeSource, RequestDigest: digest, AcceptedAt: now, QueueExpiresAt: queueExpiresAt,
			FundingAuthority: "test", AuthorizedFundingRef: quote, FundingPolicyRevision: in.Billing.PolicyRevision, InputRef: in.InputRef, WorkflowID: in.Profile.WorkflowIDPrefix + id, Namespace: in.Namespace}
		p.Input = contracts.PreparationWorkflowInputV1{SchemaVersion: 1, OperationID: id, RequestDigest: digest, ExecutionGeneration: "1", RecoveryGeneration: "1", InputRef: in.InputRef,
			Limits: contracts.PreparationWorkflowLimitsV1{MaxClarificationRounds: strconv.FormatInt(in.Limits.MaxClarificationRounds, 10), MaxQuestionsPerRound: strconv.FormatInt(in.Limits.MaxQuestionsPerRound, 10), AwaitingInputExpirySeconds: strconv.FormatInt(in.Limits.AwaitingInputExpirySeconds, 10)}}
		raw, err := canonical(p.Input)
		if err != nil || contracts.ValidatePreparationWorkflowInput(p.Input) != nil {
			return ErrInvalid
		}
		ref, err := canonical(in.InputRef)
		if err != nil {
			return ErrInvalid
		}
		_, err = tx.Exec(ctx, `INSERT INTO agent_control.preparations(operation_id,input_ref,input_digest,workflow_input,workflow_id,temporal_namespace,admission_execution_generation,admission_recovery_generation,start_state,start_window_expires_at)
			VALUES($1,$2,$3,$4,$5,$6,1,1,'start_unattempted',$7)`, id, ref, hash(raw), raw, p.WorkflowID, in.Namespace, queueExpiresAt)
		if err != nil {
			return err
		}
		body, err := p.IntakeBody()
		if err != nil {
			return err
		}
		key := fmt.Sprintf("obligations/%s/%s/%s/intake/%s", in.Environment, now.UTC().Format("2006/01/02/15"), in.TenantID, id)
		_, err = tx.Exec(ctx, `INSERT INTO agent_control.obligations(obligation_id,class,identity,tenant_id,operation_id,state,object_key,body_digest,prepared_at)
			VALUES($1,'intake',$2,$3,$2,'obligation_pending',$4,$5,$6)`, "intake-"+id, id, in.TenantID, key, hash(body), now)
		if err != nil {
			return err
		}
		result, err = readPreparation(ctx, tx, id)
		return err
	})
	if err != nil {
		return Preparation{}, err
	}
	return result, nil
}

// NewPreparationID mints the operation identity before the input artifact is
// stored under it; an unused identity leaves only an orphan object behind.
func NewPreparationID() string { return "op-" + rand.Text() }

func currentPreparation(p Preparation) bool {
	return p.Input.ExecutionGeneration == strconv.FormatInt(p.ExecutionGeneration, 10) && p.Input.RecoveryGeneration == strconv.FormatInt(p.RecoveryGeneration, 10)
}

// preparationProjection commits one lifecycle transition with its revision and
// sequence, carrying the funding authority and the preparation projection.
func (s *Store) preparationProjection(ctx context.Context, tx pgx.Tx, p *Preparation, status, stage, control, cleanup, reason string, force bool) error {
	if !force && p.Status == status && p.Stage == stage && p.ControlState == control && p.CleanupState == cleanup {
		return nil
	}
	if p.Revision == math.MaxInt64 || p.NextSequence == math.MaxInt64 {
		return ErrRevision
	}
	revision := p.Revision
	if p.NextSequence > 1 {
		revision++
	}
	payload := map[string]any{"status": status, "businessStage": stage, "operationRevision": strconv.FormatInt(revision, 10), "cleanupState": cleanup, "fundingAuthority": p.FundingAuthority, "preparation": p.Projection()}
	if reason != "" {
		payload["reasonCode"] = reason
	}
	if p.CancelRequested {
		payload["intendedTerminalOutcome"] = "canceled"
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	if s.eventSchema.Validate(map[string]any{"schemaVersion": 1, "operationId": p.ID, "eventSeq": strconv.FormatInt(p.NextSequence, 10), "type": "operation.lifecycle", "occurredAt": now.UTC().Format(time.RFC3339Nano), "payload": payload}) != nil {
		return ErrInvalid
	}
	body, err := canonical(payload)
	if err != nil {
		return err
	}
	expiry := ""
	if status == "expired" {
		expiry = reason
	}
	_, err = tx.Exec(ctx, `UPDATE agent_control.operations SET public_status=$2,business_stage=$3,control_state=$4,cleanup_state=$5,
		operation_revision=$6,next_event_seq=next_event_seq+1,updated_at=$7,expiry_reason=$8 WHERE operation_id=$1`, p.ID, status, stage, control, cleanup, revision, now, nullable(expiry))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO agent_control.operation_events(operation_id,event_seq,transition_id,event_type,body,occurred_at)
		VALUES($1,$2,$3,'operation.lifecycle',$4,$5)`, p.ID, p.NextSequence, "preparation-"+strconv.FormatInt(revision, 10), body, now)
	if err == nil {
		if events, ok := ctx.Value(localEventsKey{}).(*[]localEvent); ok {
			*events = append(*events, localEvent{p.ID, p.NextSequence})
		}
		p.Status, p.Stage, p.ControlState, p.CleanupState, p.Revision = status, stage, control, cleanup, revision
		p.NextSequence++
	}
	return err
}

func (s *Store) ConfirmPreparationIntake(ctx context.Context, expected Preparation, version string, authorizedUntil time.Time) (Preparation, error) {
	var result Preparation
	err := s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		p, err := lockPreparation(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		if p.Scope != expected.Scope || p.InputDigest != expected.InputDigest || p.ObjectDigest != version {
			return ErrConflict
		}
		if p.Recorded {
			result = p
			return nil
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if !currentPreparation(p) || p.CancelRequested || p.Resolved || !now.Before(p.QueueExpiresAt) || !now.Add(2*time.Second).Before(authorizedUntil) {
			return ErrRevision
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.obligations SET state='obligation_recorded',object_version=$2,recorded_at=$3 WHERE operation_id=$1 AND class='intake'`, p.ID, version, now)
		if err != nil {
			return err
		}
		if err := s.preparationProjection(ctx, tx, &p, "pending", "queued", "running", "not_required", "", true); err != nil {
			return err
		}
		result, err = readPreparation(ctx, tx, p.ID)
		return err
	})
	result.Existing = expected.Existing
	return result, err
}

// MarkPreparationStart grants only a same-ID start inside the original window;
// the marker commits before the caller sends anything.
func (s *Store) MarkPreparationStart(ctx context.Context, expected Preparation, authorizedUntil time.Time) (Preparation, bool, error) {
	var result Preparation
	send := false
	err := s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		send = false
		p, err := lockPreparation(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		if p.InputDigest != expected.InputDigest || !currentPreparation(p) {
			return ErrRevision
		}
		if p.Resolved || p.CancelRequested || !p.Recorded || p.RunID != "" {
			result = p
			return nil
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if !now.Before(p.QueueExpiresAt) {
			if p.StartState == "start_unattempted" {
				err = s.finishPreparationUnattempted(ctx, tx, &p, "unattempted_expired", now)
			} else {
				err = s.preparationProjection(ctx, tx, &p, "blocked", p.Stage, "blocked", p.CleanupState, "PREPARATION_START_UNRESOLVED", false)
			}
			if err != nil {
				return err
			}
			result, err = readPreparation(ctx, tx, p.ID)
			return err
		}
		if !now.Add(2 * time.Second).Before(authorizedUntil) {
			return ErrLeaseLost
		}
		if p.StartState == "start_unattempted" {
			_, err = tx.Exec(ctx, `UPDATE agent_control.preparations SET start_state='start_attempted',start_attempted_at=$2,updated_at=$2 WHERE operation_id=$1`, p.ID, now)
			if err != nil {
				return err
			}
		}
		result, err = readPreparation(ctx, tx, p.ID)
		send = err == nil
		return err
	})
	return result, send, err
}

func (s *Store) EstablishPreparationRun(ctx context.Context, expected Preparation, runID string, input contracts.PreparationWorkflowInputV1) error {
	if runID == "" {
		return ErrInvalid
	}
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		p, err := lockPreparation(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		if p.Input != input || p.InputDigest != expected.InputDigest || p.WorkflowID != expected.WorkflowID || p.Namespace != expected.Namespace || !currentPreparation(p) {
			return ErrRevision
		}
		if p.RunID != "" {
			if p.RunID != runID {
				return ErrConflict
			}
			return nil
		}
		if !p.Recorded || p.StartState != "start_attempted" || p.Resolved {
			return ErrRevision
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.preparations SET run_id=$2,start_state='start_established',start_established_at=clock_timestamp(),updated_at=clock_timestamp() WHERE operation_id=$1`, p.ID, runID)
		return err
	})
}

func (s *Store) PreparationObservationUnavailable(ctx context.Context, expected Preparation, reason string) error {
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		p, err := lockPreparation(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		if p.Resolved || !p.Recorded {
			return nil
		}
		if p.InputDigest != expected.InputDigest {
			return ErrConflict
		}
		return s.preparationProjection(ctx, tx, &p, "blocked", p.Stage, "blocked", p.CleanupState, reason, false)
	})
}

// ObservePreparationRunning records a healthy established execution: the stage
// becomes analyzing until the Workflow records a round. Rounds already
// recorded keep their stage; the observation only clears a blocked marker.
func (s *Store) ObservePreparationRunning(ctx context.Context, expected Preparation) error {
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		p, err := lockPreparation(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		if p.Resolved {
			return nil
		}
		if !currentPreparation(p) || p.RunID == "" || p.RunID != expected.RunID {
			return ErrRevision
		}
		cleanup := "not_required"
		if p.CancelRequested {
			cleanup = "pending"
		}
		stage := p.Stage
		if stage == "queued" || stage == "admission_pending" {
			stage = "analyzing"
		}
		if stage == "brief_ready" {
			return nil
		}
		return s.preparationProjection(ctx, tx, &p, PreparationPublicStatus(stage), stage, "running", cleanup, "", false)
	})
}

// RoundRecord is what RecordPreparationRound stores: the artifact the caller
// already wrote, and the verified members of the document it holds.
type RoundRecord struct {
	OperationID         string
	ExecutionGeneration int64
	Ordinal             int64
	QuestionSetRef      *Ref
	QuestionSetRevision int64
	BriefRef            *Ref
	BriefRevision       int64
}

// RoundOutcome reports the committed round: accepted, or the identical round
// again (Duplicate). A different content for an existing ordinal is ErrConflict.
type RoundOutcome struct {
	Preparation Preparation
	Round       PreparationRound
	Duplicate   bool
}

// RecordPreparationRound commits a posed question set (awaiting_input with its
// clock) or the frozen brief (brief_ready) under the operation lock.
func (s *Store) RecordPreparationRound(ctx context.Context, r RoundRecord, limits contracts.PreparationLimits) (RoundOutcome, error) {
	if !s.validIDs(r.OperationID) || r.Ordinal < 1 || r.ExecutionGeneration < 1 || (r.QuestionSetRef == nil) == (r.BriefRef == nil) {
		return RoundOutcome{}, ErrInvalid
	}
	var out RoundOutcome
	err := s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		out = RoundOutcome{}
		p, err := lockPreparation(ctx, tx, r.OperationID)
		if err != nil {
			return err
		}
		if p.ExecutionGeneration != r.ExecutionGeneration || !currentPreparation(p) {
			return ErrRevision
		}
		for i := range p.Rounds {
			existing := &p.Rounds[i]
			if existing.Ordinal != r.Ordinal {
				continue
			}
			same := (r.QuestionSetRef != nil && existing.QuestionSetRef != nil && *existing.QuestionSetRef == *r.QuestionSetRef && existing.QuestionSetRevision == r.QuestionSetRevision) ||
				(r.BriefRef != nil && existing.BriefRef != nil && *existing.BriefRef == *r.BriefRef && existing.BriefRevision == r.BriefRevision)
			if !same {
				return ErrConflict
			}
			out = RoundOutcome{Preparation: p, Round: *existing, Duplicate: true}
			return nil
		}
		if p.Resolved || p.CancelRequested || p.Stage == "brief_ready" || p.Stage == "expired" || p.Stage == "failed" || p.Stage == "canceled" {
			return ErrPreparationTerminal
		}
		current := p.Current()
		if r.Ordinal != int64(len(p.Rounds))+1 {
			return ErrRevision
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		// A new round follows an answered question set, or opens the operation.
		if current != nil && current.QuestionSetRef != nil && current.AnswerSetRef == nil {
			return ErrRevision
		}
		if current != nil && current.ExpiresAt != nil && current.AnswerSetRef == nil && !now.Before(*current.ExpiresAt) {
			return ErrPreparationTerminal
		}
		if r.QuestionSetRef != nil {
			asked := 0
			for _, existing := range p.Rounds {
				if existing.QuestionSetRef != nil {
					asked++
				}
			}
			if int64(asked) >= limits.MaxClarificationRounds || r.QuestionSetRevision != int64(asked)+1 {
				return ErrRevision
			}
			ref, err := canonical(r.QuestionSetRef)
			if err != nil {
				return ErrInvalid
			}
			expires := now.Add(time.Duration(limits.AwaitingInputExpirySeconds) * time.Second)
			_, err = tx.Exec(ctx, `INSERT INTO agent_control.preparation_rounds(operation_id,round_ordinal,question_set_ref,question_set_revision,asked_at,expires_at) VALUES($1,$2,$3,$4,$5,$6)`, p.ID, r.Ordinal, ref, r.QuestionSetRevision, now, expires)
			if err != nil {
				return err
			}
		} else {
			if r.BriefRevision != 1 {
				return ErrRevision
			}
			ref, err := canonical(r.BriefRef)
			if err != nil {
				return ErrInvalid
			}
			_, err = tx.Exec(ctx, `INSERT INTO agent_control.preparation_rounds(operation_id,round_ordinal,brief_ref,brief_revision) VALUES($1,$2,$3,$4)`, p.ID, r.Ordinal, ref, r.BriefRevision)
			if err != nil {
				return err
			}
		}
		p, err = readPreparation(ctx, tx, p.ID)
		if err != nil {
			return err
		}
		if r.QuestionSetRef != nil {
			err = s.preparationProjection(ctx, tx, &p, "pending", "awaiting_input", "running", "not_required", "", true)
		} else {
			err = s.preparationProjection(ctx, tx, &p, "succeeded", "brief_ready", "running", "complete", "", true)
		}
		if err != nil {
			return err
		}
		p, err = readPreparation(ctx, tx, p.ID)
		if err != nil {
			return err
		}
		out = RoundOutcome{Preparation: p, Round: *p.Current()}
		return nil
	})
	return out, err
}

// AnswerRecord carries the verified answer set the caller already stored.
type AnswerRecord struct {
	Scope
	OperationID, CommandID    string
	ExpectedRevision          uint64
	QuestionSetRef, AnswerRef Ref
	QuestionSetRevision       int64
}

// AnswerDecision mirrors AcceptResultDecision: accepted, duplicate or stale.
type AnswerDecision string

const (
	AnswerAccepted  AnswerDecision = "accepted"
	AnswerDuplicate AnswerDecision = "duplicate"
	AnswerStale     AnswerDecision = "stale"
)

type AnswerOutcome struct {
	Decision    AnswerDecision
	Preparation Preparation
	Round       PreparationRound
	EventSeq    int64
}

// RecordPreparationAnswers accepts one answer set for the current question-set
// revision and records its delivery intent in the same commit. A superseded
// question set or revision is ErrRevision; an ended operation is
// ErrPreparationTerminal; the same command again returns the acceptance.
func (s *Store) RecordPreparationAnswers(ctx context.Context, a AnswerRecord, authorizedUntil time.Time) (AnswerOutcome, error) {
	if !s.validIDs(a.TenantID, a.ActorID, a.OperationID, a.CommandID) || a.ExpectedRevision < 1 || a.ExpectedRevision > math.MaxInt64 || a.AnswerRef.Kind != "evidence" || a.QuestionSetRef.Kind != "evidence" {
		return AnswerOutcome{}, ErrInvalid
	}
	raw, err := canonical(map[string]any{"operationId": a.OperationID, "expectedOperationRevision": strconv.FormatUint(a.ExpectedRevision, 10), "questionSetRef": a.QuestionSetRef, "answerContentDigest": a.AnswerRef.ContentDigest})
	if err != nil {
		return AnswerOutcome{}, err
	}
	digest := hash(raw)
	var out AnswerOutcome
	err = s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		out = AnswerOutcome{}
		p, err := lockPreparation(ctx, tx, a.OperationID)
		if err != nil {
			return err
		}
		if p.Scope != a.Scope {
			return ErrNotFound
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if !now.Add(2 * time.Second).Before(authorizedUntil) {
			return ErrLeaseLost
		}
		for i := range p.Rounds {
			existing := &p.Rounds[i]
			if existing.AnswerCommandID == a.CommandID {
				if existing.AnswerDigest != digest {
					return ErrConflict
				}
				out = AnswerOutcome{Decision: AnswerDuplicate, Preparation: p, Round: *existing, EventSeq: p.NextSequence - 1}
				return nil
			}
		}
		if p.Resolved || p.CancelRequested || p.Stage != "awaiting_input" {
			return ErrPreparationTerminal
		}
		current := p.Current()
		if current == nil || current.QuestionSetRef == nil {
			return ErrPreparationTerminal
		}
		if current.ExpiresAt != nil && !now.Before(*current.ExpiresAt) {
			return ErrPreparationTerminal
		}
		if *current.QuestionSetRef != a.QuestionSetRef || current.QuestionSetRevision != a.QuestionSetRevision {
			return ErrRevision
		}
		if p.Revision != int64(a.ExpectedRevision) {
			return ErrRevision
		}
		if current.AnswerSetRef != nil {
			out = AnswerOutcome{Decision: AnswerStale, Preparation: p, Round: *current, EventSeq: p.NextSequence - 1}
			return nil
		}
		ref, err := canonical(a.AnswerRef)
		if err != nil {
			return ErrInvalid
		}
		intent := "intent-" + p.ID + "-answers-" + strconv.FormatInt(current.QuestionSetRevision, 10)
		_, err = tx.Exec(ctx, `UPDATE agent_control.preparation_rounds SET answer_set_ref=$3,answer_command_id=$4,answer_request_digest=$5,accepted_at=$6,delivery_intent_id=$7,delivery_state='pending'
			WHERE operation_id=$1 AND round_ordinal=$2`, p.ID, current.Ordinal, ref, a.CommandID, digest, now, intent)
		if err != nil {
			return err
		}
		p, err = readPreparation(ctx, tx, p.ID)
		if err != nil {
			return err
		}
		if err := s.preparationProjection(ctx, tx, &p, "pending", "awaiting_input", "running", "not_required", "", true); err != nil {
			return err
		}
		p, err = readPreparation(ctx, tx, p.ID)
		if err != nil {
			return err
		}
		out = AnswerOutcome{Decision: AnswerAccepted, Preparation: p, Round: *p.Current(), EventSeq: p.NextSequence - 1}
		return nil
	})
	return out, err
}

// PendingDelivery is one accepted answer set whose tracked Update is not yet delivered.
type PendingDelivery struct {
	Preparation Preparation
	Round       PreparationRound
}

func (s *Store) PendingPreparationDeliveries(ctx context.Context) ([]PendingDelivery, error) {
	rows, err := s.pool.Query(ctx, `SELECT operation_id FROM agent_control.preparation_rounds WHERE delivery_state='pending' ORDER BY accepted_at LIMIT 64`)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var pending []PendingDelivery
	for _, id := range ids {
		p, err := readPreparation(ctx, s.pool, id)
		if err != nil {
			return nil, err
		}
		for _, r := range p.Rounds {
			if r.DeliveryState == "pending" {
				pending = append(pending, PendingDelivery{Preparation: p, Round: r})
			}
		}
	}
	return pending, nil
}

// MarkPreparationDelivered records that the tracked Update named by the intent
// reached the Workflow (or can no longer be delivered because the execution has
// ended); delivery advances no business stage.
func (s *Store) MarkPreparationDelivered(ctx context.Context, id string, ordinal int64, intentID string) error {
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := lockPreparation(ctx, tx, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE agent_control.preparation_rounds SET delivery_state='delivered',delivered_at=clock_timestamp() WHERE operation_id=$1 AND round_ordinal=$2 AND delivery_intent_id=$3 AND delivery_state='pending'`, id, ordinal, intentID)
		return err
	})
}

// ExpireAwaitingInput ends a task whose current question-set clock ran out
// without an accepted answer set. The Workflow's own timer reports the same
// fact later; whichever observation comes first records it once.
func (s *Store) ExpireAwaitingInput(ctx context.Context, id string) (bool, error) {
	expired := false
	err := s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		expired = false
		p, err := lockPreparation(ctx, tx, id)
		if err != nil {
			return err
		}
		current := p.Current()
		if p.Resolved || p.Stage != "awaiting_input" || current == nil || current.QuestionSetRef == nil || current.AnswerSetRef != nil || current.ExpiresAt == nil {
			return nil
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if now.Before(*current.ExpiresAt) {
			return nil
		}
		expired = true
		return s.preparationProjection(ctx, tx, &p, "expired", "expired", "running", "complete", "PREPARATION_INPUT_EXPIRED", true)
	})
	return expired, err
}

// PreparationTerminal is the observed terminal fact of the original Workflow/Run.
type PreparationTerminal struct {
	WorkflowID, RunID string
	Input             contracts.PreparationWorkflowInputV1
	Type              string
	EventID           int64
	At                time.Time
	Result            json.RawMessage
}

func (e PreparationTerminal) digest() string {
	raw, _ := canonical(map[string]any{"workflowId": e.WorkflowID, "runId": e.RunID, "type": e.Type, "eventId": strconv.FormatInt(e.EventID, 10), "at": e.At.UTC().Format(time.RFC3339Nano), "resultDigest": hash(e.Result)})
	return hash(raw)
}

// AcceptPreparationTerminal verifies the original binding again under rank 2
// and retains the evidence fingerprint for every outcome. A frozen brief
// already recorded stays the accepted result whatever ended the execution.
func (s *Store) AcceptPreparationTerminal(ctx context.Context, e PreparationTerminal) error {
	if e.RunID == "" || e.EventID < 1 || e.At.IsZero() {
		return ErrInvalid
	}
	fingerprint := e.digest()
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		p, err := lockPreparation(ctx, tx, e.Input.OperationID)
		if err != nil {
			return err
		}
		if p.WorkflowID != e.WorkflowID || p.RunID != e.RunID || p.Input != e.Input || !currentPreparation(p) {
			return ErrRevision
		}
		if p.Resolved {
			if p.TerminalDigest != fingerprint {
				return ErrConflict
			}
			return nil
		}
		status, stage, reason := "", "", ""
		switch e.Type {
		case "completed":
			var result contracts.PreparationWorkflowResultV1
			if json.Unmarshal(e.Result, &result) != nil || result.OperationID != p.ID {
				status, stage, reason = "failed", "failed", "PREPARATION_RESULT_INVALID"
				break
			}
			switch result.Outcome {
			case "brief_ready":
				brief := p.Current()
				if p.Stage != "brief_ready" || brief == nil || brief.BriefRef == nil || strconv.FormatInt(brief.BriefRevision, 10) != result.BriefRevision {
					status, stage, reason = "failed", "failed", "PREPARATION_RESULT_INVALID"
				} else {
					status, stage = "succeeded", "brief_ready"
				}
			case "expired":
				status, stage, reason = "expired", "expired", "PREPARATION_INPUT_EXPIRED"
			default:
				status, stage, reason = "failed", "failed", "PREPARATION_UNRESOLVED"
			}
		case "failed", "terminated":
			status, stage, reason = "failed", "failed", "PREPARATION_EXECUTION_FAILED"
		case "timed_out":
			status, stage, reason = "expired", "expired", "PREPARATION_DEADLINE_EXPIRED"
		case "canceled":
			status, stage = "canceled", "canceled"
		default:
			return ErrInvalid
		}
		if p.Stage == "brief_ready" {
			// The frozen brief is the accepted result; a later cancel or failure
			// of the execution changes nothing the user can consume.
			status, stage, reason = "succeeded", "brief_ready", ""
		} else if p.Stage == "expired" {
			status, stage, reason = "expired", "expired", p.ExpiryReason
		} else if p.CancelRequested {
			status, stage, reason = "canceled", "canceled", ""
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.preparations SET resolved=true,accepted_run_id=$2,accepted_terminal_at=$3,accepted_terminal_event_id=$4,
			accepted_terminal_type=$5,accepted_terminal_digest=$6,updated_at=clock_timestamp() WHERE operation_id=$1`, p.ID, e.RunID, e.At, e.EventID, e.Type, fingerprint)
		if err != nil {
			return err
		}
		return s.preparationProjection(ctx, tx, &p, status, stage, "running", "complete", reason, p.Stage != stage || p.Status != status || p.CleanupState != "complete")
	})
}

func (s *Store) finishPreparationUnattempted(ctx context.Context, tx pgx.Tx, p *Preparation, kind string, now time.Time) error {
	if p.StartState != "start_unattempted" || p.Resolved {
		return ErrRevision
	}
	status, reason := "canceled", ""
	if kind == "unattempted_expired" {
		status, reason = "expired", "PREPARATION_DEADLINE_EXPIRED"
	}
	fingerprint := (PreparationTerminal{WorkflowID: p.WorkflowID, Type: kind, At: now}).digest()
	_, err := tx.Exec(ctx, `UPDATE agent_control.preparations SET resolved=true,start_state=CASE WHEN $3='unattempted_expired' THEN 'start_expired' ELSE start_state END,accepted_terminal_at=$2,accepted_terminal_event_id=0,
		accepted_terminal_type=$3,accepted_terminal_digest=$4,updated_at=$2 WHERE operation_id=$1`, p.ID, now, kind, fingerprint)
	if err != nil {
		return err
	}
	return s.preparationProjection(ctx, tx, p, status, status, "running", "complete", reason, true)
}

// CancelPreparation is the reserved-lane cancellation of a preparation; the
// caller supplies the reserved pool. A frozen brief is never canceled.
func (s *Store) CancelPreparation(ctx context.Context, scope Scope, id, commandID, reason string, expectedRevision uint64, authorizedUntil time.Time) (Preparation, bool, error) {
	if !s.validIDs(scope.ActorID, scope.TenantID, id, commandID) || expectedRevision < 1 || expectedRevision > math.MaxInt64 {
		return Preparation{}, false, ErrInvalid
	}
	raw, _ := canonical(map[string]any{"operationId": id, "expectedOperationRevision": strconv.FormatUint(expectedRevision, 10), "reasonCode": reason})
	digest := hash(raw)
	var result Preparation
	coalesced := false
	err := s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		p, err := lockPreparation(ctx, tx, id)
		if err != nil {
			return err
		}
		if p.Scope != scope {
			return ErrNotFound
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if !now.Add(2 * time.Second).Before(authorizedUntil) {
			return ErrLeaseLost
		}
		if p.CancelCommandID == commandID && p.CancelDigest != digest {
			return ErrConflict
		}
		terminal := p.Resolved || p.Stage == "brief_ready" || p.Stage == "expired"
		if terminal && p.CancelCommandID == "" {
			_, err = tx.Exec(ctx, `UPDATE agent_control.preparations SET cancel_command_id=$2,cancel_request_digest=$3,cancel_acknowledged_at=$4 WHERE operation_id=$1`, id, commandID, digest, now)
			if err != nil {
				return err
			}
			p, err = readPreparation(ctx, tx, id)
			if err != nil {
				return err
			}
		}
		if p.CancelRequested || terminal {
			result = p
			coalesced = true
			return nil
		}
		if p.Revision != int64(expectedRevision) {
			return ErrRevision
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.operations SET cancel_requested=true,intended_terminal_outcome='canceled' WHERE operation_id=$1`, id)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.preparations SET cancel_command_id=$2,cancel_request_digest=$3,cancel_acknowledged_at=$4 WHERE operation_id=$1`, id, commandID, digest, now)
		if err != nil {
			return err
		}
		p.CancelRequested = true
		if p.StartState == "start_unattempted" {
			err = s.finishPreparationUnattempted(ctx, tx, &p, "unattempted_canceled", now)
		} else {
			err = s.preparationProjection(ctx, tx, &p, p.Status, p.Stage, p.ControlState, "pending", "", true)
		}
		if err != nil {
			return err
		}
		result, err = readPreparation(ctx, tx, id)
		return err
	})
	return result, coalesced, err
}

// ExpirePreparedPreparation ends a start that was never attempted after its
// window, including after a crash before independent persistence.
func (s *Store) ExpirePreparedPreparation(ctx context.Context, id string) error {
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		p, err := lockPreparation(ctx, tx, id)
		if err != nil {
			return err
		}
		if p.Resolved {
			return nil
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if p.StartState != "start_unattempted" || now.Before(p.QueueExpiresAt) {
			return errors.New("preparation start cannot expire without evidence")
		}
		return s.finishPreparationUnattempted(ctx, tx, &p, "unattempted_expired", now)
	})
}
