package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrLocalCapacity = errors.New("OVERLOADED: queue_full")
var localEnvironment = regexp.MustCompile(`^[a-z0-9-]+$`)

// SetLocalEventLogger is configured before serving, shared by the two Control
// pools. Only acknowledged commits emit event.appended, outside every lock.
func (s *Store) SetLocalEventLogger(logger func(string, int64)) { s.localEventLogger = logger }

type localEventsKey struct{}
type localEvent struct {
	id       string
	sequence int64
}

func (s *Store) transactLocal(ctx context.Context, work func(context.Context, pgx.Tx) error) error {
	var events []localEvent
	ctx = context.WithValue(ctx, localEventsKey{}, &events)
	err := s.transact(ctx, func(tx pgx.Tx) error { events = nil; return work(ctx, tx) })
	if err == nil && s.localEventLogger != nil {
		for _, e := range events {
			s.localEventLogger(e.id, e.sequence)
		}
	}
	return err
}

func (s *Store) MatchesLocalCheckScope(ctx context.Context, scope Scope, id string) (bool, error) {
	var found bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agent_control.operations o JOIN agent_control.local_checks l USING(operation_id)
		WHERE o.operation_id=$1 AND o.tenant_id=$2 AND o.actor_id=$3 AND o.kind='local-check')`, id, scope.TenantID, scope.ActorID).Scan(&found)
	return found, err
}

// LocalCheck is the durable admission and observation snapshot. All mutations
// reload it under the operation lock; a snapshot alone conveys no authority.
type LocalCheck struct {
	Scope
	ID, CommandID, RequestDigest                          string
	AcceptedAt, QueueExpiresAt                            time.Time
	Input                                                 contracts.LocalCheckInputV1
	InputDigest, WorkflowID, Namespace, RunID, StartState string
	ExecutionGeneration, RecoveryGeneration               int64
	Revision, NextSequence                                int64
	Status, Stage, ControlState, CleanupState             string
	CancelRequested, Resolved, Recorded                   bool
	ObjectKey, ObjectDigest, ObjectVersion                string
	CancelCommandID, CancelDigest                         string
	CancelAcknowledgedAt                                  *time.Time
	TerminalType, TerminalDigest                          string
	TerminalEventID                                       int64
	Existing                                              bool
}

type localQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readLocal(ctx context.Context, q localQuerier, id string) (LocalCheck, error) {
	var c LocalCheck
	var raw []byte
	err := q.QueryRow(ctx, `SELECT o.operation_id,o.tenant_id,o.actor_id,o.command_id,o.request_digest,o.accepted_at,o.queue_expires_at,
		l.workflow_input,l.input_digest,l.workflow_id,l.temporal_namespace,coalesce(l.run_id,''),l.start_state,
		o.execution_generation,o.recovery_generation,o.operation_revision,o.next_event_seq,
		o.public_status,o.business_stage,o.control_state,o.cleanup_state,o.cancel_requested,l.resolved,
		b.state='obligation_recorded',b.object_key,b.body_digest,coalesce(b.object_version,''),
		coalesce(l.cancel_command_id,''),coalesce(l.cancel_request_digest,''),l.cancel_acknowledged_at,
		coalesce(l.accepted_terminal_type,''),coalesce(l.accepted_terminal_digest,''),coalesce(l.accepted_terminal_event_id,0)
		FROM agent_control.operations o JOIN agent_control.local_checks l USING(operation_id)
		JOIN agent_control.obligations b ON b.operation_id=o.operation_id AND b.class='intake'
		WHERE o.operation_id=$1 AND o.kind='local-check'`, id).Scan(
		&c.ID, &c.TenantID, &c.ActorID, &c.CommandID, &c.RequestDigest, &c.AcceptedAt, &c.QueueExpiresAt,
		&raw, &c.InputDigest, &c.WorkflowID, &c.Namespace, &c.RunID, &c.StartState,
		&c.ExecutionGeneration, &c.RecoveryGeneration, &c.Revision, &c.NextSequence,
		&c.Status, &c.Stage, &c.ControlState, &c.CleanupState, &c.CancelRequested, &c.Resolved,
		&c.Recorded, &c.ObjectKey, &c.ObjectDigest, &c.ObjectVersion, &c.CancelCommandID, &c.CancelDigest, &c.CancelAcknowledgedAt,
		&c.TerminalType, &c.TerminalDigest, &c.TerminalEventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, ErrNotFound
	}
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(raw, &c.Input); err != nil {
		return c, ErrInvalid
	}
	canonicalInput, err := canonical(c.Input)
	if err != nil || hash(canonicalInput) != c.InputDigest {
		return c, ErrConflict
	}
	return c, nil
}

func lockLocal(ctx context.Context, tx pgx.Tx, id string) (LocalCheck, error) {
	// Rank 2, then rank 6 local_checks, then the intake obligation. The joined
	// read below is deliberately separate so the planner cannot reorder locks.
	for _, query := range []string{
		`SELECT operation_id FROM agent_control.operations WHERE operation_id=$1 AND kind='local-check' FOR UPDATE`,
		`SELECT operation_id FROM agent_control.local_checks WHERE operation_id=$1 FOR UPDATE`,
		`SELECT operation_id FROM agent_control.obligations WHERE operation_id=$1 AND class='intake' FOR UPDATE`,
	} {
		var found string
		if err := tx.QueryRow(ctx, query, id).Scan(&found); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return LocalCheck{}, ErrNotFound
			}
			return LocalCheck{}, err
		}
	}
	return readLocal(ctx, tx, id)
}

func (s *Store) ReadLocalCheck(ctx context.Context, id string) (LocalCheck, error) {
	return readLocal(ctx, s.pool, id)
}

func (s *Store) PendingLocalCheck(ctx context.Context) (LocalCheck, error) {
	var id string
	err := s.pool.QueryRow(ctx, `SELECT operation_id FROM agent_control.local_checks WHERE NOT resolved`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return LocalCheck{}, ErrNotFound
	}
	if err != nil {
		return LocalCheck{}, err
	}
	return readLocal(ctx, s.pool, id)
}

func (c LocalCheck) IntakeBody() ([]byte, error) {
	body, err := canonical(map[string]any{"schemaVersion": 1, "class": "intake", "operationId": c.ID, "tenantId": c.TenantID,
		"kind": "local-check", "commandId": c.CommandID, "requestDigest": c.RequestDigest, "acceptedAt": c.AcceptedAt.UTC().Format(time.RFC3339Nano),
		"fixtureId": c.Input.FixtureID, "profileRef": c.Input.ProfileRef, "profileDigest": c.Input.ProfileDigest, "workflowId": c.WorkflowID})
	if err != nil || contracts.ValidateIntake(body) != nil {
		return nil, ErrInvalid
	}
	return body, nil
}

// PrepareLocalCheck reserves only the one local slot and the immutable intake
// identity. It does not acknowledge admission or call the filesystem/Temporal.
func (s *Store) PrepareLocalCheck(ctx context.Context, scope Scope, commandID, fixtureID, namespace, environment string, authorizedUntil time.Time) (LocalCheck, error) {
	fixture, err := contracts.Fixture(fixtureID)
	if err != nil || !s.validIDs(scope.TenantID, scope.ActorID, commandID) || namespace == "" || !localEnvironment.MatchString(environment) {
		return LocalCheck{}, ErrInvalid
	}
	semantic, err := canonical(map[string]any{"schemaVersion": 1, "kind": "local-check", "fixtureId": fixtureID})
	if err != nil {
		return LocalCheck{}, err
	}
	digest, id := hash(semantic), "op-"+rand.Text()
	var result LocalCheck
	err = s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if !now.Add(2 * time.Second).Before(authorizedUntil) {
			return ErrLeaseLost
		}
		// The unique command key serializes identical submissions before the
		// rank-6 unresolved-slot index is consulted. No admission is duplicated.
		var inserted string
		err := tx.QueryRow(ctx, `INSERT INTO agent_control.operations
			(operation_id,tenant_id,actor_id,kind,intake_source,command_id,request_digest,funding_state,public_status,business_stage,control_state,cleanup_state,accepted_at,queue_expires_at)
			VALUES($1,$2,$3,'local-check','api',$4,$5,'not_applicable','pending','queued','running','not_required',$6,$7)
			ON CONFLICT(tenant_id,actor_id,kind,command_id) DO NOTHING RETURNING operation_id`, id, scope.TenantID, scope.ActorID, commandID, digest, now, now.Add(contracts.LocalCheckStartWindowSeconds*time.Second)).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			var originalID, originalDigest string
			if err := tx.QueryRow(ctx, `SELECT operation_id,request_digest FROM agent_control.operations WHERE tenant_id=$1 AND actor_id=$2 AND kind='local-check' AND command_id=$3`, scope.TenantID, scope.ActorID, commandID).Scan(&originalID, &originalDigest); err != nil {
				return err
			}
			if originalDigest != digest {
				return ErrConflict
			}
			result, err = lockLocal(ctx, tx, originalID)
			result.Existing = true
			return err
		}
		if err != nil {
			return err
		}
		c := LocalCheck{Scope: scope, ID: id, CommandID: commandID, RequestDigest: digest, AcceptedAt: now, QueueExpiresAt: now.Add(contracts.LocalCheckStartWindowSeconds * time.Second), WorkflowID: contracts.LocalCheckWorkflowIDPrefix + id, Namespace: namespace}
		c.Input = contracts.LocalCheckInputV1{SchemaVersion: 1, OperationID: id, RequestDigest: digest, FixtureID: fixtureID, FixtureText: fixture.FixtureText, ProfileRef: contracts.LocalCheckProfileRef, ProfileDigest: contracts.LocalCheckProfileDigest, ExecutionGeneration: "1", RecoveryGeneration: "1"}
		raw, err := canonical(c.Input)
		if err != nil || contracts.ValidateInput(c.Input) != nil {
			return ErrInvalid
		}
		_, err = tx.Exec(ctx, `INSERT INTO agent_control.local_checks(operation_id,fixture_id,profile_ref,profile_digest,input_digest,workflow_input,workflow_id,temporal_namespace,admission_execution_generation,admission_recovery_generation,start_state,start_window_expires_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,1,'start_unattempted',$9)`, id, fixtureID, c.Input.ProfileRef, c.Input.ProfileDigest, hash(raw), raw, c.WorkflowID, namespace, c.QueueExpiresAt)
		if err != nil {
			return err
		}
		body, err := c.IntakeBody()
		if err != nil {
			return err
		}
		key := fmt.Sprintf("obligations/%s/%s/%s/intake/%s", environment, now.UTC().Format("2006/01/02/15"), scope.TenantID, id)
		_, err = tx.Exec(ctx, `INSERT INTO agent_control.obligations(obligation_id,class,identity,tenant_id,operation_id,state,object_key,body_digest,prepared_at)
			VALUES($1,'intake',$2,$3,$2,'obligation_pending',$4,$5,$6)`, "intake-"+id, id, scope.TenantID, key, hash(body), now)
		if err != nil {
			return err
		}
		result, err = readLocal(ctx, tx, id)
		return err
	})
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == "23505" && pgerr.ConstraintName == "local_checks_single_unresolved_idx" {
			return LocalCheck{}, ErrLocalCapacity
		}
		return LocalCheck{}, err
	}
	return result, nil
}

func currentLocal(c LocalCheck) bool {
	return c.Input.ExecutionGeneration == strconv.FormatInt(c.ExecutionGeneration, 10) && c.Input.RecoveryGeneration == strconv.FormatInt(c.RecoveryGeneration, 10) && c.Input.ProfileDigest == contracts.LocalCheckProfileDigest
}

func (s *Store) ConfirmLocalIntake(ctx context.Context, expected LocalCheck, version string, authorizedUntil time.Time) (LocalCheck, error) {
	var result LocalCheck
	err := s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		c, err := lockLocal(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		if c.Scope != expected.Scope || c.InputDigest != expected.InputDigest || c.ObjectDigest != version {
			return ErrConflict
		}
		if c.Recorded {
			result = c
			return nil
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if !currentLocal(c) || c.CancelRequested || c.Resolved || !now.Before(c.QueueExpiresAt) || !now.Add(2*time.Second).Before(authorizedUntil) {
			return ErrRevision
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.obligations SET state='obligation_recorded',object_version=$2,recorded_at=$3 WHERE operation_id=$1 AND class='intake'`, c.ID, version, now)
		if err != nil {
			return err
		}
		if err := s.localProjection(ctx, tx, &c, "pending", "queued", "running", "not_required", "", nil, true); err != nil {
			return err
		}
		result, err = readLocal(ctx, tx, c.ID)
		return err
	})
	result.Existing = expected.Existing
	return result, err
}
