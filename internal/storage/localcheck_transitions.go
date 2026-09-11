package storage

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/jackc/pgx/v5"
)

// localProjection commits one lifecycle transition with its revision and
// sequence. Healthy re-observation does not manufacture another event.
func (s *Store) localProjection(ctx context.Context, tx pgx.Tx, c *LocalCheck, status, stage, control, cleanup, reason string, result *contracts.LocalCheckWorkflowResultV1, force bool) error {
	if !force && c.Status == status && c.Stage == stage && c.ControlState == control && c.CleanupState == cleanup {
		return nil
	}
	if c.Revision == math.MaxInt64 || c.NextSequence == math.MaxInt64 {
		return ErrRevision
	}
	revision := c.Revision
	if c.NextSequence > 1 {
		revision++
	}
	payload := map[string]any{"status": status, "businessStage": stage, "operationRevision": strconv.FormatInt(revision, 10), "cleanupState": cleanup}
	if reason != "" {
		payload["reasonCode"] = reason
	}
	if c.CancelRequested {
		payload["intendedTerminalOutcome"] = "canceled"
	}
	if result != nil {
		payload["localCheckResult"] = map[string]any{"fixtureId": result.FixtureID, "byteLength": result.ByteLength, "contentDigest": result.ContentDigest}
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	if s.eventSchema.Validate(map[string]any{"schemaVersion": 1, "operationId": c.ID, "eventSeq": strconv.FormatInt(c.NextSequence, 10), "type": "operation.lifecycle", "occurredAt": now.UTC().Format(time.RFC3339Nano), "payload": payload}) != nil {
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
		operation_revision=$6,next_event_seq=next_event_seq+1,updated_at=$7,expiry_reason=$8 WHERE operation_id=$1`, c.ID, status, stage, control, cleanup, revision, now, nullable(expiry))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO agent_control.operation_events(operation_id,event_seq,transition_id,event_type,body,occurred_at)
		VALUES($1,$2,$3,'operation.lifecycle',$4,$5)`, c.ID, c.NextSequence, "local-"+strconv.FormatInt(revision, 10), body, now)
	if err == nil {
		if events, ok := ctx.Value(localEventsKey{}).(*[]localEvent); ok {
			*events = append(*events, localEvent{c.ID, c.NextSequence})
		}
		c.Status = status
		c.Stage = stage
		c.ControlState = control
		c.CleanupState = cleanup
		c.Revision = revision
		c.NextSequence++
	}
	return err
}

// MarkLocalStart grants only a same-ID start inside the original window. The
// marker commits before the caller sends anything. A cancel racing the call
// therefore sees attempted work and retains its slot until terminal evidence.
func (s *Store) MarkLocalStart(ctx context.Context, expected LocalCheck, authorizedUntil time.Time) (LocalCheck, bool, error) {
	var result LocalCheck
	send := false
	err := s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		send = false
		c, err := lockLocal(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		if c.InputDigest != expected.InputDigest || !currentLocal(c) {
			return ErrRevision
		}
		if c.Resolved || c.CancelRequested || !c.Recorded || c.RunID != "" {
			result = c
			return nil
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if !now.Before(c.QueueExpiresAt) {
			if c.StartState == "start_unattempted" {
				err = s.finishUnattempted(ctx, tx, &c, "unattempted_expired", now)
			} else {
				err = s.localProjection(ctx, tx, &c, "blocked", c.Stage, "blocked", c.CleanupState, "LOCAL_START_UNRESOLVED", nil, false)
			}
			if err != nil {
				return err
			}
			result, err = readLocal(ctx, tx, c.ID)
			return err
		}
		if !now.Add(2 * time.Second).Before(authorizedUntil) {
			return ErrLeaseLost
		}
		if c.StartState == "start_unattempted" {
			_, err = tx.Exec(ctx, `UPDATE agent_control.local_checks SET start_state='start_attempted',start_attempted_at=$2,updated_at=$2 WHERE operation_id=$1`, c.ID, now)
			if err != nil {
				return err
			}
		}
		result, err = readLocal(ctx, tx, c.ID)
		send = err == nil
		return err
	})
	return result, send, err
}

func (s *Store) EstablishLocalRun(ctx context.Context, expected LocalCheck, runID string, input contracts.LocalCheckInputV1) error {
	if runID == "" {
		return ErrInvalid
	}
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		c, err := lockLocal(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		if c.Input != input || c.InputDigest != expected.InputDigest || c.WorkflowID != expected.WorkflowID || c.Namespace != expected.Namespace || !currentLocal(c) {
			return ErrRevision
		}
		if c.RunID != "" {
			if c.RunID != runID {
				return ErrConflict
			}
			return nil
		}
		if !c.Recorded || c.StartState != "start_attempted" || c.Resolved {
			return ErrRevision
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.local_checks SET run_id=$2,start_state='start_established',start_established_at=clock_timestamp(),updated_at=clock_timestamp() WHERE operation_id=$1`, c.ID, runID)
		return err
	})
}

func (s *Store) LocalObservationUnavailable(ctx context.Context, expected LocalCheck, reason string) error {
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		c, err := lockLocal(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		if c.Resolved || !c.Recorded {
			return nil
		}
		if c.InputDigest != expected.InputDigest {
			return ErrConflict
		}
		return s.localProjection(ctx, tx, &c, "blocked", c.Stage, "blocked", c.CleanupState, reason, nil, false)
	})
}

type LocalTerminal struct {
	WorkflowID, RunID string
	Input             contracts.LocalCheckInputV1
	Type              string
	EventID           int64
	At                time.Time
	Result            json.RawMessage
}

func (e LocalTerminal) digest() string {
	raw, _ := canonical(map[string]any{"workflowId": e.WorkflowID, "runId": e.RunID, "type": e.Type, "eventId": strconv.FormatInt(e.EventID, 10), "at": e.At.UTC().Format(time.RFC3339Nano), "resultDigest": hash(e.Result)})
	return hash(raw)
}

// AcceptLocalTerminal verifies the original binding again under rank 2. It
// retains the evidence fingerprint for failures as well as successful results.
// No terminal evidence is inferred from an RPC error or a missing history.
func (s *Store) AcceptLocalTerminal(ctx context.Context, e LocalTerminal) error {
	if e.RunID == "" || e.EventID < 1 || e.At.IsZero() {
		return ErrInvalid
	}
	status, reason := "", ""
	switch e.Type {
	case "completed":
		status = "succeeded"
	case "failed", "terminated":
		status = "failed"
		reason = "LOCAL_EXECUTION_FAILED"
	case "timed_out":
		status = "expired"
		reason = "LOCAL_DEADLINE_EXPIRED"
	case "canceled":
		status = "canceled"
	default:
		return ErrInvalid
	}
	fingerprint := e.digest()
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		c, err := lockLocal(ctx, tx, e.Input.OperationID)
		if err != nil {
			return err
		}
		if c.WorkflowID != e.WorkflowID || c.RunID != e.RunID || c.Input != e.Input || !currentLocal(c) {
			return ErrRevision
		}
		if c.Resolved {
			if c.TerminalDigest != fingerprint {
				return ErrConflict
			}
			return nil
		}
		outcome, why := status, reason
		var result *contracts.LocalCheckWorkflowResultV1
		if e.Type == "completed" {
			var value contracts.LocalCheckWorkflowResultV1
			fixture, fixtureErr := contracts.Fixture(c.Input.FixtureID)
			if json.Unmarshal(e.Result, &value) != nil || fixtureErr != nil || value.OperationID != c.ID || value.RequestDigest != c.RequestDigest || value.FixtureID != c.Input.FixtureID || value.ProfileDigest != c.Input.ProfileDigest || value.ExecutionGeneration != c.Input.ExecutionGeneration || value.RecoveryGeneration != c.Input.RecoveryGeneration || value.ByteLength != fixture.ByteLength || value.ContentDigest != fixture.ContentDigest {
				outcome, why = "failed", "LOCAL_RESULT_INVALID"
			} else {
				result = &value
			}
		}
		if c.CancelRequested {
			outcome = "canceled"
			why = ""
			result = nil
		}
		var length, digest any
		if result != nil {
			length = result.ByteLength
			digest = result.ContentDigest
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.local_checks SET resolved=true,accepted_run_id=$2,accepted_terminal_at=$3,accepted_terminal_event_id=$4,
			accepted_terminal_type=$5,accepted_terminal_digest=$6,result_byte_length=$7,result_content_digest=$8,updated_at=clock_timestamp() WHERE operation_id=$1`, c.ID, e.RunID, e.At, e.EventID, e.Type, fingerprint, length, digest)
		if err != nil {
			return err
		}
		stage := outcome
		if outcome == "succeeded" {
			stage = "ready"
		}
		return s.localProjection(ctx, tx, &c, outcome, stage, "running", "complete", why, result, true)
	})
}

func (s *Store) ObserveLocalRunning(ctx context.Context, expected LocalCheck) error {
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		c, err := lockLocal(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		if c.Resolved {
			return nil
		}
		if !currentLocal(c) || c.RunID == "" || c.RunID != expected.RunID {
			return ErrRevision
		}
		cleanup := "not_required"
		if c.CancelRequested {
			cleanup = "pending"
		}
		return s.localProjection(ctx, tx, &c, "running", "validating", "running", cleanup, "", nil, false)
	})
}

func (s *Store) finishUnattempted(ctx context.Context, tx pgx.Tx, c *LocalCheck, kind string, now time.Time) error {
	if c.StartState != "start_unattempted" || c.Resolved {
		return ErrRevision
	}
	status, reason := "canceled", ""
	if kind == "unattempted_expired" {
		status, reason = "expired", "LOCAL_DEADLINE_EXPIRED"
	}
	fingerprint := (LocalTerminal{WorkflowID: c.WorkflowID, Type: kind, At: now}).digest()
	_, err := tx.Exec(ctx, `UPDATE agent_control.local_checks SET resolved=true,start_state=CASE WHEN $3='unattempted_expired' THEN 'start_expired' ELSE start_state END,accepted_terminal_at=$2,accepted_terminal_event_id=0,
		accepted_terminal_type=$3,accepted_terminal_digest=$4,updated_at=$2 WHERE operation_id=$1`, c.ID, now, kind, fingerprint)
	if err != nil {
		return err
	}
	return s.localProjection(ctx, tx, c, status, status, "running", "complete", reason, nil, true)
}

// CancelLocalCheck uses no admission/capacity gate. The caller gives it a
// reserved database pool, so new intake cannot consume its connection headroom.
func (s *Store) CancelLocalCheck(ctx context.Context, scope Scope, id, commandID, reason string, expectedRevision uint64, authorizedUntil time.Time) (LocalCheck, bool, error) {
	if !s.validIDs(scope.ActorID, scope.TenantID, id, commandID) || expectedRevision < 1 || expectedRevision > math.MaxInt64 {
		return LocalCheck{}, false, ErrInvalid
	}
	raw, _ := canonical(map[string]any{"operationId": id, "expectedOperationRevision": strconv.FormatUint(expectedRevision, 10), "reasonCode": reason})
	digest := hash(raw)
	var result LocalCheck
	coalesced := false
	err := s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		c, err := lockLocal(ctx, tx, id)
		if err != nil {
			return err
		}
		if c.Scope != scope {
			return ErrNotFound
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if !now.Add(2 * time.Second).Before(authorizedUntil) {
			return ErrLeaseLost
		}
		if c.CancelCommandID == commandID && c.CancelDigest != digest {
			return ErrConflict
		}
		if c.Resolved && c.CancelCommandID == "" {
			_, err = tx.Exec(ctx, `UPDATE agent_control.local_checks SET cancel_command_id=$2,cancel_request_digest=$3,cancel_acknowledged_at=$4 WHERE operation_id=$1`, id, commandID, digest, now)
			if err != nil {
				return err
			}
			c, err = readLocal(ctx, tx, id)
			if err != nil {
				return err
			}
		}
		if c.CancelRequested || c.Resolved {
			result = c
			coalesced = true
			return nil
		}
		if c.Revision != int64(expectedRevision) {
			return ErrRevision
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.operations SET cancel_requested=true,intended_terminal_outcome='canceled' WHERE operation_id=$1`, id)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.local_checks SET cancel_command_id=$2,cancel_request_digest=$3,cancel_acknowledged_at=$4 WHERE operation_id=$1`, id, commandID, digest, now)
		if err != nil {
			return err
		}
		c.CancelRequested = true
		if c.StartState == "start_unattempted" {
			err = s.finishUnattempted(ctx, tx, &c, "unattempted_canceled", now)
		} else {
			err = s.localProjection(ctx, tx, &c, c.Status, c.Stage, c.ControlState, "pending", "", nil, true)
		}
		if err != nil {
			return err
		}
		result, err = readLocal(ctx, tx, id)
		return err
	})
	return result, coalesced, err
}

// ExpirePreparedLocalCheck also covers a crash before independent persistence.
// It never starts or acknowledges that operation after the original deadline.
func (s *Store) ExpirePreparedLocalCheck(ctx context.Context, id string) error {
	return s.transactLocal(ctx, func(ctx context.Context, tx pgx.Tx) error {
		c, err := lockLocal(ctx, tx, id)
		if err != nil {
			return err
		}
		if c.Resolved {
			return nil
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if c.StartState != "start_unattempted" || now.Before(c.QueueExpiresAt) {
			return errors.New("local start cannot expire without evidence")
		}
		return s.finishUnattempted(ctx, tx, &c, "unattempted_expired", now)
	})
}
