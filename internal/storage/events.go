package storage

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// StepEvent corresponds to the existing RecordStepEvent fields. Payload is the
// canonical per-type public payload, checked against retained schemas. These are
// internal storage arguments; no additional transport contract is introduced.
type StepEvent struct {
	Scope
	OperationID, TransitionID, StepID, ActionID, ActionVersion, Type string
	DefinitionSegment, Visit                                         int64
	Payload                                                          json.RawMessage
}
type RecordedEvent struct {
	Sequence        int64
	StepExecutionID string
	Existing        bool
}

func (s *Store) checkStepEvent(event StepEvent) (map[string]any, []byte, error) {
	if !s.validIDs(event.TenantID, event.ActorID, event.OperationID, event.TransitionID, event.StepID) || event.DefinitionSegment < 1 || event.Visit < 1 {
		return nil, nil, ErrInvalid
	}
	if event.Type != "step.started" && event.Type != "step.waiting" && event.Type != "step.finished" {
		return nil, nil, ErrInvalid
	}
	data, body, err := boundedObject(event.Payload, 32768)
	if err != nil {
		return nil, nil, err
	}
	if s.eventSchema.Validate(map[string]any{"schemaVersion": 1, "operationId": event.OperationID, "eventSeq": "1", "type": event.Type, "occurredAt": "2026-09-10T00:00:00Z", "payload": data}) != nil {
		return nil, nil, ErrInvalid
	}
	if data["definitionSegment"] != strconv.FormatInt(event.DefinitionSegment, 10) {
		return nil, nil, ErrInvalid
	}
	if event.Type == "step.started" && (data["stepId"] != event.StepID || data["visit"] != strconv.FormatInt(event.Visit, 10) || data["actionId"] != event.ActionID || data["actionVersion"] != event.ActionVersion) {
		return nil, nil, ErrInvalid
	}
	// The canonical value schema expresses the decimal representation; the
	// storage boundary additionally proves every public uint64 fits its bound.
	if !boundedCounters(data) {
		return nil, nil, ErrInvalid
	}
	return data, body, nil
}

func boundedCounters(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			if key == "sizeBytes" || key == "observationSequence" || key == "definitionSegment" || key == "visit" {
				text, ok := item.(string)
				if !ok {
					return false
				}
				if _, err := strconv.ParseUint(text, 10, 64); err != nil {
					return false
				}
			}
			if !boundedCounters(item) {
				return false
			}
		}
	case []any:
		for _, item := range v {
			if !boundedCounters(item) {
				return false
			}
		}
	}
	return true
}

// RecordStepEvent takes rank 2 before rank 7. Projection, operation revision and
// sequence allocation commit with the immutable event. It never selects a next
// business step or changes financial/dispatch authority.
func (s *Store) RecordStepEvent(ctx context.Context, event StepEvent) (RecordedEvent, error) {
	data, body, err := s.checkStepEvent(event)
	if err != nil {
		return RecordedEvent{}, err
	}
	sid := data["stepExecutionId"].(string)
	// Resolve the immutable activation before any ranked lock. Recheck its
	// operation binding under rank 2 before applying a new transition.
	var activationID, definitionDigest string
	err = s.pool.QueryRow(ctx, `SELECT o.activation_id,a.definition_digest FROM agent_control.operations o
		JOIN definition_contract.activations a ON a.activation_id=o.activation_id
		WHERE o.operation_id=$1 AND o.tenant_id=$2 AND o.actor_id=$3`, event.OperationID, event.TenantID, event.ActorID).Scan(&activationID, &definitionDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return RecordedEvent{}, ErrNotFound
	}
	if err != nil {
		return RecordedEvent{}, err
	}
	var result RecordedEvent
	err = s.transact(ctx, func(tx pgx.Tx) error {
		result = RecordedEvent{}
		var next, revision, segment int64
		var currentActivation string
		err := tx.QueryRow(ctx, `SELECT next_event_seq,operation_revision,definition_segment,activation_id FROM agent_control.operations
			WHERE operation_id=$1 AND tenant_id=$2 AND actor_id=$3 FOR UPDATE`, event.OperationID, event.TenantID, event.ActorID).Scan(&next, &revision, &segment, &currentActivation)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		// Replays are resolved before current segment/status checks so the same
		// transition retains its original result after later state advances.
		var same bool
		err = tx.QueryRow(ctx, `SELECT event_seq, event_type=$3 AND body=$4::jsonb FROM agent_control.operation_events WHERE operation_id=$1 AND transition_id=$2`, event.OperationID, event.TransitionID, event.Type, body).Scan(&result.Sequence, &same)
		if err == nil {
			if !same {
				return ErrConflict
			}
			var matches bool
			err = tx.QueryRow(ctx, `SELECT definition_segment=$3 AND step_id=$4 AND visit=$5 AND action_id=$6 AND action_version=$7
				FROM agent_control.step_executions WHERE operation_id=$1 AND step_execution_id=$2`, event.OperationID, sid, event.DefinitionSegment, event.StepID, event.Visit, event.ActionID, event.ActionVersion).Scan(&matches)
			if err != nil {
				return err
			}
			if !matches {
				return ErrConflict
			}
			result.StepExecutionID = sid
			result.Existing = true
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if segment != event.DefinitionSegment || next == math.MaxInt64 || revision == math.MaxInt64 {
			return ErrRevision
		}
		if currentActivation != activationID || (event.Type == "step.started" && data["definitionDigest"] != definitionDigest) {
			return ErrRevision
		}
		var occurred time.Time
		if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&occurred); err != nil {
			return err
		}
		if event.Type == "step.started" {
			inputs, _ := json.Marshal(data["inputRefs"])
			profile, _ := data["profileRef"].(string)
			tag, err := tx.Exec(ctx, `INSERT INTO agent_control.step_executions
				(step_execution_id,operation_id,definition_segment,definition_digest,step_id,visit,action_id,action_version,status,started_at,input_refs,profile_ref,covered_seq)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'started',$9,$10,$11,$12) ON CONFLICT DO NOTHING`, sid, event.OperationID, segment, data["definitionDigest"], event.StepID, event.Visit, event.ActionID, event.ActionVersion, occurred, inputs, nullable(profile), next)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return ErrRevision
			}
		} else {
			var status string
			var started time.Time
			err = tx.QueryRow(ctx, `SELECT status,started_at FROM agent_control.step_executions WHERE operation_id=$1 AND step_execution_id=$2
				AND definition_segment=$3 AND step_id=$4 AND visit=$5 AND action_id=$6 AND action_version=$7 FOR UPDATE`, event.OperationID, sid, segment, event.StepID, event.Visit, event.ActionID, event.ActionVersion).Scan(&status, &started)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			if status == "finished" {
				return ErrRevision
			}
			if event.Type == "step.waiting" {
				_, err = tx.Exec(ctx, `UPDATE agent_control.step_executions SET status='waiting',covered_seq=$3 WHERE operation_id=$1 AND step_execution_id=$2`, event.OperationID, sid, next)
			} else {
				ended, parseErr := time.Parse(time.RFC3339Nano, data["endedAt"].(string))
				if parseErr != nil || ended.Before(started) {
					return ErrInvalid
				}
				outputs, _ := json.Marshal(data["outputRefs"])
				_, err = tx.Exec(ctx, `UPDATE agent_control.step_executions SET status='finished',outcome=$3,output_refs=$4,ended_at=$5,covered_seq=$6 WHERE operation_id=$1 AND step_execution_id=$2`, event.OperationID, sid, data["outcome"], outputs, ended, next)
			}
			if err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.operations SET next_event_seq=next_event_seq+1,operation_revision=operation_revision+1,current_step_id=$2,current_step_execution_id=$3,updated_at=$4 WHERE operation_id=$1`, event.OperationID, event.StepID, sid, occurred)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO agent_control.operation_events (operation_id,event_seq,transition_id,event_type,body,occurred_at) VALUES ($1,$2,$3,$4,$5,$6)`, event.OperationID, next, event.TransitionID, event.Type, body, occurred)
		if err != nil {
			return err
		}
		result = RecordedEvent{Sequence: next, StepExecutionID: sid}
		return nil
	})
	if err != nil {
		return RecordedEvent{}, err
	}
	return result, nil
}
