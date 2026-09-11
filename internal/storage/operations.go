package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

// Scope must come from a trusted caller; storage rechecks exact ownership on
// every operation lookup, including duplicate commands and transitions.
type Scope struct{ TenantID, ActorID string }
type OperationCommand struct {
	Scope
	Kind, IntakeSource, CommandID, ActivationID string
	Request                                     json.RawMessage
}
type Operation struct {
	ID         string
	AcceptedAt time.Time
	Existing   bool
}

// PersistOperation stores command identity and the initial projection/event.
// It is a persistence primitive, not admission: no RPC calls it in CONTROL-02.
// The parent test tooling invokes it with explicitly synthetic, unfunded input.
// Future admission must compose the separate funding/intake/obligation gates.
func (s *Store) PersistOperation(ctx context.Context, command OperationCommand) (Operation, error) {
	if !s.validIDs(command.TenantID, command.ActorID, command.CommandID, command.ActivationID) || !slices.Contains([]string{"generation", "preview", "release", "re-certification"}, command.Kind) || !slices.Contains([]string{"ui", "api", "platform"}, command.IntakeSource) {
		return Operation{}, ErrInvalid
	}
	request, _, err := boundedObject(command.Request, 65536)
	if err != nil {
		return Operation{}, err
	}
	semantic, err := canonical(map[string]any{"activationId": command.ActivationID, "intakeSource": command.IntakeSource, "request": request})
	if err != nil {
		return Operation{}, err
	}
	digest, id := hash(semantic), "op-"+rand.Text()
	stage := map[string]string{"generation": "admission_pending", "preview": "queued", "release": "certifying", "re-certification": "certifying"}[command.Kind]
	payload, _ := canonical(map[string]any{"status": "pending", "businessStage": stage, "operationRevision": "1", "cleanupState": "not_required"})
	var result Operation
	err = s.transact(ctx, func(tx pgx.Tx) error {
		result = Operation{}
		// Rank 1 explicitly precedes the operation insert's foreign-key check.
		var activation string
		if err := tx.QueryRow(ctx, `SELECT activation_id FROM definition_contract.activations WHERE activation_id=$1 FOR KEY SHARE`, command.ActivationID).Scan(&activation); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		err := tx.QueryRow(ctx, `INSERT INTO agent_control.operations
			(operation_id,tenant_id,actor_id,kind,intake_source,command_id,request_digest,activation_id,funding_state,public_status,business_stage,control_state,cleanup_state,accepted_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'quote_missing','pending',$9,'running','not_required',clock_timestamp())
			ON CONFLICT (tenant_id,actor_id,kind,command_id) DO NOTHING RETURNING operation_id,accepted_at`, id, command.TenantID, command.ActorID, command.Kind, command.IntakeSource, command.CommandID, digest, command.ActivationID, stage).Scan(&result.ID, &result.AcceptedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			var original string
			err = tx.QueryRow(ctx, `SELECT operation_id,accepted_at,request_digest FROM agent_control.operations WHERE tenant_id=$1 AND actor_id=$2 AND kind=$3 AND command_id=$4`, command.TenantID, command.ActorID, command.Kind, command.CommandID).Scan(&result.ID, &result.AcceptedAt, &original)
			if err != nil {
				return err
			}
			if original != digest {
				return ErrConflict
			}
			result.Existing = true
			return nil
		}
		if err != nil {
			return err
		}
		// The inserted rank-2 row is held before its rank-7 event FK/index locks.
		_, err = tx.Exec(ctx, `INSERT INTO agent_control.operation_events (operation_id,event_seq,transition_id,event_type,body,occurred_at)
			VALUES ($1,1,$2,'operation.lifecycle',$3,$4)`, id, "intake-"+rand.Text(), payload, result.AcceptedAt)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.operations SET next_event_seq=2 WHERE operation_id=$1`, id)
		return err
	})
	if err != nil {
		return Operation{}, err
	}
	return result, nil
}
