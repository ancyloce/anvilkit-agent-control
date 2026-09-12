package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Artifact transfers (DD-02 #artifact-adapter-mode, development plan S1-T05,
// 2026-09-12). A transfer is a durable `artifact-transfer` effect registered at
// rank 6 after the operation (rank 2), its current Attempt and physical
// instance (rank 3) have been verified, with the business-write obligation
// record of the recovery inventory prepared in the same transaction. The
// capability is handed out only after the inventory object is persisted
// outside every lock and the same facts are rechecked; a finalization is
// accepted only against the bytes the object store actually holds and only
// while the same Attempt, instance, generation, lease and deadline are current.
// These methods contain SQL only; the adapter performs every storage call.

var ErrTransferExpired = errors.New("FAILED_PRECONDITION: artifact transfer capability expired")
var ErrTransferState = errors.New("FAILED_PRECONDITION: artifact transfer is not in a state that permits this step")

// TransferBinding is the scope a capability is bound to. Every field is checked
// against current records when the transfer is issued and again when it is
// finalized; the binding alone never grants anything. The recovery generation
// is the operation's deployment fence (DD-02 #s-4-7-1-1): an identity issued
// before a recovery rotated it obtains no new capability and completes no
// acceptance, exactly as a superseded execution generation.
type TransferBinding struct {
	Scope
	OperationID, AttemptID, InstanceID      string
	ExecutionGeneration, RecoveryGeneration int64
}

// TransferRequest is the adapter's request to issue one transfer.
type TransferRequest struct {
	TransferBinding
	CommandID, Operation, Kind, RefID, Purpose, ObjectKey string
	DeclaredSizeBytes                                     uint64
	DeclaredContentDigest                                 string
	SecretDigest                                          string
}

// Transfer is the durable projection of one artifact-transfer effect.
type Transfer struct {
	TransferBinding
	EffectID, CommandID, RequestDigest, Operation, Kind, RefID, Purpose, ObjectKey string
	StepExecutionID                                                                string
	ByteCeiling, DeclaredSizeBytes                                                 uint64
	DeclaredContentDigest, SecretDigest                                            string
	ExpiresAt                                                                      time.Time
	DispatchState, ObligationState, ObligationKey, ObligationDigest                string
	ObligationVersion, ReceiptDigest                                               string
	Receipt                                                                        json.RawMessage
	Existing                                                                       bool
}

// TransferState maps the effect's dispatch state onto the public transferState
// vocabulary.
func (t Transfer) TransferState(now time.Time) string {
	switch t.DispatchState {
	case "registered":
		return "issued"
	case "dispatched":
		if !now.Before(t.ExpiresAt) {
			return "expired"
		}
		return "in_progress"
	case "confirmed":
		return "finalized"
	case "canceled", "superseded":
		return "rejected"
	}
	return "unknown"
}

// transferCapabilityLifetime is the DD-02 development default: a capability
// lives for the remaining Attempt deadline, capped at 300 seconds.
const transferCapabilityLifetime = 300 * time.Second

// currentAttempt reads the rank-3 facts a transfer depends on under a share
// lock (attempt, then physical instance) and reports whether the binding is
// current. The operation row must already be locked at rank 2.
type currentAttempt struct {
	stepExecutionID string
	deadline        time.Time
	current         bool
	reason          string
}

func readCurrentAttempt(ctx context.Context, tx pgx.Tx, b TransferBinding, now time.Time) (currentAttempt, error) {
	var a currentAttempt
	var tenant string
	var cancelRequested bool
	var operationGeneration, recoveryGeneration int64
	var leaseExpires *time.Time
	err := tx.QueryRow(ctx, `SELECT tenant_id,cancel_requested,execution_generation,recovery_generation,lease_expires_at FROM agent_control.operations WHERE operation_id=$1 FOR UPDATE`, b.OperationID).Scan(&tenant, &cancelRequested, &operationGeneration, &recoveryGeneration, &leaseExpires)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	if err != nil {
		return a, err
	}
	if tenant != b.TenantID {
		return a, ErrNotFound
	}
	var attemptState string
	var attemptGeneration int64
	err = tx.QueryRow(ctx, `SELECT step_execution_id,execution_generation,deadline,state FROM agent_control.attempts WHERE attempt_id=$1 AND operation_id=$2 FOR SHARE`, b.AttemptID, b.OperationID).Scan(&a.stepExecutionID, &attemptGeneration, &a.deadline, &attemptState)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	if err != nil {
		return a, err
	}
	var instanceState string
	var instanceGeneration int64
	err = tx.QueryRow(ctx, `SELECT execution_generation,state FROM agent_control.physical_instances WHERE instance_id=$1 AND attempt_id=$2 AND operation_id=$3 FOR SHARE`, b.InstanceID, b.AttemptID, b.OperationID).Scan(&instanceGeneration, &instanceState)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	if err != nil {
		return a, err
	}
	switch {
	case cancelRequested:
		a.reason = "operation fenced"
	case operationGeneration != b.ExecutionGeneration || attemptGeneration != b.ExecutionGeneration || instanceGeneration != b.ExecutionGeneration:
		a.reason = "execution generation superseded"
	case recoveryGeneration != b.RecoveryGeneration:
		a.reason = "recovery generation superseded"
	case attemptState != "registered" && attemptState != "running":
		a.reason = "attempt not current"
	case instanceState != "registered" && instanceState != "running":
		a.reason = "physical instance not current"
	case leaseExpires != nil && !now.Before(*leaseExpires):
		a.reason = "lease expired"
	case !now.Before(a.deadline):
		a.reason = "attempt deadline exhausted"
	default:
		a.current = true
	}
	return a, nil
}

func (t *Transfer) subjectRef() map[string]any {
	return map[string]any{"transferId": t.EffectID, "tenantId": t.TenantID, "attemptId": t.AttemptID, "instanceId": t.InstanceID,
		"executionGeneration": fmt.Sprint(t.ExecutionGeneration), "recoveryGeneration": fmt.Sprint(t.RecoveryGeneration), "operation": t.Operation, "kind": t.Kind, "refId": t.RefID, "purpose": t.Purpose,
		"objectKey": t.ObjectKey, "byteCeiling": fmt.Sprint(t.ByteCeiling), "expiresAt": t.ExpiresAt.UTC().Format(time.RFC3339Nano),
		"declaredSizeBytes": fmt.Sprint(t.DeclaredSizeBytes), "declaredContentDigest": t.DeclaredContentDigest, "secretDigest": t.SecretDigest}
}

// ObligationBody is the business-write inventory record of the transfer: the
// minimum DD-02 #recovery-discovery content, never the capability or a receipt.
func (t Transfer) ObligationBody() ([]byte, error) {
	return canonical(map[string]any{"schemaVersion": 1, "class": "business-write", "effectId": t.EffectID, "operationId": t.OperationID, "tenantId": t.TenantID,
		"commandId": t.CommandID, "kind": "artifact-transfer", "subjectRef": map[string]any{"operation": t.Operation, "kind": t.Kind, "refId": t.RefID, "objectKey": t.ObjectKey},
		"requestDigest": t.RequestDigest, "expectedExecutionGeneration": fmt.Sprint(t.ExecutionGeneration), "expectedRecoveryGeneration": fmt.Sprint(t.RecoveryGeneration), "attemptId": t.AttemptID, "instanceId": t.InstanceID,
		"expiresAt": t.ExpiresAt.UTC().Format(time.RFC3339Nano)})
}

func readTransfer(ctx context.Context, q localQuerier, effectID string) (Transfer, error) {
	var t Transfer
	var subject []byte
	var step, version, receiptDigest *string
	var receipt []byte
	err := q.QueryRow(ctx, `SELECT e.effect_id,e.operation_id,o.tenant_id,o.actor_id,e.step_execution_id,e.command_id,e.request_digest,e.expected_execution_generation,
		e.dispatch_state,e.obligation_state,e.obligation_object_version,e.subject_ref,e.receipt_ref,e.receipt_digest,b.object_key,b.body_digest
		FROM agent_control.effects e JOIN agent_control.operations o USING(operation_id)
		JOIN agent_control.obligations b ON b.class='business-write' AND b.identity=e.effect_id
		WHERE e.effect_id=$1 AND e.kind='artifact-transfer'`, effectID).Scan(
		&t.EffectID, &t.OperationID, &t.TenantID, &t.ActorID, &step, &t.CommandID, &t.RequestDigest, &t.ExecutionGeneration,
		&t.DispatchState, &t.ObligationState, &version, &subject, &receipt, &receiptDigest, &t.ObligationKey, &t.ObligationDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrNotFound
	}
	if err != nil {
		return t, err
	}
	if step != nil {
		t.StepExecutionID = *step
	}
	if version != nil {
		t.ObligationVersion = *version
	}
	if receiptDigest != nil {
		t.ReceiptDigest = *receiptDigest
	}
	t.Receipt = receipt
	var s struct {
		AttemptID, InstanceID, ExecutionGeneration, RecoveryGeneration, Operation, Kind, RefID, Purpose, ObjectKey string
		ByteCeiling, ExpiresAt, DeclaredSizeBytes, DeclaredContentDigest, SecretDigest                             string
	}
	if err := json.Unmarshal(subject, &s); err != nil {
		return t, ErrInvalid
	}
	t.AttemptID, t.InstanceID, t.Operation, t.Kind, t.RefID, t.Purpose, t.ObjectKey = s.AttemptID, s.InstanceID, s.Operation, s.Kind, s.RefID, s.Purpose, s.ObjectKey
	t.DeclaredContentDigest, t.SecretDigest = s.DeclaredContentDigest, s.SecretDigest
	if _, err := fmt.Sscan(s.RecoveryGeneration, &t.RecoveryGeneration); err != nil || t.RecoveryGeneration < 1 {
		return t, ErrInvalid
	}
	if _, err := fmt.Sscan(s.ByteCeiling, &t.ByteCeiling); err != nil {
		return t, ErrInvalid
	}
	if _, err := fmt.Sscan(s.DeclaredSizeBytes, &t.DeclaredSizeBytes); err != nil {
		return t, ErrInvalid
	}
	if t.ExpiresAt, err = time.Parse(time.RFC3339Nano, s.ExpiresAt); err != nil {
		return t, ErrInvalid
	}
	return t, nil
}

func lockTransfer(ctx context.Context, tx pgx.Tx, b TransferBinding, effectID string, now time.Time) (Transfer, currentAttempt, error) {
	// Rank 2 (operation, FOR UPDATE) and rank 3 (attempt, instance) first,
	// then the rank-6 effect and its obligation row.
	attempt, err := readCurrentAttempt(ctx, tx, b, now)
	if err != nil {
		return Transfer{}, attempt, err
	}
	for _, query := range []string{
		`SELECT effect_id FROM agent_control.effects WHERE effect_id=$1 AND operation_id=$2 AND kind='artifact-transfer' FOR UPDATE`,
		`SELECT identity FROM agent_control.obligations WHERE class='business-write' AND identity=$1 AND operation_id=$2 FOR UPDATE`,
	} {
		var found string
		if err := tx.QueryRow(ctx, query, effectID, b.OperationID).Scan(&found); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return Transfer{}, attempt, ErrNotFound
			}
			return Transfer{}, attempt, err
		}
	}
	t, err := readTransfer(ctx, tx, effectID)
	if err != nil {
		return t, attempt, err
	}
	if t.TenantID != b.TenantID || t.AttemptID != b.AttemptID || t.InstanceID != b.InstanceID || t.ExecutionGeneration != b.ExecutionGeneration || t.RecoveryGeneration != b.RecoveryGeneration {
		return Transfer{}, attempt, ErrNotFound
	}
	return t, attempt, nil
}

// PrepareArtifactTransfer is phase 1 of issuing a transfer: verify the binding
// against current records, register the effect and prepare its obligation
// record. It hands out no capability and performs no storage I/O; the same
// scoped command replays to the original transfer.
func (s *Store) PrepareArtifactTransfer(ctx context.Context, r TransferRequest, ceiling uint64, environment string) (Transfer, error) {
	if !s.validIDs(r.TenantID, r.ActorID, r.OperationID, r.AttemptID, r.InstanceID, r.CommandID, r.RefID) || r.ExecutionGeneration < 1 || r.RecoveryGeneration < 1 || (r.Operation != "read" && r.Operation != "write") || !localEnvironment.MatchString(environment) || r.ObjectKey == "" || !digestShape(r.SecretDigest) || ceiling == 0 {
		return Transfer{}, ErrInvalid
	}
	if r.DeclaredContentDigest != "" && !digestShape(r.DeclaredContentDigest) {
		return Transfer{}, ErrInvalid
	}
	semantic, err := canonical(map[string]any{"operation": r.Operation, "kind": r.Kind, "refId": r.RefID, "purpose": r.Purpose, "objectKey": r.ObjectKey, "attemptId": r.AttemptID, "instanceId": r.InstanceID, "executionGeneration": fmt.Sprint(r.ExecutionGeneration), "recoveryGeneration": fmt.Sprint(r.RecoveryGeneration), "declaredSizeBytes": fmt.Sprint(r.DeclaredSizeBytes), "declaredContentDigest": r.DeclaredContentDigest})
	if err != nil {
		return Transfer{}, err
	}
	digest := hash(semantic)
	var result Transfer
	err = s.transact(ctx, func(tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		attempt, err := readCurrentAttempt(ctx, tx, r.TransferBinding, now)
		if err != nil {
			return err
		}
		var existingID, existingDigest string
		err = tx.QueryRow(ctx, `SELECT effect_id,request_digest FROM agent_control.effects WHERE operation_id=$1 AND command_id=$2`, r.OperationID, r.CommandID).Scan(&existingID, &existingDigest)
		if err == nil {
			if existingDigest != digest {
				return ErrConflict
			}
			result, _, err = lockTransfer(ctx, tx, r.TransferBinding, existingID, now)
			if err != nil {
				return err
			}
			if result.DispatchState == "registered" {
				// Never dispatched, so no holder exists: the replay's fresh
				// secret replaces the unreachable one. The request digest and
				// the obligation body exclude the secret, so identity holds.
				if _, err := tx.Exec(ctx, `UPDATE agent_control.effects SET subject_ref=jsonb_set(subject_ref,'{secretDigest}',to_jsonb($2::text)) WHERE effect_id=$1`, existingID, r.SecretDigest); err != nil {
					return err
				}
				result.SecretDigest = r.SecretDigest
			}
			result.Existing = true
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if !attempt.current {
			return fmt.Errorf("%w: %s", ErrRevision, attempt.reason)
		}
		expires := now.Add(transferCapabilityLifetime)
		if attempt.deadline.Before(expires) {
			expires = attempt.deadline
		}
		t := Transfer{TransferBinding: r.TransferBinding, EffectID: "transfer-" + rand.Text(), CommandID: r.CommandID, RequestDigest: digest, Operation: r.Operation, Kind: r.Kind, RefID: r.RefID, Purpose: r.Purpose,
			ObjectKey: r.ObjectKey, StepExecutionID: attempt.stepExecutionID, ByteCeiling: ceiling, DeclaredSizeBytes: r.DeclaredSizeBytes, DeclaredContentDigest: r.DeclaredContentDigest, SecretDigest: r.SecretDigest, ExpiresAt: expires,
			DispatchState: "registered", ObligationState: "obligation_pending"}
		subject, err := canonical(t.subjectRef())
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO agent_control.effects(effect_id,operation_id,step_execution_id,command_id,kind,subject_ref,request_digest,expected_execution_generation,dispatch_state,obligation_state)
			VALUES($1,$2,$3,$4,'artifact-transfer',$5,$6,$7,'registered','obligation_pending')`, t.EffectID, t.OperationID, t.StepExecutionID, t.CommandID, subject, digest, t.ExecutionGeneration)
		if err != nil {
			return err
		}
		body, err := t.ObligationBody()
		if err != nil {
			return err
		}
		key := fmt.Sprintf("obligations/%s/%s/%s/business-write/%s", environment, now.UTC().Format("2006/01/02/15"), t.TenantID, t.EffectID)
		_, err = tx.Exec(ctx, `INSERT INTO agent_control.obligations(obligation_id,class,identity,tenant_id,operation_id,state,object_key,body_digest,prepared_at)
			VALUES($1,'business-write',$2,$3,$4,'obligation_pending',$5,$6,$7)`, "business-write-"+t.EffectID, t.EffectID, t.TenantID, t.OperationID, key, hash(body), now)
		if err != nil {
			return err
		}
		result, err = readTransfer(ctx, tx, t.EffectID)
		return err
	})
	if err != nil {
		return Transfer{}, err
	}
	return result, nil
}

// ConfirmArtifactTransfer is phase 4: with the inventory object persisted at
// `version`, recheck the binding and commit the obligation as recorded and the
// effect as dispatched, which is the moment the capability may be handed out.
// A replay of an already dispatched transfer returns it unchanged.
func (s *Store) ConfirmArtifactTransfer(ctx context.Context, expected Transfer, version string) (Transfer, error) {
	if version != expected.ObligationDigest {
		return Transfer{}, ErrConflict
	}
	var result Transfer
	err := s.transact(ctx, func(tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		t, attempt, err := lockTransfer(ctx, tx, expected.TransferBinding, expected.EffectID, now)
		if err != nil {
			return err
		}
		if t.RequestDigest != expected.RequestDigest || t.ObligationDigest != expected.ObligationDigest {
			return ErrConflict
		}
		if t.DispatchState != "registered" {
			result = t
			return nil
		}
		if !attempt.current || !now.Before(t.ExpiresAt) {
			return fmt.Errorf("%w: %s", ErrRevision, attempt.reason)
		}
		if _, err := tx.Exec(ctx, `UPDATE agent_control.effects SET dispatch_state='dispatched',obligation_state='obligation_recorded',obligation_object_version=$2,dispatched_at=$3 WHERE effect_id=$1`, t.EffectID, version, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE agent_control.obligations SET state='obligation_recorded',object_version=$2,recorded_at=$3 WHERE class='business-write' AND identity=$1`, t.EffectID, version, now); err != nil {
			return err
		}
		result, err = readTransfer(ctx, tx, t.EffectID)
		return err
	})
	if err != nil {
		return Transfer{}, err
	}
	result.Existing = expected.Existing
	return result, nil
}

// ReadArtifactTransfer is an unlocked read of one transfer for capability
// checks outside a transaction; it conveys no authority.
func (s *Store) ReadArtifactTransfer(ctx context.Context, effectID string) (Transfer, error) {
	return readTransfer(ctx, s.pool, effectID)
}

// StoredArtifact is what the adapter verified in the object store, outside
// every lock, before asking for acceptance.
type StoredArtifact struct {
	ContentDigest, ObjectVersion string
	SizeBytes                    uint64
}

// AcceptArtifactTransfer is the finalization transaction: recheck the binding
// and accept the verified stored object as the transfer's receipt. A
// finalization for a binding that is no longer current is retained as evidence
// on the effect (superseded, with the presented claim's digest) and rejected.
func (s *Store) AcceptArtifactTransfer(ctx context.Context, expected Transfer, stored StoredArtifact, subjectDigest string) (json.RawMessage, error) {
	if !digestShape(stored.ContentDigest) || !digestShape(subjectDigest) || stored.SizeBytes == 0 || stored.ObjectVersion == "" {
		return nil, ErrInvalid
	}
	receipt, err := canonical(map[string]any{"kind": expected.Kind, "refId": expected.RefID, "subjectDigest": subjectDigest, "contentDigest": stored.ContentDigest, "sizeBytes": fmt.Sprint(stored.SizeBytes), "objectVersion": stored.ObjectVersion})
	if err != nil {
		return nil, err
	}
	var fenced error
	err = s.transact(ctx, func(tx pgx.Tx) error {
		fenced = nil
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		t, attempt, err := lockTransfer(ctx, tx, expected.TransferBinding, expected.EffectID, now)
		if err != nil {
			return err
		}
		if t.RequestDigest != expected.RequestDigest {
			return ErrConflict
		}
		if t.DispatchState == "confirmed" {
			if t.ReceiptDigest != hash(receipt) {
				return ErrConflict
			}
			return nil
		}
		if t.DispatchState != "dispatched" {
			return ErrTransferState
		}
		if !attempt.current {
			// Retained as evidence, rejected as a result: the evidence commits
			// and the rejection is reported after the commit.
			_, err := tx.Exec(ctx, `UPDATE agent_control.effects SET dispatch_state='superseded',disposition='abandon-unresolved',disposition_evidence_ref=$2 WHERE effect_id=$1`, t.EffectID, hash(receipt))
			if err != nil {
				return err
			}
			fenced = fmt.Errorf("%w: %s", ErrRevision, attempt.reason)
			return nil
		}
		if !now.Before(t.ExpiresAt) {
			return ErrTransferExpired
		}
		if t.DeclaredContentDigest != "" && t.DeclaredContentDigest != stored.ContentDigest {
			return ErrInvalid
		}
		if t.DeclaredSizeBytes != 0 && t.DeclaredSizeBytes != stored.SizeBytes {
			return ErrInvalid
		}
		if stored.SizeBytes > t.ByteCeiling {
			return ErrInvalid
		}
		_, err = tx.Exec(ctx, `UPDATE agent_control.effects SET dispatch_state='confirmed',receipt_ref=$2,receipt_digest=$3 WHERE effect_id=$1`, t.EffectID, receipt, hash(receipt))
		return err
	})
	if err != nil {
		return nil, err
	}
	if fenced != nil {
		return nil, fenced
	}
	return receipt, nil
}

// ObligationIdentities lists the identities of one class whose rows survived,
// prepared in the window, for recovery discovery: the inventory listing minus
// this set is the reconciliation set. A row in any state counts, because a
// pending row is reconciled rather than dispatched.
func (s *Store) ObligationIdentities(ctx context.Context, class string, from, to time.Time) (map[string]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT identity,state FROM agent_control.obligations WHERE class=$1 AND prepared_at>=$2 AND prepared_at<$3`, class, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	identities := map[string]string{}
	for rows.Next() {
		var identity, state string
		if err := rows.Scan(&identity, &state); err != nil {
			return nil, err
		}
		identities[identity] = state
	}
	return identities, rows.Err()
}
