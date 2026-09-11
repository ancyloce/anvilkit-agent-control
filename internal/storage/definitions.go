package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"slices"
	"time"

	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
	"github.com/jackc/pgx/v5"
)

type ImmutableRecord struct {
	Digest, Kind   string
	CanonicalBytes []byte
}

// PutImmutable persists already validated canonical content in an unranked
// transaction. Content integrity is rechecked here; this grants no qualification.
func (s *Store) PutImmutable(ctx context.Context, record ImmutableRecord) error {
	if !slices.Contains([]string{"definition", "descriptor", "policy", "binding", "runtime-profile", "validation-report"}, record.Kind) || len(record.CanonicalBytes) == 0 || len(record.CanonicalBytes) > 65536 || hash(record.CanonicalBytes) != record.Digest {
		return ErrInvalid
	}
	encoded, err := jsoncanonicalizer.Transform(record.CanonicalBytes)
	if err != nil || !bytes.Equal(encoded, record.CanonicalBytes) {
		return ErrInvalid
	}
	return s.transact(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO definition_contract.immutable_records (digest,record_kind,canonical_bytes)
			VALUES ($1,$2,$3) ON CONFLICT (digest) DO NOTHING`, record.Digest, record.Kind, record.CanonicalBytes)
		if err != nil {
			return err
		}
		var same bool
		err = tx.QueryRow(ctx, `SELECT record_kind=$2 AND canonical_bytes=$3 FROM definition_contract.immutable_records WHERE digest=$1`, record.Digest, record.Kind, record.CanonicalBytes).Scan(&same)
		if err != nil {
			return err
		}
		if !same {
			return ErrConflict
		}
		return nil
	})
}

type ActivationCommand struct {
	Family, DefinitionID, CommandID, DefinitionDigest, RuntimeProfileRef string
	ExpectedCurrentActivationID                                          *string
	ValidatorReportRef, ActivatedBy                                      string
}
type Activation struct {
	ID, DefinitionDigest, RuntimeProfileRef, ValidatorReportRef, ActivatedBy string
	ActivatedAt                                                              time.Time
	Existing                                                                 bool
}

// CompareAndSwapActivation is the internal storage transaction, not an activation
// interface. A future caller must authorize the developer and qualify the exact
// definition/runtime before entering it. Tests use explicitly synthetic records.
func (s *Store) CompareAndSwapActivation(ctx context.Context, command ActivationCommand) (Activation, error) {
	var result Activation
	if command.Family != "component" || !s.validIDs(command.DefinitionID, command.CommandID, command.RuntimeProfileRef, command.ValidatorReportRef, command.ActivatedBy) || !digestShape(command.DefinitionDigest) || (command.ExpectedCurrentActivationID != nil && !s.validIDs(*command.ExpectedCurrentActivationID)) {
		return result, ErrInvalid
	}
	// Resolve immutable content before taking rank 1. Nothing locks it for update.
	var kind string
	err := s.pool.QueryRow(ctx, `SELECT record_kind FROM definition_contract.immutable_records WHERE digest=$1`, command.DefinitionDigest).Scan(&kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrNotFound
	}
	if err != nil {
		return result, err
	}
	if kind != "definition" {
		return result, ErrInvalid
	}
	request := map[string]any{"definitionDigest": command.DefinitionDigest, "runtimeProfileRef": command.RuntimeProfileRef}
	if command.ExpectedCurrentActivationID != nil {
		request["expectedCurrentActivationId"] = *command.ExpectedCurrentActivationID
	}
	requestBytes, _ := canonical(request)
	requestDigest, id := hash(requestBytes), "act-"+rand.Text()
	err = s.transact(ctx, func(tx pgx.Tx) error {
		result = Activation{}
		// Rank 1: pointer insertion/unique conflict, then scoped pointer lock,
		// then activation insertion (including its immutable foreign-key read).
		_, err := tx.Exec(ctx, `INSERT INTO definition_contract.activation_pointers (family,definition_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, command.Family, command.DefinitionID)
		if err != nil {
			return err
		}
		var current *string
		if err := tx.QueryRow(ctx, `SELECT activation_id FROM definition_contract.activation_pointers WHERE family=$1 AND definition_id=$2 FOR UPDATE`, command.Family, command.DefinitionID).Scan(&current); err != nil {
			return err
		}
		var originalDigest string
		err = tx.QueryRow(ctx, `SELECT activation_id,request_digest,definition_digest,runtime_profile_ref,validator_report_ref,activated_by,activated_at FROM definition_contract.activations WHERE family=$1 AND definition_id=$2 AND command_id=$3`, command.Family, command.DefinitionID, command.CommandID).Scan(&result.ID, &originalDigest, &result.DefinitionDigest, &result.RuntimeProfileRef, &result.ValidatorReportRef, &result.ActivatedBy, &result.ActivatedAt)
		if err == nil {
			if originalDigest != requestDigest {
				return ErrConflict
			}
			result.Existing = true
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if (current == nil) != (command.ExpectedCurrentActivationID == nil) || (current != nil && *current != *command.ExpectedCurrentActivationID) {
			return ErrRevision
		}
		err = tx.QueryRow(ctx, `INSERT INTO definition_contract.activations (activation_id,family,definition_id,command_id,request_digest,definition_digest,runtime_profile_ref,validator_report_ref,activated_by,activated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,clock_timestamp()) RETURNING activation_id,definition_digest,runtime_profile_ref,validator_report_ref,activated_by,activated_at`, id, command.Family, command.DefinitionID, command.CommandID, requestDigest, command.DefinitionDigest, command.RuntimeProfileRef, command.ValidatorReportRef, command.ActivatedBy).Scan(&result.ID, &result.DefinitionDigest, &result.RuntimeProfileRef, &result.ValidatorReportRef, &result.ActivatedBy, &result.ActivatedAt)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE definition_contract.activation_pointers SET activation_id=$3 WHERE family=$1 AND definition_id=$2`, command.Family, command.DefinitionID, id)
		return err
	})
	if err != nil {
		return Activation{}, err
	}
	return result, nil
}
