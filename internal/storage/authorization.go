package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type RefreshLease struct {
	TenantID, Owner, ID string
	Epoch               int64
	Until               time.Time
}
type Evidence struct {
	ObservedAt, FreshUntil time.Time
	MemberRoles            map[string][]string
	Revision               int64
}

// AcquireRefresh claims an unranked, persisted lease; no row lock survives the
// method. Callers fetch complete evidence outside all database transactions.
func (s *Store) AcquireRefresh(ctx context.Context, tenant, owner string) (RefreshLease, error) {
	if !s.validIDs(tenant, owner) {
		return RefreshLease{}, ErrInvalid
	}
	id := "lease-" + rand.Text()
	var result RefreshLease
	err := s.transact(ctx, func(tx pgx.Tx) error {
		result = RefreshLease{}
		_, err := tx.Exec(ctx, `INSERT INTO agent_control.authorization_evidence (tenant_id) VALUES ($1) ON CONFLICT DO NOTHING`, tenant)
		if err != nil {
			return err
		}
		result.TenantID, result.Owner, result.ID = tenant, owner, id
		err = tx.QueryRow(ctx, `UPDATE agent_control.authorization_evidence SET refresh_owner=$2,refresh_lease_id=$3,refresh_epoch=refresh_epoch+1,
			refresh_lease_until=clock_timestamp()+make_interval(secs=>$4),updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND (refresh_lease_until IS NULL OR refresh_lease_until<clock_timestamp()) RETURNING refresh_epoch,refresh_lease_until`, tenant, owner, id, refreshLeaseSeconds).Scan(&result.Epoch, &result.Until)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		return err
	})
	if err != nil {
		return RefreshLease{}, err
	}
	return result, nil
}

// CompleteRefresh stores evidence only for its exact, still-live acquisition.
// FreshUntil must already include credential/response deadlines computed by the
// caller; storage also enforces the first-send-plus-30-second ceiling.
func (s *Store) CompleteRefresh(ctx context.Context, lease RefreshLease, evidence Evidence) (int64, error) {
	if !s.validIDs(lease.TenantID, lease.Owner, lease.ID) || lease.Epoch < 1 || evidence.ObservedAt.IsZero() || !evidence.FreshUntil.After(evidence.ObservedAt) || evidence.FreshUntil.After(evidence.ObservedAt.Add(30*time.Second)) || evidence.MemberRoles == nil {
		return 0, ErrInvalid
	}
	for actor, roles := range evidence.MemberRoles {
		if !s.validIDs(actor) || roles == nil {
			return 0, ErrInvalid
		}
		for _, role := range roles {
			if !s.validIDs(role) {
				return 0, ErrInvalid
			}
		}
	}
	roles, err := canonical(evidence.MemberRoles)
	if err != nil || len(roles) > 65536 {
		return 0, ErrInvalid
	}
	var revision int64
	err = s.transact(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `UPDATE agent_control.authorization_evidence SET observed_at=$5,fresh_until=$6,evidence_revision=evidence_revision+1,
			members_digest=$7,member_roles=$8,refresh_owner=NULL,refresh_lease_id=NULL,refresh_lease_until=NULL,updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND refresh_owner=$2 AND refresh_lease_id=$3 AND refresh_epoch=$4
			AND refresh_lease_until>clock_timestamp() AND $6::timestamptz>clock_timestamp() AND $5::timestamptz<=clock_timestamp()
			RETURNING evidence_revision`, lease.TenantID, lease.Owner, lease.ID, lease.Epoch, evidence.ObservedAt, evidence.FreshUntil, hash(roles), roles).Scan(&revision)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	return revision, nil
}

func (s *Store) ReleaseRefresh(ctx context.Context, lease RefreshLease) error {
	if !s.validIDs(lease.TenantID, lease.Owner, lease.ID) || lease.Epoch < 1 {
		return ErrInvalid
	}
	return s.transact(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE agent_control.authorization_evidence SET last_failure_at=clock_timestamp(),refresh_owner=NULL,refresh_lease_id=NULL,refresh_lease_until=NULL,updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND refresh_owner=$2 AND refresh_lease_id=$3 AND refresh_epoch=$4`, lease.TenantID, lease.Owner, lease.ID, lease.Epoch)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrLeaseLost
		}
		return nil
	})
}

// ReadEvidence never returns stale data as current authority. Membership alone
// is insufficient: callers also check action, service and exact resource scope.
func (s *Store) ReadEvidence(ctx context.Context, tenant string) (Evidence, error) {
	if !s.validIDs(tenant) {
		return Evidence{}, ErrInvalid
	}
	var evidence Evidence
	var roles []byte
	err := s.pool.QueryRow(ctx, `SELECT observed_at,fresh_until,evidence_revision,member_roles FROM agent_control.authorization_evidence WHERE tenant_id=$1 AND fresh_until>clock_timestamp()`, tenant).Scan(&evidence.ObservedAt, &evidence.FreshUntil, &evidence.Revision, &roles)
	if errors.Is(err, pgx.ErrNoRows) {
		return Evidence{}, ErrLeaseLost
	}
	if err != nil {
		return Evidence{}, err
	}
	if err = json.Unmarshal(roles, &evidence.MemberRoles); err != nil {
		return Evidence{}, err
	}
	return evidence, nil
}

// MatchesOperationScope reports only whether the exact operation is owned by
// this tenant and actor. It never returns protected projection fields.
func (s *Store) MatchesOperationScope(ctx context.Context, scope Scope, operationID string) (bool, error) {
	if !s.validIDs(scope.TenantID, scope.ActorID, operationID) {
		return false, ErrInvalid
	}
	var matches bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM agent_control.operations
		WHERE operation_id=$1 AND tenant_id=$2 AND actor_id=$3)`, operationID, scope.TenantID, scope.ActorID).Scan(&matches)
	return matches && err == nil, err
}

// TakeAuthorizationRead consumes the shared membership-read budget. A token
// permits one source read, not disclosure. The transaction ends before source
// I/O; all replicas use the same unranked bucket and database clock.
func (s *Store) TakeAuthorizationRead(ctx context.Context) (bool, error) {
	const bucket = "authorization-membership"
	var allowed bool
	err := s.transact(ctx, func(tx pgx.Tx) error {
		allowed = false
		_, err := tx.Exec(ctx, `INSERT INTO agent_control.authorization_read_budget
			(bucket_id,tokens,capacity,refill_per_second) VALUES ($1,$2::integer,$2::integer,$3)
			ON CONFLICT (bucket_id) DO NOTHING`, bucket, authorizationReadBurst, authorizationReadsPerSecond)
		if err != nil {
			return err
		}
		// Lock before sampling the refill clock so a blocked claimant cannot
		// overwrite another claimant's token consumption with a stale value.
		var id string
		err = tx.QueryRow(ctx, `SELECT bucket_id FROM agent_control.authorization_read_budget
			WHERE bucket_id=$1 AND capacity=$2 AND refill_per_second=$3 FOR UPDATE`, bucket, authorizationReadBurst, authorizationReadsPerSecond).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrInvalid
		}
		if err != nil {
			return err
		}
		return tx.QueryRow(ctx, `WITH tick AS MATERIALIZED (SELECT clock_timestamp() AS at),
			refill AS MATERIALIZED (SELECT at, LEAST(capacity,tokens +
				GREATEST(0,extract(epoch FROM (at-refilled_at))) * refill_per_second) AS available
				FROM agent_control.authorization_read_budget,tick WHERE bucket_id=$1)
			UPDATE agent_control.authorization_read_budget b SET
				tokens=CASE WHEN available>=1 THEN available-1 ELSE available END,
				refilled_at=GREATEST(b.refilled_at,refill.at)
			FROM refill WHERE b.bucket_id=$1 RETURNING available>=1`, bucket).Scan(&allowed)
	})
	return allowed && err == nil, err
}
