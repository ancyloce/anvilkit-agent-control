package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Effects implements the guarded business-write obligation (DD-02 §5
// business-write class, DD-06 §3): inventory before permission in two
// ordered transactions with the obligation written between them, one
// single-use permit per mutation bound to its registered physical owner,
// idempotent outcome observations, original-result queries and the
// not-sent/outcome resolutions that dispositions apply. No inventory or
// upstream I/O happens while a database lock is held.
type Effects struct {
	store     Store
	inventory Inventory
	queries   OutcomeQuery
	clock     domain.Clock
	log       *slog.Logger
}

func NewEffects(store Store, inventory Inventory, queries OutcomeQuery, clock domain.Clock, log *slog.Logger) *Effects {
	return &Effects{store: store, inventory: inventory, queries: queries, clock: clock, log: log}
}

// Permit is the answer to one prepare request: the effect record, whether
// this request consumed the single permit, and the denial code when the
// mutation is (or was) refused.
type Permit struct {
	Effect     *domain.Effect
	Permitted  bool
	DenialCode string
}

// effectContext loads the current state under Control's lock ranks:
// operation (2), attempt (3); the effect itself (6) is locked by the
// caller when it exists. The admission closure of the tenant is read in
// the same transaction. The clock is read only after the locks are held,
// so a deadline or lease checked under lock contention is checked against
// the time the decision is made, not a time sampled while waiting.
func effectContext(ctx context.Context, r Repo, operationID, attemptID, tenantID string, clock domain.Clock) (domain.EffectContext, error) {
	op, err := r.LockOperation(ctx, operationID)
	if err != nil {
		return domain.EffectContext{}, err
	}
	if op.TenantID != tenantID {
		return domain.EffectContext{}, domain.ErrNotFound
	}
	c := domain.EffectContext{Operation: op}
	if attemptID != "" {
		at, err := r.LockAttempt(ctx, attemptID)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return domain.EffectContext{}, err
		}
		c.Attempt = at
	}
	closed, err := r.IsAdmissionClosed(ctx, op.TenantID)
	if err != nil {
		return domain.EffectContext{}, err
	}
	c.AdmissionClosed = closed
	c.Now = clock.Now()
	return c, nil
}

// Prepare is the guarded-mutation admission (DD-06 §3, the sequence of
// DD-02 §4 for a business write).
//
//  1. One transaction, in lock order (operation, attempt, effect),
//     verifies scope, admission closure, epoch, fences, deadlines and the
//     lease; the same command returns the existing record (no permit), a
//     changed binding conflicts, a second command for the same
//     (operation, kind, occurrence) conflicts, a refusal is recorded as
//     denied, and the first eligible request records PREPARED.
//  2. Outside the transaction the immutable obligation is written; an
//     uncertain write is recorded and grants nothing.
//  3. A second transaction reacquires the locks of the persisted effect's
//     own operation and attempt, verifies the record is the caller's,
//     revalidates the same rules against the persisted binding (the lease
//     and its expiry as recorded, never as the reentering request states
//     them) with the clock read under the locks, verifies the object
//     version and consumes the permit: only the request completing that
//     transition is told permitted=true. A request that finds the permit
//     consumed is never told so again; a request reentering a PREPARED
//     effect completes the same sequence once.
func (s *Effects) Prepare(ctx context.Context, cmd domain.CommandIdentity, req domain.EffectRequest) (Permit, error) {
	if req.RequestDigest == "" {
		req.RequestDigest = cmd.RequestDigest
	}
	if req.RequestDigest != cmd.RequestDigest {
		return Permit{}, fmt.Errorf("%w: the effect digest must be the command digest", domain.ErrInvalid)
	}
	var e *domain.Effect
	proceed := false
	for attempt := 0; attempt < 2; attempt++ {
		err := s.store.Tx(ctx, func(r Repo) error {
			found, err := r.GetEffectByCommand(ctx, cmd.TenantID, cmd.CommandID)
			switch {
			case err == nil:
				if err := found.Binds(cmd.TenantID, req, cmd.RequestDigest); err != nil {
					return err
				}
				e, proceed = found, found.State == domain.EffectPrepared
				return nil
			case !errors.Is(err, domain.ErrNotFound):
				return err
			}
			c, err := effectContext(ctx, r, req.OperationID, req.AttemptID, cmd.TenantID, s.clock)
			if err != nil {
				return err
			}
			if other, err := r.GetEffectByOccurrence(ctx, c.Operation.ID, req.Kind, req.Occurrence); err == nil {
				if other.CommandID != cmd.CommandID {
					return fmt.Errorf("%w: %s #%d of operation %s is effect %s under command %s", domain.ErrIdempotencyConflict, req.Kind, req.Occurrence, c.Operation.ID, other.ID, other.CommandID)
				}
				// Another replica committed this command between the lookup
				// by command and the operation lock: reenter its record.
				if err := other.Binds(cmd.TenantID, req, cmd.RequestDigest); err != nil {
					return err
				}
				e, proceed = other, other.State == domain.EffectPrepared
				return nil
			} else if !errors.Is(err, domain.ErrNotFound) {
				return err
			}
			if err := domain.CheckEffect(req, c); err != nil {
				var denial *domain.Denial
				if !errors.As(err, &denial) {
					return err
				}
				e = domain.DeniedEffect(req, c, cmd, denial)
				return r.InsertEffect(ctx, e)
			}
			fresh := domain.NewEffect(req, c, cmd)
			if err := r.InsertEffect(ctx, fresh); err != nil {
				return err
			}
			e, proceed = fresh, true
			return nil
		})
		if errors.Is(err, ErrDuplicateKey) && attempt == 0 {
			continue // another replica prepared the same command first; reload it
		}
		if err != nil {
			return Permit{}, err
		}
		break
	}
	if !proceed {
		return Permit{Effect: e, DenialCode: e.DenialCode}, nil
	}

	key, body := effectObligation(e)
	version, err := s.inventory.Put(ctx, key, body)
	if err != nil {
		if merr := s.markInventoryUncertain(ctx, e.ID); merr != nil {
			s.log.Warn("effect inventory state not recorded", "effectId", e.ID, "error", merr)
		}
		return Permit{}, fmt.Errorf("%w: %s inventory write for effect %s: %v", domain.ErrEffectUncertain, ClassBusinessWrite, e.ID, err)
	}

	permitted := false
	err = s.store.Tx(ctx, func(r Repo) error {
		c, err := effectContext(ctx, r, e.OperationID, e.AttemptID, cmd.TenantID, s.clock)
		if err != nil {
			return err
		}
		locked, err := r.LockEffect(ctx, e.ID)
		if err != nil {
			return err
		}
		e = locked
		if err := locked.Binds(cmd.TenantID, req, cmd.RequestDigest); err != nil {
			return err
		}
		if locked.State != domain.EffectPrepared {
			return nil // consumed by another request; never a second permit
		}
		// The clock is read with every lock held and the rules run against
		// the persisted binding: the lease expiry and deadline the effect
		// was prepared with, as of now.
		c.Now = s.clock.Now()
		now := c.Now
		locked.UpdatedAt = now
		if err := domain.CheckEffect(locked.Request(), c); err != nil {
			var denial *domain.Denial
			if !errors.As(err, &denial) {
				return err
			}
			if err := locked.Deny(denial.Code); err != nil {
				return err
			}
			locked.Inventory, locked.InventoryVersion = domain.InventoryConfirmed, version
			return r.UpdateEffect(ctx, locked)
		}
		if err := locked.Permit(version); err != nil {
			return err
		}
		if err := r.UpdateEffect(ctx, locked); err != nil {
			return err
		}
		permitted = true
		return nil
	})
	if err != nil {
		return Permit{}, err
	}
	return Permit{Effect: e, Permitted: permitted, DenialCode: e.DenialCode}, nil
}

func (s *Effects) markInventoryUncertain(ctx context.Context, effectID string) error {
	return s.store.Tx(ctx, func(r Repo) error {
		e, err := r.LockEffect(ctx, effectID)
		if err != nil {
			return err
		}
		if e.State != domain.EffectPrepared || e.Inventory != domain.InventoryPending {
			return nil
		}
		e.Inventory, e.UpdatedAt = domain.InventoryUncertain, s.clock.Now()
		return r.UpdateEffect(ctx, e)
	})
}

// Get returns the effect by id or by its mutation identity (operation,
// kind, occurrence) within the caller's tenant: the original-result query
// of a lost response. It never returns a permit.
func (s *Effects) Get(ctx context.Context, tenantID, effectID, operationID string, kind domain.EffectKind, occurrence uint64) (*domain.Effect, error) {
	var e *domain.Effect
	err := s.store.Read(ctx, func(r Repo) error {
		var err error
		switch {
		case effectID != "":
			e, err = r.GetEffect(ctx, effectID)
		case operationID != "" && kind != "" && occurrence > 0:
			e, err = r.GetEffectByOccurrence(ctx, operationID, kind, occurrence)
		default:
			return fmt.Errorf("%w: an effect id or an operation, kind and occurrence is required", domain.ErrInvalid)
		}
		if err != nil {
			return err
		}
		if e.TenantID != tenantID {
			return domain.ErrNotFound
		}
		return nil
	})
	return e, err
}

// Observe applies one idempotent outcome observation of a permitted
// mutation. The same (source, sequence) is never applied twice; the first
// definite outcome settles the effect and later reports are recorded as
// duplicates that change nothing (a late outcome after a disposition
// included); an unknown outcome keeps the effect unresolved.
func (s *Effects) Observe(ctx context.Context, tenantID, effectID, source string, sequence uint64, outcome domain.EffectOutcome, receiptDigest, nativeRef string, observedAt time.Time) (e *domain.Effect, existing bool, err error) {
	err = s.store.Tx(ctx, func(r Repo) error {
		peek, err := r.GetEffect(ctx, effectID)
		if err != nil {
			return err
		}
		if peek.TenantID != tenantID {
			return domain.ErrNotFound
		}
		if _, err := r.LockOperation(ctx, peek.OperationID); err != nil {
			return err
		}
		locked, err := r.LockEffect(ctx, effectID)
		if err != nil {
			return err
		}
		e = locked
		if _, err := r.GetEffectObservation(ctx, locked.ID, source, sequence); err == nil {
			existing = true
			return nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		if _, err := locked.Observe(outcome, receiptDigest, observedAt); err != nil {
			return err
		}
		locked.UpdatedAt = s.clock.Now()
		if err := r.InsertEffectObservation(ctx, &domain.EffectObservation{ID: domain.NewID("eobs"), EffectID: locked.ID, Source: source, Sequence: sequence, Outcome: outcome, ReceiptDigest: receiptDigest, NativeReference: nativeRef, ObservedAt: observedAt}); err != nil {
			return err
		}
		return r.UpdateEffect(ctx, locked)
	})
	return e, existing, err
}

// QueryOriginal asks the upstream about the effect's original identity
// (DD-06 §3 "lost response queries original identity") outside every lock
// and records what came back as an observation. A query that cannot be
// made is an error; an upstream without a record leaves the effect as it
// is and reports Known false.
func (s *Effects) QueryOriginal(ctx context.Context, tenantID, effectID string) (*domain.Effect, EffectOutcomeReport, error) {
	e, err := s.Get(ctx, tenantID, effectID, "", "", 0)
	if err != nil {
		return nil, EffectOutcomeReport{}, err
	}
	if !e.Unresolved() {
		return e, EffectOutcomeReport{}, nil
	}
	report, err := s.queries.QueryEffect(ctx, e)
	if err != nil {
		return nil, EffectOutcomeReport{}, fmt.Errorf("original-identity query of effect %s: %w", e.ID, err)
	}
	if !report.Known {
		return e, report, nil
	}
	e, _, err = s.Observe(ctx, tenantID, e.ID, report.Source, report.Sequence, report.Outcome, report.ReceiptRef, report.NativeReference, report.ObservedAt)
	return e, report, err
}

// SendersQuiescent extends the operation's quiescence with unresolved
// effects: a consumed permit whose outcome is unknown means a writer may
// still be acting.
func unresolvedEffects(ctx context.Context, r Repo, operationID string) (bool, error) {
	unresolved, err := r.HasUnresolvedEffect(ctx, operationID)
	if err != nil || unresolved {
		return unresolved, err
	}
	return r.HasUnsettledDispatch(ctx, operationID)
}
