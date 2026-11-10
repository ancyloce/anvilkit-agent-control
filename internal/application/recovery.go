package application

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Recovery implements the rollback-window reconciliation of DD-02 §5 and
// platform.md "recovery gates" in the order the design fixes: close new
// admission for the scope and fence every active execution identity under
// a new recovery epoch (Begin, one commit), enumerate the five obligation
// classes of the window completely with resumable cursors (Enumerate),
// compare each obligation with the restored database and restore the
// identities the database lost (Reconcile), query and record original
// outcomes (Reconcile, RecordOutcome), record evidence-bound operator
// dispositions (Dispose) and reopen the scope only when every finding is
// settled (Evaluate). An interrupted listing, an unavailable inventory or
// an outcome that cannot be established never counts as an empty set or a
// zero cost: the scope stays restricted. Temporal drives the steps; this
// service owns the state.
type Recovery struct {
	store     Store
	inventory Inventory
	queries   OutcomeQuery
	dispatch  *Dispatch
	effects   *Effects
	exec      *Execution
	profiles  map[string]domain.Profile
	notSent   NotSentEvidence
	evidence  DispositionEvidence
	clock     domain.Clock
	page      int
}

func NewRecovery(store Store, inventory Inventory, queries OutcomeQuery, dispatch *Dispatch, effects *Effects, exec *Execution, profiles []domain.Profile, notSent NotSentEvidence, evidence DispositionEvidence, clock domain.Clock, page int) *Recovery {
	m := make(map[string]domain.Profile, len(profiles))
	for _, p := range profiles {
		m[p.ID] = p
	}
	if page <= 0 {
		page = 500
	}
	return &Recovery{store: store, inventory: inventory, queries: queries, dispatch: dispatch, effects: effects, exec: exec, profiles: m, notSent: notSent, evidence: evidence, clock: clock, page: page}
}

// The operator actions (P0.3): decided by the verified roles of the user
// scope the API carries, never by a caller's assertion or a fixture.
const (
	ActionRecovery    = "recovery"
	ActionDisposition = "disposition"
)

// operator authorizes an operator action: the scope is the command's own
// tenant and actor and holds the operator role; a platform-wide recovery
// scope ("*") needs the platform_operator role.
func operator(scope domain.Scope, cmd domain.CommandIdentity, scopeKey, action string) error {
	switch {
	case scope.TenantID != cmd.TenantID || scope.ActorID != cmd.ActorID:
		return fmt.Errorf("%w: the operator scope is not the command's tenant and actor", domain.ErrForbidden)
	case scopeKey == domain.ScopeAll && !scope.HasRole(domain.RolePlatformOperator):
		return fmt.Errorf("%w: actor %s may not %s for every tenant: the %s role is required", domain.ErrForbidden, cmd.ActorID, action, domain.RolePlatformOperator)
	case !scope.HasRole(domain.RoleOperator) && !scope.HasRole(domain.RolePlatformOperator):
		return fmt.Errorf("%w: actor %s may not %s for scope %s: the %s role is required", domain.ErrForbidden, cmd.ActorID, action, scopeKey, domain.RoleOperator)
	}
	return nil
}

// Begin closes new admission for the scope, fences every active
// operation of the scope under a new recovery epoch and records the run,
// all in one commit; the run's workflow is then started by the relay. The
// same command returns the existing run. Admission stays closed until
// Evaluate reopens it.
func (s *Recovery) Begin(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, scopeKey string, windowStart, windowEnd time.Time, skew time.Duration, reason string) (run *domain.RecoveryRun, existing bool, err error) {
	if scopeKey != domain.ScopeAll && scopeKey != cmd.TenantID {
		return nil, false, fmt.Errorf("%w: recovery scope %q is not the command tenant", domain.ErrInvalid, scopeKey)
	}
	if err := operator(scope, cmd, scopeKey, ActionRecovery); err != nil {
		return nil, false, err
	}
	now := s.clock.Now()
	for attempt := 0; attempt < 2; attempt++ {
		err = s.store.Tx(ctx, func(r Repo) error {
			found, err := r.GetRecoveryRunByCommand(ctx, scopeKey, cmd.CommandID)
			switch {
			case err == nil:
				if found.RequestDigest != cmd.RequestDigest {
					return fmt.Errorf("%w: recovery command %s", domain.ErrIdempotencyConflict, cmd.CommandID)
				}
				run, existing = found, true
				return nil
			case !errors.Is(err, domain.ErrNotFound):
				return err
			}
			// One run at a time per scope (P1.5, CTL-APP-08): an open closure of
			// this scope or of every scope refuses a second run until the first
			// reopens admission.
			open, err := r.IsAdmissionClosed(ctx, scopeKey)
			if err == nil && scopeKey == domain.ScopeAll && !open {
				open, err = r.AnyAdmissionClosed(ctx)
			}
			if err != nil {
				return err
			}
			if open {
				return fmt.Errorf("%w: RECOVERY_IN_PROGRESS: a recovery run holds the admission closure of scope %s", domain.ErrStaleExecution, scopeKey)
			}
			epoch, err := r.NextRecoveryEpoch(ctx)
			if err != nil {
				return err
			}
			fresh, err := domain.NewRecoveryRun(cmd, scopeKey, epoch, windowStart, windowEnd, skew, reason, now)
			if err != nil {
				return err
			}
			if err := r.InsertRecoveryRun(ctx, fresh); err != nil {
				return err
			}
			if err := r.UpsertAdmissionClosure(ctx, &domain.AdmissionClosure{ScopeKey: scopeKey, RunID: fresh.ID, ClosedAt: now}); err != nil {
				return err
			}
			active, err := r.ListActiveOperations(ctx, scopeKey)
			if err != nil {
				return err
			}
			for _, peek := range active {
				op, err := r.LockOperation(ctx, peek.ID)
				if err != nil {
					return err
				}
				if op.Lifecycle.Terminal() {
					continue
				}
				ev := op.Transition("recovery:"+fresh.ID+":fenced", now, func(o *domain.Operation) { fence(o, fresh.RecoveryEpoch) })
				if err := r.UpdateOperation(ctx, op); err != nil {
					return err
				}
				if err := r.InsertEvent(ctx, ev); err != nil {
					return err
				}
				fresh.FencedCount++
			}
			for _, class := range ObligationClasses {
				if err := r.UpsertRecoveryProgress(ctx, &domain.RecoveryProgress{RunID: fresh.ID, Class: class}); err != nil {
					return err
				}
			}
			if err := r.UpdateRecoveryRun(ctx, fresh); err != nil {
				return err
			}
			run = fresh
			return nil
		})
		if errors.Is(err, ErrDuplicateKey) && attempt == 0 {
			continue
		}
		break
	}
	return run, existing, err
}

// fence retires the operation's current execution identity: every
// attempt, launch, dispatch and effect bound to the old epoch is stale for
// new admission, issued effects and costs are kept, the operation
// reconciles under the new recovery epoch, and a started workflow run is
// canceled by the relay so no old writer keeps acting.
func fence(o *domain.Operation, epoch uint64) {
	o.ExecutionEpoch++
	o.RecoveryEpoch = epoch
	o.Lifecycle = domain.LifecycleReconciling
	o.Phase = "recovery_fenced"
	o.FailureCode = "RECOVERY_FENCED"
	switch o.Relay {
	case domain.RelayPending:
		o.Relay = domain.RelaySettled
	case domain.RelayStarted:
		o.Relay = domain.RelayCancelPending
	}
}

// Started records that the run's workflow was started (the relay's mark).
func (s *Recovery) Started(ctx context.Context, runID string) error {
	now := s.clock.Now()
	return s.store.Tx(ctx, func(r Repo) error {
		run, err := r.LockRecoveryRun(ctx, runID)
		if err != nil {
			return err
		}
		if run.Phase != domain.RecoveryFenced {
			return nil
		}
		run.Phase, run.UpdatedAt = domain.RecoveryEnumerating, now
		return r.UpdateRecoveryRun(ctx, run)
	})
}

// Get returns the run, its enumeration progress and the number of
// findings still keeping the scope restricted.
func (s *Recovery) Get(ctx context.Context, runID string) (run *domain.RecoveryRun, progress []*domain.RecoveryProgress, unsettled uint64, err error) {
	err = s.store.Read(ctx, func(r Repo) error {
		var err error
		if run, err = r.GetRecoveryRun(ctx, runID); err != nil {
			return err
		}
		if progress, err = r.ListRecoveryProgress(ctx, runID); err != nil {
			return err
		}
		unsettled, err = r.CountUnsettledFindings(ctx, runID)
		return err
	})
	return run, progress, unsettled, err
}

// Discloses decides an operator's read of a run (API-17): the operator or
// platform_operator role in the run's own tenant, or the platform_operator
// role for a run of every tenant ("*"), as Begin decided who could open it.
// Anything else is NOT_FOUND: a run outside the reader's scope is not
// disclosed, not even its existence. The run's scope key never changes, so
// the decision holds for every later read of the same run.
func Discloses(run *domain.RecoveryRun, viewer domain.Scope) error {
	switch {
	case run.ScopeKey == domain.ScopeAll && viewer.HasRole(domain.RolePlatformOperator):
		return nil
	case run.ScopeKey != domain.ScopeAll && run.ScopeKey == viewer.TenantID && (viewer.HasRole(domain.RoleOperator) || viewer.HasRole(domain.RolePlatformOperator)):
		return nil
	}
	return fmt.Errorf("%w: recovery run %s", domain.ErrNotFound, run.ID)
}

// Viewable reads the run and decides an operator's read of it (Discloses).
func (s *Recovery) Viewable(ctx context.Context, runID string, viewer domain.Scope) error {
	return s.store.Read(ctx, func(r Repo) error {
		run, err := r.GetRecoveryRun(ctx, runID)
		if err != nil {
			return err
		}
		return Discloses(run, viewer)
	})
}

// FindingCursor is the opaque position of a findings listing: the key of
// the last finding of the previous page in the fixed (class, obligation)
// order. "" starts at the beginning.
func FindingCursor(f *domain.RecoveryFinding) string { return f.Class + "/" + f.ObligationID }

// Findings lists one page of the run's findings from the cursor, optionally
// by status. Complete is true when no finding follows the page; otherwise
// NextCursor resumes after it. A caller that walks every page reaches every
// finding, however many precede it and whatever state they are in: an
// unresolved finding early in the order never hides the ones after it.
func (s *Recovery) Findings(ctx context.Context, runID string, status domain.FindingStatus, cursor string, limit int) (out []*domain.RecoveryFinding, nextCursor string, complete bool, err error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	afterClass, afterID, ok := strings.Cut(cursor, "/")
	if cursor != "" && !ok {
		return nil, "", false, fmt.Errorf("%w: findings cursor %q", domain.ErrInvalid, cursor)
	}
	err = s.store.Read(ctx, func(r Repo) error {
		if _, err := r.GetRecoveryRun(ctx, runID); err != nil {
			return err
		}
		var err error
		if out, err = r.ListFindings(ctx, runID, status, afterClass, afterID, limit+1); err != nil {
			return err
		}
		complete = len(out) <= limit
		if !complete {
			out = out[:limit]
			nextCursor = FindingCursor(out[len(out)-1])
		}
		for _, f := range out {
			if f.Class != ClassJobLaunch {
				continue
			}
			l, err := r.GetLaunch(ctx, f.ObligationID)
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			f.LaunchKey = l.LaunchKey
		}
		return nil
	})
	return out, nextCursor, complete, err
}

// Evaluate decides the run's gate: with every class enumerated completely
// and every finding settled the scope reopens (admission closure lifted)
// and the run is reopened; otherwise the run is restricted and admission
// stays closed. It is safe to repeat.
func (s *Recovery) Evaluate(ctx context.Context, runID string) (run *domain.RecoveryRun, unsettled uint64, err error) {
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockRecoveryRun(ctx, runID)
		if err != nil {
			return err
		}
		run = locked
		if locked.Phase == domain.RecoveryReopened {
			return nil
		}
		progress, err := r.ListRecoveryProgress(ctx, runID)
		if err != nil {
			return err
		}
		complete := len(progress) == len(ObligationClasses)
		for _, p := range progress {
			complete = complete && p.Complete
		}
		unsettled, err = r.CountUnsettledFindings(ctx, runID)
		if err != nil {
			return err
		}
		switch {
		case !complete:
			if locked.Phase == domain.RecoveryFenced {
				return nil
			}
			locked.Phase = domain.RecoveryEnumerating
			unsettled++ // an incomplete enumeration is itself unsettled
		case unsettled > 0:
			locked.Phase = domain.RecoveryRestricted
		default:
			locked.Phase, locked.ReopenedAt = domain.RecoveryReopened, &now
			if err := r.ReopenAdmission(ctx, locked.ScopeKey, locked.ID, now); err != nil {
				return err
			}
		}
		locked.UpdatedAt = now
		return r.UpdateRecoveryRun(ctx, locked)
	})
	return run, unsettled, err
}
