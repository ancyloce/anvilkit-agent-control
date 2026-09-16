package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	authority Authority
	notSent   NotSentEvidence
	evidence  DispositionEvidence
	clock     domain.Clock
	log       *slog.Logger
	page      int
}

func NewRecovery(store Store, inventory Inventory, queries OutcomeQuery, dispatch *Dispatch, effects *Effects, exec *Execution, profiles []domain.Profile, authority Authority, notSent NotSentEvidence, evidence DispositionEvidence, clock domain.Clock, log *slog.Logger, page int) *Recovery {
	m := make(map[string]domain.Profile, len(profiles))
	for _, p := range profiles {
		m[p.ID] = p
	}
	if page <= 0 {
		page = 500
	}
	return &Recovery{store: store, inventory: inventory, queries: queries, dispatch: dispatch, effects: effects, exec: exec, profiles: m, authority: authority, notSent: notSent, evidence: evidence, clock: clock, log: log, page: page}
}

// The operator actions the authority port decides.
const (
	ActionRecovery    = "recovery"
	ActionDisposition = "disposition"
)

func (s *Recovery) operator(ctx context.Context, tenantID, actorID, action string) error {
	decision, err := s.authority.CheckOperator(ctx, domain.Scope{TenantID: tenantID, ActorID: actorID}, action)
	if err != nil {
		return fmt.Errorf("operator authority: %w", err)
	}
	if !decision.Current(s.clock.Now()) {
		reason := decision.ReasonCode
		if reason == "" {
			reason = "no current authority"
		}
		return fmt.Errorf("%w: actor %s may not %s for scope %s: %s", domain.ErrForbidden, actorID, action, tenantID, reason)
	}
	return nil
}

// Begin closes new admission for the scope, fences every active
// operation of the scope under a new recovery epoch and records the run,
// all in one commit; the run's workflow is then started by the relay. The
// same command returns the existing run. Admission stays closed until
// Evaluate reopens it.
func (s *Recovery) Begin(ctx context.Context, cmd domain.CommandIdentity, scopeKey string, windowStart, windowEnd time.Time, skew time.Duration, reason string) (run *domain.RecoveryRun, existing bool, err error) {
	if scopeKey != domain.ScopeAll && scopeKey != cmd.TenantID {
		return nil, false, fmt.Errorf("%w: recovery scope %q is not the command tenant", domain.ErrInvalid, scopeKey)
	}
	if err := s.operator(ctx, scopeKey, cmd.ActorID, ActionRecovery); err != nil {
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
			epoch, err := r.MaxRecoveryEpoch(ctx)
			if err != nil {
				return err
			}
			fresh, err := domain.NewRecoveryRun(cmd, scopeKey, epoch+1, windowStart, windowEnd, skew, reason, now)
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

func validClass(class string) error {
	for _, c := range ObligationClasses {
		if c == class {
			return nil
		}
	}
	return fmt.Errorf("%w: obligation class %q", domain.ErrInvalid, class)
}

// Enumerate reads one page of one obligation class from the inventory
// outside every lock, decodes each object in the widened window and
// records, in one commit, a finding per obligation (present in the
// restored database or missing) together with the class cursor. A page
// that cannot be listed or an object that cannot be read is an error that
// leaves the cursor where it was: the listing is complete only when the
// backend said so. When every class is complete the run moves to
// reconciling.
func (s *Recovery) Enumerate(ctx context.Context, runID, class string) (*domain.RecoveryProgress, error) {
	if err := validClass(class); err != nil {
		return nil, err
	}
	var run *domain.RecoveryRun
	var progress *domain.RecoveryProgress
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		if run, err = r.GetRecoveryRun(ctx, runID); err != nil {
			return err
		}
		progress, err = r.GetRecoveryProgress(ctx, runID, class)
		return err
	}); err != nil {
		return nil, err
	}
	if progress.Complete {
		return progress, nil
	}
	if run.Phase != domain.RecoveryFenced && run.Phase != domain.RecoveryEnumerating {
		return nil, fmt.Errorf("%w: recovery run %s is %s", domain.ErrStaleExecution, run.ID, run.Phase)
	}
	page, err := s.inventory.List(ctx, class+"/", progress.Cursor, s.page)
	if err != nil {
		return nil, fmt.Errorf("%w: listing class %s from its cursor: %v", domain.ErrEffectUncertain, class, err)
	}
	type candidate struct {
		object     InventoryObject
		obligation Obligation
		integrity  error
	}
	var candidates []candidate
	for _, o := range page.Objects {
		body, version, err := s.inventory.Get(ctx, o.Key)
		if err != nil {
			return nil, fmt.Errorf("%w: reading %s: %v", domain.ErrEffectUncertain, DescribeKey(o.Key), err)
		}
		if version != "" {
			o.Version = version
		}
		ob, err := DecodeObligation(o.Key, body)
		if err != nil {
			if errors.Is(err, domain.ErrInvalid) {
				continue // not an obligation key (attestations, probes)
			}
			if run.Covers(o.ModifiedAt) {
				candidates = append(candidates, candidate{object: o, integrity: err})
			}
			continue
		}
		if !run.Covers(ob.RecordedAt) && !run.Covers(o.ModifiedAt) {
			continue
		}
		if !run.InScope(ob.TenantID) {
			continue
		}
		candidates = append(candidates, candidate{object: o, obligation: ob})
	}
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockRecoveryRun(ctx, runID)
		if err != nil {
			return err
		}
		current, err := r.GetRecoveryProgress(ctx, runID, class)
		if err != nil {
			return err
		}
		if current.Cursor != progress.Cursor || current.Complete {
			progress = current
			return nil // another replica advanced this class; its page stands
		}
		for _, c := range candidates {
			_, id, _ := ParseObligationKey(c.object.Key)
			if _, err := r.GetFindingByObligation(ctx, runID, class, id); err == nil {
				continue
			} else if !errors.Is(err, domain.ErrNotFound) {
				return err
			}
			f := &domain.RecoveryFinding{ID: domain.NewID("fnd"), RunID: runID, Class: class, ObligationID: id, InventoryKey: c.object.Key, InventoryVersion: c.object.Version, RecordedAt: c.object.ModifiedAt, CreatedAt: now, UpdatedAt: now}
			if c.integrity != nil {
				f.TenantID, f.Status, f.Detail = locked.TenantID, domain.FindingUnresolved, "object does not decode as its class: "+c.integrity.Error()
			} else {
				f.TenantID = c.obligation.TenantID
				if !c.obligation.RecordedAt.IsZero() {
					f.RecordedAt = c.obligation.RecordedAt
				}
				status, detail, err := settlement(ctx, r, c.obligation)
				if err != nil {
					return err
				}
				f.Status, f.Detail = status, detail
			}
			if f.Status == domain.FindingUnresolved {
				// An operator's evidence-bound decision about the obligation
				// stands across runs: a retained exposure is not re-decided by
				// every later run, and only a definite outcome (which would
				// have made the record present) supersedes it.
				if d, err := r.GetDisposition(ctx, class, id); err == nil {
					f.Status, f.Outcome, f.EvidenceRef = domain.FindingDisposed, string(d.Decision), d.EvidenceRef
					f.Detail = "disposed by command " + d.CommandID + " under recovery epoch " + domain.Revision(d.RecoveryEpoch).String() + ": " + f.Detail
				} else if !errors.Is(err, domain.ErrNotFound) {
					return err
				}
			}
			if err := r.InsertFinding(ctx, f); err != nil {
				return err
			}
		}
		current.Cursor, current.Complete, current.Seen = page.NextCursor, page.Complete, current.Seen+uint64(len(page.Objects))
		if err := r.UpsertRecoveryProgress(ctx, current); err != nil {
			return err
		}
		progress = current
		all, err := r.ListRecoveryProgress(ctx, runID)
		if err != nil {
			return err
		}
		complete := len(all) == len(ObligationClasses)
		for _, p := range all {
			complete = complete && p.Complete
		}
		if complete && locked.Phase == domain.RecoveryEnumerating {
			locked.Phase, locked.UpdatedAt = domain.RecoveryReconciling, now
			return r.UpdateRecoveryRun(ctx, locked)
		}
		return nil
	})
	return progress, err
}

// settlement classifies an enumerated obligation against the restored
// database. That the database holds the identity is not the same as the
// external effect being settled: a launch whose cleanup was never
// confirmed, a send whose permit was consumed without a definite outcome
// and a permitted business write without an observed result are present
// and still unresolved, and they keep the scope restricted exactly like a
// lost identity until evidence or a disposition settles them. Only a
// record whose effect is settled (or was never permitted) is present.
func settlement(ctx context.Context, r Repo, ob Obligation) (domain.FindingStatus, string, error) {
	switch ob.Class {
	case ClassIntake:
		if _, err := r.GetOperationScoped(ctx, ob.ID, ob.TenantID); err != nil {
			return missingOr(err)
		}
		return domain.FindingPresent, "", nil
	case ClassJobLaunch:
		l, err := r.GetLaunch(ctx, ob.ID)
		if err != nil {
			return missingOr(err)
		}
		at, err := r.GetAttempt(ctx, l.AttemptID)
		if errors.Is(err, domain.ErrNotFound) {
			return domain.FindingUnresolved, "launch present but its attempt is absent; the launcher's evidence is required", nil
		}
		if err != nil {
			return "", "", err
		}
		if at.TenantID != ob.TenantID {
			return "", "", fmt.Errorf("%w: launch %s belongs to another tenant than its inventory record", domain.ErrIntegrity, ob.ID)
		}
		if status, detail, ok := launchSettled(at); ok {
			return status, detail, nil
		}
		return domain.FindingUnresolved, fmt.Sprintf("launch present; cleanup not confirmed (attempt %s, cleanup %q): the launcher's evidence is required", at.State, at.Cleanup), nil
	case ClassModelDispatch, ClassToolDispatch:
		d, err := r.GetDispatch(ctx, ob.ID)
		if err != nil {
			return missingOr(err)
		}
		if d.TenantID != ob.TenantID {
			return "", "", fmt.Errorf("%w: dispatch %s belongs to another tenant than its inventory record", domain.ErrIntegrity, ob.ID)
		}
		if d.State == domain.DispatchAuthorized || d.State == domain.DispatchUnknown {
			return domain.FindingUnresolved, fmt.Sprintf("dispatch present; send permission consumed and outcome %q: the original identity must be queried", d.State), nil
		}
		return domain.FindingPresent, "", nil
	case ClassBusinessWrite:
		e, err := r.GetEffect(ctx, ob.ID)
		if err != nil {
			return missingOr(err)
		}
		if e.TenantID != ob.TenantID {
			return "", "", fmt.Errorf("%w: effect %s belongs to another tenant than its inventory record", domain.ErrIntegrity, ob.ID)
		}
		if e.Unresolved() {
			return domain.FindingUnresolved, fmt.Sprintf("business write present; permit consumed and outcome %q: the original identity must be queried", e.State), nil
		}
		return domain.FindingPresent, "", nil
	}
	return "", "", fmt.Errorf("%w: obligation class %q", domain.ErrInvalid, ob.Class)
}

func missingOr(err error) (domain.FindingStatus, string, error) {
	if errors.Is(err, domain.ErrNotFound) {
		return domain.FindingMissing, "", nil
	}
	return "", "", err
}

// launchSettled reports whether the attempt's own close proves the launch
// stopped: a closed attempt with definite cleanup evidence. An open attempt
// or a cleanup recorded as unknown proves nothing.
func launchSettled(at *domain.Attempt) (domain.FindingStatus, string, bool) {
	if at.State == domain.AttemptClosed && (at.Cleanup == domain.CleanupComplete || at.Cleanup == domain.CleanupNotRequired) {
		return domain.FindingPresent, fmt.Sprintf("attempt closed (%s) with cleanup %s", at.Outcome, at.Cleanup), true
	}
	return "", "", false
}

// Reconcile advances one finding: a missing identity is restored from its
// inventory object under the run's epoch (fenced, reconciling, exposure
// retained), then the original outcome of a dispatch or effect is queried
// through the outcome port and recorded as an observation. A job-launch
// finding waits for the trusted launcher's evidence (RecordOutcome); an
// intake finding is settled by its restoration, since the restored
// operation admits nothing under the old epoch and its sends and writes
// are findings of their own. An outcome the upstream cannot establish
// leaves the finding unresolved and the scope restricted.
func (s *Recovery) Reconcile(ctx context.Context, runID, findingID string) (*domain.RecoveryFinding, error) {
	var f *domain.RecoveryFinding
	var run *domain.RecoveryRun
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		if f, err = r.GetFinding(ctx, findingID); err != nil {
			return err
		}
		if f.RunID != runID {
			return domain.ErrNotFound
		}
		run, err = r.GetRecoveryRun(ctx, runID)
		return err
	}); err != nil {
		return nil, err
	}
	if f.Status.Settled() {
		return f, nil
	}
	if f.Status == domain.FindingMissing {
		body, version, err := s.inventory.Get(ctx, f.InventoryKey)
		if err != nil {
			return nil, fmt.Errorf("%w: reading %s: %v", domain.ErrEffectUncertain, DescribeKey(f.InventoryKey), err)
		}
		ob, err := DecodeObligation(f.InventoryKey, body)
		if errors.Is(err, domain.ErrIntegrity) {
			// The object cannot be restored as the obligation it was
			// published under; only an operator disposition settles it.
			return s.settleFinding(ctx, f.ID, domain.FindingUnresolved, "", "", "object does not decode as its class: "+err.Error())
		}
		if err != nil {
			return nil, err
		}
		if ob.TenantID != f.TenantID {
			return nil, fmt.Errorf("%w: %s names tenant %s, the finding %s", domain.ErrIntegrity, DescribeKey(f.InventoryKey), ob.TenantID, f.TenantID)
		}
		if version == "" {
			version = f.InventoryVersion
		}
		if f, err = s.restore(ctx, run, f, ob, version); err != nil {
			return nil, err
		}
	}
	switch f.Class {
	case ClassJobLaunch:
		return s.settleLaunchRecords(ctx, f)
	case ClassModelDispatch, ClassToolDispatch:
		return s.queryDispatch(ctx, f)
	case ClassBusinessWrite:
		return s.queryEffect(ctx, f)
	}
	return f, nil
}

// settleLaunchRecords settles a job-launch finding from Control's own
// records when they carry definite cleanup evidence: the attempt closed by
// its original owner with cleanup complete or not required. Anything else
// (an open attempt, a cleanup recorded as unknown, an absent attempt) keeps
// the finding where it is until the trusted launcher's evidence arrives
// through RecordOutcome; elapsed time never settles it. A launch whose
// inventory confirmation the database lost is confirmed against the
// enumerated object so the launcher's Pods can be registered under it.
func (s *Recovery) settleLaunchRecords(ctx context.Context, f *domain.RecoveryFinding) (*domain.RecoveryFinding, error) {
	var l *domain.Launch
	var at *domain.Attempt
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		if l, err = r.GetLaunch(ctx, f.ObligationID); err != nil {
			return err
		}
		at, err = r.GetAttempt(ctx, l.AttemptID)
		if errors.Is(err, domain.ErrNotFound) {
			at, err = nil, nil
		}
		return err
	}); err != nil {
		return nil, err
	}
	if l.Inventory != domain.InventoryConfirmed {
		if err := s.confirmLaunchInventory(ctx, l.ID, f.InventoryVersion); err != nil {
			return nil, err
		}
	}
	if at == nil {
		return s.settleFinding(ctx, f.ID, f.Status, f.Outcome, "", "launch present but its attempt is absent; the launcher's evidence is required")
	}
	if _, detail, ok := launchSettled(at); ok {
		return s.settleFinding(ctx, f.ID, domain.FindingResolved, "cleanup_confirmed", f.EvidenceRef, "the original owner closed the launch: "+detail)
	}
	return s.settleFinding(ctx, f.ID, f.Status, f.Outcome, "", fmt.Sprintf("launch cleanup not confirmed (attempt %s, cleanup %q); the launcher must observe the original launch key", at.State, at.Cleanup))
}

// confirmLaunchInventory records the inventory version of a launch whose
// object the enumeration read; the obligation exists, whatever the row's
// pending or uncertain state says.
func (s *Recovery) confirmLaunchInventory(ctx context.Context, launchID, version string) error {
	now := s.clock.Now()
	return s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockLaunch(ctx, launchID)
		if err != nil {
			return err
		}
		if locked.Inventory == domain.InventoryConfirmed {
			return nil
		}
		locked.Inventory, locked.InventoryVersion, locked.UpdatedAt = domain.InventoryConfirmed, version, now
		return r.UpdateLaunchInventory(ctx, locked)
	})
}

// restore recreates the lost identity under the run's epoch in one commit
// and marks the finding restored (resolved for an intake).
func (s *Recovery) restore(ctx context.Context, run *domain.RecoveryRun, f *domain.RecoveryFinding, ob Obligation, version string) (*domain.RecoveryFinding, error) {
	now := s.clock.Now()
	err := s.store.Tx(ctx, func(r Repo) error {
		if _, err := r.LockRecoveryRun(ctx, run.ID); err != nil {
			return err
		}
		locked, err := r.LockFinding(ctx, f.ID)
		if err != nil {
			return err
		}
		if locked.Status != domain.FindingMissing {
			f = locked
			return nil
		}
		status, outcome := domain.FindingRestored, ""
		switch ob.Class {
		case ClassIntake:
			rec := ob.Intake
			if err := s.restoreOperation(ctx, r, run, rec.TenantID, rec.OperationID, rec.CommandID, domain.OperationKind(rec.Kind), rec.ProfileID, domain.Digest(rec.SubjectDigest), domain.Digest(rec.RequestDigest), ob.RecordedAt, rec.ExecutionEpoch, version, now); err != nil {
				return err
			}
			status, outcome = domain.FindingResolved, "restored"
		case ClassJobLaunch:
			rec := ob.Launch
			if err := s.restoreOperation(ctx, r, run, rec.TenantID, rec.OperationID, "", domain.KindLocalCheck, rec.ProfileID, "", "", ob.RecordedAt, rec.ExecutionEpoch, "", now); err != nil {
				return err
			}
			deadline, _ := time.Parse(time.RFC3339Nano, rec.Deadline)
			if err := s.restoreAttempt(ctx, r, rec.TenantID, rec.OperationID, rec.AttemptID, rec.ProfileID, rec.ExecutionEpoch, deadline.UTC(), ob.RecordedAt, now); err != nil {
				return err
			}
			if _, err := r.GetLaunch(ctx, rec.LaunchID); errors.Is(err, domain.ErrNotFound) {
				l := &domain.Launch{
					ID: rec.LaunchID, AttemptID: rec.AttemptID, OperationID: rec.OperationID, LaunchKey: rec.LaunchKey, Backend: rec.Backend, ProfileID: rec.ProfileID,
					ImageDigest: domain.Digest(rec.ImageDigest), ExecutionEpoch: rec.ExecutionEpoch, LaunchEpoch: rec.LaunchEpoch, Deadline: deadline.UTC(),
					CommandID: "recovery:" + rec.LaunchID, RequestDigest: DigestOf([]byte(f.InventoryKey)), Inventory: domain.InventoryConfirmed, InventoryVersion: version,
					CreatedAt: ob.RecordedAt, UpdatedAt: now,
				}
				if err := r.InsertLaunch(ctx, l); err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
		case ClassModelDispatch, ClassToolDispatch:
			rec := ob.Dispatch
			if err := s.restoreOperation(ctx, r, run, rec.TenantID, rec.OperationID, "", domain.KindLocalCheck, "", "", "", ob.RecordedAt, rec.ExecutionEpoch, "", now); err != nil {
				return err
			}
			if err := s.restoreAttempt(ctx, r, rec.TenantID, rec.OperationID, rec.AttemptID, "", rec.ExecutionEpoch, mustTime(rec.Deadline), ob.RecordedAt, now); err != nil {
				return err
			}
			if _, err := r.GetDispatch(ctx, rec.DispatchID); errors.Is(err, domain.ErrNotFound) {
				exposure, err := domain.ParseMoney(rec.Currency, rec.Exposure)
				if err != nil {
					return fmt.Errorf("%w: %s: %v", domain.ErrIntegrity, f.InventoryKey, err)
				}
				kind := domain.DispatchModel
				if ob.Class == ClassToolDispatch {
					kind = domain.DispatchTool
				}
				d := &domain.Dispatch{
					ID: rec.DispatchID, Kind: kind, TenantID: rec.TenantID, OperationID: rec.OperationID, AttemptID: rec.AttemptID, InstanceID: rec.InstanceID, CallID: rec.CallID,
					Owner: rec.Owner, RequestDigest: domain.Digest(rec.RequestDigest), RouteID: rec.RouteID, GrantID: rec.GrantID, GrantRevision: rec.GrantRevision,
					ExecutionEpoch: rec.ExecutionEpoch, State: domain.DispatchUnknown, Outcome: domain.DispatchOutcomeUnknown, Reserved: exposure, MeterRevision: rec.MeterRevision,
					Inventory: domain.InventoryConfirmed, InventoryVersion: version, Deadline: mustTime(rec.Deadline), AdmittedAt: ob.RecordedAt, ObservedAt: &now,
				}
				if err := r.InsertDispatch(ctx, d); err != nil {
					return err
				}
				if err := r.InsertCostEntry(ctx, &domain.CostEntry{ID: domain.NewID("cost"), OperationID: d.OperationID, DispatchID: d.ID, Kind: domain.CostEstimate, Amount: d.Reserved, CreatedAt: now}); err != nil {
					return err
				}
				op, err := r.LockOperation(ctx, rec.OperationID)
				if err != nil {
					return err
				}
				if op.Finance != domain.FinanceExposureUnknown {
					ev := op.Transition("recovery:"+run.ID+":dispatch:"+d.ID, now, func(o *domain.Operation) { o.Finance = domain.FinanceExposureUnknown })
					if err := r.UpdateOperation(ctx, op); err != nil {
						return err
					}
					if err := r.InsertEvent(ctx, ev); err != nil {
						return err
					}
				}
			} else if err != nil {
				return err
			}
		case ClassBusinessWrite:
			rec := ob.Effect
			if err := s.restoreOperation(ctx, r, run, rec.TenantID, rec.OperationID, "", domain.KindLocalCheck, "", "", "", ob.RecordedAt, rec.ExecutionEpoch, "", now); err != nil {
				return err
			}
			if _, err := r.GetEffect(ctx, rec.EffectID); errors.Is(err, domain.ErrNotFound) {
				e := &domain.Effect{
					ID: rec.EffectID, TenantID: rec.TenantID, OperationID: rec.OperationID, AttemptID: rec.AttemptID, Kind: domain.EffectKind(rec.Kind), Occurrence: rec.Occurrence,
					CommandID: rec.CommandID, Owner: rec.Owner, CanonicalSubject: rec.CanonicalSubject, RequestDigest: domain.Digest(rec.RequestDigest), ExpectedRevision: rec.ExpectedRevision,
					ExecutionEpoch: rec.ExecutionEpoch, RecoveryEpoch: run.RecoveryEpoch, LeaseID: rec.LeaseID, LeaseFence: rec.LeaseFence, State: domain.EffectUnknown,
					Outcome: domain.EffectOutcomeUnknown, Inventory: domain.InventoryConfirmed, InventoryVersion: version, Deadline: mustTime(rec.Deadline), CreatedAt: ob.RecordedAt, UpdatedAt: now, ObservedAt: &now,
				}
				if err := r.InsertEffect(ctx, e); err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
		}
		locked.Status, locked.Outcome, locked.UpdatedAt = status, outcome, now
		if err := r.UpdateFinding(ctx, locked); err != nil {
			return err
		}
		f = locked
		return nil
	})
	return f, err
}

func mustTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t.UTC()
}

// restoreOperation recreates a lost operation as reconciling under the
// run's epoch, its old execution epoch retired. An operation restored from
// a launch, dispatch or effect record (its intake object outside the
// window or lost) carries synthetic digests derived from its identity and
// is marked as such; nothing can be admitted for it under the old epoch.
func (s *Recovery) restoreOperation(ctx context.Context, r Repo, run *domain.RecoveryRun, tenantID, operationID, commandID string, kind domain.OperationKind, profileID string, subjectDigest, requestDigest domain.Digest, recordedAt time.Time, executionEpoch uint64, intakeVersion string, now time.Time) error {
	if _, err := r.GetOperationScoped(ctx, operationID, tenantID); err == nil {
		return nil
	} else if !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	if commandID == "" {
		commandID = "recovery:" + operationID
	}
	if subjectDigest == "" {
		subjectDigest = DigestOf([]byte("recovered-subject:" + operationID))
	}
	if requestDigest == "" {
		requestDigest = DigestOf([]byte("recovered-request:" + operationID))
	}
	if recordedAt.IsZero() {
		recordedAt = now
	}
	deadline := recordedAt
	if p, ok := s.profiles[profileID]; ok {
		deadline = recordedAt.Add(p.OperationDeadline)
	}
	op := &domain.Operation{
		ID: operationID, TenantID: tenantID, ActorID: "recovery", CommandID: commandID, Kind: kind, Subject: domain.Subject{ProfileID: profileID, SubjectDigest: subjectDigest},
		SemanticDigest: requestDigest, Lifecycle: domain.LifecycleReconciling, Phase: "recovered", Control: domain.ControlNone, Cleanup: domain.CleanupUnknown,
		Finance: domain.FinanceExposureUnknown, FailureCode: "RECOVERED_FROM_INVENTORY", Revision: 0, NextEventSeq: 1, ExecutionEpoch: executionEpoch + 1,
		RecoveryEpoch: run.RecoveryEpoch, Deadline: deadline, Intake: domain.IntakeConfirmed, IntakeVersion: intakeVersion, Relay: domain.RelaySettled,
		CreatedAt: recordedAt, UpdatedAt: now,
	}
	if op.Subject.ProfileID == "" {
		op.Subject.ProfileID = "recovered"
	}
	if op.Kind == "" {
		op.Kind = domain.KindLocalCheck
	}
	ev := op.Transition("recovery:"+run.ID+":restored", now, func(*domain.Operation) {})
	if err := r.InsertOperation(ctx, op); err != nil {
		return err
	}
	return r.InsertEvent(ctx, ev)
}

// restoreAttempt recreates a lost attempt under its recorded epoch (which
// the restored operation has retired).
func (s *Recovery) restoreAttempt(ctx context.Context, r Repo, tenantID, operationID, attemptID, profileID string, executionEpoch uint64, deadline, recordedAt, now time.Time) error {
	if attemptID == "" {
		return nil
	}
	if _, err := r.GetAttempt(ctx, attemptID); err == nil {
		return nil
	} else if !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	step := "recovered"
	if p, ok := s.profiles[profileID]; ok {
		step = p.StepID
	}
	n, err := r.CountAttempts(ctx, operationID, step, 0)
	if err != nil {
		return err
	}
	if deadline.IsZero() {
		deadline = recordedAt
	}
	return r.InsertAttempt(ctx, &domain.Attempt{
		ID: attemptID, OperationID: operationID, TenantID: tenantID, StepID: step, VisitOrdinal: 0, AttemptOrdinal: n + 1, ProfileID: firstNonEmpty(profileID, "recovered"),
		ExecutionEpoch: executionEpoch, CommandID: "recovery:" + attemptID, RequestDigest: DigestOf([]byte("recovered-attempt:" + attemptID)), State: domain.AttemptLaunchPrepared,
		Deadline: deadline, CreatedAt: recordedAt, UpdatedAt: now,
	})
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// queryDispatch asks the outcome port about the call's original identity
// and records the answer through the dispatch observation path (usage
// metered under the frozen revision, exposure released only by a definite
// outcome with usage).
func (s *Recovery) queryDispatch(ctx context.Context, f *domain.RecoveryFinding) (*domain.RecoveryFinding, error) {
	var d *domain.Dispatch
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		d, err = r.GetDispatch(ctx, f.ObligationID)
		return err
	}); err != nil {
		return nil, err
	}
	if d.TenantID != f.TenantID {
		return nil, fmt.Errorf("%w: dispatch %s", domain.ErrIntegrity, d.ID)
	}
	if d.State == domain.DispatchObserved || d.State == domain.DispatchConfirmedNotSent || d.State == domain.DispatchDenied {
		return s.settleFinding(ctx, f.ID, domain.FindingResolved, string(d.State)+"/"+string(d.Outcome), "", "settled before or outside the run")
	}
	report, err := s.queries.QueryDispatch(ctx, d)
	if err != nil {
		return nil, fmt.Errorf("%w: original-identity query of dispatch %s: %v", domain.ErrEffectUncertain, d.ID, err)
	}
	if !report.Known {
		return s.settleFinding(ctx, f.ID, domain.FindingUnresolved, "", "", "no upstream record of the original identity; exposure retained")
	}
	observed, _, err := s.dispatch.Observe(ctx, d.ID, report.Source, report.Sequence, report.Outcome, report.Usage, report.NativeReference, report.ObservedAt)
	if err != nil {
		return s.settleFinding(ctx, f.ID, domain.FindingUnresolved, "", "", "upstream outcome could not be recorded: "+err.Error())
	}
	if observed.State != domain.DispatchObserved {
		return s.settleFinding(ctx, f.ID, domain.FindingUnresolved, string(observed.Outcome), "", "upstream reports an unknown outcome; exposure retained")
	}
	return s.settleFinding(ctx, f.ID, domain.FindingResolved, string(observed.Outcome), report.Source, "outcome recorded from the original-identity query")
}

func (s *Recovery) queryEffect(ctx context.Context, f *domain.RecoveryFinding) (*domain.RecoveryFinding, error) {
	e, report, err := s.effects.QueryOriginal(ctx, f.TenantID, f.ObligationID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrEffectUncertain, err)
	}
	if !e.Unresolved() {
		return s.settleFinding(ctx, f.ID, domain.FindingResolved, string(e.State), report.Source, "outcome recorded")
	}
	if !report.Known {
		return s.settleFinding(ctx, f.ID, domain.FindingUnresolved, "", "", "no upstream record of the original identity")
	}
	return s.settleFinding(ctx, f.ID, domain.FindingUnresolved, string(report.Outcome), report.Source, "upstream reports an unknown outcome")
}

// settleFinding records a status; a finding an operator disposed in the
// meantime keeps its disposition.
func (s *Recovery) settleFinding(ctx context.Context, findingID string, status domain.FindingStatus, outcome, evidenceRef, detail string) (*domain.RecoveryFinding, error) {
	now := s.clock.Now()
	var f *domain.RecoveryFinding
	err := s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockFinding(ctx, findingID)
		if err != nil {
			return err
		}
		if locked.Status == domain.FindingDisposed {
			f = locked
			return nil
		}
		locked.Status, locked.Outcome, locked.Detail, locked.UpdatedAt = status, outcome, detail, now
		if evidenceRef != "" {
			locked.EvidenceRef = evidenceRef
		}
		if locked.Outcome == "" {
			locked.Outcome = outcome
		}
		f = locked
		return r.UpdateFinding(ctx, locked)
	})
	return f, err
}

// PodEvidence is one Pod the trusted launcher observed under the original
// launch key.
type PodEvidence struct {
	PodUID     string
	Phase      domain.InstancePhase
	ExitCode   *int32
	ObservedAt time.Time
}

// LaunchOutcome is the trusted launcher's evidence about a restored
// launch: what exists under the launch key now, and whether the launcher
// confirmed the backend holds nothing (the old writer is stopped).
type LaunchOutcome struct {
	JobUID  string
	Pods    []PodEvidence
	Stopped bool
}

// LaunchEvidence names what a RecordOutcome call records on the finding:
// the outcome word and the evidence reference of an observed launch.
const (
	launchOutcomeObserved = "observed"
	launchOutcomeStopped  = "job_stopped"
	launchEvidencePrefix  = "job:"
)

// RecordOutcome records the trusted launcher's evidence about a job-launch
// finding whose launch the database holds (restored or present). The
// evidence comes in two steps and the second is accepted only after the
// first is durable here:
//
//  1. An observation (Stopped false): the Job and Pods reported under the
//     original launch key. Every Pod is registered as a physical instance
//     under the original launch identity and the finding records the
//     observed Job as its evidence. A call that observed nothing records
//     nothing: absence within a settle window, an observation failure or a
//     temporary gap is never evidence of cleanup.
//  2. The stop confirmation (Stopped true): the launcher reports the key
//     empty after deleting what was observed. It is accepted only for a
//     finding that already carries a recorded observation; without one the
//     confirmation proves nothing about what was stopped and the finding
//     stays unresolved. Then the attempt is closed (an open attempt with an
//     unknown outcome; a closed attempt keeps its outcome and settles its
//     unknown cleanup) with complete cleanup, and the finding resolves.
//
// A launch whose attempt its original owner already closed with definite
// cleanup resolves from that record. A stopped Job proves nothing about
// the result: unknown never becomes success and the operation stays
// reconciling.
func (s *Recovery) RecordOutcome(ctx context.Context, runID, findingID string, outcome LaunchOutcome) (*domain.RecoveryFinding, error) {
	var f *domain.RecoveryFinding
	var l *domain.Launch
	var at *domain.Attempt
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		if f, err = r.GetFinding(ctx, findingID); err != nil {
			return err
		}
		if f.RunID != runID || f.Class != ClassJobLaunch {
			return fmt.Errorf("%w: finding %s is not a job-launch finding of run %s", domain.ErrInvalid, findingID, runID)
		}
		if l, err = r.GetLaunch(ctx, f.ObligationID); err != nil {
			return err
		}
		at, err = r.GetAttempt(ctx, l.AttemptID)
		return err
	}); err != nil {
		return nil, err
	}
	if f.Status.Settled() {
		return f, nil
	}
	if f.Status == domain.FindingMissing {
		return nil, fmt.Errorf("%w: finding %s is not restored yet", domain.ErrStaleExecution, f.ID)
	}
	if at.TenantID != f.TenantID {
		return nil, fmt.Errorf("%w: launch %s belongs to another tenant than finding %s", domain.ErrIntegrity, l.ID, f.ID)
	}
	if _, detail, ok := launchSettled(at); ok {
		return s.settleFinding(ctx, f.ID, domain.FindingResolved, "cleanup_confirmed", f.EvidenceRef, "the original owner closed the launch: "+detail)
	}
	if l.Inventory != domain.InventoryConfirmed {
		if err := s.confirmLaunchInventory(ctx, l.ID, f.InventoryVersion); err != nil {
			return nil, err
		}
	}
	cmd := func(suffix string) domain.CommandIdentity {
		return domain.CommandIdentity{TenantID: f.TenantID, CommandID: "recovery:" + runID + ":" + suffix, ActorID: "recovery", RequestDigest: DigestOf([]byte(runID + suffix))}
	}
	for _, pod := range outcome.Pods {
		inst, _, err := s.exec.RegisterInstance(ctx, cmd("register:"+pod.PodUID), l.AttemptID, l.LaunchKey, l.Backend, outcome.JobUID, pod.PodUID, l.ImageDigest)
		if err != nil {
			return s.settleFinding(ctx, f.ID, domain.FindingUnresolved, f.Outcome, f.EvidenceRef, "observed pod not registered: "+err.Error())
		}
		if _, err := s.exec.ObserveInstance(ctx, l.AttemptID, inst.ID, pod.Phase, pod.ExitCode, pod.ObservedAt); err != nil {
			return s.settleFinding(ctx, f.ID, domain.FindingUnresolved, f.Outcome, f.EvidenceRef, "observed pod state not recorded: "+err.Error())
		}
	}
	observed := outcome.JobUID != "" || len(outcome.Pods) > 0
	if observed {
		ref := launchEvidencePrefix + outcome.JobUID
		if outcome.JobUID == "" {
			ref = launchEvidencePrefix + "pods-only"
		}
		f.Outcome, f.EvidenceRef = launchOutcomeObserved, ref
	}
	if !outcome.Stopped {
		if !observed {
			return s.settleFinding(ctx, f.ID, f.Status, f.Outcome, f.EvidenceRef, "nothing observed under the original launch key; absence within a settle window is not cleanup evidence")
		}
		return s.settleFinding(ctx, f.ID, f.Status, launchOutcomeObserved, f.EvidenceRef, fmt.Sprintf("launch observed on the backend (%d pods registered); the old writer is not confirmed stopped", len(outcome.Pods)))
	}
	if f.EvidenceRef == "" {
		return s.settleFinding(ctx, f.ID, domain.FindingUnresolved, f.Outcome, "", "stop reported without a recorded observation of the launch; what was stopped is not established, cleanup stays unknown")
	}
	closeOutcome, failureCode := domain.OutcomeUnknown, "RECOVERY_OUTCOME_UNKNOWN"
	if at.State == domain.AttemptClosed {
		closeOutcome, failureCode = at.Outcome, at.FailureCode
	}
	if _, _, _, err := s.exec.CloseAttempt(ctx, cmd("close:"+l.AttemptID), l.AttemptID, closeOutcome, domain.CleanupComplete, failureCode); err != nil && !errors.Is(err, domain.ErrIdempotencyConflict) {
		return s.settleFinding(ctx, f.ID, domain.FindingUnresolved, f.Outcome, f.EvidenceRef, "attempt close refused: "+err.Error())
	}
	return s.settleFinding(ctx, f.ID, domain.FindingResolved, launchOutcomeStopped, f.EvidenceRef, "launcher confirmed the launch key empty after the recorded observation; attempt closed with complete cleanup")
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

// DispositionRequest is one evidence-bound operator decision.
type DispositionRequest struct {
	Class          string
	ObligationID   string
	Decision       domain.DispositionDecision
	EvidenceRef    string
	EvidenceDigest domain.Digest
	RecoveryEpoch  uint64
	RunID          string
	Reason         string
}

// Dispose records an operator disposition of one unresolved obligation:
// the actor's authority for the tenant, the tenant scope of the
// obligation and the evidence object are verified before anything changes
// (the evidence read is external I/O and happens outside every lock); the
// decision then applies to the obligation, the disposition is recorded and
// the run's finding is settled in one commit under the operation lock,
// where the operation's current recovery epoch is checked first. A stale
// or conflicting disposition therefore returns its error with no partial
// change: no effect is resolved, no exposure released, no finding settled.
// A not-sent confirmation releases exposure, an outcome resolution records
// the attested outcome, a retained exposure changes the obligation not at
// all. The same command returns the original disposition; there is no
// bulk form.
func (s *Recovery) Dispose(ctx context.Context, cmd domain.CommandIdentity, req DispositionRequest) (d *domain.Disposition, existing bool, err error) {
	if err := validClass(req.Class); err != nil {
		return nil, false, err
	}
	allowed := map[string][]domain.DispositionDecision{
		ClassIntake:        {domain.DisposeRetainExposure},
		ClassJobLaunch:     {domain.DisposeRetainExposure},
		ClassModelDispatch: {domain.DisposeConfirmNotSent, domain.DisposeRetainExposure},
		ClassToolDispatch:  {domain.DisposeConfirmNotSent, domain.DisposeRetainExposure},
		ClassBusinessWrite: {domain.DisposeConfirmNotSent, domain.DisposeResolveSucceeded, domain.DisposeResolveFailed, domain.DisposeRetainExposure},
	}
	valid := false
	for _, dec := range allowed[req.Class] {
		valid = valid || dec == req.Decision
	}
	if !valid {
		return nil, false, fmt.Errorf("%w: decision %q does not apply to %s obligations", domain.ErrInvalid, req.Decision, req.Class)
	}
	// Reentry and the obligation's binding, read before any verification.
	// An obligation whose identity the database does not hold and cannot
	// restore (an object that does not decode as its class) has no
	// operation to bind the epoch to: such a finding of the named run is
	// settled against the run's own epoch, and only by retaining exposure.
	var op *domain.Operation
	var run *domain.RecoveryRun
	var dispatch *domain.Dispatch
	if err := s.store.Read(ctx, func(r Repo) error {
		found, err := r.GetDispositionByCommand(ctx, cmd.TenantID, cmd.CommandID)
		switch {
		case err == nil:
			if found.Class != req.Class || found.ObligationID != req.ObligationID || found.Decision != req.Decision || found.EvidenceRef != req.EvidenceRef || found.RequestDigest != cmd.RequestDigest {
				return fmt.Errorf("%w: disposition command %s", domain.ErrIdempotencyConflict, cmd.CommandID)
			}
			d, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		if other, err := r.GetDisposition(ctx, req.Class, req.ObligationID); err == nil {
			return fmt.Errorf("%w: %s %s was disposed by command %s", domain.ErrIdempotencyConflict, req.Class, req.ObligationID, other.CommandID)
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		operationID := ""
		switch req.Class {
		case ClassIntake:
			operationID = req.ObligationID
		case ClassJobLaunch:
			l, err := r.GetLaunch(ctx, req.ObligationID)
			if errors.Is(err, domain.ErrNotFound) {
				return s.unrestorable(ctx, r, cmd, req, &run)
			}
			if err != nil {
				return err
			}
			operationID = l.OperationID
		case ClassModelDispatch, ClassToolDispatch:
			if dispatch, err = r.GetDispatch(ctx, req.ObligationID); errors.Is(err, domain.ErrNotFound) {
				return s.unrestorable(ctx, r, cmd, req, &run)
			} else if err != nil {
				return err
			}
			operationID = dispatch.OperationID
		case ClassBusinessWrite:
			e, err := r.GetEffect(ctx, req.ObligationID)
			if errors.Is(err, domain.ErrNotFound) {
				return s.unrestorable(ctx, r, cmd, req, &run)
			}
			if err != nil {
				return err
			}
			operationID = e.OperationID
		}
		op, err = r.GetOperationScoped(ctx, operationID, cmd.TenantID)
		if errors.Is(err, domain.ErrNotFound) {
			return s.unrestorable(ctx, r, cmd, req, &run)
		}
		return err
	}); err != nil {
		return nil, false, err
	}
	if existing {
		return d, true, nil
	}
	// An early answer for a stale epoch; the authoritative check is made
	// again under the lock before anything is written.
	if op != nil {
		if err := currentRecoveryEpoch(op, req.RecoveryEpoch); err != nil {
			return nil, false, err
		}
	} else if run.RecoveryEpoch != req.RecoveryEpoch {
		return nil, false, fmt.Errorf("%w: disposition names recovery epoch %d, run %s is at epoch %d", domain.ErrStaleExecution, req.RecoveryEpoch, run.ID, run.RecoveryEpoch)
	}
	if err := s.operator(ctx, cmd.TenantID, cmd.ActorID, ActionDisposition); err != nil {
		return nil, false, err
	}
	if dispatch != nil && req.Decision == domain.DisposeConfirmNotSent {
		if err := s.notSent.VerifyNotSent(ctx, dispatch, req.EvidenceRef, req.EvidenceDigest); err != nil {
			return nil, false, err
		}
	} else if err := s.evidence.VerifyDisposition(ctx, req.Class, req.ObligationID, req.Decision, req.EvidenceRef, req.EvidenceDigest); err != nil {
		return nil, false, err
	}
	// One commit: epoch under the operation lock, the obligation's change,
	// the disposition row and the finding.
	guard := func(locked *domain.Operation) error { return currentRecoveryEpoch(locked, req.RecoveryEpoch) }
	err = s.store.Tx(ctx, func(r Repo) error {
		var lockedOp *domain.Operation
		switch {
		case op == nil:
			// No operation to bind: the run row (rank 1) carries the epoch.
			lockedRun, err := r.LockRecoveryRun(ctx, run.ID)
			if err != nil {
				return err
			}
			if lockedRun.RecoveryEpoch != req.RecoveryEpoch {
				return fmt.Errorf("%w: disposition names recovery epoch %d, run %s is at epoch %d", domain.ErrStaleExecution, req.RecoveryEpoch, lockedRun.ID, lockedRun.RecoveryEpoch)
			}
		case dispatch != nil && req.Decision == domain.DisposeConfirmNotSent:
			// Allocations (1), operation (2) and dispatch (4) in rank order;
			// the guard runs under the operation lock before the change.
			if _, _, err := s.dispatch.ConfirmNotSentLocked(ctx, r, op.ID, dispatch.ID, req.EvidenceRef, guard); err != nil {
				return err
			}
		default:
			locked, err := r.LockOperation(ctx, op.ID)
			if err != nil {
				return err
			}
			if err := guard(locked); err != nil {
				return err
			}
			lockedOp = locked
		}
		now := s.clock.Now()
		if req.Class == ClassBusinessWrite && req.Decision != domain.DisposeRetainExposure {
			e, err := r.LockEffect(ctx, req.ObligationID)
			if err != nil {
				return err
			}
			switch req.Decision {
			case domain.DisposeConfirmNotSent:
				if err := e.ConfirmNotSent(req.EvidenceRef); err != nil {
					return err
				}
			default:
				outcome := domain.EffectOutcomeSucceeded
				if req.Decision == domain.DisposeResolveFailed {
					outcome = domain.EffectOutcomeFailed
				}
				if _, err := r.GetEffectObservation(ctx, e.ID, "disposition", req.RecoveryEpoch); errors.Is(err, domain.ErrNotFound) {
					if _, err := e.Observe(outcome, req.EvidenceRef, now); err != nil {
						return err
					}
					if err := r.InsertEffectObservation(ctx, &domain.EffectObservation{ID: domain.NewID("eobs"), EffectID: e.ID, Source: "disposition", Sequence: req.RecoveryEpoch, Outcome: outcome, ReceiptDigest: string(req.EvidenceDigest), ObservedAt: now}); err != nil {
						return err
					}
				} else if err != nil {
					return err
				}
			}
			e.UpdatedAt = now
			if err := r.UpdateEffect(ctx, e); err != nil {
				return err
			}
		}
		_ = lockedOp
		d = &domain.Disposition{
			ID: domain.NewID("dsp"), Class: req.Class, ObligationID: req.ObligationID, TenantID: cmd.TenantID, RunID: req.RunID, RecoveryEpoch: req.RecoveryEpoch,
			CommandID: cmd.CommandID, ActorID: cmd.ActorID, RequestDigest: cmd.RequestDigest, Decision: req.Decision, EvidenceRef: req.EvidenceRef,
			EvidenceDigest: req.EvidenceDigest, Reason: req.Reason, DecidedAt: now,
		}
		if err := r.InsertDisposition(ctx, d); err != nil {
			return err
		}
		if req.RunID == "" {
			return nil
		}
		f, err := r.GetFindingByObligation(ctx, req.RunID, req.Class, req.ObligationID)
		if errors.Is(err, domain.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if lockedF, err := r.LockFinding(ctx, f.ID); err == nil && !lockedF.Status.Settled() {
			lockedF.Status, lockedF.Outcome, lockedF.EvidenceRef, lockedF.Detail, lockedF.UpdatedAt = domain.FindingDisposed, string(req.Decision), req.EvidenceRef, req.Reason, now
			return r.UpdateFinding(ctx, lockedF)
		} else if err != nil {
			return err
		}
		return nil
	})
	if errors.Is(err, ErrDuplicateKey) {
		return s.Dispose(ctx, cmd, req)
	}
	return d, false, err
}

// unrestorable binds a disposition to the run's finding when the database
// holds no operation for the obligation: the finding must exist in the
// named run, the command must be scoped to the run and the only decision
// that applies is to retain the exposure (nothing else can be established
// about an obligation that cannot be read as its class).
func (s *Recovery) unrestorable(ctx context.Context, r Repo, cmd domain.CommandIdentity, req DispositionRequest, run **domain.RecoveryRun) error {
	if req.RunID == "" {
		return fmt.Errorf("%w: %s %s: no such obligation", domain.ErrNotFound, req.Class, req.ObligationID)
	}
	if req.Decision != domain.DisposeRetainExposure {
		return fmt.Errorf("%w: %s %s has no restorable identity; only retain_exposure applies", domain.ErrInvalid, req.Class, req.ObligationID)
	}
	found, err := r.GetRecoveryRun(ctx, req.RunID)
	if err != nil {
		return err
	}
	if found.ScopeKey != cmd.TenantID {
		return domain.ErrNotFound
	}
	if _, err := r.GetFindingByObligation(ctx, found.ID, req.Class, req.ObligationID); err != nil {
		return err
	}
	*run = found
	return nil
}

// currentRecoveryEpoch refuses a disposition that names another recovery
// epoch than the operation's current one.
func currentRecoveryEpoch(op *domain.Operation, epoch uint64) error {
	if op.RecoveryEpoch != epoch {
		return fmt.Errorf("%w: disposition names recovery epoch %d, operation %s is at epoch %d", domain.ErrStaleExecution, epoch, op.ID, op.RecoveryEpoch)
	}
	return nil
}
