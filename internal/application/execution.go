package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Execution implements attempts, the job-launch obligation, physical
// instance registration and accepted results (DD-02 §5–§6, DD-03 §4).
type Execution struct {
	store     Store
	inventory Inventory
	manifests ManifestValidator
	profiles  map[string]domain.Profile
	clock     domain.Clock
	log       *slog.Logger
}

func NewExecution(store Store, inventory Inventory, manifests ManifestValidator, profiles []domain.Profile, clock domain.Clock, log *slog.Logger) *Execution {
	m := make(map[string]domain.Profile, len(profiles))
	for _, p := range profiles {
		m[p.ID] = p
	}
	return &Execution{store: store, inventory: inventory, manifests: manifests, profiles: m, clock: clock, log: log}
}

// multiStep reports whether the operation's profile settles its business
// outcome through SettleOperation (an unknown profile is single-step).
func (s *Execution) multiStep(op *domain.Operation) bool {
	return s.profiles[op.Subject.ProfileID].MultiStep
}

// OpenAttempt allocates a logical attempt under the current epoch; the same
// command returns the original attempt.
func (s *Execution) OpenAttempt(ctx context.Context, cmd domain.CommandIdentity, operationID, stepID string, visit uint64, profileID string) (at *domain.Attempt, existing bool, err error) {
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		op, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if op.TenantID != cmd.TenantID {
			return domain.ErrNotFound
		}
		found, err := r.GetAttemptByCommand(ctx, op.ID, cmd.CommandID)
		switch {
		case err == nil:
			if found.RequestDigest != cmd.RequestDigest {
				return fmt.Errorf("%w: command %s", domain.ErrIdempotencyConflict, cmd.CommandID)
			}
			at, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		if err := admissionOpen(ctx, r, op.TenantID); err != nil {
			return err
		}
		if err := s.bootstrapGate(ctx, r, op, now); err != nil {
			return err
		}
		n, err := r.CountAttempts(ctx, op.ID, stepID, visit)
		if err != nil {
			return err
		}
		fresh, err := domain.NewAttempt(op, cmd, stepID, visit, n+1, profileID, now)
		if err != nil {
			return err
		}
		if err := r.InsertAttempt(ctx, fresh); err != nil {
			return err
		}
		ev := op.Transition("attempt:"+fresh.ID+":opened", now, func(o *domain.Operation) {
			o.Lifecycle = domain.LifecycleRunning
			o.Phase = "attempt_open"
		})
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		at = fresh
		return nil
	})
	return at, existing, err
}

// bootstrapGate denies an attempt of a queued profile (a Generation)
// whose bootstrap is incomplete: no active execution permit, no funding
// occurrence or no confirmed lease covering now (DD-01 §4: admission,
// funding and lease before authored execution).
func (s *Execution) bootstrapGate(ctx context.Context, r Repo, op *domain.Operation, now time.Time) error {
	profile := s.profiles[op.Subject.ProfileID]
	if !profile.MultiStep {
		return nil
	}
	// A new attempt of a multi-step operation is a recovery of the work
	// so far: it starts only once the original effects and costs are
	// reconciled — an unknown dispatch (exposure unknown) or an unresolved
	// business-write effect never permits a resend (DD-01 §5, P13-04).
	if op.Finance == domain.FinanceExposureUnknown {
		return fmt.Errorf("%w: operation %s carries an unknown dispatch; reconcile it before a new attempt", domain.ErrEffectUncertain, op.ID)
	}
	if unresolved, err := unresolvedEffects(ctx, r, op.ID); err != nil {
		return err
	} else if unresolved {
		return fmt.Errorf("%w: operation %s carries an unresolved effect; reconcile it before a new attempt", domain.ErrEffectUncertain, op.ID)
	}
	if profile.QueuePool == "" {
		return nil
	}
	if _, err := r.GetActivePermit(ctx, profile.QueuePool, "operation", op.ID); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("%w: operation %s holds no execution permit", domain.ErrStaleExecution, op.ID)
		}
		return err
	}
	if _, err := r.GetFunding(ctx, op.ID); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("%w: operation %s is not funded", domain.ErrBudgetExhausted, op.ID)
		}
		return err
	}
	if !op.Lease.Valid(now) {
		return fmt.Errorf("%w: operation %s holds no confirmed lease covering %s (%s)", domain.ErrStaleExecution, op.ID, now.UTC().Format(time.RFC3339), op.Lease.State)
	}
	return nil
}

// admissionOpen denies new admission (attempts, launches) for a tenant
// whose scope a recovery run closed; issued effects and costs are kept.
func admissionOpen(ctx context.Context, r Repo, tenantID string) error {
	closed, err := r.IsAdmissionClosed(ctx, tenantID)
	if err != nil {
		return err
	}
	if closed {
		return fmt.Errorf("%w: %s: admission for tenant %s is closed by a recovery run", domain.ErrStaleExecution, domain.DenyRecoveryRestricted, tenantID)
	}
	return nil
}

// lockOperationThenAttempt takes the operation lock (rank 2) before the
// attempt lock (rank 3) and verifies the caller's tenant.
func lockOperationThenAttempt(ctx context.Context, r Repo, attemptID, tenantID string) (*domain.Operation, *domain.Attempt, error) {
	peek, err := r.GetAttempt(ctx, attemptID)
	if err != nil {
		return nil, nil, err
	}
	if peek.TenantID != tenantID {
		return nil, nil, domain.ErrNotFound
	}
	op, err := r.LockOperation(ctx, peek.OperationID)
	if err != nil {
		return nil, nil, err
	}
	at, err := r.LockAttempt(ctx, attemptID)
	if err != nil {
		return nil, nil, err
	}
	return op, at, nil
}

// PrepareLaunch persists the job-launch obligation, inventories it outside
// locks and confirms it. Only a confirmed launch may be created on the
// backend; a fenced operation is denied with ErrStaleExecution.
func (s *Execution) PrepareLaunch(ctx context.Context, cmd domain.CommandIdentity, attemptID, launchKey, backend string, image domain.Digest, deadline time.Time) (l *domain.Launch, existing bool, err error) {
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		op, at, err := lockOperationThenAttempt(ctx, r, attemptID, cmd.TenantID)
		if err != nil {
			return err
		}
		found, err := r.GetLaunchByCommand(ctx, at.ID, cmd.CommandID)
		switch {
		case err == nil:
			if found.RequestDigest != cmd.RequestDigest {
				return fmt.Errorf("%w: command %s", domain.ErrIdempotencyConflict, cmd.CommandID)
			}
			l, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		if byKey, err := r.GetLaunchByKey(ctx, backend, launchKey); err == nil && byKey.AttemptID != at.ID {
			return fmt.Errorf("%w: launch key %s belongs to attempt %s", domain.ErrIdempotencyConflict, launchKey, byKey.AttemptID)
		} else if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		if err := admissionOpen(ctx, r, op.TenantID); err != nil {
			return err
		}
		fresh, err := domain.NewLaunch(op, at, cmd, launchKey, backend, image, deadline, now)
		if err != nil {
			return err
		}
		if err := r.InsertLaunch(ctx, fresh); err != nil {
			return err
		}
		at.State = domain.AttemptLaunchPrepared
		at.UpdatedAt = now
		if err := r.UpdateAttempt(ctx, at); err != nil {
			return err
		}
		ev := op.Transition("launch:"+fresh.ID, now, func(o *domain.Operation) {
			o.Phase = "launch_prepared"
			o.Cleanup = domain.CleanupPending
		})
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		l = fresh
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if l.Inventory != domain.InventoryConfirmed {
		if err := s.confirmLaunch(ctx, l, cmd.TenantID); err != nil {
			return nil, false, err
		}
	}
	return l, existing, nil
}

func (s *Execution) confirmLaunch(ctx context.Context, l *domain.Launch, tenantID string) error {
	key, body := launchObligation(l, tenantID)
	version, err := s.inventory.Put(ctx, key, body)
	if err != nil {
		return fmt.Errorf("%w: job-launch inventory write for %s: %v", domain.ErrEffectUncertain, l.ID, err)
	}
	now := s.clock.Now()
	return s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockLaunch(ctx, l.ID)
		if err != nil {
			return err
		}
		if locked.Inventory != domain.InventoryConfirmed {
			locked.Inventory, locked.InventoryVersion, locked.UpdatedAt = domain.InventoryConfirmed, version, now
			if err := r.UpdateLaunchInventory(ctx, locked); err != nil {
				return err
			}
		}
		*l = *locked
		return nil
	})
}

// RegisterInstance records real Job/Pod identities for a confirmed launch.
// The first Pod becomes current; duplicates are recorded as non-current.
// Evidence registered for an attempt that is already closed (the run's
// cleanup observing the original launch key while the operation reconciles
// an unknown cleanup) is recorded as an instance but never reopens the
// attempt or moves the projection: the settlement close carries the
// cleanup evidence.
func (s *Execution) RegisterInstance(ctx context.Context, cmd domain.CommandIdentity, attemptID, launchKey, backend, jobUID, podUID string, image domain.Digest) (inst *domain.Instance, existing bool, err error) {
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		op, at, err := lockOperationThenAttempt(ctx, r, attemptID, cmd.TenantID)
		if err != nil {
			return err
		}
		found, err := r.GetInstanceByPod(ctx, backend, podUID)
		switch {
		case err == nil:
			if found.AttemptID != at.ID {
				return fmt.Errorf("%w: pod %s belongs to attempt %s", domain.ErrStaleExecution, podUID, found.AttemptID)
			}
			inst, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		l, err := r.GetLaunchByKey(ctx, backend, launchKey)
		if err != nil {
			return err
		}
		if l.AttemptID != at.ID || l.Inventory != domain.InventoryConfirmed {
			return fmt.Errorf("%w: launch %s is not a confirmed launch of attempt %s", domain.ErrStaleExecution, launchKey, at.ID)
		}
		hasCurrent, err := r.HasCurrentInstance(ctx, at.ID)
		if err != nil {
			return err
		}
		fresh, err := domain.NewInstance(l, jobUID, podUID, image, hasCurrent, now)
		if err != nil {
			return err
		}
		if err := r.InsertInstance(ctx, fresh); err != nil {
			return err
		}
		if fresh.Current && at.State != domain.AttemptClosed {
			at.State, at.UpdatedAt = domain.AttemptRunning, now
			if err := r.UpdateAttempt(ctx, at); err != nil {
				return err
			}
			ev := op.Transition("instance:"+fresh.ID+":registered", now, func(o *domain.Operation) { o.Phase = "running" })
			if err := r.UpdateOperation(ctx, op); err != nil {
				return err
			}
			if err := r.InsertEvent(ctx, ev); err != nil {
				return err
			}
		}
		inst = fresh
		return nil
	})
	return inst, existing, err
}

// ObserveInstance records a backend observation on the instance row only.
func (s *Execution) ObserveInstance(ctx context.Context, attemptID, instanceID string, phase domain.InstancePhase, exitCode *int32, observedAt time.Time) (*domain.Instance, error) {
	var inst *domain.Instance
	err := s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockInstance(ctx, instanceID)
		if err != nil {
			return err
		}
		if locked.AttemptID != attemptID {
			return domain.ErrNotFound
		}
		locked.Phase, locked.ExitCode, locked.ObservedAt = phase, exitCode, &observedAt
		if err := r.UpdateInstanceObservation(ctx, locked); err != nil {
			return err
		}
		inst = locked
		return nil
	})
	return inst, err
}

// AcceptResult accepts exactly one result per attempt from the current
// physical instance under one commit (DD-02 §6, DD-03 §3). The manifest
// bytes must hash to the declared digest and validate against the jobs
// contract; its provenance (attempt, profile, verdict and failure code of
// the request, the job kind of the reviewed profile, the launch the
// accepting instance came from) must be what the request and the records
// say; its outputs must be what the reviewed profile prescribes for the
// verdict (a certified fixture result names the fixed result, reviewed
// digest and size, exactly once) and every output must name a finalized
// transfer of the same scope, class, digest and size under the current
// epochs, except that one fixed result a qualification fixture profile
// embeds, and the accepted stage binds those exact object versions.
// observerEpoch, when given, is the
// execution epoch the observer acted under and must be the operation's
// current one. A repeated acceptance of the same digest from the same
// instance returns the original stage; a different digest or instance for
// the same attempt conflicts. A refusal writes nothing.
func (s *Execution) AcceptResult(ctx context.Context, cmd domain.CommandIdentity, attemptID, instanceID, profileID string, verdict domain.Verdict, failureCode string, resultDigest domain.Digest, manifest []byte, observer string, observerEpoch *uint64) (st *domain.Stage, existing bool, err error) {
	if DigestOf(manifest) != resultDigest {
		return nil, false, fmt.Errorf("%w: result manifest bytes do not hash to %s", domain.ErrInvalid, resultDigest)
	}
	if err := s.manifests.ValidateResultManifest(manifest); err != nil {
		return nil, false, fmt.Errorf("%w: result manifest: %v", domain.ErrInvalid, err)
	}
	m, err := parseResultManifest(manifest)
	if err != nil {
		return nil, false, err
	}
	profile, err := s.manifests.JobProfile(profileID)
	if err != nil {
		return nil, false, fmt.Errorf("%w: result profile %q is not a reviewed job profile", domain.ErrStaleExecution, profileID)
	}
	if err := m.Claims(attemptID, profileID, verdict, failureCode, profile); err != nil {
		return nil, false, err
	}
	err = s.store.Tx(ctx, func(r Repo) error {
		op, at, err := lockOperationThenAttempt(ctx, r, attemptID, cmd.TenantID)
		if err != nil {
			return err
		}
		found, err := r.GetStageByAttempt(ctx, at.ID)
		switch {
		case err == nil:
			if found.ResultDigest != resultDigest || found.InstanceID != instanceID {
				return fmt.Errorf("%w: attempt %s already accepted stage %s", domain.ErrIdempotencyConflict, at.ID, found.ID)
			}
			if found.Artifacts, err = r.ListStageArtifacts(ctx, found.ID); err != nil {
				return err
			}
			st, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		inst, err := r.LockInstance(ctx, instanceID)
		if err != nil {
			return err
		}
		now := s.clock.Now()
		fresh, err := domain.AcceptStage(op, at, inst, cmd, profileID, verdict, failureCode, resultDigest, manifest, observer, observerEpoch, now)
		if err != nil {
			return err
		}
		if err := m.ProducedBy(inst); err != nil {
			return err
		}
		bound, err := BindOutputs(ctx, r, op, at, m, profile)
		if err != nil {
			return err
		}
		if err := r.InsertStage(ctx, fresh); err != nil {
			return err
		}
		for i := range bound {
			bound[i].StageID = fresh.ID
			if err := r.InsertStageArtifact(ctx, &bound[i]); err != nil {
				return err
			}
		}
		fresh.Artifacts = bound
		at.State, at.AcceptedStageID, at.UpdatedAt = domain.AttemptResultAccepted, fresh.ID, now
		if err := r.UpdateAttempt(ctx, at); err != nil {
			return err
		}
		ev := op.Transition("stage:"+fresh.ID, now, func(o *domain.Operation) { o.Phase = "result_accepted" })
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		st = fresh
		return nil
	})
	return st, existing, err
}

// parseResultManifest reads the checked view of validated manifest bytes.
func parseResultManifest(manifest []byte) (domain.ResultManifest, error) {
	var declared struct {
		LaunchID    string `json:"launchId"`
		AttemptID   string `json:"attemptId"`
		JobKind     string `json:"jobKind"`
		ProfileID   string `json:"profileId"`
		Verdict     string `json:"verdict"`
		FailureCode string `json:"failureCode"`
		Outputs     []struct {
			Class     string `json:"class"`
			Digest    string `json:"digest"`
			SizeBytes string `json:"sizeBytes"`
			Handle    string `json:"handle"`
		} `json:"outputs"`
	}
	if err := json.Unmarshal(manifest, &declared); err != nil {
		return domain.ResultManifest{}, fmt.Errorf("%w: result manifest: %v", domain.ErrInvalid, err)
	}
	m := domain.ResultManifest{LaunchID: declared.LaunchID, AttemptID: declared.AttemptID, JobKind: declared.JobKind, ProfileID: declared.ProfileID, Verdict: domain.Verdict(declared.Verdict), FailureCode: declared.FailureCode}
	for _, o := range declared.Outputs {
		size, err := domain.ParseRevision(o.SizeBytes)
		if err != nil {
			return domain.ResultManifest{}, fmt.Errorf("%w: result manifest output size %q", domain.ErrInvalid, o.SizeBytes)
		}
		m.Outputs = append(m.Outputs, domain.ManifestOutput{Class: o.Class, Digest: domain.Digest(o.Digest), SizeBytes: int64(size), Handle: o.Handle})
	}
	return m, nil
}

// GetAcceptedStage returns the accepted stage of an attempt with the
// artifacts it binds; tenantID, when given, scopes the read (a stage of
// another tenant is not found). When the original bytes are retained they
// are verified against result_digest on every read; a mismatch is an
// integrity failure, never a served result. A stage without retained
// bytes (accepted before migration 00002) is returned with a nil manifest
// and is not byte-verified.
//
// operationID, when given, is the authorized relationship of a cross-attempt
// read (P13): the stage must be the accepted stage of an attempt of that
// operation; a stage of another operation is not found.
func (s *Execution) GetAcceptedStage(ctx context.Context, attemptID, tenantID, operationID string) (*domain.Stage, error) {
	var st *domain.Stage
	err := s.store.Read(ctx, func(r Repo) error {
		if tenantID != "" || operationID != "" {
			at, err := r.GetAttempt(ctx, attemptID)
			if err != nil {
				return err
			}
			if tenantID != "" && at.TenantID != tenantID {
				return domain.ErrNotFound
			}
			if operationID != "" && at.OperationID != operationID {
				return domain.ErrNotFound
			}
		}
		var err error
		if st, err = r.GetStageByAttempt(ctx, attemptID); err != nil {
			return err
		}
		st.Artifacts, err = r.ListStageArtifacts(ctx, st.ID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if st.ResultManifest != nil && DigestOf(st.ResultManifest) != st.ResultDigest {
		return nil, fmt.Errorf("%w: stored result bytes of stage %s do not hash to %s", domain.ErrIntegrity, st.ID, st.ResultDigest)
	}
	return st, nil
}

// CloseAttempt closes the attempt with its outcome and cleanup evidence and
// settles the operation lifecycle and any pending cancel commands. A close
// of an already closed attempt returns the original for the same outcome
// and conflicts for a different one, except when it carries later cleanup
// evidence for a cleanup recorded as unknown: that settles the reconciling
// operation (and its pending cancel) under the same attempt identity.
func (s *Execution) CloseAttempt(ctx context.Context, cmd domain.CommandIdentity, attemptID string, outcome domain.AttemptOutcome, cleanup domain.CleanupState, failureCode string) (at *domain.Attempt, op *domain.Operation, existing bool, err error) {
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		lockedOp, locked, err := lockOperationThenAttempt(ctx, r, attemptID, cmd.TenantID)
		if err != nil {
			return err
		}
		transition := "attempt:" + locked.ID + ":closed"
		if locked.State == domain.AttemptClosed {
			switch {
			case locked.Outcome != outcome:
				return fmt.Errorf("%w: attempt %s closed as %s", domain.ErrIdempotencyConflict, locked.ID, locked.Outcome)
			case locked.CanSettleCleanup(outcome, cleanup):
				transition = "attempt:" + locked.ID + ":cleanup-settled"
			default:
				at, op, existing = locked, lockedOp, true
				return nil
			}
		}
		stage, err := r.GetStageByAttempt(ctx, locked.ID)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		locked.UpdatedAt = now
		ev := lockedOp.Transition(transition, now, func(o *domain.Operation) {
			domain.SettleClose(o, locked, stage, outcome, cleanup, failureCode, s.multiStep(o))
			if o.Lifecycle.Terminal() {
				o.Relay = domain.RelaySettled
			}
		})
		if err := r.UpdateAttempt(ctx, locked); err != nil {
			return err
		}
		if err := r.UpdateOperation(ctx, lockedOp); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		if err := settleTerminal(ctx, r, lockedOp, now); err != nil {
			return err
		}
		at, op = locked, lockedOp
		return nil
	})
	return at, op, existing, err
}

// settleTerminal records what a terminal operation settles beside its
// projection: an applied cancel settles the pending cancel commands and
// every terminal operation releases its execution permit (the pool's
// capacity is freed by the evidence of the close, never by a TTL).
func settleTerminal(ctx context.Context, r Repo, op *domain.Operation, now time.Time) error {
	if op.Control == domain.ControlCancelApplied {
		if err := r.SettlePendingCommands(ctx, op.ID, domain.CommandCancel, domain.OutcomeApplied, op.Revision, now); err != nil {
			return err
		}
	}
	if op.Lifecycle.Terminal() {
		return r.ReleasePermits(ctx, "operation", op.ID, now, "operation:"+string(op.Lifecycle))
	}
	return nil
}

// SettleOperation records the business outcome of a multi-step operation
// (DD-01 §2) under the same command identity: the same command returns the
// original; a terminal operation answers its state; an operation whose
// cleanup is unknown settles through reconciliation instead.
func (s *Execution) SettleOperation(ctx context.Context, cmd domain.CommandIdentity, operationID string, outcome domain.OperationOutcome, failureCode, phase string) (op *domain.Operation, existing bool, err error) {
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if locked.TenantID != cmd.TenantID {
			return domain.ErrNotFound
		}
		if !s.multiStep(locked) {
			return fmt.Errorf("%w: profile %s settles through its attempt close", domain.ErrInvalid, locked.Subject.ProfileID)
		}
		if locked.Lifecycle.Terminal() {
			op, existing = locked, true
			return nil
		}
		open, err := r.HasOpenAttempt(ctx, locked.ID)
		if err != nil {
			return err
		}
		if open {
			return fmt.Errorf("%w: operation %s still has an open attempt", domain.ErrStaleExecution, locked.ID)
		}
		unresolved, err := unresolvedEffects(ctx, r, locked.ID)
		if err != nil {
			return err
		}
		unresolved = unresolved || locked.Finance == domain.FinanceExposureUnknown
		var settle error
		ev := locked.Transition("settle:"+cmd.CommandID, now, func(o *domain.Operation) {
			if unresolved {
				// An unresolved obligation (an unknown dispatch or effect)
				// keeps the operation reconciling under the stated phase:
				// settlement continues, it never disappears behind a
				// completed flag (DD-01 §2).
				o.Lifecycle, o.FailureCode = domain.LifecycleReconciling, "EFFECT_UNCERTAIN"
				if phase != "" {
					o.Phase = phase
				}
				return
			}
			settle = o.SettleOperation(outcome, failureCode, phase)
		})
		if settle != nil {
			return settle
		}
		if err := r.UpdateOperation(ctx, locked); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		if err := settleTerminal(ctx, r, locked, now); err != nil {
			return err
		}
		op = locked
		return nil
	})
	return op, existing, err
}
