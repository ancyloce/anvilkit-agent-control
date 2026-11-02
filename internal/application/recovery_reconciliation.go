package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Recovery: reconciliation of findings — restoring lost identities,
// querying original outcomes and recording the trusted launcher's evidence.

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
			if err := s.restoreOperation(ctx, r, run, rec.TenantID, rec.ProjectID, rec.OperationID, rec.CommandID, domain.OperationKind(rec.Kind), rec.ProfileID, domain.Digest(rec.SubjectDigest), domain.Digest(rec.RequestDigest), ob.RecordedAt, rec.ExecutionEpoch, version, now); err != nil {
				return err
			}
			status, outcome = domain.FindingResolved, "restored"
		case ClassJobLaunch:
			rec := ob.Launch
			if err := s.restoreOperation(ctx, r, run, rec.TenantID, "", rec.OperationID, "", domain.KindLocalCheck, rec.ProfileID, "", "", ob.RecordedAt, rec.ExecutionEpoch, "", now); err != nil {
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
			if err := s.restoreOperation(ctx, r, run, rec.TenantID, "", rec.OperationID, "", domain.KindLocalCheck, "", "", "", ob.RecordedAt, rec.ExecutionEpoch, "", now); err != nil {
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
				// The rollback lost the send's reservation together with the
				// dispatch: the restored exposure is held again on every
				// allocation of the operation (rank 1 before the operation),
				// so the budget never shows headroom the unresolved send may
				// have spent and its settlement releases or charges what it
				// holds. Allocations created after the restore point are gone
				// with their operation and hold nothing.
				b, err := lockBudget(ctx, r, rec.OperationID)
				if err != nil {
					return err
				}
				if err := b.restore(d.Reserved); err != nil {
					return err
				}
				if err := b.save(ctx, r); err != nil {
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
			if err := s.restoreOperation(ctx, r, run, rec.TenantID, "", rec.OperationID, "", domain.KindLocalCheck, "", "", "", ob.RecordedAt, rec.ExecutionEpoch, "", now); err != nil {
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
func (s *Recovery) restoreOperation(ctx context.Context, r Repo, run *domain.RecoveryRun, tenantID, projectID, operationID, commandID string, kind domain.OperationKind, profileID string, subjectDigest, requestDigest domain.Digest, recordedAt time.Time, executionEpoch uint64, intakeVersion string, now time.Time) error {
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
		ID: operationID, TenantID: tenantID, ProjectID: projectID, ActorID: "recovery", CommandID: commandID, Kind: kind, Subject: domain.Subject{ProfileID: profileID, SubjectDigest: subjectDigest},
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
	observed, _, err := s.dispatch.Observe(ctx, Reader{TenantID: f.TenantID}, d.ID, report.Source, report.Sequence, report.Outcome, report.Usage, report.NativeReference, report.ObservedAt)
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
		if _, err := s.exec.ObserveInstance(ctx, f.TenantID, l.AttemptID, inst.ID, pod.Phase, pod.ExitCode, pod.ObservedAt); err != nil {
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
