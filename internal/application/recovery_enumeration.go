package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Recovery: enumeration of the window's obligations and their settlement in
// the restored database (the run's order is described on Recovery).

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
