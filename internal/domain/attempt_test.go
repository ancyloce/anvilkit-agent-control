package domain

import (
	"errors"
	"testing"
	"time"
)

var (
	testCmd     = CommandIdentity{TenantID: "tenant_a", CommandID: "cmd", ActorID: "u", RequestDigest: "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"}
	testImage   = Digest("sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0")
	testProfile = Profile{ID: "local-check-v1", Kind: KindLocalCheck, OperationDeadline: 15 * time.Minute, StepID: "local-check", JobProfileID: "local-check-v1"}
)

func openOperation(t *testing.T, now time.Time) *Operation {
	t.Helper()
	op, err := NewOperation(testCmd, Scope{TenantID: "tenant_a", ActorID: "u"}, KindLocalCheck, Subject{ProfileID: testProfile.ID, SubjectDigest: testCmd.RequestDigest}, testProfile, now)
	if err != nil {
		t.Fatal(err)
	}
	op.Intake = IntakeConfirmed
	return op
}

func TestLaunchRefusedAfterAbsoluteDeadline(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	op := openOperation(t, now)
	at, err := NewAttempt(op, testCmd, "local-check", 0, 1, testProfile.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewLaunch(op, at, testCmd, "lc-1", "kind", testImage, now.Add(2*time.Minute), now); err != nil {
		t.Fatalf("launch inside the deadline: %v", err)
	}
	// A launch that would end after the deadline is clamped, never extended.
	l, err := NewLaunch(op, at, testCmd, "lc-1", "kind", testImage, op.Deadline.Add(time.Hour), now)
	if err != nil || !l.Deadline.Equal(at.Deadline) {
		t.Fatalf("launch deadline %v, err %v; want clamped to %v", l, err, at.Deadline)
	}
	// At or after the absolute deadline no launch is recorded, even for a
	// retried command that was valid earlier.
	for _, late := range []time.Time{op.Deadline, op.Deadline.Add(time.Second)} {
		if _, err := NewLaunch(op, at, testCmd, "lc-1", "kind", testImage, late.Add(2*time.Minute), late); !errors.Is(err, ErrStaleExecution) {
			t.Fatalf("launch at %v: got %v, want ErrStaleExecution", late, err)
		}
		if _, err := NewAttempt(op, testCmd, "local-check", 0, 2, testProfile.ID, late); !errors.Is(err, ErrStaleExecution) {
			t.Fatalf("attempt at %v: got %v, want ErrStaleExecution", late, err)
		}
	}
	// A requested launch deadline that has already passed is refused too.
	if _, err := NewLaunch(op, at, testCmd, "lc-1", "kind", testImage, now.Add(-time.Second), now); !errors.Is(err, ErrStaleExecution) {
		t.Fatalf("passed launch deadline: got %v", err)
	}
}

func TestSettleCloseKeepsCancelPendingWhileCleanupUnknown(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	op := openOperation(t, now)
	at, err := NewAttempt(op, testCmd, "local-check", 0, 1, testProfile.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	op.Control, op.Cleanup, op.Lifecycle = ControlCancelPending, CleanupPending, LifecycleRunning

	SettleClose(op, at, nil, OutcomeCanceled, CleanupUnknown, "CANCELED", false)
	if op.Lifecycle != LifecycleReconciling || op.Control != ControlCancelPending || op.Cleanup != CleanupUnknown {
		t.Fatalf("unknown cleanup settled as %s/%s/%s; want reconciling/cancel_pending/unknown", op.Lifecycle, op.Control, op.Cleanup)
	}
	if !op.FencedForNewDispatch() || op.SendersQuiescent(false) {
		t.Fatal("a reconciling operation must stay fenced and never count as quiescent")
	}
	d, err := op.DecideCancel(op.Revision, op.SendersQuiescent(false))
	if err != nil || d.Outcome != OutcomePending || d.Applied {
		t.Fatalf("repeated cancel while reconciling: %+v, %v; want pending and not applied", d, err)
	}

	// The same close reported with confirmed cleanup applies the cancel.
	op2 := openOperation(t, now)
	at2, err := NewAttempt(op2, testCmd, "local-check", 0, 1, testProfile.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	op2.Control, op2.Cleanup, op2.Lifecycle = ControlCancelPending, CleanupPending, LifecycleRunning
	SettleClose(op2, at2, nil, OutcomeCanceled, CleanupComplete, "CANCELED", false)
	if op2.Lifecycle != LifecycleCanceled || op2.Control != ControlCancelApplied {
		t.Fatalf("confirmed cleanup settled as %s/%s; want canceled/cancel_applied", op2.Lifecycle, op2.Control)
	}

	// Without a cancel, unknown cleanup after a completed attempt is still not success.
	op3 := openOperation(t, now)
	at3, _ := NewAttempt(op3, testCmd, "local-check", 0, 1, testProfile.ID, now)
	st := &Stage{Verdict: VerdictCertified}
	SettleClose(op3, at3, st, OutcomeCompleted, CleanupUnknown, "", false)
	if op3.Lifecycle != LifecycleReconciling || op3.FailureCode != "EFFECT_UNCERTAIN" || op3.Control != ControlNone {
		t.Fatalf("certified with unknown cleanup: %s/%s/%s", op3.Lifecycle, op3.FailureCode, op3.Control)
	}
	op4 := openOperation(t, now)
	at4, _ := NewAttempt(op4, testCmd, "local-check", 0, 1, testProfile.ID, now)
	SettleClose(op4, at4, st, OutcomeCompleted, CleanupComplete, "", false)
	if op4.Lifecycle != LifecycleSucceeded {
		t.Fatalf("certified with complete cleanup: %s", op4.Lifecycle)
	}
}
