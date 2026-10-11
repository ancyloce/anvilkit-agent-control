package domain

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestEffectUsesFixedActiveDeadlineAndNarrowerBounds(t *testing.T) {
	now := time.Now()
	active := now.Add(time.Hour)
	op := &Operation{ID: "op", Lifecycle: LifecycleRunning, Control: ControlNone, ExecutionEpoch: 1, Deadline: now.Add(-time.Minute), ActiveDeadline: &active}
	at := &Attempt{ID: "attempt", OperationID: op.ID, State: AttemptOpen, ExecutionEpoch: 1, Deadline: now.Add(30 * time.Minute)}
	req := EffectRequest{OperationID: op.ID, AttemptID: at.ID, Owner: "workflow", Kind: EffectBusinessWrite, Occurrence: 1, CanonicalSubject: "source", ExecutionEpoch: 1, Deadline: now.Add(10 * time.Minute)}
	c := EffectContext{Operation: op, Attempt: at, Now: now}
	require.NoError(t, CheckEffect(req, c), "a passed queue deadline cannot shorten the active execution window")
	req.Deadline = at.Deadline.Add(time.Second)
	require.Error(t, CheckEffect(req, c))
	req.Deadline = now.Add(10 * time.Minute)
	req.LeaseID = "lease"
	req.LeaseExpiresAt = now.Add(5 * time.Minute)
	require.Error(t, CheckEffect(req, c), "effect cannot extend its lease")
	req.LeaseExpiresAt = now.Add(15 * time.Minute)
	at.Deadline = active.Add(time.Hour)
	req.Deadline = active.Add(time.Minute)
	require.Error(t, CheckEffect(req, c), "attempt cannot extend the fixed active deadline")
}

// P1.5 (CTL-APP-06): a business write of a lease-protected profile needs
// the operation's confirmed, valid lease, named by the request with its fence.
func TestBusinessWriteNeedsTheConfirmedLease(t *testing.T) {
	now := time.Now()
	expires := now.Add(10 * time.Minute)
	op := &Operation{ID: "op", Lifecycle: LifecycleRunning, Control: ControlNone, ExecutionEpoch: 1, Deadline: now.Add(time.Hour),
		Lease: LeaseRecord{State: LeaseHeld, LeaseID: "lease_x", Fence: 7, ExpiresAt: &expires}}
	req := EffectRequest{OperationID: op.ID, Owner: "workflow", Kind: EffectBusinessWrite, Occurrence: 1, CanonicalSubject: "source", ExecutionEpoch: 1,
		Deadline: now.Add(5 * time.Minute), LeaseID: "lease_x", LeaseFence: 7, LeaseExpiresAt: expires}
	c := EffectContext{Operation: op, Now: now, LeaseRequired: true}
	require.NoError(t, CheckEffect(req, c), "the confirmed lease, named with its fence, permits the write")
	// The request's word alone never permits: a mismatched lease or fence is refused.
	other := req
	other.LeaseID = "lease_other"
	require.Error(t, CheckEffect(other, c))
	other = req
	other.LeaseFence = 8
	require.Error(t, CheckEffect(other, c))
	// An expired or lost confirmed lease refuses even a matching request.
	past := now.Add(-time.Second)
	op.Lease.ExpiresAt = &past
	req.LeaseExpiresAt = now.Add(5 * time.Minute) // the request claims more than the record
	require.Error(t, CheckEffect(req, c), "the operation's recorded expiry decides, not the request's")
	op.Lease = LeaseRecord{State: LeaseLost}
	require.Error(t, CheckEffect(req, c))
	// Without a lease-protected profile the earlier rules alone apply.
	op.Lease = LeaseRecord{State: LeaseNone}
	op.Lifecycle = LifecycleRunning
	req.LeaseID, req.LeaseFence, req.LeaseExpiresAt = "", 0, time.Time{}
	require.NoError(t, CheckEffect(req, EffectContext{Operation: op, Now: now}))
}
