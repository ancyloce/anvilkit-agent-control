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
