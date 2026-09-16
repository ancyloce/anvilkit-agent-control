package application

import (
	"context"
	"log/slog"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Relay is Control's durable Temporal start/cancel relay (DD-01 §1). It
// polls intents committed with the operation and applies them idempotently
// by operation identity; it never chooses the next business action.
type Relay struct {
	store     Store
	workflows WorkflowRelay
	ops       *Operations
	recovery  *Recovery
	clock     domain.Clock
	log       *slog.Logger
	interval  time.Duration
}

func NewRelay(store Store, workflows WorkflowRelay, ops *Operations, recovery *Recovery, clock domain.Clock, log *slog.Logger, interval time.Duration) *Relay {
	return &Relay{store: store, workflows: workflows, ops: ops, recovery: recovery, clock: clock, log: log, interval: interval}
}

// Run polls until ctx is done.
func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		if _, err := r.ops.ReconcileIntake(ctx, r.interval, 50); err != nil && ctx.Err() == nil {
			r.log.Warn("intake reconciliation failed", "error", err)
		}
		if err := r.Tick(ctx); err != nil && ctx.Err() == nil {
			r.log.Warn("relay tick failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick applies every pending start and cancel intent once, and starts the
// reconciliation workflow of every recovery run that has none yet.
func (r *Relay) Tick(ctx context.Context) error {
	var pending []*domain.Operation
	var runs []*domain.RecoveryRun
	if err := r.store.Read(ctx, func(repo Repo) error {
		var err error
		if pending, err = repo.ListRelayPending(ctx, 100); err != nil {
			return err
		}
		runs, err = repo.ListRecoveryPending(ctx, 10)
		return err
	}); err != nil {
		return err
	}
	for _, run := range runs {
		if _, err := r.workflows.StartRecovery(ctx, run.ID); err != nil {
			r.log.Warn("recovery workflow start deferred", "runId", run.ID, "error", err)
			continue
		}
		if err := r.recovery.Started(ctx, run.ID); err != nil {
			return err
		}
	}
	for _, op := range pending {
		switch op.Relay {
		case domain.RelayPending:
			runID, err := r.workflows.Start(ctx, op.ID, op.TenantID)
			if err != nil {
				r.log.Warn("workflow start deferred", "operationId", op.ID, "error", err)
				continue
			}
			if err := r.mark(ctx, op.ID, "relay:started", func(o *domain.Operation) {
				if o.Relay != domain.RelayPending {
					return
				}
				o.Relay, o.RelayRunID = domain.RelayStarted, runID
				// The started run may already have opened its attempt
				// before this mark lands: the phase only advances from the
				// intake phase, never back over a later one.
				if o.Phase == "accepted" {
					o.Phase = "scheduled"
				}
			}); err != nil {
				return err
			}
		case domain.RelayCancelPending:
			if err := r.workflows.Cancel(ctx, op.ID); err != nil {
				r.log.Warn("workflow cancel deferred", "operationId", op.ID, "error", err)
				continue
			}
			if err := r.mark(ctx, op.ID, "relay:cancel-sent", func(o *domain.Operation) {
				if o.Relay == domain.RelayCancelPending {
					o.Relay = domain.RelaySettled
				}
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Relay) mark(ctx context.Context, operationID, transition string, apply func(*domain.Operation)) error {
	now := r.clock.Now()
	return r.store.Tx(ctx, func(repo Repo) error {
		op, err := repo.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		before := op.Relay
		ev := op.Transition(transition, now, apply)
		if op.Relay == before {
			return nil // another replica applied it first; nothing to commit
		}
		if err := repo.UpdateOperation(ctx, op); err != nil {
			return err
		}
		return repo.InsertEvent(ctx, ev)
	})
}
