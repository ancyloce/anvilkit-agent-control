package application

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Relay is Control's durable Temporal start/cancel relay (DD-01 §1). It
// polls intents committed with the operation and applies them idempotently
// by operation identity; it never chooses the next business action.
type Relay struct {
	store        Store
	workflows    WorkflowRelay
	ops          *Operations
	recovery     *Recovery
	preparations *Preparations
	generations  *Generations
	clock        domain.Clock
	log          *slog.Logger
	interval     time.Duration
}

func NewRelay(store Store, workflows WorkflowRelay, ops *Operations, recovery *Recovery, preparations *Preparations, generations *Generations, clock domain.Clock, log *slog.Logger, interval time.Duration) *Relay {
	return &Relay{store: store, workflows: workflows, ops: ops, recovery: recovery, preparations: preparations, generations: generations, clock: clock, log: log, interval: interval}
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

// Tick applies every pending start and cancel intent once, starts the
// reconciliation workflow of every recovery run that has none yet, and
// issues the tracked Updates of accepted answers and supported control
// commands (each under its stable update identity, so a lost receipt is
// repaired by the same call, never by a second identity).
func (r *Relay) Tick(ctx context.Context) error {
	var pending []*domain.Operation
	var runs []*domain.RecoveryRun
	var answers []*domain.Answer
	var commands []*domain.Command
	if err := r.store.Read(ctx, func(repo Repo) error {
		var err error
		if pending, err = repo.ListRelayPending(ctx, 100); err != nil {
			return err
		}
		if runs, err = repo.ListRecoveryPending(ctx, 10); err != nil {
			return err
		}
		if answers, err = repo.ListAnswerRelayPending(ctx, 50); err != nil {
			return err
		}
		commands, err = repo.ListCommandRelayPending(ctx, 50)
		return err
	}); err != nil {
		return err
	}
	for _, a := range answers {
		r.relayAnswer(ctx, a)
	}
	for _, c := range commands {
		r.relayCommand(ctx, c)
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
			runID, err := r.workflows.Start(ctx, op.ID, op.TenantID, op.Kind)
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

// relayAnswer issues the answer's Update. The intent moves to sent before
// the call so a receipt lost afterwards is retried under the same update
// id on the next tick; the handler's answer settles it; a run that cannot
// take it (closed) settles it as rejected.
func (r *Relay) relayAnswer(ctx context.Context, a *domain.Answer) {
	if a.Relay == domain.AnswerRelayPending {
		if _, err := r.preparations.RecordAnswerRelay(ctx, a.TenantID, a.ID, domain.AnswerRelaySent); err != nil {
			r.log.Warn("answer relay mark deferred", "answerId", a.ID, "error", err)
			return
		}
	}
	outcome, err := r.workflows.UpdateAnswer(ctx, a.OperationID, a)
	switch {
	case errors.Is(err, ErrRelayClosed):
		r.log.Warn("answer update refused: the run cannot take it", "answerId", a.ID, "operationId", a.OperationID, "error", err)
		outcome = RelayOutcome{Outcome: domain.OutcomeRejected, ReasonCode: "RUN_CLOSED"}
	case err != nil:
		r.log.Warn("answer update receipt not obtained; the same update is reissued", "answerId", a.ID, "error", err)
		return
	}
	state := domain.AnswerRelayApplied
	if outcome.Outcome != domain.OutcomeApplied {
		state = domain.AnswerRelayRejected
	}
	if _, err := r.preparations.RecordAnswerRelay(ctx, a.TenantID, a.ID, state); err != nil {
		r.log.Warn("answer relay outcome not recorded", "answerId", a.ID, "error", err)
	}
}

// relayCommand issues a hold, resume or change_definition Update under the
// command id and records the handler's answer.
func (r *Relay) relayCommand(ctx context.Context, c *domain.Command) {
	if c.Relay == domain.CommandRelayPending {
		if err := r.generations.MarkCommandRelay(ctx, c.TenantID, c.CommandID, domain.CommandRelaySent); err != nil {
			r.log.Warn("command relay mark deferred", "commandId", c.CommandID, "error", err)
			return
		}
	}
	outcome, err := r.workflows.UpdateCommand(ctx, c.OperationID, c)
	switch {
	case errors.Is(err, ErrRelayClosed):
		outcome = RelayOutcome{Outcome: domain.OutcomeRejected, ReasonCode: "RUN_CLOSED"}
	case err != nil:
		r.log.Warn("command update receipt not obtained; the same update is reissued", "commandId", c.CommandID, "error", err)
		return
	}
	if _, err := r.generations.RecordCommandRelay(ctx, c.TenantID, c.CommandID, outcome.Outcome, outcome.ReasonCode); err != nil {
		r.log.Warn("command relay outcome not recorded", "commandId", c.CommandID, "error", err)
	}
}
