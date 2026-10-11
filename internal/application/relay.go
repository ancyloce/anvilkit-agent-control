package application

import (
	"context"
	"errors"
	"log/slog"
	"sync"
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
	// updateTimeout bounds one tracked Update call (P1.4): an Update still
	// running at the bound stays sent and a later tick settles it.
	updateTimeout time.Duration
	// Lag gauges of the last tick per lane (pending intents seen).
	lagMu sync.Mutex
	lag   map[string]int
	// reconcile is the optional dispatch reconciliation lane.
	reconcile func(context.Context)
	// settlements is the optional settlement-intent lane (P1.6).
	settlements func(context.Context)
}

func NewRelay(store Store, workflows WorkflowRelay, ops *Operations, recovery *Recovery, preparations *Preparations, generations *Generations, clock domain.Clock, log *slog.Logger, interval time.Duration) *Relay {
	return &Relay{store: store, workflows: workflows, ops: ops, recovery: recovery, preparations: preparations, generations: generations, clock: clock, log: log, interval: interval, updateTimeout: 5 * time.Second, lag: map[string]int{}}
}

// WithUpdateTimeout sets the bound of one tracked Update call.
func (r *Relay) WithUpdateTimeout(d time.Duration) *Relay {
	if d > 0 {
		r.updateTimeout = d
	}
	return r
}

// Lag reports the pending intents the last tick of each lane saw
// (starts, answers, commands): the relay's backlog gauge.
func (r *Relay) Lag() map[string]int {
	r.lagMu.Lock()
	defer r.lagMu.Unlock()
	out := make(map[string]int, len(r.lag))
	for k, v := range r.lag {
		out[k] = v
	}
	return out
}

func (r *Relay) setLag(lane string, n int) {
	r.lagMu.Lock()
	r.lag[lane] = n
	r.lagMu.Unlock()
}

// Run polls until ctx is done. The three lanes — starts/cancels with the
// intake reconciliation, answer Updates, command Updates — run in their
// own goroutines (P1.4, B-24): a tenant's hold waiting on a quiescent step
// never delays another tenant's start, cancel or recovery.
func (r *Relay) Run(ctx context.Context) {
	var wg sync.WaitGroup
	lane := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(r.interval)
			defer t.Stop()
			for {
				if err := fn(ctx); err != nil && ctx.Err() == nil {
					r.log.Warn("relay lane failed", "lane", name, "error", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
	lane("starts", func(ctx context.Context) error {
		if _, err := r.ops.ReconcileIntake(ctx, r.interval, 50); err != nil && ctx.Err() == nil {
			r.log.Warn("intake reconciliation failed", "error", err)
		}
		return r.TickStarts(ctx)
	})
	lane("answers", r.TickAnswers)
	lane("commands", r.TickCommands)
	if r.reconcile != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.reconcile(ctx)
		}()
	}
	if r.settlements != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.settlements(ctx)
		}()
	}
	wg.Wait()
}

// WithSettlementReconciler adds the lane that re-applies stored settlement
// intents once the last obligation of a reconciling operation resolved
// (P1.6), at its own interval.
func (r *Relay) WithSettlementReconciler(exec *Execution, interval time.Duration, limit int) *Relay {
	if exec == nil || interval <= 0 {
		return r
	}
	r.settlements = func(ctx context.Context) {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			if n, err := exec.ReconcileSettlements(ctx, limit); err != nil && ctx.Err() == nil {
				r.log.Warn("settlement reconciliation failed", "error", err)
			} else if n > 0 {
				r.log.Info("operations settled from their stored intents", "count", n)
			}
		}
	}
	return r
}

// WithDispatchReconciler adds the periodic original-identity reconciliation
// of unresolved dispatches (P1.5) as a lane of its own, at its own interval.
func (r *Relay) WithDispatchReconciler(dispatch *Dispatch, queries OutcomeQuery, interval, age time.Duration, limit int) *Relay {
	if dispatch == nil || queries == nil || interval <= 0 {
		return r
	}
	r.reconcile = func(ctx context.Context) {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			if n, err := dispatch.ReconcileUnresolved(ctx, queries, age, limit); err != nil && ctx.Err() == nil {
				r.log.Warn("dispatch reconciliation failed", "error", err)
			} else if n > 0 {
				r.log.Info("unresolved dispatches settled from their original identities", "count", n)
			}
		}
	}
	return r
}

// Tick runs the three lanes once, in sequence (tests and one-shot callers).
func (r *Relay) Tick(ctx context.Context) error {
	if err := r.TickAnswers(ctx); err != nil {
		return err
	}
	if err := r.TickCommands(ctx); err != nil {
		return err
	}
	return r.TickStarts(ctx)
}

// TickAnswers issues the tracked Updates of accepted answers, each under
// its stable update identity (a lost receipt is repaired by the same call).
func (r *Relay) TickAnswers(ctx context.Context) error {
	var answers []*domain.Answer
	if err := r.store.Read(ctx, func(repo Repo) error {
		var err error
		answers, err = repo.ListAnswerRelayPending(ctx, 50)
		return err
	}); err != nil {
		return err
	}
	r.setLag("answers", len(answers))
	for _, a := range answers {
		r.relayAnswer(ctx, a)
	}
	return nil
}

// TickCommands issues the tracked Updates of supported control commands.
func (r *Relay) TickCommands(ctx context.Context) error {
	var commands []*domain.Command
	if err := r.store.Read(ctx, func(repo Repo) error {
		var err error
		commands, err = repo.ListCommandRelayPending(ctx, 50)
		return err
	}); err != nil {
		return err
	}
	r.setLag("commands", len(commands))
	for _, c := range commands {
		r.relayCommand(ctx, c)
	}
	return nil
}

// TickStarts applies every pending start and cancel intent once and starts
// the reconciliation workflow of every recovery run that has none yet. It
// never waits on an Update.
func (r *Relay) TickStarts(ctx context.Context) error {
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
	r.setLag("starts", len(pending)+len(runs))
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
	bounded, cancel := context.WithTimeout(ctx, r.updateTimeout)
	outcome, err := r.workflows.UpdateAnswer(bounded, a.OperationID, a)
	cancel()
	switch {
	case errors.Is(err, ErrRelayClosed):
		r.log.Warn("answer update refused: the run cannot take it", "answerId", a.ID, "operationId", a.OperationID, "error", err)
		outcome = RelayOutcome{Outcome: domain.OutcomeRejected, ReasonCode: "RUN_CLOSED"}
	case errors.Is(err, ErrRelayPending):
		r.log.Info("answer update accepted and still running; settled by a later tick", "answerId", a.ID, "operationId", a.OperationID)
		return
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
	bounded, cancel := context.WithTimeout(ctx, r.updateTimeout)
	outcome, err := r.workflows.UpdateCommand(bounded, c.OperationID, c)
	cancel()
	switch {
	case errors.Is(err, ErrRelayClosed):
		outcome = RelayOutcome{Outcome: domain.OutcomeRejected, ReasonCode: "RUN_CLOSED"}
	case errors.Is(err, ErrRelayPending):
		r.log.Info("command update accepted and still running; settled by a later tick", "commandId", c.CommandID, "operationId", c.OperationID)
		return
	case err != nil:
		r.log.Warn("command update receipt not obtained; the same update is reissued", "commandId", c.CommandID, "error", err)
		return
	}
	if _, err := r.generations.RecordCommandRelay(ctx, c.TenantID, c.CommandID, outcome.Outcome, outcome.ReasonCode); err != nil {
		r.log.Warn("command relay outcome not recorded", "commandId", c.CommandID, "error", err)
	}
}
