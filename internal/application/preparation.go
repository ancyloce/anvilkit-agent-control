package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Preparations implements the durable human-input records of a Preparation
// (DD-01 §3, delivery.md P13-02): grouped question rounds with their
// immutable absolute expiry, one accepted answer per question set revision
// committed with its Update relay intent, and the frozen brief. Every
// mutation runs under the operation lock (rank 2) with the clock read
// after it; nothing here touches the network.
type Preparations struct {
	store    Store
	profiles map[string]domain.Profile
	clock    domain.Clock
	log      *slog.Logger
}

func NewPreparations(store Store, profiles []domain.Profile, clock domain.Clock, log *slog.Logger) *Preparations {
	m := make(map[string]domain.Profile, len(profiles))
	for _, p := range profiles {
		m[p.ID] = p
	}
	return &Preparations{store: store, profiles: m, clock: clock, log: log}
}

// PreparationView is the intake, question sets and brief of a Preparation
// with the reviewed clarification bounds of its profile.
type PreparationView struct {
	Operation    *domain.Operation
	PromptHandle string
	QuestionSets []*domain.QuestionSet
	Brief        *domain.Brief
	Bounds       domain.ClarificationBounds
}

// Get reads the preparation inside the tenant.
func (s *Preparations) Get(ctx context.Context, tenantID, operationID string) (PreparationView, error) {
	var v PreparationView
	err := s.store.Read(ctx, func(r Repo) error {
		op, err := r.GetOperationScoped(ctx, operationID, tenantID)
		if err != nil {
			return err
		}
		if op.Kind != domain.KindPreparation || op.Preparation == nil {
			return domain.ErrNotFound
		}
		v.Operation = op
		v.Bounds = s.profiles[op.Subject.ProfileID].Clarification
		if t, err := r.GetTransfer(ctx, op.Preparation.Prompt.TransferID); err == nil {
			v.PromptHandle = t.Handle
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		if v.QuestionSets, err = r.ListQuestionSets(ctx, op.ID); err != nil {
			return err
		}
		b, err := r.GetCurrentBrief(ctx, op.ID)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		v.Brief = b
		return nil
	})
	return v, err
}

// OpenQuestionSet reads the open question set of an operation (nil when
// none), for the projection.
func OpenQuestionSet(ctx context.Context, r Repo, operationID string) (*domain.QuestionSet, error) {
	qs, err := r.GetOpenQuestionSet(ctx, operationID)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}
	return qs, err
}

// RecordQuestionSet records a grouped round (the analysis Activity's
// durable command): the same command returns the original set; the
// operation becomes waiting/awaiting_input with the expiry the set holds.
func (s *Preparations) RecordQuestionSet(ctx context.Context, cmd domain.CommandIdentity, operationID string, round uint64, questions []domain.Question) (qs *domain.QuestionSet, existing bool, err error) {
	err = s.store.Tx(ctx, func(r Repo) error {
		op, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if op.TenantID != cmd.TenantID {
			return domain.ErrNotFound
		}
		found, err := r.GetQuestionSetByCommand(ctx, op.ID, cmd.CommandID)
		switch {
		case err == nil:
			if found.RequestDigest != cmd.RequestDigest {
				return fmt.Errorf("%w: command %s", domain.ErrIdempotencyConflict, cmd.CommandID)
			}
			qs, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		if err := admissionOpen(ctx, r, op.TenantID); err != nil {
			return err
		}
		open, err := OpenQuestionSet(ctx, r, op.ID)
		if err != nil {
			return err
		}
		profile, ok := s.profiles[op.Subject.ProfileID]
		if !ok {
			return fmt.Errorf("%w: %q", domain.ErrProfileUnqualified, op.Subject.ProfileID)
		}
		now := s.clock.Now()
		fresh, err := domain.NewQuestionSet(op, profile, cmd, round, questions, open != nil, now)
		if err != nil {
			return err
		}
		if err := r.InsertQuestionSet(ctx, fresh); err != nil {
			return err
		}
		ev := op.Transition("questions:"+fresh.ID, now, func(o *domain.Operation) {
			o.Lifecycle, o.Phase = domain.LifecycleWaiting, "awaiting_input"
		})
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		qs = fresh
		return nil
	})
	if errors.Is(err, ErrDuplicateKey) {
		return s.RecordQuestionSet(ctx, cmd, operationID, round, questions)
	}
	return qs, existing, err
}

// SubmitAnswer accepts an answer (API-06): the same command returns the
// original receipt; a stale revision conflicts; an expired or closed set,
// or a second answer for the same revision under another command, is
// refused. The answer and its relay intent commit together and the
// operation runs again (the Update carries the answer to the run).
func (s *Preparations) SubmitAnswer(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, operationID, questionSetID string, revision uint64, binding domain.ArtifactBinding) (a *domain.Answer, existing bool, err error) {
	if scope.TenantID != cmd.TenantID {
		return nil, false, fmt.Errorf("%w: command tenant differs from scope", domain.ErrInvalid)
	}
	err = s.store.Tx(ctx, func(r Repo) error {
		found, err := r.GetAnswerByCommand(ctx, cmd.TenantID, cmd.CommandID)
		switch {
		case err == nil:
			if found.RequestDigest != cmd.RequestDigest || found.OperationID != operationID {
				return fmt.Errorf("%w: command %s", domain.ErrIdempotencyConflict, cmd.CommandID)
			}
			a, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		scoped, err := r.GetOperationScoped(ctx, operationID, scope.TenantID)
		if err != nil {
			return err
		}
		op, err := r.LockOperation(ctx, scoped.ID)
		if err != nil {
			return err
		}
		qs, err := r.LockQuestionSet(ctx, questionSetID)
		if errors.Is(err, domain.ErrNotFound) || (err == nil && qs.OperationID != op.ID) {
			return domain.ErrNotFound
		}
		if err != nil {
			return err
		}
		if prior, err := r.GetAnswerByQuestionSet(ctx, qs.ID, qs.Revision); err == nil {
			return fmt.Errorf("%w: question set %s revision %d is already answered by %s", domain.ErrIdempotencyConflict, qs.ID, qs.Revision, prior.ID)
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		t, err := r.GetTransfer(ctx, binding.TransferID)
		if errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("%w: answer transfer %s is not a transfer of this scope", domain.ErrInvalid, binding.TransferID)
		}
		if err != nil {
			return err
		}
		now := s.clock.Now()
		fresh, err := domain.NewAnswer(op, qs, t, cmd, revision, binding, now)
		if err != nil {
			return err
		}
		if err := r.InsertAnswer(ctx, fresh); err != nil {
			return err
		}
		if err := r.UpdateQuestionSetState(ctx, qs.ID, domain.QuestionSetAnswered); err != nil {
			return err
		}
		ev := op.Transition("answer:"+fresh.ID, now, func(o *domain.Operation) {
			o.Lifecycle, o.Phase = domain.LifecycleRunning, "answered"
		})
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		a = fresh
		return nil
	})
	if errors.Is(err, ErrDuplicateKey) {
		return s.SubmitAnswer(ctx, cmd, scope, operationID, questionSetID, revision, binding)
	}
	return a, existing, err
}

// GetAnswer reads an accepted answer by id or by its question set.
func (s *Preparations) GetAnswer(ctx context.Context, tenantID, operationID, answerID, questionSetID string) (*domain.Answer, error) {
	var a *domain.Answer
	err := s.store.Read(ctx, func(r Repo) error {
		var err error
		switch {
		case answerID != "":
			a, err = r.GetAnswer(ctx, answerID)
		case questionSetID != "":
			qs, qerr := r.GetQuestionSet(ctx, questionSetID)
			if qerr != nil {
				return qerr
			}
			a, err = r.GetAnswerByQuestionSet(ctx, qs.ID, qs.Revision)
		default:
			return fmt.Errorf("%w: an answer id or a question set id is required", domain.ErrInvalid)
		}
		if err != nil {
			return err
		}
		if a.TenantID != tenantID || a.OperationID != operationID {
			return domain.ErrNotFound
		}
		return nil
	})
	return a, err
}

// RecordAnswerRelay records the relay's outcome for an answer.
func (s *Preparations) RecordAnswerRelay(ctx context.Context, tenantID, answerID string, state domain.AnswerRelayState) (*domain.Answer, error) {
	var a *domain.Answer
	now := s.clock.Now()
	err := s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockAnswer(ctx, answerID)
		if err != nil {
			return err
		}
		if locked.TenantID != tenantID {
			return domain.ErrNotFound
		}
		if locked.Relay == domain.AnswerRelayApplied || locked.Relay == domain.AnswerRelayRejected {
			a = locked
			return nil // settled once; a later report never reopens it
		}
		if err := r.UpdateAnswerRelay(ctx, locked.ID, state, &now); err != nil {
			return err
		}
		locked.Relay, locked.RelayedAt = state, &now
		a = locked
		return nil
	})
	return a, err
}

// RecordBrief freezes the brief of a preparation under a durable command:
// the same command returns the original brief; a new brief supersedes the
// operation's current one. The brief artifact must be a finalized brief
// transfer bound to the operation.
func (s *Preparations) RecordBrief(ctx context.Context, cmd domain.CommandIdentity, operationID string, binding domain.ArtifactBinding, requirements domain.Digest, sources []domain.SourceReference, brands, assets []domain.ContentDigest) (b *domain.Brief, existing bool, err error) {
	err = s.store.Tx(ctx, func(r Repo) error {
		op, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if op.TenantID != cmd.TenantID {
			return domain.ErrNotFound
		}
		found, err := r.GetBriefByCommand(ctx, op.ID, cmd.CommandID)
		switch {
		case err == nil:
			if found.RequestDigest != cmd.RequestDigest {
				return fmt.Errorf("%w: command %s", domain.ErrIdempotencyConflict, cmd.CommandID)
			}
			b, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		t, err := r.GetTransfer(ctx, binding.TransferID)
		if errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("%w: brief transfer %s is not a transfer of this scope", domain.ErrInvalid, binding.TransferID)
		}
		if err != nil {
			return err
		}
		n, err := r.CountBriefs(ctx, op.ID)
		if err != nil {
			return err
		}
		now := s.clock.Now()
		fresh, err := domain.NewBrief(op, t, cmd, n+1, binding, requirements, sources, brands, assets, now)
		if err != nil {
			return err
		}
		if err := r.SupersedeBriefs(ctx, op.ID); err != nil {
			return err
		}
		if err := r.InsertBrief(ctx, fresh); err != nil {
			return err
		}
		ev := op.Transition("brief:"+fresh.ID, now, func(o *domain.Operation) {
			o.Subject.BriefID = fresh.ID
			o.Phase = "brief_frozen"
		})
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		b = fresh
		return nil
	})
	if errors.Is(err, ErrDuplicateKey) {
		return s.RecordBrief(ctx, cmd, operationID, binding, requirements, sources, brands, assets)
	}
	return b, existing, err
}

// GetBrief reads a brief inside the tenant.
func (s *Preparations) GetBrief(ctx context.Context, tenantID, briefID string) (*domain.Brief, error) {
	var b *domain.Brief
	err := s.store.Read(ctx, func(r Repo) error {
		found, err := r.GetBrief(ctx, briefID)
		if err != nil {
			return err
		}
		if found.TenantID != tenantID {
			return domain.ErrNotFound
		}
		b = found
		return nil
	})
	return b, err
}
