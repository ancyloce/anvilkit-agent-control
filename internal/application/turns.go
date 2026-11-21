package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Assistant replies (design 0001 D3, DEC-068). Where replies are enabled,
// every accepted user message opens one turn and its chat_turn operation in
// the message's own transaction; the relay starts the Workflow's chat turn
// once the operation's intake is confirmed, exactly as for any operation.
// The chat Activity then claims the reply writer, reads the turn's context
// and records the reply's outcome through ConversationExecutionService;
// funding, attempts, dispatch admission and settlement are Control's
// existing paths. Reply text is user content: it is never logged.

// replies is the turn configuration of a Conversations module: the
// operations it opens and cancels turns through, the chat_turn profile and
// the context bound. open says whether a new message opens a turn (DEC-068:
// off by default); existing turns are still read, stopped and answered when
// it is off. nil means the module has no turns at all.
type replies struct {
	ops          *Operations
	profile      domain.Profile
	contextBytes int
	open         bool
}

// WithReplies configures assistant replies: a chat_turn operation runs under
// profile, a turn's context keeps at most contextBytes of message text (0:
// the default bound), and open makes each accepted message open a turn.
func (s *Conversations) WithReplies(ops *Operations, profile domain.Profile, contextBytes int, open bool) *Conversations {
	if contextBytes <= 0 {
		contextBytes = domain.DefaultTurnContextBytes
	}
	s.replies = &replies{ops: ops, profile: profile, contextBytes: contextBytes, open: open}
	return s
}

// RepliesEnabled reports whether a new message opens a turn.
func (s *Conversations) RepliesEnabled() bool { return s.replies != nil && s.replies.open }

// TurnView is a turn as a reader sees it, with its operation's finance.
type TurnView struct {
	Turn    *domain.Turn
	Finance domain.FinanceState
}

// viewOf derives the reader's view without writing: an open turn whose
// operation ended is shown sealed, as the next write will seal it, and a
// stop that sealed while its send metered shows the sender stopped once the
// operation ended.
func viewOf(t *domain.Turn, op *domain.Operation) *TurnView {
	v := *t
	if op == nil {
		return &TurnView{Turn: &v, Finance: domain.FinanceNotFunded}
	}
	v.EndWithOperation(op, v.UpdatedAt)
	if !v.Open() && v.Sender == domain.SenderActive && op.Lifecycle.Terminal() {
		v.Sender = domain.SenderStopped
	}
	return &TurnView{Turn: &v, Finance: op.Finance}
}

// freeSlot checks the conversation's reply slot under its lock: an open turn
// whose operation already ended is sealed now; any other open turn refuses
// the message with TURN_BUSY, so nothing is stored.
func freeSlot(ctx context.Context, r Repo, conv *domain.Conversation, now time.Time) error {
	open, err := r.GetOpenConversationTurn(ctx, conv.ID, conv.TenantID)
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return nil
	case err != nil:
		return err
	}
	op, err := r.LockOperation(ctx, open.OperationID)
	if err != nil {
		return err
	}
	if !open.EndWithOperation(op, now) {
		return fmt.Errorf("%w: conversation %s is answering turn %s; send the message once it ends", domain.ErrTurnBusy, conv.ID, open.ID)
	}
	return r.UpdateConversationTurn(ctx, open)
}

// openTurn opens the turn answering the stored message and its chat_turn
// operation (rank 2, inserted under the conversation's rank-1 lock). The
// operation carries the owner's scope; its subject digest is the message's.
func (s *Conversations) openTurn(ctx context.Context, r Repo, cmd domain.CommandIdentity, scope domain.Scope, m *domain.Message, at time.Time) (*domain.Turn, *domain.Operation, error) {
	closed, err := r.IsAdmissionClosed(ctx, scope.TenantID)
	if err != nil {
		return nil, nil, err
	}
	if closed {
		return nil, nil, fmt.Errorf("%w: %s: admission for tenant %s is closed by a recovery run", domain.ErrStaleExecution, domain.DenyRecoveryRestricted, scope.TenantID)
	}
	turn := domain.NewTurn(m, at)
	opCmd := domain.CommandIdentity{TenantID: cmd.TenantID, CommandID: turn.OperationCommandID(), ActorID: cmd.ActorID, RequestDigest: cmd.RequestDigest}
	owner := domain.Scope{TenantID: scope.TenantID, ProjectID: scope.ProjectID, ActorID: scope.ActorID}
	op, err := domain.NewOperation(opCmd, owner, domain.KindChatTurn, domain.Subject{ProfileID: s.replies.profile.ID, SubjectDigest: m.ContentDigest}, s.replies.profile, at)
	if err != nil {
		return nil, nil, err
	}
	op.Revision = 0
	ev := op.Transition("accepted", at, func(*domain.Operation) {})
	if err := r.InsertOperation(ctx, op); err != nil {
		return nil, nil, err
	}
	if err := r.InsertEvent(ctx, ev); err != nil {
		return nil, nil, err
	}
	turn.OperationID = op.ID
	if err := r.InsertConversationTurn(ctx, turn); err != nil {
		return nil, nil, err
	}
	return turn, op, nil
}

// confirmTurn confirms the turn operation's intake after the commit, so the
// relay starts it; a lost confirmation is resumed by the intake
// reconciliation, and the caller's retry answers the same receipt.
func (s *Conversations) confirmTurn(ctx context.Context, op *domain.Operation) error {
	if s.replies == nil || op == nil || op.Intake != domain.IntakePending {
		return nil
	}
	return s.replies.ops.confirmIntake(ctx, op)
}

// GetTurn reads one turn of the scope's own conversation.
func (s *Conversations) GetTurn(ctx context.Context, scope domain.Scope, conversationID, turnID string) (*TurnView, error) {
	var view *TurnView
	err := s.store.Read(ctx, func(r Repo) error {
		conv, err := owned(ctx, r, scope, conversationID, false)
		if err != nil {
			return err
		}
		turn, err := r.GetConversationTurn(ctx, turnID, scope.TenantID)
		if err != nil {
			return err
		}
		if turn.ConversationID != conv.ID {
			return domain.ErrNotFound
		}
		op, err := r.GetOperationScoped(ctx, turn.OperationID, scope.TenantID)
		if err != nil {
			return err
		}
		view = viewOf(turn, op)
		return nil
	})
	return view, err
}

// StopTurn stops this turn's reply only (design 0001 §11.3): its chat_turn
// operation is canceled at its current revision through the operation cancel
// path, which fences new dispatch and is relayed to the Workflow; the same
// command replays there. An operation canceled at once ends the turn now;
// otherwise the turn waits in cancel_pending for the chat Activity's outcome.
// A sealed turn answers itself unchanged.
func (s *Conversations) StopTurn(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, conversationID, turnID string) (view *TurnView, existing bool, err error) {
	if err := conversationCommand(scope, cmd); err != nil {
		return nil, false, err
	}
	if s.replies == nil {
		return nil, false, domain.ErrNotFound
	}
	for attempt := 0; attempt < 3; attempt++ {
		v, err := s.GetTurn(ctx, scope, conversationID, turnID)
		if err != nil {
			return nil, false, err
		}
		if !v.Turn.Open() {
			return v, false, nil
		}
		var op *domain.Operation
		if err := s.store.Read(ctx, func(r Repo) error {
			var err error
			op, err = r.GetOperationScoped(ctx, v.Turn.OperationID, scope.TenantID)
			return err
		}); err != nil {
			return nil, false, err
		}
		_, existing, err = s.replies.ops.SubmitCommand(ctx, cmd, scope, op.ID, domain.CommandCancel, op.Revision, "")
		if errors.Is(err, domain.ErrRevisionConflict) {
			continue // the operation moved between the read and the cancel: read it again
		}
		if err != nil {
			return nil, false, err
		}
		now := s.clock.Now()
		err = s.store.Tx(ctx, func(r Repo) error {
			conv, err := owned(ctx, r, scope, conversationID, true)
			if err != nil {
				return err
			}
			turn, err := r.GetConversationTurn(ctx, turnID, scope.TenantID)
			if err != nil {
				return err
			}
			if turn.ConversationID != conv.ID {
				return domain.ErrNotFound
			}
			current, err := r.GetOperationScoped(ctx, turn.OperationID, scope.TenantID)
			if err != nil {
				return err
			}
			if turn.RequestStop(current.Lifecycle == domain.LifecycleCanceled, now) {
				if err := r.UpdateConversationTurn(ctx, turn); err != nil {
					return err
				}
				s.log.Info("conversation turn stop recorded", "conversationId", conv.ID, "turnId", turn.ID, "state", turn.State, "operationId", turn.OperationID)
			}
			view = viewOf(turn, current)
			return nil
		})
		return view, existing, err
	}
	return nil, false, fmt.Errorf("%w: turn %s's operation keeps moving; stop it again", domain.ErrRevisionConflict, turnID)
}

// ReplyBinding is the chat Activity's attempt binding (design 0001 §7.2): the
// turn's operation, the attempt Control opened for it and the execution
// epoch. A chat attempt runs no Job, so it names no instance.
type ReplyBinding struct {
	OperationID    string
	AttemptID      string
	ExecutionEpoch uint64
}

// lockTurn locks the turn's conversation (rank 1) and rereads the turn under
// it; the caller is the trusted Workflow, so the tenant alone scopes it.
func lockTurn(ctx context.Context, r Repo, tenantID, turnID string) (*domain.Conversation, *domain.Turn, error) {
	peek, err := r.GetConversationTurn(ctx, turnID, tenantID)
	if err != nil {
		return nil, nil, err
	}
	conv, err := r.LockConversation(ctx, peek.ConversationID, tenantID)
	if err != nil {
		return nil, nil, err
	}
	turn, err := r.GetConversationTurn(ctx, turnID, tenantID)
	if err != nil {
		return nil, nil, err
	}
	return conv, turn, nil
}

// checkBinding locks the turn's operation (rank 2) and the bound attempt
// (rank 3) and verifies the binding: the turn's operation, an open attempt of
// it under the current execution epoch, and an operation that has not ended.
// It answers the locked operation.
func checkBinding(ctx context.Context, r Repo, tenantID string, turn *domain.Turn, b ReplyBinding) (*domain.Operation, error) {
	if b.OperationID != turn.OperationID {
		return nil, fmt.Errorf("%w: turn %s is answered by operation %s, not %s", domain.ErrStaleExecution, turn.ID, turn.OperationID, b.OperationID)
	}
	op, at, err := lockOperationThenAttempt(ctx, r, b.AttemptID, tenantID)
	if err != nil {
		return nil, err
	}
	switch {
	case at.OperationID != op.ID || op.ID != turn.OperationID:
		return nil, fmt.Errorf("%w: attempt %s is not an attempt of operation %s", domain.ErrStaleExecution, at.ID, turn.OperationID)
	case op.Kind != domain.KindChatTurn:
		return nil, fmt.Errorf("%w: operation %s is not a chat turn", domain.ErrStaleExecution, op.ID)
	case b.ExecutionEpoch != op.ExecutionEpoch:
		return nil, fmt.Errorf("%w: binding epoch %d, operation epoch %d", domain.ErrStaleExecution, b.ExecutionEpoch, op.ExecutionEpoch)
	case at.State != domain.AttemptOpen:
		return nil, fmt.Errorf("%w: attempt %s is %s", domain.ErrStaleExecution, at.ID, at.State)
	case op.Lifecycle.Terminal():
		return nil, fmt.Errorf("%w: operation %s is %s", domain.ErrStaleExecution, op.ID, op.Lifecycle)
	}
	return op, nil
}

// ClaimReplyWriter claims the reply writer of the binding's chat_turn
// operation for the chat Activity (design 0001 §7.2) and answers the turn and
// its writer epoch: the Workflow starts from the operation's identity only,
// and one operation answers exactly one turn. A sealed turn is answered as it
// is, without a takeover, so a late retry learns the outcome.
func (s *Conversations) ClaimReplyWriter(ctx context.Context, cmd domain.CommandIdentity, b ReplyBinding, activityKey string, attempt uint64, invocation string) (turn *domain.Turn, existing bool, err error) {
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		peek, err := r.GetConversationTurnByOperation(ctx, b.OperationID, cmd.TenantID)
		if err != nil {
			return err
		}
		_, locked, err := lockTurn(ctx, r, cmd.TenantID, peek.ID)
		if err != nil {
			return err
		}
		turn = locked
		if !locked.Open() {
			existing = true
			return nil
		}
		if _, err := checkBinding(ctx, r, cmd.TenantID, locked, b); err != nil {
			return err
		}
		if existing, err = locked.ClaimWriter(activityKey, attempt, invocation, now); err != nil {
			return err
		}
		if existing {
			return nil
		}
		return r.UpdateConversationTurn(ctx, locked)
	})
	if err == nil && !existing {
		s.log.Info("conversation reply writer claimed", "turnId", turn.ID, "operationId", turn.OperationID, "writerEpoch", turn.WriterEpoch, "temporalAttempt", turn.TemporalAttempt)
	}
	return turn, existing, err
}

// GetTurnContext reads the turn's context for the current writer: the
// conversation's messages up to and including the input message, newest
// first within the context bound, in ascending sequence, with their digest.
// The input of an earlier turn stopped before its send is left out (DEC-069)
// and stays in the history. The same turn always yields the same context
// (messages are append-only, a reply is stored after it, and every earlier
// turn was sealed before this one opened).
func (s *Conversations) GetTurnContext(ctx context.Context, tenantID string, b ReplyBinding, turnID string, epoch uint64) (messages []*domain.Message, digest domain.Digest, err error) {
	budget := domain.DefaultTurnContextBytes
	if s.replies != nil {
		budget = s.replies.contextBytes
	}
	err = s.store.Tx(ctx, func(r Repo) error {
		_, turn, err := lockTurn(ctx, r, tenantID, turnID)
		if err != nil {
			return err
		}
		if _, err := checkBinding(ctx, r, tenantID, turn, b); err != nil {
			return err
		}
		if err := turn.CheckWriter(epoch); err != nil {
			return err
		}
		var newestFirst []*domain.Message
		leftOut := map[string]bool{}
		before, bytes := turn.InputMessageSeq+1, 0
		for before > 1 && bytes <= budget {
			page, err := r.ListConversationMessages(ctx, turn.ConversationID, tenantID, before, domain.MaxHistoryPage)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				break
			}
			if err := markStoppedInputs(ctx, r, turn, page, leftOut); err != nil {
				return err
			}
			for _, m := range page {
				if !leftOut[m.ID] {
					bytes += len(m.Text)
				}
			}
			newestFirst = append(newestFirst, page...)
			before = page[len(page)-1].Seq
		}
		if len(newestFirst) == 0 || newestFirst[0].ID != turn.InputMessageID {
			return fmt.Errorf("%w: turn %s's input message is missing from its conversation", domain.ErrIntegrity, turn.ID)
		}
		messages = domain.SelectTurnContext(newestFirst, turn.InputMessageSeq, budget, leftOut)
		digest = domain.ContextDigest(messages)
		return nil
	})
	return messages, digest, err
}

// markStoppedInputs adds to leftOut the page's user messages, other than the
// turn's own input, whose turns were stopped before their send (DEC-069).
func markStoppedInputs(ctx context.Context, r Repo, turn *domain.Turn, page []*domain.Message, leftOut map[string]bool) error {
	ids := make([]string, 0, len(page))
	for _, m := range page {
		if m.Role == domain.MessageRoleUser && m.ID != turn.InputMessageID {
			ids = append(ids, m.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	turns, err := r.ListConversationTurnsByInputs(ctx, turn.ConversationID, turn.TenantID, ids)
	if err != nil {
		return err
	}
	for _, earlier := range turns {
		if earlier.StoppedBeforeSend() {
			leftOut[earlier.InputMessageID] = true
		}
	}
	return nil
}

// RecordReplyOutcome seals or reconciles the turn's reply under the current
// writer epoch (design 0001 §7.4). A sealing record with text stores it as
// the next assistant message in the same transaction; a record whose sender
// is unknown keeps the turn reconciling, holding the slot. An accepted stop
// wins (B-016): when the operation carries a cancel intent, a sealing record
// seals the turn canceled and its text is stored as the canceled message. A
// sealed turn replays the command that sealed it and refuses any other.
func (s *Conversations) RecordReplyOutcome(ctx context.Context, cmd domain.CommandIdentity, b ReplyBinding, turnID string, epoch uint64, callID string, reported domain.ReplyRecord) (turn *domain.Turn, reply *domain.Message, existing bool, err error) {
	if err := reported.Check(); err != nil {
		return nil, nil, false, err
	}
	rec := reported
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		conv, locked, err := lockTurn(ctx, r, cmd.TenantID, turnID)
		if err != nil {
			return err
		}
		turn, reply = locked, nil
		if !locked.Open() {
			if err := locked.Replays(cmd); err != nil {
				return err
			}
			existing = true
			if locked.AssistantMessageID != "" {
				reply, err = r.GetConversationMessage(ctx, locked.AssistantMessageID, cmd.TenantID)
			}
			return err
		}
		op, err := checkBinding(ctx, r, cmd.TenantID, locked, b)
		if err != nil {
			return err
		}
		if err := locked.CheckWriter(epoch); err != nil {
			return err
		}
		if callID != domain.ReplyCallID(locked.OperationID) {
			return fmt.Errorf("%w: call %s is not turn %s's reply call", domain.ErrInvalid, callID, locked.ID)
		}
		rec = reported.UnderCancel(op)
		var messageID string
		if rec.Seals() && rec.HasText {
			m, err := conv.AppendAssistantMessage(rec.MessageState(), rec.Text, now)
			if err != nil {
				return err
			}
			if err := r.UpdateConversation(ctx, conv); err != nil {
				return err
			}
			if err := r.InsertConversationMessage(ctx, m); err != nil {
				return err
			}
			reply, messageID = m, m.ID
		}
		locked.Record(rec, messageID, cmd, now)
		return r.UpdateConversationTurn(ctx, locked)
	})
	if errors.Is(err, ErrDuplicateKey) && depthOf(ctx) < duplicateRetries {
		return s.RecordReplyOutcome(deeper(ctx), cmd, b, turnID, epoch, callID, reported)
	}
	if err == nil && !existing {
		bytes := 0
		if reply != nil {
			bytes = len(reply.Text)
		}
		s.log.Info("conversation reply recorded", "turnId", turn.ID, "operationId", turn.OperationID, "state", turn.State, "outcome", rec.Outcome, "reportedOutcome", reported.Outcome, "sender", rec.Sender, "failureCode", rec.FailureCode, "bytes", bytes)
	}
	return turn, reply, existing, err
}

// GetTurnForWorkflow is the view of a turn the chat Activity just claimed or
// recorded, with its operation's finance.
func (s *Conversations) GetTurnForWorkflow(ctx context.Context, tenantID string, turn *domain.Turn) (*TurnView, error) {
	var op *domain.Operation
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		op, err = r.GetOperationScoped(ctx, turn.OperationID, tenantID)
		return err
	}); err != nil {
		return nil, err
	}
	return viewOf(turn, op), nil
}
