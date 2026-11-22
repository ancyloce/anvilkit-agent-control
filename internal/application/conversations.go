package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Conversations is Control's conversation module (DEC-067, design 0001
// first slice): personal conversations and their user messages, written
// only here. A conversation is visible to its owner alone (tenant, project
// and actor of the caller's scope); any other caller is answered NOT_FOUND
// so existence is not disclosed. Commands are reentrant per (tenant,
// command id). Message text and titles are user content: they are never
// logged.
type Conversations struct {
	store Store
	clock domain.Clock
	log   *slog.Logger
	// replies, when set, opens a turn for each accepted message (DEC-068).
	replies *replies
}

func NewConversations(store Store, clock domain.Clock, log *slog.Logger) *Conversations {
	return &Conversations{store: store, clock: clock, log: log}
}

// conversationCommand authorizes a write: the command is the scope's own.
func conversationCommand(scope domain.Scope, cmd domain.CommandIdentity) error {
	if scope.TenantID != cmd.TenantID || scope.ActorID != cmd.ActorID || scope.ActorID == "" {
		return fmt.Errorf("%w: the command is not the caller's tenant and actor", domain.ErrForbidden)
	}
	return nil
}

// owned reads the scope's own conversation, locked for a write when lock
// is set; another owner's, project's or tenant's is not found.
func owned(ctx context.Context, r Repo, scope domain.Scope, conversationID string, lock bool) (*domain.Conversation, error) {
	read := r.GetConversation
	if lock {
		read = r.LockConversation
	}
	c, err := read(ctx, conversationID, scope.TenantID)
	if err != nil {
		return nil, err
	}
	if !c.OwnedBy(scope) {
		return nil, domain.ErrNotFound
	}
	return c, nil
}

// replayed reads the command recorded under cmd's identity: nil when the
// command is new, an error when the id was used by another request.
func replayed(ctx context.Context, r Repo, cmd domain.CommandIdentity, kind domain.ConversationCommandKind, conversationID string) (*domain.ConversationCommand, error) {
	prior, err := r.GetConversationCommand(ctx, cmd.TenantID, cmd.CommandID)
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	}
	if err := prior.Replays(cmd, kind, conversationID); err != nil {
		return nil, err
	}
	return prior, nil
}

// Create makes a conversation owned by the scope; the same command answers
// the existing conversation as it is now, while the caller still owns it.
func (s *Conversations) Create(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, title string) (conv *domain.Conversation, existing bool, err error) {
	if err := conversationCommand(scope, cmd); err != nil {
		return nil, false, err
	}
	now := s.clock.Now()
	fresh, err := domain.NewConversation(scope, title, now)
	if err != nil {
		return nil, false, err
	}
	err = s.store.Tx(ctx, func(r Repo) error {
		prior, err := replayed(ctx, r, cmd, domain.ConversationCommandCreate, "")
		if err != nil {
			return err
		}
		if prior != nil {
			conv, existing = nil, true
			conv, err = owned(ctx, r, scope, prior.ConversationID, false)
			return err
		}
		conv, existing = fresh, false
		if err := r.InsertConversation(ctx, fresh); err != nil {
			return err
		}
		return r.InsertConversationCommand(ctx, &domain.ConversationCommand{
			TenantID: cmd.TenantID, CommandID: cmd.CommandID, Kind: domain.ConversationCommandCreate, ActorID: cmd.ActorID,
			RequestDigest: cmd.RequestDigest, ConversationID: fresh.ID, AcceptedAt: now,
		})
	})
	if errors.Is(err, ErrDuplicateKey) && depthOf(ctx) < duplicateRetries {
		return s.Create(deeper(ctx), cmd, scope, title)
	}
	if err == nil && !existing {
		s.log.Info("conversation created", "conversationId", conv.ID, "tenantId", conv.TenantID, "actor", cmd.ActorID)
	}
	return conv, existing, err
}

// Get reads the scope's own conversation.
func (s *Conversations) Get(ctx context.Context, scope domain.Scope, conversationID string) (*domain.Conversation, error) {
	var conv *domain.Conversation
	err := s.store.Read(ctx, func(r Repo) error {
		var err error
		conv, err = owned(ctx, r, scope, conversationID, false)
		return err
	})
	return conv, err
}

// List reads one page of the scope's conversations, newest first; status
// empty lists every status. next is the cursor of the following page, empty
// on the last one.
func (s *Conversations) List(ctx context.Context, scope domain.Scope, status domain.ConversationStatus, cursor string, limit int) (page []*domain.Conversation, next string, err error) {
	if status != "" && status != domain.ConversationActive && status != domain.ConversationArchived {
		return nil, "", fmt.Errorf("%w: conversation status %q", domain.ErrInvalid, status)
	}
	after, err := domain.ParseConversationCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit, err = domain.PageLimit(limit, domain.DefaultConversationPage); err != nil {
		return nil, "", err
	}
	err = s.store.Read(ctx, func(r Repo) error {
		rows, err := r.ListConversations(ctx, scope, status, after, limit+1)
		if err != nil {
			return err
		}
		if len(rows) > limit {
			rows = rows[:limit]
			last := rows[limit-1]
			next = domain.ConversationCursor{CreatedAt: last.CreatedAt, ID: last.ID}.Encode()
		}
		page = rows
		return nil
	})
	return page, next, err
}

// Update renames, archives or restores the scope's conversation at the
// expected revision; the same command answers the conversation as it is now.
func (s *Conversations) Update(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, conversationID string, expected uint64, title *string, status domain.ConversationStatus) (conv *domain.Conversation, existing bool, err error) {
	if err := conversationCommand(scope, cmd); err != nil {
		return nil, false, err
	}
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		// Rank 1 before the command record (rank 6): a concurrent retry of
		// the same command waits here and then finds it.
		locked, err := owned(ctx, r, scope, conversationID, true)
		if err != nil {
			return err
		}
		prior, err := replayed(ctx, r, cmd, domain.ConversationCommandUpdate, conversationID)
		if err != nil {
			return err
		}
		conv, existing = locked, prior != nil
		if existing {
			return nil
		}
		if err := locked.Update(expected, title, status, now); err != nil {
			return err
		}
		ev, err := locked.ConversationChangedEvent(now)
		if err != nil {
			return err
		}
		if err := emit(ctx, r, locked, []*domain.ConversationEvent{ev}); err != nil {
			return err
		}
		return r.InsertConversationCommand(ctx, &domain.ConversationCommand{
			TenantID: cmd.TenantID, CommandID: cmd.CommandID, Kind: domain.ConversationCommandUpdate, ActorID: cmd.ActorID,
			RequestDigest: cmd.RequestDigest, ConversationID: conversationID, AcceptedAt: now,
		})
	})
	if errors.Is(err, ErrDuplicateKey) && depthOf(ctx) < duplicateRetries {
		return s.Update(deeper(ctx), cmd, scope, conversationID, expected, title, status)
	}
	if err == nil && !existing {
		s.log.Info("conversation updated", "conversationId", conv.ID, "revision", conv.Revision, "status", conv.Status, "actor", cmd.ActorID)
	}
	return conv, existing, err
}

// Append stores the scope's text message in its active conversation at the
// expected revision. The same command answers the original message, even
// after the conversation was archived, while the caller still owns it.
// Where replies are enabled (DEC-068) the message also opens its turn and
// chat_turn operation in the same transaction, refused with TURN_BUSY while
// the previous turn is open (nothing is stored then); turn is nil where
// replies are disabled.
func (s *Conversations) Append(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, conversationID string, expected uint64, text string) (msg *domain.Message, turn *domain.Turn, existing bool, err error) {
	if err := conversationCommand(scope, cmd); err != nil {
		return nil, nil, false, err
	}
	now := s.clock.Now()
	var op *domain.Operation
	err = s.store.Tx(ctx, func(r Repo) error {
		locked, err := owned(ctx, r, scope, conversationID, true)
		if err != nil {
			return err
		}
		prior, err := replayed(ctx, r, cmd, domain.ConversationCommandAppend, conversationID)
		if err != nil {
			return err
		}
		if prior != nil {
			existing, turn, op = true, nil, nil
			if msg, err = r.GetConversationMessage(ctx, prior.MessageID, scope.TenantID); err != nil {
				return err
			}
			found, err := r.GetConversationTurnByInput(ctx, msg.ID, scope.TenantID)
			switch {
			case errors.Is(err, domain.ErrNotFound):
				return nil
			case err != nil:
				return err
			}
			turn = found
			op, err = r.GetOperationScoped(ctx, found.OperationID, scope.TenantID)
			return err
		}
		msg, turn, op, existing = nil, nil, nil, false
		var events []*domain.ConversationEvent
		if s.RepliesEnabled() {
			ev, err := freeSlot(ctx, r, locked, now)
			if err != nil {
				return err
			}
			if ev != nil {
				events = append(events, ev)
			}
		}
		m, err := locked.AppendUserMessage(expected, text, now)
		if err != nil {
			return err
		}
		if err := r.UpdateConversation(ctx, locked); err != nil {
			return err
		}
		if err := r.InsertConversationMessage(ctx, m); err != nil {
			return err
		}
		msg = m
		turnID := ""
		if s.RepliesEnabled() {
			if turn, op, err = s.openTurn(ctx, r, cmd, scope, m, now); err != nil {
				return err
			}
			turnID = turn.ID
		}
		ev, err := locked.MessageAcceptedEvent(m, turnID, now)
		if err != nil {
			return err
		}
		events = append(events, ev)
		if turn != nil {
			ev, err := locked.TurnChangedEvent(turn, op.Finance, now)
			if err != nil {
				return err
			}
			events = append(events, ev)
		}
		if err := emit(ctx, r, locked, events); err != nil {
			return err
		}
		return r.InsertConversationCommand(ctx, &domain.ConversationCommand{
			TenantID: cmd.TenantID, CommandID: cmd.CommandID, Kind: domain.ConversationCommandAppend, ActorID: cmd.ActorID,
			RequestDigest: cmd.RequestDigest, ConversationID: conversationID, MessageID: m.ID, AcceptedAt: now,
		})
	})
	if errors.Is(err, ErrDuplicateKey) && depthOf(ctx) < duplicateRetries {
		return s.Append(deeper(ctx), cmd, scope, conversationID, expected, text)
	}
	if err != nil {
		return nil, nil, false, err
	}
	if !existing {
		if turn != nil {
			s.log.Info("conversation message stored", "conversationId", conversationID, "messageId", msg.ID, "messageSeq", msg.Seq, "bytes", len(msg.Text), "actor", cmd.ActorID, "turnId", turn.ID, "operationId", turn.OperationID)
		} else {
			s.log.Info("conversation message stored", "conversationId", conversationID, "messageId", msg.ID, "messageSeq", msg.Seq, "bytes", len(msg.Text), "actor", cmd.ActorID)
		}
	}
	if err := s.confirmTurn(ctx, op); err != nil {
		return nil, nil, false, err
	}
	return msg, turn, existing, nil
}

// Messages reads one page of the scope's conversation history before the
// sequence (0: from the newest message), ascending within the page. The
// page ends at limit messages or once its text reaches the page budget,
// and holds at least one message when any precedes the cursor. nextBefore
// is the sequence to read the page before this one from, 0 when none.
func (s *Conversations) Messages(ctx context.Context, scope domain.Scope, conversationID string, before uint64, limit int) (page []*domain.Message, nextBefore uint64, err error) {
	if limit, err = domain.PageLimit(limit, domain.DefaultMessagePage); err != nil {
		return nil, 0, err
	}
	err = s.store.Read(ctx, func(r Repo) error {
		if _, err := owned(ctx, r, scope, conversationID, false); err != nil {
			return err
		}
		rows, err := r.ListConversationMessages(ctx, conversationID, scope.TenantID, before, limit+1)
		if err != nil {
			return err
		}
		kept, bytes := 0, 0
		for kept < len(rows) && kept < limit && (kept == 0 || bytes+len(rows[kept].Text) <= domain.MessagePageTextBudget) {
			bytes += len(rows[kept].Text)
			kept++
		}
		page = make([]*domain.Message, 0, kept)
		for i := kept - 1; i >= 0; i-- {
			page = append(page, rows[i])
		}
		if kept < len(rows) {
			nextBefore = rows[kept-1].Seq
		}
		return nil
	})
	return page, nextBefore, err
}
