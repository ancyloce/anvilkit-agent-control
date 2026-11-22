package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Streaming persistence and the conversation event log (design 0001 §8,
// D4a, DEC-070). The chat Activity commits a reply's original frames in
// batches (AppendReplyBatch) under the writer epoch; a batch with text
// becomes a chunk, and the first one reserves the reply's assistant message.
// Every write that changes what a reader sees appends conversation events
// in the same transaction, with the conversation.changed wake-up. A stop of
// a running reply seals the committed chunks at once (Q3 A).

// emit writes the conversation (its event and message sequences) and the
// events of one commit; nothing when there are none.
func emit(ctx context.Context, r Repo, conv *domain.Conversation, events []*domain.ConversationEvent) error {
	if len(events) == 0 {
		return nil
	}
	if err := r.UpdateConversation(ctx, conv); err != nil {
		return err
	}
	return r.InsertConversationEvents(ctx, events)
}

// sealAtStop seals the turn at its stop (DEC-070 Q3 A): its committed chunks
// become the canceled assistant message (none without text), the turn ends
// canceled with its sender as it is (a send still active meters to its end,
// DEC-068 Q3), and the slot frees. It answers the commit's events.
func sealAtStop(ctx context.Context, r Repo, conv *domain.Conversation, turn *domain.Turn, cmd domain.CommandIdentity, finance domain.FinanceState, now time.Time) ([]*domain.ConversationEvent, error) {
	var events []*domain.ConversationEvent
	var msg *domain.Message
	if turn.LastChunkSeq > 0 {
		text, err := r.ReplyText(ctx, turn.ID)
		if err != nil {
			return nil, err
		}
		if msg, err = conv.SealReply(turn, domain.MessageCanceled, text, now); err != nil {
			return nil, err
		}
		if err := r.InsertConversationMessage(ctx, msg); err != nil {
			return nil, err
		}
		turn.SealStop(msg.ID, cmd, now)
	} else {
		turn.SealStop("", cmd, now)
	}
	if err := r.UpdateConversationTurn(ctx, turn); err != nil {
		return nil, err
	}
	if msg != nil {
		ev, err := conv.MessageSealedEvent(turn, msg, now)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	ev, err := conv.TurnChangedEvent(turn, finance, now)
	if err != nil {
		return nil, err
	}
	return append(events, ev), nil
}

// cancelIntent reports whether the operation carries an accepted cancel.
func cancelIntent(op *domain.Operation) bool {
	return op.Control == domain.ControlCancelPending || op.Control == domain.ControlCancelApplied
}

// intentCommand names a seal made because the operation's cancel arrived
// before the stop's own transaction sealed the turn.
func intentCommand(op *domain.Operation) domain.CommandIdentity {
	return domain.CommandIdentity{TenantID: op.TenantID, CommandID: "cancel:" + op.ID, RequestDigest: domain.TextDigest(op.ID)}
}

// ConversationSnapshot is a consistent read of a conversation (design 0001
// §8.4): the conversation, the last event the read covers and its open turn.
type ConversationSnapshot struct {
	Conversation    *domain.Conversation
	CoveredEventSeq uint64
	ActiveTurn      *TurnView
}

// Snapshot reads the scope's conversation with its covered event and open
// turn in one transaction; a client subscribes to the events after it.
func (s *Conversations) Snapshot(ctx context.Context, scope domain.Scope, conversationID string) (*ConversationSnapshot, error) {
	var snap *ConversationSnapshot
	err := s.store.Read(ctx, func(r Repo) error {
		conv, err := owned(ctx, r, scope, conversationID, false)
		if err != nil {
			return err
		}
		snap = &ConversationSnapshot{Conversation: conv, CoveredEventSeq: conv.CoveredEventSeq()}
		open, err := r.GetOpenConversationTurn(ctx, conv.ID, scope.TenantID)
		switch {
		case errors.Is(err, domain.ErrNotFound):
			return nil
		case err != nil:
			return err
		}
		op, err := r.GetOperationScoped(ctx, open.OperationID, scope.TenantID)
		if err != nil {
			return err
		}
		if v := viewOf(open, op); v.Turn.Open() {
			snap.ActiveTurn = v
		}
		return nil
	})
	return snap, err
}

// GetMessage reads one message of the scope's conversation: a stored one, or
// a streaming reply's reserved message assembled from its committed chunks.
func (s *Conversations) GetMessage(ctx context.Context, scope domain.Scope, conversationID, messageID string) (*domain.Message, error) {
	var msg *domain.Message
	err := s.store.Read(ctx, func(r Repo) error {
		conv, err := owned(ctx, r, scope, conversationID, false)
		if err != nil {
			return err
		}
		stored, err := r.GetConversationMessage(ctx, messageID, scope.TenantID)
		switch {
		case err == nil:
			if stored.ConversationID != conv.ID {
				return domain.ErrNotFound
			}
			msg = stored
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		turn, err := r.GetConversationTurnByReservedMessage(ctx, messageID, scope.TenantID)
		if err != nil {
			return err
		}
		if turn.ConversationID != conv.ID || turn.LastChunkSeq == 0 {
			return domain.ErrNotFound
		}
		text, err := r.ReplyText(ctx, turn.ID)
		if err != nil {
			return err
		}
		msg = domain.StreamingMessage(turn, text)
		return nil
	})
	return msg, err
}

// ConversationEventPage is one page of a conversation's events with the
// covered sequence; ResetRequired means the cursor cannot be honored.
type ConversationEventPage struct {
	Events          []*domain.ConversationEvent
	CoveredEventSeq uint64
	ResetRequired   bool
}

// ListEvents reads the scope's conversation events after the cursor. A
// cursor beyond the covered sequence, or a page that does not continue
// right after it, requires a reset: the caller reads the conversation's
// snapshot and continues after its covered sequence.
func (s *Conversations) ListEvents(ctx context.Context, scope domain.Scope, conversationID string, after uint64, limit int) (ConversationEventPage, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	var page ConversationEventPage
	err := s.store.Read(ctx, func(r Repo) error {
		conv, err := owned(ctx, r, scope, conversationID, false)
		if err != nil {
			return err
		}
		page.CoveredEventSeq = conv.CoveredEventSeq()
		if after > page.CoveredEventSeq {
			page.ResetRequired = true
			return nil
		}
		if page.Events, err = r.ListConversationEvents(ctx, conv.ID, scope.TenantID, after, limit); err != nil {
			return err
		}
		if !domain.ConversationEventsContiguous(after, page.Events) {
			page.Events, page.ResetRequired = nil, true
		}
		return nil
	})
	return page, err
}

// BatchReceipt answers one AppendReplyBatch: the turn as it stands, the
// committed chunk (nil for a refused or sealed batch), and whether the batch
// repeated a committed one or found the turn sealed.
type BatchReceipt struct {
	Turn     *domain.Turn
	Chunk    *domain.ReplyChunk
	Existing bool
	Sealed   bool
}

// AppendReplyBatch commits one batch of the reply's original frames under
// the current writer epoch (design 0001 §8.2). A sealed turn, or an open one
// whose operation carries a cancel (sealed here), answers Sealed and stores
// nothing: the Activity stops relaying and leaves the send to meter to its
// end. An inconsistent batch stops the appends: the turn goes reconciling
// and the call answers ErrIntegrity after that state is committed.
func (s *Conversations) AppendReplyBatch(ctx context.Context, cmd domain.CommandIdentity, b ReplyBinding, turnID string, epoch uint64, callID string, batch domain.ReplyBatch) (BatchReceipt, error) {
	if err := batch.Check(); err != nil {
		return BatchReceipt{}, err
	}
	now := s.clock.Now()
	var rec BatchReceipt
	var refused error
	err := s.store.Tx(ctx, func(r Repo) error {
		conv, turn, err := lockTurn(ctx, r, cmd.TenantID, turnID)
		if err != nil {
			return err
		}
		rec, refused = BatchReceipt{Turn: turn}, nil
		if !turn.Open() {
			rec.Sealed = true
			return nil
		}
		op, err := checkBinding(ctx, r, cmd.TenantID, turn, b)
		if err != nil {
			return err
		}
		if err := turn.CheckWriter(epoch); err != nil {
			return err
		}
		if callID != domain.ReplyCallID(turn.OperationID) {
			return fmt.Errorf("%w: call %s is not turn %s's reply call", domain.ErrInvalid, callID, turn.ID)
		}
		if cancelIntent(op) && turn.CanSealAtStop() {
			events, err := sealAtStop(ctx, r, conv, turn, intentCommand(op), op.Finance, now)
			if err != nil {
				return err
			}
			rec.Sealed = true
			return emit(ctx, r, conv, events)
		}
		if turn.State == domain.TurnReconciling {
			return fmt.Errorf("%w: %s: turn %s accepts no further batch", domain.ErrIntegrity, domain.ReasonStreamInconsistent, turn.ID)
		}
		var stored *domain.ReplyChunk
		if batch.From < turn.NextProviderSeq() {
			stored, err = r.GetReplyChunkByFrom(ctx, turn.ID, batch.From)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return err
			}
		}
		class, cerr := turn.ClassifyBatch(batch, stored)
		switch {
		case errors.Is(cerr, domain.ErrIntegrity):
			turn.ReconcileStream(now)
			if err := r.UpdateConversationTurn(ctx, turn); err != nil {
				return err
			}
			ev, err := conv.TurnChangedEvent(turn, op.Finance, now)
			if err != nil {
				return err
			}
			refused = cerr
			return emit(ctx, r, conv, []*domain.ConversationEvent{ev})
		case cerr != nil:
			return cerr
		case class == domain.BatchReplay:
			rec.Chunk, rec.Existing = stored, true
			return nil
		}
		chunk := turn.ApplyBatch(batch, now)
		var events []*domain.ConversationEvent
		if batch.HasText {
			fresh, err := conv.ReserveReply(turn, now)
			if err != nil {
				return err
			}
			if fresh {
				ev, err := conv.MessageStartedEvent(turn, now)
				if err != nil {
					return err
				}
				events = append(events, ev)
			}
		}
		if err := r.UpdateConversationTurn(ctx, turn); err != nil {
			return err
		}
		if err := r.InsertReplyChunk(ctx, chunk); err != nil {
			return err
		}
		if chunk.ChunkSeq > 0 {
			ev, err := conv.MessageDeltaEvent(turn, chunk, now)
			if err != nil {
				return err
			}
			events = append(events, ev)
		}
		rec.Chunk = chunk
		if len(events) == 0 {
			return nil
		}
		return emit(ctx, r, conv, events)
	})
	if errors.Is(err, ErrDuplicateKey) && depthOf(ctx) < duplicateRetries {
		return s.AppendReplyBatch(deeper(ctx), cmd, b, turnID, epoch, callID, batch)
	}
	if err != nil {
		return BatchReceipt{}, err
	}
	if refused != nil {
		s.log.Warn("conversation reply batch refused", "turnId", rec.Turn.ID, "operationId", rec.Turn.OperationID, "from", batch.From, "through", batch.Through, "reason", domain.ReasonStreamInconsistent)
		return rec, refused
	}
	return rec, nil
}
