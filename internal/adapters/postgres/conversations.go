package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/outbox"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Conversations (DEC-067, migration 00021).

func toConversation(m sqlc.Conversation) *domain.Conversation {
	return &domain.Conversation{
		ID: m.ConversationID, TenantID: m.TenantID, ProjectID: m.ProjectID, OwnerActorID: m.OwnerActorID,
		Status: domain.ConversationStatus(m.Status), Title: deref(m.Title), Revision: uint64(m.Revision),
		LastMessageSeq: uint64(m.LastMessageSeq), NextEventSeq: uint64(m.NextEventSeq), CreatedAt: fromTs(m.CreatedAt), UpdatedAt: fromTs(m.UpdatedAt),
	}
}

func toConversationMessage(m sqlc.ConversationMessage) *domain.Message {
	return &domain.Message{
		ID: m.MessageID, TenantID: m.TenantID, ConversationID: m.ConversationID, Seq: uint64(m.MessageSeq),
		Role: domain.MessageRole(m.Role), State: domain.MessageState(m.State), Text: m.Body,
		ContentDigest: domain.Digest(m.ContentDigest), ConversationRevision: uint64(m.ConversationRevision), CreatedAt: fromTs(m.CreatedAt),
		ContentRevision: uint64(m.ContentRevision), ThroughChunkSeq: uint64(m.ThroughChunkSeq),
	}
}

func (r *repo) InsertConversation(ctx context.Context, c *domain.Conversation) error {
	return mapErr(r.q.InsertConversation(ctx, sqlc.InsertConversationParams{
		ConversationID: c.ID, TenantID: c.TenantID, ProjectID: c.ProjectID, OwnerActorID: c.OwnerActorID, Status: string(c.Status),
		Title: strPtr(c.Title), Revision: int64(c.Revision), LastMessageSeq: int64(c.LastMessageSeq), CreatedAt: ts(c.CreatedAt), UpdatedAt: ts(c.UpdatedAt),
		NextEventSeq: int64(eventSeqOrFirst(c.NextEventSeq)),
	}))
}

func (r *repo) GetConversation(ctx context.Context, conversationID, tenantID string) (*domain.Conversation, error) {
	m, err := r.q.GetConversation(ctx, sqlc.GetConversationParams{ConversationID: conversationID, TenantID: tenantID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toConversation(m), nil
}

func (r *repo) LockConversation(ctx context.Context, conversationID, tenantID string) (*domain.Conversation, error) {
	m, err := r.q.LockConversation(ctx, sqlc.LockConversationParams{ConversationID: conversationID, TenantID: tenantID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toConversation(m), nil
}

func (r *repo) UpdateConversation(ctx context.Context, c *domain.Conversation) error {
	return mapErr(r.q.UpdateConversation(ctx, sqlc.UpdateConversationParams{
		ConversationID: c.ID, Status: string(c.Status), Title: strPtr(c.Title), Revision: int64(c.Revision),
		LastMessageSeq: int64(c.LastMessageSeq), UpdatedAt: ts(c.UpdatedAt), NextEventSeq: int64(eventSeqOrFirst(c.NextEventSeq)),
	}))
}

func (r *repo) ListConversations(ctx context.Context, owner domain.Scope, status domain.ConversationStatus, after *domain.ConversationCursor, limit int) ([]*domain.Conversation, error) {
	p := sqlc.ListConversationsParams{TenantID: owner.TenantID, ProjectID: owner.ProjectID, OwnerActorID: owner.ActorID, Status: string(status), AfterCreatedAt: ts(time.Unix(0, 0)), RowLimit: int32(limit)}
	if after != nil {
		p.AfterID, p.AfterCreatedAt = after.ID, ts(after.CreatedAt)
	}
	rows, err := r.q.ListConversations(ctx, p)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Conversation, 0, len(rows))
	for _, m := range rows {
		out = append(out, toConversation(m))
	}
	return out, nil
}

func (r *repo) InsertConversationMessage(ctx context.Context, m *domain.Message) error {
	return mapErr(r.q.InsertConversationMessage(ctx, sqlc.InsertConversationMessageParams{
		MessageID: m.ID, TenantID: m.TenantID, ConversationID: m.ConversationID, MessageSeq: int64(m.Seq), Role: string(m.Role),
		State: string(m.State), Body: m.Text, ContentDigest: string(m.ContentDigest), ConversationRevision: int64(m.ConversationRevision), CreatedAt: ts(m.CreatedAt),
		ContentRevision: int64(max(m.ContentRevision, 1)), ThroughChunkSeq: int64(m.ThroughChunkSeq),
	}))
}

func (r *repo) GetConversationMessage(ctx context.Context, messageID, tenantID string) (*domain.Message, error) {
	m, err := r.q.GetConversationMessage(ctx, sqlc.GetConversationMessageParams{MessageID: messageID, TenantID: tenantID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toConversationMessage(m), nil
}

// ListConversationMessages reads up to limit messages before the sequence,
// newest first; before 0 reads from the newest message.
func (r *repo) ListConversationMessages(ctx context.Context, conversationID, tenantID string, before uint64, limit int) ([]*domain.Message, error) {
	bound := int64(math.MaxInt64)
	if before > 0 && before < math.MaxInt64 {
		bound = int64(before)
	}
	rows, err := r.q.ListConversationMessages(ctx, sqlc.ListConversationMessagesParams{ConversationID: conversationID, TenantID: tenantID, BeforeSeq: bound, RowLimit: int32(limit)})
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Message, 0, len(rows))
	for _, m := range rows {
		out = append(out, toConversationMessage(m))
	}
	return out, nil
}

func (r *repo) GetConversationCommand(ctx context.Context, tenantID, commandID string) (*domain.ConversationCommand, error) {
	m, err := r.q.GetConversationCommand(ctx, sqlc.GetConversationCommandParams{TenantID: tenantID, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return &domain.ConversationCommand{
		TenantID: m.TenantID, CommandID: m.CommandID, Kind: domain.ConversationCommandKind(m.Kind), ActorID: m.ActorID,
		RequestDigest: domain.Digest(m.RequestDigest), ConversationID: m.ConversationID, MessageID: deref(m.MessageID), AcceptedAt: fromTs(m.AcceptedAt),
	}, nil
}

func (r *repo) InsertConversationCommand(ctx context.Context, c *domain.ConversationCommand) error {
	return mapErr(r.q.InsertConversationCommand(ctx, sqlc.InsertConversationCommandParams{
		TenantID: c.TenantID, CommandID: c.CommandID, Kind: string(c.Kind), ActorID: c.ActorID, RequestDigest: string(c.RequestDigest),
		ConversationID: c.ConversationID, MessageID: strPtr(c.MessageID), AcceptedAt: ts(c.AcceptedAt),
	}))
}

// Turns (DEC-068, migration 00022).

func toConversationTurn(m sqlc.ConversationTurn) *domain.Turn {
	return &domain.Turn{
		ID: m.TurnID, TenantID: m.TenantID, ConversationID: m.ConversationID, InputMessageID: m.InputMessageID,
		InputMessageSeq: uint64(m.InputMessageSeq), OperationID: m.OperationID, State: domain.TurnState(m.State),
		Content: domain.ContentStatus(m.ContentStatus), Sender: domain.SenderStatus(m.SenderStatus),
		AssistantMessageID: deref(m.AssistantMessageID), FailureCode: deref(m.FailureCode), ActivityKey: deref(m.ActivityKey),
		TemporalAttempt: uint64(m.TemporalAttempt), InvocationID: deref(m.InvocationID), WriterEpoch: uint64(m.WriterEpoch),
		OutcomeCommandID: deref(m.OutcomeCommandID), OutcomeDigest: domain.Digest(deref(m.OutcomeDigest)),
		OriginalStreamID: deref(m.OriginalStreamID), LastProviderSeq: u64Ptr(m.LastProviderSeq), LastBatchSeq: uint64(m.LastBatchSeq),
		LastChunkSeq: uint64(m.LastChunkSeq), CommittedBytes: uint64(m.CommittedBytes), ReservedMessageID: deref(m.ReservedMessageID),
		ReservedMessageSeq: uint64(derefInt(m.ReservedMessageSeq)),
		CreatedAt:          fromTs(m.CreatedAt), UpdatedAt: fromTs(m.UpdatedAt),
	}
}

func u64Ptr(v *int64) *uint64 {
	if v == nil {
		return nil
	}
	u := uint64(*v)
	return &u
}

// eventSeqOrFirst keeps a conversation built before D4a (NextEventSeq 0) at
// the first event sequence.
func eventSeqOrFirst(v uint64) uint64 {
	if v == 0 {
		return 1
	}
	return v
}

func (r *repo) InsertConversationTurn(ctx context.Context, t *domain.Turn) error {
	return mapErr(r.q.InsertConversationTurn(ctx, sqlc.InsertConversationTurnParams{
		TurnID: t.ID, TenantID: t.TenantID, ConversationID: t.ConversationID, InputMessageID: t.InputMessageID,
		InputMessageSeq: int64(t.InputMessageSeq), OperationID: t.OperationID, State: string(t.State),
		ContentStatus: string(t.Content), SenderStatus: string(t.Sender), AssistantMessageID: strPtr(t.AssistantMessageID),
		FailureCode: strPtr(t.FailureCode), ActivityKey: strPtr(t.ActivityKey), TemporalAttempt: int64(t.TemporalAttempt),
		InvocationID: strPtr(t.InvocationID), WriterEpoch: int64(t.WriterEpoch), OutcomeCommandID: strPtr(t.OutcomeCommandID),
		OutcomeDigest: strPtr(string(t.OutcomeDigest)), CreatedAt: ts(t.CreatedAt), UpdatedAt: ts(t.UpdatedAt),
	}))
}

func (r *repo) GetConversationTurn(ctx context.Context, turnID, tenantID string) (*domain.Turn, error) {
	m, err := r.q.GetConversationTurn(ctx, sqlc.GetConversationTurnParams{TurnID: turnID, TenantID: tenantID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toConversationTurn(m), nil
}

func (r *repo) GetConversationTurnByInput(ctx context.Context, messageID, tenantID string) (*domain.Turn, error) {
	m, err := r.q.GetConversationTurnByInput(ctx, sqlc.GetConversationTurnByInputParams{InputMessageID: messageID, TenantID: tenantID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toConversationTurn(m), nil
}

func (r *repo) ListConversationTurnsByInputs(ctx context.Context, conversationID, tenantID string, messageIDs []string) ([]*domain.Turn, error) {
	rows, err := r.q.ListConversationTurnsByInputs(ctx, sqlc.ListConversationTurnsByInputsParams{TenantID: tenantID, ConversationID: conversationID, InputMessageIds: messageIDs})
	if err != nil {
		return nil, mapErr(err)
	}
	turns := make([]*domain.Turn, 0, len(rows))
	for _, m := range rows {
		turns = append(turns, toConversationTurn(m))
	}
	return turns, nil
}

func (r *repo) GetConversationTurnByOperation(ctx context.Context, operationID, tenantID string) (*domain.Turn, error) {
	m, err := r.q.GetConversationTurnByOperation(ctx, sqlc.GetConversationTurnByOperationParams{OperationID: operationID, TenantID: tenantID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toConversationTurn(m), nil
}

// GetOpenConversationTurn reads the conversation's one open turn (the slot
// the partial unique index keeps); ErrNotFound when the slot is free.
func (r *repo) GetOpenConversationTurn(ctx context.Context, conversationID, tenantID string) (*domain.Turn, error) {
	m, err := r.q.GetOpenConversationTurn(ctx, sqlc.GetOpenConversationTurnParams{ConversationID: conversationID, TenantID: tenantID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toConversationTurn(m), nil
}

func (r *repo) UpdateConversationTurn(ctx context.Context, t *domain.Turn) error {
	return mapErr(r.q.UpdateConversationTurn(ctx, sqlc.UpdateConversationTurnParams{
		TurnID: t.ID, State: string(t.State), ContentStatus: string(t.Content), SenderStatus: string(t.Sender),
		AssistantMessageID: strPtr(t.AssistantMessageID), FailureCode: strPtr(t.FailureCode), ActivityKey: strPtr(t.ActivityKey),
		TemporalAttempt: int64(t.TemporalAttempt), InvocationID: strPtr(t.InvocationID), WriterEpoch: int64(t.WriterEpoch),
		OutcomeCommandID: strPtr(t.OutcomeCommandID), OutcomeDigest: strPtr(string(t.OutcomeDigest)), UpdatedAt: ts(t.UpdatedAt),
		OriginalStreamID: strPtr(t.OriginalStreamID), LastProviderSeq: lastProviderSeq(t.LastProviderSeq), LastBatchSeq: int64(t.LastBatchSeq),
		LastChunkSeq: int64(t.LastChunkSeq), CommittedBytes: int64(t.CommittedBytes), ReservedMessageID: strPtr(t.ReservedMessageID),
		ReservedMessageSeq: int64Ptr(t.ReservedMessageSeq, t.ReservedMessageID != ""),
	}))
}

func lastProviderSeq(v *uint64) *int64 {
	if v == nil {
		return nil
	}
	i := int64(*v)
	return &i
}

func (r *repo) GetConversationTurnByReservedMessage(ctx context.Context, messageID, tenantID string) (*domain.Turn, error) {
	m, err := r.q.GetConversationTurnByReservedMessage(ctx, sqlc.GetConversationTurnByReservedMessageParams{ReservedMessageID: &messageID, TenantID: tenantID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toConversationTurn(m), nil
}

// Reply batches and the conversation event log (DEC-070, migration 00023).

func (r *repo) InsertReplyChunk(ctx context.Context, c *domain.ReplyChunk) error {
	return mapErr(r.q.InsertReplyChunk(ctx, sqlc.InsertReplyChunkParams{
		TurnID: c.TurnID, TenantID: c.TenantID, BatchSeq: int64(c.BatchSeq), ChunkSeq: int64Ptr(c.ChunkSeq, c.ChunkSeq > 0),
		ProviderFromSeq: int64(c.From), ProviderThroughSeq: int64(c.Through), SourceFramesDigest: string(c.SourceFramesDigest),
		Text: strPtr(c.Text), TextDigest: strPtr(string(c.TextDigest)), CreatedAt: ts(c.CreatedAt),
	}))
}

// GetReplyChunkByFrom reads the committed batch of the turn starting at the
// provider sequence; ErrNotFound when none does.
func (r *repo) GetReplyChunkByFrom(ctx context.Context, turnID string, from uint64) (*domain.ReplyChunk, error) {
	m, err := r.q.GetReplyChunkByFrom(ctx, sqlc.GetReplyChunkByFromParams{TurnID: turnID, ProviderFromSeq: int64(from)})
	if err != nil {
		return nil, mapErr(err)
	}
	return &domain.ReplyChunk{
		TurnID: m.TurnID, TenantID: m.TenantID, BatchSeq: uint64(m.BatchSeq), ChunkSeq: uint64(derefInt(m.ChunkSeq)),
		From: uint64(m.ProviderFromSeq), Through: uint64(m.ProviderThroughSeq), SourceFramesDigest: domain.Digest(m.SourceFramesDigest),
		Text: deref(m.Text), TextDigest: domain.Digest(deref(m.TextDigest)), CreatedAt: fromTs(m.CreatedAt),
	}, nil
}

// ReplyText is the turn's committed text: its chunks in order.
func (r *repo) ReplyText(ctx context.Context, turnID string) (string, error) {
	rows, err := r.q.ListReplyChunkTexts(ctx, turnID)
	if err != nil {
		return "", mapErr(err)
	}
	var text []byte
	for _, row := range rows {
		text = append(text, deref(row.Text)...)
	}
	return string(text), nil
}

// InsertConversationEvents appends a commit's conversation events and, in
// the same transaction, the conversation.changed wake-up through the last of
// them (outbox, design 0001 §8.5) and a NOTIFY with the conversation id on
// the event channel, which the operation streams share (ids never collide).
func (r *repo) InsertConversationEvents(ctx context.Context, events []*domain.ConversationEvent) error {
	if len(events) == 0 {
		return nil
	}
	tx, ok := r.db.(pgx.Tx)
	if !ok {
		return errors.New("conversation events are inserted inside a transaction")
	}
	for _, ev := range events {
		payload, err := json.Marshal(ev.Payload)
		if err != nil {
			return err
		}
		if err := r.q.InsertConversationEvent(ctx, sqlc.InsertConversationEventParams{
			ConversationID: ev.ConversationID, TenantID: ev.TenantID, EventSeq: int64(ev.Seq), EventType: string(ev.Type),
			MessageID: strPtr(ev.MessageID), TurnID: strPtr(ev.TurnID), ChunkSeq: int64Ptr(ev.ChunkSeq, ev.ChunkSeq > 0),
			Payload: payload, OccurredAt: ts(ev.OccurredAt),
		}); err != nil {
			return mapErr(err)
		}
	}
	last := events[len(events)-1]
	env, err := domain.ConversationChangedEnvelope(last.ConversationID, last.TenantID, last.Seq, last.OccurredAt)
	if err != nil {
		return err
	}
	if err := outbox.Publish(ctx, tx, env); err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, "SELECT pg_notify($1, $2)", EventChannel, last.ConversationID)
	return err
}

// ListConversationEvents reads up to limit events after the cursor, each
// message.delta with its chunk's text.
func (r *repo) ListConversationEvents(ctx context.Context, conversationID, tenantID string, after uint64, limit int) ([]*domain.ConversationEvent, error) {
	rows, err := r.q.ListConversationEvents(ctx, sqlc.ListConversationEventsParams{ConversationID: conversationID, TenantID: tenantID, AfterSeq: int64(after), RowLimit: int32(limit)})
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.ConversationEvent, 0, len(rows))
	for _, m := range rows {
		ev := &domain.ConversationEvent{
			ConversationID: conversationID, TenantID: tenantID, Seq: uint64(m.EventSeq), Type: domain.ConversationEventType(m.EventType),
			MessageID: deref(m.MessageID), TurnID: deref(m.TurnID), ChunkSeq: uint64(derefInt(m.ChunkSeq)), OccurredAt: fromTs(m.OccurredAt),
			Text: deref(m.ChunkText),
		}
		if err := json.Unmarshal(m.Payload, &ev.Payload); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}
