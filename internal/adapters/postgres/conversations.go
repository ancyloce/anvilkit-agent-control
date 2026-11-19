package postgres

import (
	"context"
	"math"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Conversations (DEC-067, migration 00021).

func toConversation(m sqlc.Conversation) *domain.Conversation {
	return &domain.Conversation{
		ID: m.ConversationID, TenantID: m.TenantID, ProjectID: m.ProjectID, OwnerActorID: m.OwnerActorID,
		Status: domain.ConversationStatus(m.Status), Title: deref(m.Title), Revision: uint64(m.Revision),
		LastMessageSeq: uint64(m.LastMessageSeq), CreatedAt: fromTs(m.CreatedAt), UpdatedAt: fromTs(m.UpdatedAt),
	}
}

func toConversationMessage(m sqlc.ConversationMessage) *domain.Message {
	return &domain.Message{
		ID: m.MessageID, TenantID: m.TenantID, ConversationID: m.ConversationID, Seq: uint64(m.MessageSeq),
		Role: domain.MessageRole(m.Role), State: domain.MessageState(m.State), Text: m.Body,
		ContentDigest: domain.Digest(m.ContentDigest), ConversationRevision: uint64(m.ConversationRevision), CreatedAt: fromTs(m.CreatedAt),
	}
}

func (r *repo) InsertConversation(ctx context.Context, c *domain.Conversation) error {
	return mapErr(r.q.InsertConversation(ctx, sqlc.InsertConversationParams{
		ConversationID: c.ID, TenantID: c.TenantID, ProjectID: c.ProjectID, OwnerActorID: c.OwnerActorID, Status: string(c.Status),
		Title: strPtr(c.Title), Revision: int64(c.Revision), LastMessageSeq: int64(c.LastMessageSeq), CreatedAt: ts(c.CreatedAt), UpdatedAt: ts(c.UpdatedAt),
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
		LastMessageSeq: int64(c.LastMessageSeq), UpdatedAt: ts(c.UpdatedAt),
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
