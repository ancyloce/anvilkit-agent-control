package grpc

import (
	"context"
	"strconv"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"

	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// conversationServer is ConversationService (DEC-067): the API's calls on
// behalf of the verified principal, whose Scope decides ownership.
type conversationServer struct {
	controlv1.UnimplementedConversationServiceServer
	conversations *application.Conversations
}

var conversationStatusToProto = map[domain.ConversationStatus]controlv1.ConversationStatus{
	domain.ConversationActive:   controlv1.ConversationStatus_CONVERSATION_STATUS_ACTIVE,
	domain.ConversationArchived: controlv1.ConversationStatus_CONVERSATION_STATUS_ARCHIVED,
}

var conversationStatusFromProto = map[controlv1.ConversationStatus]domain.ConversationStatus{
	controlv1.ConversationStatus_CONVERSATION_STATUS_ACTIVE:   domain.ConversationActive,
	controlv1.ConversationStatus_CONVERSATION_STATUS_ARCHIVED: domain.ConversationArchived,
}

var messageRoleToProto = map[domain.MessageRole]controlv1.MessageRole{
	domain.MessageRoleUser:      controlv1.MessageRole_MESSAGE_ROLE_USER,
	domain.MessageRoleAssistant: controlv1.MessageRole_MESSAGE_ROLE_ASSISTANT,
}

var messageStateToProto = map[domain.MessageState]controlv1.MessageState{
	domain.MessagePending:       controlv1.MessageState_MESSAGE_STATE_PENDING,
	domain.MessageStreaming:     controlv1.MessageState_MESSAGE_STATE_STREAMING,
	domain.MessageCompleted:     controlv1.MessageState_MESSAGE_STATE_COMPLETED,
	domain.MessagePartialFailed: controlv1.MessageState_MESSAGE_STATE_PARTIAL_FAILED,
	domain.MessageCanceled:      controlv1.MessageState_MESSAGE_STATE_CANCELED,
	domain.MessageReconciling:   controlv1.MessageState_MESSAGE_STATE_RECONCILING,
}

func toConversation(c *domain.Conversation) *controlv1.Conversation {
	return &controlv1.Conversation{
		ConversationId: c.ID, TenantId: c.TenantID, ProjectId: c.ProjectID, OwnerActorId: c.OwnerActorID,
		Status: conversationStatusToProto[c.Status], Title: optString(c.Title),
		Revision: strconv.FormatUint(c.Revision, 10), LastMessageSeq: strconv.FormatUint(c.LastMessageSeq, 10),
		CreatedAt: timestamppb.New(c.CreatedAt), UpdatedAt: timestamppb.New(c.UpdatedAt),
	}
}

func toMessage(m *domain.Message) *controlv1.Message {
	return &controlv1.Message{
		MessageId: m.ID, ConversationId: m.ConversationID, MessageSeq: strconv.FormatUint(m.Seq, 10),
		Role: messageRoleToProto[m.Role], State: messageStateToProto[m.State], Text: m.Text,
		ContentDigest: string(m.ContentDigest), CreatedAt: timestamppb.New(m.CreatedAt),
	}
}

func toMessageReceipt(m *domain.Message) *controlv1.MessageReceipt {
	return &controlv1.MessageReceipt{
		ConversationId: m.ConversationID, MessageId: m.ID, MessageSeq: strconv.FormatUint(m.Seq, 10),
		ConversationRevision: strconv.FormatUint(m.ConversationRevision, 10), CreatedAt: timestamppb.New(m.CreatedAt),
	}
}

func (s *conversationServer) CreateConversation(ctx context.Context, req *controlv1.CreateConversationRequest) (*controlv1.CreateConversationResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	c, existing, err := s.conversations.Create(ctx, cmd, scope(ctx, req.GetScope()), req.GetTitle())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.CreateConversationResponse{Conversation: toConversation(c), Existing: existing}, nil
}

func (s *conversationServer) GetConversation(ctx context.Context, req *controlv1.GetConversationRequest) (*controlv1.GetConversationResponse, error) {
	c, err := s.conversations.Get(ctx, scope(ctx, req.GetScope()), req.GetConversationId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetConversationResponse{Conversation: toConversation(c)}, nil
}

func (s *conversationServer) ListConversations(ctx context.Context, req *controlv1.ListConversationsRequest) (*controlv1.ListConversationsResponse, error) {
	page, next, err := s.conversations.List(ctx, scope(ctx, req.GetScope()), conversationStatusFromProto[req.GetStatus()], req.GetCursor(), int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &controlv1.ListConversationsResponse{Conversations: make([]*controlv1.Conversation, 0, len(page)), NextCursor: optString(next)}
	for _, c := range page {
		out.Conversations = append(out.Conversations, toConversation(c))
	}
	return out, nil
}

func (s *conversationServer) UpdateConversation(ctx context.Context, req *controlv1.UpdateConversationRequest) (*controlv1.UpdateConversationResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	expected, err := domain.ParseRevision(req.GetExpectedRevision())
	if err != nil {
		return nil, toStatus(err)
	}
	c, existing, err := s.conversations.Update(ctx, cmd, scope(ctx, req.GetScope()), req.GetConversationId(), uint64(expected), req.Title, conversationStatusFromProto[req.GetStatus()])
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.UpdateConversationResponse{Conversation: toConversation(c), Existing: existing}, nil
}

func (s *conversationServer) AppendMessage(ctx context.Context, req *controlv1.AppendMessageRequest) (*controlv1.AppendMessageResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	expected, err := domain.ParseRevision(req.GetExpectedRevision())
	if err != nil {
		return nil, toStatus(err)
	}
	m, existing, err := s.conversations.Append(ctx, cmd, scope(ctx, req.GetScope()), req.GetConversationId(), uint64(expected), req.GetText())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.AppendMessageResponse{Receipt: toMessageReceipt(m), Existing: existing}, nil
}

func (s *conversationServer) ListMessages(ctx context.Context, req *controlv1.ListMessagesRequest) (*controlv1.ListMessagesResponse, error) {
	var before uint64
	if req.BeforeMessageSeq != nil {
		seq, err := domain.ParseRevision(req.GetBeforeMessageSeq())
		if err != nil {
			return nil, toStatus(err)
		}
		before = uint64(seq)
	}
	page, nextBefore, err := s.conversations.Messages(ctx, scope(ctx, req.GetScope()), req.GetConversationId(), before, int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &controlv1.ListMessagesResponse{Messages: make([]*controlv1.Message, 0, len(page))}
	for _, m := range page {
		out.Messages = append(out.Messages, toMessage(m))
	}
	if nextBefore > 0 {
		out.NextBeforeMessageSeq = optString(strconv.FormatUint(nextBefore, 10))
	}
	return out, nil
}
