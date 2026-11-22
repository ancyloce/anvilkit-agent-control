package grpc

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/grpc/metadata"
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
	// streams bounds the open event streams per tenant, shared with the
	// operation streams (D4a); notifier wakes a stream when an event of its
	// conversation commits on any replica; the poll is the fallback.
	streams  *tenantStreams
	notifier application.EventNotifier
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
		ContentRevision: strconv.FormatUint(max(m.ContentRevision, 1), 10), ThroughChunkSeq: strconv.FormatUint(m.ThroughChunkSeq, 10),
	}
}

// toMessageReceipt names the turn the message opened, where replies are
// enabled (DEC-068); the assistant message is named by the turn view once its
// text is stored.
func toMessageReceipt(m *domain.Message, turn *domain.Turn) *controlv1.MessageReceipt {
	r := &controlv1.MessageReceipt{
		ConversationId: m.ConversationID, MessageId: m.ID, MessageSeq: strconv.FormatUint(m.Seq, 10),
		ConversationRevision: strconv.FormatUint(m.ConversationRevision, 10), CreatedAt: timestamppb.New(m.CreatedAt),
	}
	if turn != nil {
		r.TurnId, r.OperationId = optString(turn.ID), optString(turn.OperationID)
	}
	return r
}

var turnStateToProto = map[domain.TurnState]controlv1.TurnState{
	domain.TurnQueued: controlv1.TurnState_TURN_STATE_QUEUED, domain.TurnRunning: controlv1.TurnState_TURN_STATE_RUNNING,
	domain.TurnCompleted: controlv1.TurnState_TURN_STATE_COMPLETED, domain.TurnFailed: controlv1.TurnState_TURN_STATE_FAILED,
	domain.TurnCancelPending: controlv1.TurnState_TURN_STATE_CANCEL_PENDING, domain.TurnCanceled: controlv1.TurnState_TURN_STATE_CANCELED,
	domain.TurnReconciling: controlv1.TurnState_TURN_STATE_RECONCILING,
}

var contentStatusToProto = map[domain.ContentStatus]controlv1.ContentStatus{
	domain.ContentComplete: controlv1.ContentStatus_CONTENT_STATUS_COMPLETE, domain.ContentPartial: controlv1.ContentStatus_CONTENT_STATUS_PARTIAL,
	domain.ContentUnknown: controlv1.ContentStatus_CONTENT_STATUS_UNKNOWN,
}

var senderStatusToProto = map[domain.SenderStatus]controlv1.SenderStatus{
	domain.SenderNotStarted: controlv1.SenderStatus_SENDER_STATUS_NOT_STARTED, domain.SenderActive: controlv1.SenderStatus_SENDER_STATUS_ACTIVE,
	domain.SenderStopped: controlv1.SenderStatus_SENDER_STATUS_STOPPED, domain.SenderUnknown: controlv1.SenderStatus_SENDER_STATUS_UNKNOWN,
}

var senderStatusFromProto = map[controlv1.SenderStatus]domain.SenderStatus{
	controlv1.SenderStatus_SENDER_STATUS_NOT_STARTED: domain.SenderNotStarted, controlv1.SenderStatus_SENDER_STATUS_ACTIVE: domain.SenderActive,
	controlv1.SenderStatus_SENDER_STATUS_STOPPED: domain.SenderStopped, controlv1.SenderStatus_SENDER_STATUS_UNKNOWN: domain.SenderUnknown,
}

var replyOutcomeFromProto = map[controlv1.ReplyOutcome]domain.ReplyOutcome{
	controlv1.ReplyOutcome_REPLY_OUTCOME_COMPLETED: domain.ReplyCompleted, controlv1.ReplyOutcome_REPLY_OUTCOME_PARTIAL_FAILED: domain.ReplyPartialFailed,
	controlv1.ReplyOutcome_REPLY_OUTCOME_CANCELED: domain.ReplyCanceled, controlv1.ReplyOutcome_REPLY_OUTCOME_FAILED: domain.ReplyFailed,
}

// toTurn is a turn with its operation's finance; the stored writer and
// outcome command stay Control's.
func toTurn(t *domain.Turn, finance domain.FinanceState) *controlv1.Turn {
	return &controlv1.Turn{
		TurnId: t.ID, ConversationId: t.ConversationID, InputMessageId: t.InputMessageID, AssistantMessageId: optString(t.PublicMessageID()),
		OperationId: t.OperationID, State: turnStateToProto[t.State], ContentStatus: contentStatusToProto[t.Content],
		SenderStatus: senderStatusToProto[t.Sender], Finance: financeToProto[finance], FailureCode: optString(t.FailureCode),
		CreatedAt: timestamppb.New(t.CreatedAt), UpdatedAt: timestamppb.New(t.UpdatedAt),
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
	snap, err := s.conversations.Snapshot(ctx, scope(ctx, req.GetScope()), req.GetConversationId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &controlv1.GetConversationResponse{Conversation: toConversation(snap.Conversation), CoveredEventSeq: strconv.FormatUint(snap.CoveredEventSeq, 10)}
	if snap.ActiveTurn != nil {
		out.ActiveTurn = toTurn(snap.ActiveTurn.Turn, snap.ActiveTurn.Finance)
	}
	return out, nil
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
	m, turn, existing, err := s.conversations.Append(ctx, cmd, scope(ctx, req.GetScope()), req.GetConversationId(), uint64(expected), req.GetText())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.AppendMessageResponse{Receipt: toMessageReceipt(m, turn), Existing: existing}, nil
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

func (s *conversationServer) GetTurn(ctx context.Context, req *controlv1.GetTurnRequest) (*controlv1.GetTurnResponse, error) {
	v, err := s.conversations.GetTurn(ctx, scope(ctx, req.GetScope()), req.GetConversationId(), req.GetTurnId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetTurnResponse{Turn: toTurn(v.Turn, v.Finance)}, nil
}

func (s *conversationServer) StopTurn(ctx context.Context, req *controlv1.StopTurnRequest) (*controlv1.StopTurnResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	v, existing, err := s.conversations.StopTurn(ctx, cmd, scope(ctx, req.GetScope()), req.GetConversationId(), req.GetTurnId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.StopTurnResponse{Turn: toTurn(v.Turn, v.Finance), Existing: existing}, nil
}

func (s *conversationServer) GetMessage(ctx context.Context, req *controlv1.GetMessageRequest) (*controlv1.GetMessageResponse, error) {
	m, err := s.conversations.GetMessage(ctx, scope(ctx, req.GetScope()), req.GetConversationId(), req.GetMessageId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetMessageResponse{Message: toMessage(m)}, nil
}

// toConversationEvent maps a stored event; a message.delta carries its
// chunk's text, served only here.
func toConversationEvent(ev *domain.ConversationEvent) *controlv1.ConversationEvent {
	out := &controlv1.ConversationEvent{
		ConversationId: ev.ConversationID, EventSeq: strconv.FormatUint(ev.Seq, 10), EventType: string(ev.Type), OccurredAt: timestamppb.New(ev.OccurredAt),
	}
	p := ev.Payload
	switch ev.Type {
	case domain.EventMessageAccepted:
		out.Payload = &controlv1.ConversationEvent_MessageAccepted{MessageAccepted: &controlv1.MessageAcceptedPayload{
			MessageId: ev.MessageID, MessageSeq: strconv.FormatUint(p.MessageSeq, 10), Role: messageRoleToProto[p.Role], TurnId: optString(ev.TurnID)}}
	case domain.EventMessageStarted:
		out.Payload = &controlv1.ConversationEvent_MessageStarted{MessageStarted: &controlv1.MessageStartedPayload{MessageId: ev.MessageID, TurnId: ev.TurnID}}
	case domain.EventMessageDelta:
		out.Payload = &controlv1.ConversationEvent_MessageDelta{MessageDelta: &controlv1.MessageDeltaPayload{
			MessageId: ev.MessageID, ChunkSeq: strconv.FormatUint(ev.ChunkSeq, 10), ContentRevision: strconv.FormatUint(p.ContentRevision, 10), Text: ev.Text}}
	case domain.EventMessageCompleted, domain.EventMessageFailed, domain.EventMessageCanceled:
		out.Payload = &controlv1.ConversationEvent_MessageSealed{MessageSealed: &controlv1.MessageSealedPayload{
			MessageId: ev.MessageID, State: messageStateToProto[p.State], ContentRevision: strconv.FormatUint(max(p.ContentRevision, 1), 10),
			ThroughChunkSeq: strconv.FormatUint(p.ThroughChunkSeq, 10), ContentDigest: string(p.ContentDigest), TurnState: turnStateToProto[p.TurnState],
			FailureCode: optString(p.FailureCode)}}
	case domain.EventTurnChanged:
		if t := p.Turn; t != nil {
			out.Payload = &controlv1.ConversationEvent_TurnChanged{TurnChanged: &controlv1.TurnChangedPayload{Turn: &controlv1.Turn{
				TurnId: t.TurnID, ConversationId: ev.ConversationID, InputMessageId: t.InputMessageID, AssistantMessageId: optString(t.AssistantMessageID),
				OperationId: t.OperationID, State: turnStateToProto[t.State], ContentStatus: contentStatusToProto[t.Content],
				SenderStatus: senderStatusToProto[t.Sender], Finance: financeToProto[t.Finance], FailureCode: optString(t.FailureCode),
				CreatedAt: timestamppb.New(t.CreatedAt), UpdatedAt: timestamppb.New(t.UpdatedAt),
			}}}
		}
	case domain.EventConversationChanged:
		out.Payload = &controlv1.ConversationEvent_ConversationChanged{ConversationChanged: &controlv1.ConversationChangedPayload{
			ConversationRevision: strconv.FormatUint(p.ConversationRevision, 10), Status: conversationStatusToProto[p.Status]}}
	}
	return out
}

func (s *conversationServer) ListConversationEvents(ctx context.Context, req *controlv1.ListConversationEventsRequest) (*controlv1.ListConversationEventsResponse, error) {
	after, err := parseAfter(req.GetAfterEventSeq())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := s.conversations.ListEvents(ctx, scope(ctx, req.GetScope()), req.GetConversationId(), after, int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &controlv1.ListConversationEventsResponse{CoveredEventSeq: strconv.FormatUint(page.CoveredEventSeq, 10), ResetRequired: page.ResetRequired}
	for _, ev := range page.Events {
		out.Events = append(out.Events, toConversationEvent(ev))
	}
	return out, nil
}

// StreamConversationEvents re-reads committed events after the cursor until
// the client goes away or the server drains (D4a): a conversation has no
// terminal state. It never buffers beyond one page, so a slow consumer only
// delays itself, and a cursor it cannot honor ends the stream INVALID_ARGUMENT
// (the API answers reset_required).
func (s *conversationServer) StreamConversationEvents(req *controlv1.StreamConversationEventsRequest, stream controlv1.ConversationService_StreamConversationEventsServer) error {
	ctx := stream.Context()
	after, err := parseAfter(req.GetAfterEventSeq())
	if err != nil {
		return toStatus(err)
	}
	sc := scope(ctx, req.GetScope())
	first, err := s.conversations.ListEvents(ctx, sc, req.GetConversationId(), after, 1)
	if err != nil {
		return toStatus(err)
	}
	if first.ResetRequired {
		return toStatus(domain.ErrInvalid)
	}
	if s.streams != nil {
		release, err := s.streams.acquire(req.GetScope().GetTenantId())
		if err != nil {
			return err
		}
		defer release()
	}
	if err := stream.SendHeader(metadata.Pairs("anvilkit-stream", "admitted")); err != nil {
		return err
	}
	var wake <-chan struct{}
	if s.notifier != nil {
		var cancel func()
		wake, cancel = s.notifier.Subscribe(req.GetConversationId())
		defer cancel()
	}
	for {
		page, err := s.conversations.ListEvents(ctx, sc, req.GetConversationId(), after, 200)
		if err != nil {
			return toStatus(err)
		}
		if page.ResetRequired {
			return toStatus(domain.ErrInvalid)
		}
		for _, ev := range page.Events {
			if err := stream.Send(&controlv1.StreamConversationEventsResponse{Event: toConversationEvent(ev)}); err != nil {
				return err
			}
			after = ev.Seq
		}
		if len(page.Events) == 200 {
			continue
		}
		poll := time.NewTimer(streamPoll)
		select {
		case <-ctx.Done():
			poll.Stop()
			return toStatus(ctx.Err())
		case <-wake:
			poll.Stop()
		case <-poll.C:
		}
	}
}
