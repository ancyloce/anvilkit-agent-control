package grpc

import (
	"context"
	"fmt"
	"math"
	"strconv"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"

	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// conversationExecutionServer is ConversationExecutionService (DEC-068,
// design 0001 §7.1): the Workflow's fixed chat Activity claims a turn's reply
// writer, reads its context and records the reply's outcome. The policy
// admits the Workflow workload only; no method grants a model send.
type conversationExecutionServer struct {
	controlv1.UnimplementedConversationExecutionServiceServer
	conversations *application.Conversations
}

// replyBinding reads the attempt binding of a chat attempt; a chat attempt
// runs no Job, so an instance id is refused.
func replyBinding(b *controlv1.ExecutionBinding) (application.ReplyBinding, error) {
	if b.GetInstanceId() != "" {
		return application.ReplyBinding{}, fmt.Errorf("%w: a chat attempt names no instance", domain.ErrInvalid)
	}
	epoch, err := strconv.ParseUint(b.GetExecutionEpoch(), 10, 64)
	if err != nil || epoch > math.MaxInt64 {
		return application.ReplyBinding{}, fmt.Errorf("%w: execution epoch %q", domain.ErrInvalid, b.GetExecutionEpoch())
	}
	return application.ReplyBinding{OperationID: b.GetOperationId(), AttemptID: b.GetAttemptId(), ExecutionEpoch: epoch}, nil
}

// positive reads a canonical positive decimal (an epoch or an attempt
// ordinal, already pattern-checked by the validator).
func positive(s, what string) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || v == 0 || v > math.MaxInt64 {
		return 0, fmt.Errorf("%w: %s %q", domain.ErrInvalid, what, s)
	}
	return v, nil
}

func (s *conversationExecutionServer) ClaimReplyWriter(ctx context.Context, req *controlv1.ClaimReplyWriterRequest) (*controlv1.ClaimReplyWriterResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	b, err := replyBinding(req.GetBinding())
	if err != nil {
		return nil, toStatus(err)
	}
	attempt, err := positive(req.GetTemporalAttempt(), "temporal attempt")
	if err != nil {
		return nil, toStatus(err)
	}
	turn, existing, err := s.conversations.ClaimReplyWriter(ctx, cmd, b, req.GetActivityKey(), attempt, req.GetInvocationId())
	if err != nil {
		return nil, toStatus(err)
	}
	view, err := s.conversations.GetTurnForWorkflow(ctx, cmd.TenantID, turn)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.ClaimReplyWriterResponse{WriterEpoch: strconv.FormatUint(turn.WriterEpoch, 10), Turn: toTurn(view.Turn, view.Finance), Existing: existing}, nil
}

func (s *conversationExecutionServer) GetTurnContext(ctx context.Context, req *controlv1.GetTurnContextRequest) (*controlv1.GetTurnContextResponse, error) {
	b, err := replyBinding(req.GetBinding())
	if err != nil {
		return nil, toStatus(err)
	}
	epoch, err := positive(req.GetWriterEpoch(), "writer epoch")
	if err != nil {
		return nil, toStatus(err)
	}
	messages, digest, err := s.conversations.GetTurnContext(ctx, req.GetTenantId(), b, req.GetTurnId(), epoch)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &controlv1.GetTurnContextResponse{Messages: make([]*controlv1.Message, 0, len(messages)), ContextDigest: string(digest)}
	for _, m := range messages {
		out.Messages = append(out.Messages, toMessage(m))
	}
	return out, nil
}

func (s *conversationExecutionServer) RecordReplyOutcome(ctx context.Context, req *controlv1.RecordReplyOutcomeRequest) (*controlv1.RecordReplyOutcomeResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	b, err := replyBinding(req.GetBinding())
	if err != nil {
		return nil, toStatus(err)
	}
	epoch, err := positive(req.GetWriterEpoch(), "writer epoch")
	if err != nil {
		return nil, toStatus(err)
	}
	outcome, ok := replyOutcomeFromProto[req.GetOutcome()]
	if !ok {
		return nil, toStatus(fmt.Errorf("%w: reply outcome %s", domain.ErrInvalid, req.GetOutcome()))
	}
	sender, ok := senderStatusFromProto[req.GetSenderStatus()]
	if !ok {
		return nil, toStatus(fmt.Errorf("%w: sender status %s", domain.ErrInvalid, req.GetSenderStatus()))
	}
	rec := domain.ReplyRecord{Outcome: outcome, HasText: req.Text != nil, Text: req.GetText(), Digest: domain.Digest(req.GetContentDigest()), Sender: sender, FailureCode: req.GetFailureCode()}
	turn, reply, existing, err := s.conversations.RecordReplyOutcome(ctx, cmd, b, req.GetTurnId(), epoch, req.GetCallId(), rec)
	if err != nil {
		return nil, toStatus(err)
	}
	view, err := s.conversations.GetTurnForWorkflow(ctx, cmd.TenantID, turn)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &controlv1.RecordReplyOutcomeResponse{Turn: toTurn(view.Turn, view.Finance), Existing: existing}
	if reply != nil {
		out.AssistantMessage = toMessage(reply)
	}
	return out, nil
}
