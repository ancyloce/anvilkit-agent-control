package application_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// TestConversationTurns (DEC-068, design 0001 D3) proves the reply turn on a
// real PostgreSQL: a message opens one turn and its chat_turn operation, the
// slot refuses a second message with TURN_BUSY and stores nothing, the chat
// Activity's writer, context and outcome are fenced by the attempt binding,
// the writer epoch and the reply call, a sealed outcome replays and frees the
// slot without moving the conversation's revision, and a stop of an unstarted
// turn cancels it at once.
func TestConversationTurns(t *testing.T) {
	inst := testdb.Start(t)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	store := postgres.NewStore(inst.Pool(t))
	chat := domain.Profile{ID: domain.ChatTurnProfileID, Kind: domain.KindChatTurn, OperationDeadline: 150 * time.Second,
		StepID: domain.ChatTurnStepID, MultiStep: true, AttemptWindow: 120 * time.Second}
	ops := application.NewOperations(store, inv, []domain.Profile{chat}, domain.SystemClock{}, testLog)
	exec := application.NewExecution(store, inv, manifests, []domain.Profile{chat}, domain.SystemClock{}, testLog)
	conversations := application.NewConversations(store, domain.SystemClock{}, testLog).WithReplies(ops, chat, 64, true)
	ctx := context.Background()

	conv, _, err := conversations.Create(ctx, convCmd(convAlice, "turn_conv", "t"), convAlice, "")
	require.NoError(t, err)
	m, turn, existing, err := conversations.Append(ctx, convCmd(convAlice, "turn_msg_1", "a"), convAlice, conv.ID, 1, "Make it black")
	require.NoError(t, err)
	require.False(t, existing)
	require.NotNil(t, turn, "a message opens a turn where replies are enabled")
	require.Equal(t, domain.TurnQueued, turn.State)
	require.Equal(t, m.ID, turn.InputMessageID)
	op, err := ops.Get(ctx, convAlice, turn.OperationID)
	require.NoError(t, err)
	require.Equal(t, domain.KindChatTurn, op.Kind)
	require.Equal(t, domain.IntakeConfirmed, op.Intake, "the turn's operation is confirmed, so the relay starts it")

	_, again, existing, err := conversations.Append(ctx, convCmd(convAlice, "turn_msg_1", "a"), convAlice, conv.ID, 1, "Make it black")
	require.NoError(t, err)
	require.True(t, existing)
	require.Equal(t, turn.ID, again.ID, "a replay answers the same turn")

	_, _, _, err = conversations.Append(ctx, convCmd(convAlice, "turn_msg_busy", "b"), convAlice, conv.ID, 2, "And a search box")
	require.ErrorIs(t, err, domain.ErrTurnBusy)
	page, _, err := conversations.Messages(ctx, convAlice, conv.ID, 0, 10)
	require.NoError(t, err)
	require.Len(t, page, 1, "a refused message stores nothing")

	_, _, err = ops.Create(ctx, convCmd(convAlice, "turn_direct", "x"), convAlice, domain.KindChatTurn, domain.Subject{ProfileID: chat.ID}, nil)
	require.ErrorIs(t, err, domain.ErrInvalid, "a chat turn is never created through CreateOperation")

	wf := func(id string) domain.CommandIdentity {
		return domain.CommandIdentity{TenantID: convAlice.TenantID, CommandID: turn.OperationID + ":" + id, ActorID: "anvilkit-agent-workflow", RequestDigest: application.DigestOf([]byte(id))}
	}
	at, _, err := exec.OpenAttempt(ctx, wf("open"), turn.OperationID, domain.ChatTurnStepID, 0, chat.ID)
	require.NoError(t, err)
	b := application.ReplyBinding{OperationID: turn.OperationID, AttemptID: at.ID, ExecutionEpoch: 1}

	t.Run("writer, context and outcome", func(t *testing.T) {
		_, _, err := conversations.ClaimReplyWriter(ctx, wf("claim"), application.ReplyBinding{OperationID: turn.OperationID, AttemptID: at.ID, ExecutionEpoch: 2}, "chat/"+turn.ID, 1, "inv_1")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a binding of another epoch")
		claimed, existing, err := conversations.ClaimReplyWriter(ctx, wf("claim"), b, "chat/"+turn.ID, 1, "inv_1")
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, uint64(1), claimed.WriterEpoch)
		require.Equal(t, domain.TurnRunning, claimed.State)
		_, _, err = conversations.ClaimReplyWriter(ctx, wf("claim2"), b, "chat/"+turn.ID, 1, "inv_2")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "the same attempt from another invocation")

		messages, digest, err := conversations.GetTurnContext(ctx, convAlice.TenantID, b, turn.ID, 1)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		require.Equal(t, m.ID, messages[0].ID)
		require.Equal(t, domain.ContextDigest(messages), digest)
		_, _, err = conversations.GetTurnContext(ctx, convAlice.TenantID, b, turn.ID, 2)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "another writer epoch reads nothing")

		text := "Black, with a search box on the right."
		rec := domain.ReplyRecord{Outcome: domain.ReplyCompleted, HasText: true, Text: text, Digest: domain.TextDigest(text), Sender: domain.SenderStopped}
		_, _, _, err = conversations.RecordReplyOutcome(ctx, wf("outcome"), b, turn.ID, 1, "op_other:reply:1", rec)
		require.ErrorIs(t, err, domain.ErrInvalid, "only the turn's reply call")
		sealed, reply, existing, err := conversations.RecordReplyOutcome(ctx, wf("outcome"), b, turn.ID, 1, domain.ReplyCallID(turn.OperationID), rec)
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.TurnCompleted, sealed.State)
		require.Equal(t, domain.ContentComplete, sealed.Content)
		require.NotNil(t, reply)
		require.Equal(t, domain.MessageRoleAssistant, reply.Role)
		require.Equal(t, m.Seq+1, reply.Seq)
		require.Equal(t, reply.ID, sealed.AssistantMessageID)
		_, replayed, existing, err := conversations.RecordReplyOutcome(ctx, wf("outcome"), b, turn.ID, 1, domain.ReplyCallID(turn.OperationID), rec)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, reply.ID, replayed.ID, "the sealing command replays")
		other := domain.ReplyRecord{Outcome: domain.ReplyFailed, Sender: domain.SenderStopped, FailureCode: "UPSTREAM_UNREACHABLE"}
		_, _, _, err = conversations.RecordReplyOutcome(ctx, wf("outcome2"), b, turn.ID, 1, domain.ReplyCallID(turn.OperationID), other)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "a sealed turn refuses another outcome")
		got, err := conversations.Get(ctx, convAlice, conv.ID)
		require.NoError(t, err)
		require.Equal(t, uint64(2), got.Revision, "a reply does not move the business revision")
		require.Equal(t, reply.Seq, got.LastMessageSeq)
		view, err := conversations.GetTurn(ctx, convAlice, conv.ID, turn.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TurnCompleted, view.Turn.State)
		_, err = conversations.GetTurn(ctx, convBob, conv.ID, turn.ID)
		require.ErrorIs(t, err, domain.ErrNotFound, "a turn is its owner's only")
	})

	t.Run("the freed slot and a stop before the start", func(t *testing.T) {
		_, next, _, err := conversations.Append(ctx, convCmd(convAlice, "turn_msg_2", "b"), convAlice, conv.ID, 2, "And a search box")
		require.NoError(t, err)
		require.NotNil(t, next, "the sealed turn freed the slot")
		view, _, err := conversations.StopTurn(ctx, convCmd(convAlice, "turn_stop_2", "s"), convAlice, conv.ID, next.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TurnCanceled, view.Turn.State, "an unstarted turn's operation is canceled at once")
		require.Equal(t, domain.SenderNotStarted, view.Turn.Sender)
		again, _, err := conversations.StopTurn(ctx, convCmd(convAlice, "turn_stop_2b", "s"), convAlice, conv.ID, next.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TurnCanceled, again.Turn.State, "a sealed turn answers itself unchanged")
		stopped, err := ops.Get(ctx, convAlice, next.OperationID)
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleCanceled, stopped.Lifecycle)
	})

	t.Run("closed replies open no turn", func(t *testing.T) {
		closed := application.NewConversations(store, domain.SystemClock{}, testLog).WithReplies(ops, chat, 0, false)
		got, err := closed.Get(ctx, convAlice, conv.ID)
		require.NoError(t, err)
		_, none, _, err := closed.Append(ctx, convCmd(convAlice, "turn_msg_3", "c"), convAlice, conv.ID, got.Revision, "No reply needed")
		require.NoError(t, err)
		require.Nil(t, none)
	})
}
