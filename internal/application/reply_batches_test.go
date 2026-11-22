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

func replyBatch(from, through uint64, text string) domain.ReplyBatch {
	b := domain.ReplyBatch{OriginalStreamID: "str_test_1", From: from, Through: through, SourceFramesDigest: domain.TextDigest("frames")}
	if text != "" {
		b.HasText, b.Text, b.TextDigest = true, text, domain.TextDigest(text)
	}
	return b
}

// TestReplyBatchesAndStopSeal (design 0001 D4a, DEC-070; B-016's amended
// criterion) proves on a real PostgreSQL: a reply's batches commit from
// provider sequence 0 and an identical batch replays; its first text
// reserves the assistant message, readable while it streams; the
// conversation's events record the accepted message, the claim, the start,
// each chunk and the seal in one sequence; a stop of the running reply
// seals it canceled at once with exactly the committed chunks and frees the
// slot; the next batch answers sealed and stores nothing; a later reply
// record is answered with the sealed turn; a gap stops the appends and keeps
// the slot reconciling; a cursor ahead of the events needs a reset.
func TestReplyBatchesAndStopSeal(t *testing.T) {
	inst := testdb.Start(t)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	store := postgres.NewStore(inst.Pool(t))
	chat := domain.Profile{ID: domain.ChatTurnProfileID, Kind: domain.KindChatTurn, OperationDeadline: 150 * time.Second,
		StepID: domain.ChatTurnStepID, MultiStep: true, AttemptWindow: 120 * time.Second}
	ops := application.NewOperations(store, inv, []domain.Profile{chat}, domain.SystemClock{}, testLog)
	exec := application.NewExecution(store, inv, manifests, []domain.Profile{chat}, domain.SystemClock{}, testLog)
	conversations := application.NewConversations(store, domain.SystemClock{}, testLog).WithReplies(ops, chat, 0, true)
	ctx := context.Background()

	// start opens a turn on a new message and claims its writer.
	start := func(conv *domain.Conversation, mid, text string) (*domain.Turn, application.ReplyBinding, func(string) domain.CommandIdentity) {
		got, err := conversations.Get(ctx, convAlice, conv.ID)
		require.NoError(t, err)
		_, turn, _, err := conversations.Append(ctx, convCmd(convAlice, mid, mid), convAlice, conv.ID, got.Revision, text)
		require.NoError(t, err)
		require.NotNil(t, turn)
		wf := func(id string) domain.CommandIdentity {
			return domain.CommandIdentity{TenantID: convAlice.TenantID, CommandID: turn.OperationID + ":" + id, ActorID: "anvilkit-agent-workflow", RequestDigest: application.DigestOf([]byte(id))}
		}
		at, _, err := exec.OpenAttempt(ctx, wf("open"), turn.OperationID, domain.ChatTurnStepID, 0, chat.ID)
		require.NoError(t, err)
		b := application.ReplyBinding{OperationID: turn.OperationID, AttemptID: at.ID, ExecutionEpoch: 1}
		claimed, _, err := conversations.ClaimReplyWriter(ctx, wf("claim"), b, "chat/"+turn.ID, 1, "inv_1")
		require.NoError(t, err)
		require.Equal(t, uint64(1), claimed.WriterEpoch)
		return claimed, b, wf
	}

	conv, _, err := conversations.Create(ctx, convCmd(convAlice, "d4a_conv", "d4a"), convAlice, "")
	require.NoError(t, err)
	turn, b, wf := start(conv, "d4a_msg_1", "Tell me about the checkout page.")
	call := domain.ReplyCallID(turn.OperationID)

	t.Run("a first batch starts at 0", func(t *testing.T) {
		_, err := conversations.AppendReplyBatch(ctx, wf("batch:1"), b, turn.ID, 1, call, replyBatch(1, 1, "late"))
		require.ErrorIs(t, err, domain.ErrIntegrity, "a first batch that does not start at 0 is a gap")
		snap, err := conversations.Snapshot(ctx, convAlice, conv.ID)
		require.NoError(t, err)
		require.NotNil(t, snap.ActiveTurn)
		require.Equal(t, domain.TurnReconciling, snap.ActiveTurn.Turn.State, "the gap stops the appends and keeps the slot")
		_, err = conversations.AppendReplyBatch(ctx, wf("batch:0"), b, turn.ID, 1, call, replyBatch(0, 0, ""))
		require.ErrorIs(t, err, domain.ErrIntegrity, "a reconciling turn takes no further batch")
	})

	conv2, _, err := conversations.Create(ctx, convCmd(convAlice, "d4a_conv_2", "d4a2"), convAlice, "")
	require.NoError(t, err)
	turn, b, wf = start(conv2, "d4a_msg_2", "Tell me about the checkout page again.")
	call = domain.ReplyCallID(turn.OperationID)

	t.Run("batches commit and replay; a differing replay is refused", func(t *testing.T) {
		rec, err := conversations.AppendReplyBatch(ctx, wf("batch:0"), b, turn.ID, 1, call, replyBatch(0, 0, ""))
		require.NoError(t, err)
		require.NotNil(t, rec.Turn.LastProviderSeq, "the textless admitted frame sets the watermark")
		require.Equal(t, uint64(0), rec.Chunk.ChunkSeq, "a textless batch gets no chunk")
		rec, err = conversations.AppendReplyBatch(ctx, wf("batch:1"), b, turn.ID, 1, call, replyBatch(1, 2, "Hello, "))
		require.NoError(t, err)
		require.Equal(t, uint64(1), rec.Chunk.ChunkSeq)
		reserved := rec.Turn.ReservedMessageID
		require.NotEmpty(t, reserved, "the first text reserves the assistant message")
		again, err := conversations.AppendReplyBatch(ctx, wf("batch:1"), b, turn.ID, 1, call, replyBatch(1, 2, "Hello, "))
		require.NoError(t, err)
		require.True(t, again.Existing, "the same range and digests answer the first receipt")
		_, err = conversations.AppendReplyBatch(ctx, wf("batch:1b"), b, turn.ID, 1, call, replyBatch(1, 2, "Howdy, "))
		require.ErrorIs(t, err, domain.ErrIntegrity, "the same range with other text is refused")
	})

	// The refused replay above stopped this turn's appends; stream a third.
	conv3, _, err := conversations.Create(ctx, convCmd(convAlice, "d4a_conv_3", "d4a3"), convAlice, "")
	require.NoError(t, err)
	turn, b, wf = start(conv3, "d4a_msg_3", "And the payment step?")
	call = domain.ReplyCallID(turn.OperationID)

	t.Run("a stop seals the committed chunks at once", func(t *testing.T) {
		for i, part := range []struct {
			from, through uint64
			text          string
		}{{0, 0, ""}, {1, 2, "Hello, "}, {3, 4, "world"}} {
			_, err := conversations.AppendReplyBatch(ctx, wf("batch:"+string(rune('a'+i))), b, turn.ID, 1, call, replyBatch(part.from, part.through, part.text))
			require.NoError(t, err)
		}
		snap, err := conversations.Snapshot(ctx, convAlice, conv3.ID)
		require.NoError(t, err)
		require.NotNil(t, snap.ActiveTurn)
		require.Equal(t, turn.ID, snap.ActiveTurn.Turn.ID)
		streamingID := snap.ActiveTurn.Turn.PublicMessageID()
		streaming, err := conversations.GetMessage(ctx, convAlice, conv3.ID, streamingID)
		require.NoError(t, err)
		require.Equal(t, domain.MessageStreaming, streaming.State)
		require.Equal(t, "Hello, world", streaming.Text)
		require.Equal(t, uint64(2), streaming.ContentRevision)

		view, _, err := conversations.StopTurn(ctx, convCmd(convAlice, "d4a_stop_3", "stop"), convAlice, conv3.ID, turn.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TurnCanceled, view.Turn.State, "the stop seals the running reply at once (DEC-070 Q3 A)")
		require.Equal(t, domain.FailureCanceled, view.Turn.FailureCode)
		require.False(t, view.Turn.Open(), "the slot is free")
		sealed, err := conversations.GetMessage(ctx, convAlice, conv3.ID, streamingID)
		require.NoError(t, err)
		require.Equal(t, domain.MessageCanceled, sealed.State)
		require.Equal(t, "Hello, world", sealed.Text, "exactly the committed chunks")
		require.Equal(t, domain.TextDigest("Hello, world"), sealed.ContentDigest)

		late, err := conversations.AppendReplyBatch(ctx, wf("batch:late"), b, turn.ID, 1, call, replyBatch(5, 6, "!"))
		require.NoError(t, err)
		require.True(t, late.Sealed, "the next batch learns of the seal and stores nothing")
		after, err := conversations.GetMessage(ctx, convAlice, conv3.ID, streamingID)
		require.NoError(t, err)
		require.Equal(t, "Hello, world", after.Text)

		rec := domain.ReplyRecord{Outcome: domain.ReplyCompleted, Digest: domain.TextDigest("Hello, world!"), Sender: domain.SenderStopped}
		recorded, _, existing, err := conversations.RecordReplyOutcome(ctx, wf("outcome"), b, turn.ID, 1, call, rec, true)
		require.NoError(t, err, "a record after the stop's seal is answered, never a conflict")
		require.True(t, existing)
		require.Equal(t, domain.TurnCanceled, recorded.State)

		page, err := conversations.ListEvents(ctx, convAlice, conv3.ID, 0, 100)
		require.NoError(t, err)
		require.False(t, page.ResetRequired)
		types := make([]domain.ConversationEventType, 0, len(page.Events))
		for _, ev := range page.Events {
			types = append(types, ev.Type)
		}
		require.Equal(t, []domain.ConversationEventType{
			domain.EventMessageAccepted, domain.EventTurnChanged, domain.EventTurnChanged, domain.EventMessageStarted,
			domain.EventMessageDelta, domain.EventMessageDelta, domain.EventMessageCanceled, domain.EventTurnChanged,
		}, types)
		require.Equal(t, "Hello, ", page.Events[4].Text, "a delta is served with its chunk's text")
		require.Equal(t, "world", page.Events[5].Text)
		require.Equal(t, page.Events[len(page.Events)-1].Seq, page.CoveredEventSeq)
		ahead, err := conversations.ListEvents(ctx, convAlice, conv3.ID, page.CoveredEventSeq+5, 100)
		require.NoError(t, err)
		require.True(t, ahead.ResetRequired, "a cursor ahead of the events needs a reset")
		_, err = conversations.ListEvents(ctx, convBob, conv3.ID, 0, 100)
		require.ErrorIs(t, err, domain.ErrNotFound, "another user's events are not found")

		got, err := conversations.Get(ctx, convAlice, conv3.ID)
		require.NoError(t, err)
		_, next, _, err := conversations.Append(ctx, convCmd(convAlice, "d4a_msg_4", "next"), convAlice, conv3.ID, got.Revision, "Thanks.")
		require.NoError(t, err, "the slot freed at the seal while the stopped send meters")
		require.NotNil(t, next)
	})

	// The writer that lost track of its batches seals them as they stand: a
	// record without a digest; a wrong digest and a whole-text record on a
	// turn with batches are refused.
	conv4, _, err := conversations.Create(ctx, convCmd(convAlice, "d4a_conv_4", "d4a4"), convAlice, "")
	require.NoError(t, err)
	turn, b, wf = start(conv4, "d4a_msg_5", "Summarise it.")
	call = domain.ReplyCallID(turn.OperationID)

	t.Run("a record from the batches seals them as they stand", func(t *testing.T) {
		for i, part := range []struct {
			from, through uint64
			text          string
		}{{0, 0, ""}, {1, 2, "Partial "}} {
			_, err := conversations.AppendReplyBatch(ctx, wf("batch:"+string(rune('a'+i))), b, turn.ID, 1, call, replyBatch(part.from, part.through, part.text))
			require.NoError(t, err)
		}
		lost := domain.ReplyRecord{Outcome: domain.ReplyPartialFailed, Sender: domain.SenderStopped, FailureCode: "STREAM_INCONSISTENT"}
		wrong := lost
		wrong.Digest = domain.TextDigest("Other text")
		_, _, _, err := conversations.RecordReplyOutcome(ctx, wf("outcome:wrong"), b, turn.ID, 1, call, wrong, true)
		require.ErrorIs(t, err, domain.ErrInvalid, "a digest must be the committed text's")
		whole := domain.ReplyRecord{Outcome: domain.ReplyCompleted, HasText: true, Text: "Partial reply", Digest: domain.TextDigest("Partial reply"), Sender: domain.SenderStopped}
		_, _, _, err = conversations.RecordReplyOutcome(ctx, wf("outcome:whole"), b, turn.ID, 1, call, whole, false)
		require.ErrorIs(t, err, domain.ErrInvalid, "a turn with batches is sealed from them")
		sealed, reply, existing, err := conversations.RecordReplyOutcome(ctx, wf("outcome"), b, turn.ID, 1, call, lost, true)
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.TurnFailed, sealed.State)
		require.NotNil(t, reply)
		require.Equal(t, "Partial ", reply.Text)
		require.Equal(t, domain.MessagePartialFailed, reply.State)
		require.Equal(t, uint64(1), reply.ThroughChunkSeq)
		require.Equal(t, sealed.AssistantMessageID, reply.ID)
		page, err := conversations.ListEvents(ctx, convAlice, conv4.ID, 0, 100)
		require.NoError(t, err)
		last := page.Events[len(page.Events)-2:]
		require.Equal(t, []domain.ConversationEventType{domain.EventMessageFailed, domain.EventTurnChanged}, []domain.ConversationEventType{last[0].Type, last[1].Type})
	})
}
