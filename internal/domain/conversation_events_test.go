package domain

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func batch(from, through uint64, text string) ReplyBatch {
	b := ReplyBatch{OriginalStreamID: "str_1", From: from, Through: through, SourceFramesDigest: TextDigest("frames")}
	if text != "" {
		b.HasText, b.Text, b.TextDigest = true, text, TextDigest(text)
	}
	return b
}

// TestReplyBatchRules (design 0001 §8.2, AT-11, AT-45, AT-46; DEC-070): the
// first batch starts at provider sequence 0 (the watermark is unset before
// it), each later one at the watermark plus one; a textless batch advances
// the watermark without a chunk; an exact replay is the first receipt; a
// differing replay, a gap and a changed stream are integrity refusals; the
// committed text stays within the reply bound.
func TestReplyBatchRules(t *testing.T) {
	_, _, turn, now := turnFixture(t)
	if turn.NextProviderSeq() != 0 {
		t.Fatalf("before any batch the next sequence is 0: %d", turn.NextProviderSeq())
	}
	if _, err := turn.ClassifyBatch(batch(1, 1, ""), nil); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("a first batch that does not start at 0 is a gap: %v", err)
	}
	admitted := batch(0, 0, "")
	if err := admitted.Check(); err != nil {
		t.Fatal(err)
	}
	if class, err := turn.ClassifyBatch(admitted, nil); err != nil || class != BatchAppend {
		t.Fatalf("the admitted frame appends: %v %v", class, err)
	}
	c0 := turn.ApplyBatch(admitted, now)
	if c0.ChunkSeq != 0 || turn.LastChunkSeq != 0 || *turn.LastProviderSeq != 0 || turn.LastBatchSeq != 1 || turn.OriginalStreamID != "str_1" {
		t.Fatalf("a textless batch advances the watermark without a chunk: %+v %+v", c0, turn)
	}
	text := "日本語の返事 😀 and ASCII."
	first := batch(1, 3, text)
	if class, err := turn.ClassifyBatch(first, nil); err != nil || class != BatchAppend {
		t.Fatalf("the next batch starts at the watermark plus one: %v %v", class, err)
	}
	c1 := turn.ApplyBatch(first, now)
	if c1.ChunkSeq != 1 || turn.LastChunkSeq != 1 || turn.CommittedBytes != uint64(len(text)) || *turn.LastProviderSeq != 3 {
		t.Fatalf("a text batch becomes chunk 1: %+v %+v", c1, turn)
	}
	if class, err := turn.ClassifyBatch(first, c1); err != nil || class != BatchReplay {
		t.Fatalf("the same range and digests replay: %v %v", class, err)
	}
	changed := batch(1, 3, "another text")
	if _, err := turn.ClassifyBatch(changed, c1); !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), ReasonStreamInconsistent) {
		t.Fatalf("a replayed range with other content is refused: %v", err)
	}
	if _, err := turn.ClassifyBatch(batch(1, 2, text), nil); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("a range below the watermark with no committed batch at its start is refused: %v", err)
	}
	if _, err := turn.ClassifyBatch(batch(5, 6, "x"), nil); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("a gap is refused: %v", err)
	}
	other := batch(4, 4, "x")
	other.OriginalStreamID = "str_2"
	if _, err := turn.ClassifyBatch(other, nil); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("a changed original stream is refused: %v", err)
	}
	turn.CommittedBytes = MaxReplyBytes - 1
	if _, err := turn.ClassifyBatch(batch(4, 4, "xy"), nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("text beyond the reply bound is refused: %v", err)
	}
	turn.ReconcileStream(now)
	if turn.State != TurnReconciling || turn.FailureCode != ReasonStreamInconsistent || !turn.Open() {
		t.Fatalf("an inconsistent stream keeps the slot reconciling: %+v", turn)
	}
}

func TestReplyBatchCheck(t *testing.T) {
	for _, c := range []struct {
		name string
		b    ReplyBatch
	}{
		{"no stream", ReplyBatch{From: 0, Through: 0, SourceFramesDigest: TextDigest("f")}},
		{"through before from", batch(2, 1, "")},
		{"beyond BIGINT", batch(0, math.MaxInt64, "")},
		{"malformed frames digest", ReplyBatch{OriginalStreamID: "s", SourceFramesDigest: "sha256:x"}},
		{"text with NUL", batch(0, 0, "a\x00b")},
		{"text digest of other text", func() ReplyBatch { b := batch(0, 0, "a"); b.TextDigest = TextDigest("b"); return b }()},
		{"textless with a digest", func() ReplyBatch { b := batch(0, 0, ""); b.TextDigest = TextDigest("a"); return b }()},
		{"invalid UTF-8", batch(0, 0, "\xff\xfe")},
	} {
		if err := c.b.Check(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	if err := batch(0, 7, strings.Repeat("é", MaxReplyBytes/2)).Check(); err != nil {
		t.Fatalf("whole characters up to the bound are accepted: %v", err)
	}
}

// TestReservedReplyAndStopSeal (DEC-070 Q3 A): the first text reserves the
// reply's message id and the next sequence once; the seal stores the reply
// under them with its chunk revision; a stop of a claimed, running reply
// seals it canceled at once and marks the outcome as the stop's.
func TestReservedReplyAndStopSeal(t *testing.T) {
	c, m, turn, now := turnFixture(t)
	if turn.CanSealAtStop() {
		t.Fatal("an unclaimed turn waits for its operation's cancel")
	}
	if _, err := turn.ClaimWriter("chat/a", 1, "inv_1", now); err != nil {
		t.Fatal(err)
	}
	if !turn.CanSealAtStop() {
		t.Fatal("a claimed, running reply seals at its stop")
	}
	turn.ApplyBatch(batch(0, 2, "Hello"), now)
	fresh, err := c.ReserveReply(turn, now)
	if err != nil || !fresh || turn.ReservedMessageSeq != m.Seq+1 || c.LastMessageSeq != m.Seq+1 || turn.PublicMessageID() != turn.ReservedMessageID {
		t.Fatalf("the first text reserves the next sequence: %v %v %+v", fresh, err, turn)
	}
	if again, _ := c.ReserveReply(turn, now); again || c.LastMessageSeq != m.Seq+1 {
		t.Fatal("a reservation is made once")
	}
	streaming := StreamingMessage(turn, "Hello")
	if streaming.State != MessageStreaming || streaming.ContentRevision != 1 || streaming.ThroughChunkSeq != 1 || streaming.ID != turn.ReservedMessageID {
		t.Fatalf("a streaming message shows its committed chunks: %+v", streaming)
	}
	revision := c.Revision
	sealed, err := c.SealReply(turn, MessageCanceled, "Hello", now)
	if err != nil || sealed.ID != turn.ReservedMessageID || sealed.Seq != turn.ReservedMessageSeq || sealed.ContentRevision != 1 || sealed.ThroughChunkSeq != 1 || c.Revision != revision {
		t.Fatalf("the seal stores the reserved message without moving the revision: %+v %v", sealed, err)
	}
	cmd := CommandIdentity{TenantID: "tenant_a", CommandID: "cmd_stop", ActorID: "user_a", RequestDigest: TextDigest("stop")}
	turn.SealStop(sealed.ID, cmd, now)
	if turn.State != TurnCanceled || turn.Content != ContentPartial || turn.FailureCode != FailureCanceled || turn.Sender != SenderActive ||
		turn.Open() || !turn.SealedByStop() || turn.AssistantMessageID != sealed.ID {
		t.Fatalf("the stop seals canceled with the sender metering: %+v", turn)
	}
	whole, _, plain, _ := turnFixture(t)
	reply, err := whole.SealReply(plain, MessageCompleted, "Whole", now)
	if err != nil || reply.ContentRevision != 1 || reply.ThroughChunkSeq != 0 || reply.Seq != 2 {
		t.Fatalf("a reply recorded whole takes the next sequence (D3): %+v %v", reply, err)
	}
}

// TestConversationEventLog (design 0001 §8.1): events take the conversation's
// sequence from 1; the covered sequence is the last one; a sealed message's
// event type follows its state; a page must continue right after its cursor.
func TestConversationEventLog(t *testing.T) {
	c, m, turn, now := turnFixture(t)
	if c.CoveredEventSeq() != 0 {
		t.Fatalf("no event yet: %d", c.CoveredEventSeq())
	}
	accepted, err := c.MessageAcceptedEvent(m, turn.ID, now)
	if err != nil || accepted.Seq != 1 || accepted.Payload.MessageSeq != m.Seq || accepted.Payload.Role != MessageRoleUser {
		t.Fatalf("the first event is 1: %+v %v", accepted, err)
	}
	changed, _ := c.TurnChangedEvent(turn, FinanceFunded, now)
	if changed.Seq != 2 || changed.Payload.Turn == nil || changed.Payload.Turn.Finance != FinanceFunded || c.CoveredEventSeq() != 2 {
		t.Fatalf("a turn change carries the turn and its finance: %+v", changed)
	}
	turn.ApplyBatch(batch(0, 1, "Hi"), now)
	if _, err := c.ReserveReply(turn, now); err != nil {
		t.Fatal(err)
	}
	chunk := &ReplyChunk{ChunkSeq: 1}
	delta, _ := c.MessageDeltaEvent(turn, chunk, now)
	if delta.Type != EventMessageDelta || delta.ChunkSeq != 1 || delta.MessageID != turn.ReservedMessageID || delta.Text != "" {
		t.Fatalf("a delta names its chunk and holds no text: %+v", delta)
	}
	for state, want := range map[MessageState]ConversationEventType{MessageCompleted: EventMessageCompleted, MessagePartialFailed: EventMessageFailed, MessageCanceled: EventMessageCanceled} {
		ev, _ := c.MessageSealedEvent(turn, &Message{ID: "msg_r", State: state, ContentRevision: 1, ThroughChunkSeq: 1}, now)
		if ev.Type != want || ev.Payload.State != state {
			t.Errorf("a %s seal is %s, got %s", state, want, ev.Type)
		}
	}
	events := []*ConversationEvent{{Seq: 3}, {Seq: 4}}
	if !ConversationEventsContiguous(2, events) || ConversationEventsContiguous(1, events) || ConversationEventsContiguous(2, []*ConversationEvent{{Seq: 3}, {Seq: 5}}) {
		t.Fatal("a page continues right after its cursor without a hole")
	}
	env, err := ConversationChangedEnvelope(c.ID, c.TenantID, 7, now)
	if err != nil || env.EventType != "conversation.changed" || env.Subject != SubjectConversationChanged || env.AggregateType != AggregateConversation ||
		env.AggregateID != c.ID || env.AggregateRevision != "7" || !strings.Contains(string(env.Payload), `"throughEventSeq":"7"`) {
		t.Fatalf("the wake-up names the conversation and its covered event: %+v %v", env, err)
	}
}
