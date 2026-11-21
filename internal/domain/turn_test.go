package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func turnFixture(t *testing.T) (*Conversation, *Message, *Turn, time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 19, 23, 0, 0, 0, time.UTC)
	c, err := NewConversation(conversationOwner, "", now)
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.AppendUserMessage(1, "Make it black", now)
	if err != nil {
		t.Fatal(err)
	}
	turn := NewTurn(m, now)
	turn.OperationID = "op_chat_1"
	return c, m, turn, now
}

func TestTurnOpenAndSlot(t *testing.T) {
	_, m, turn, _ := turnFixture(t)
	if !strings.HasPrefix(turn.ID, "trn_") || turn.InputMessageID != m.ID || turn.InputMessageSeq != m.Seq {
		t.Fatalf("new turn: %+v", turn)
	}
	if turn.State != TurnQueued || turn.Content != ContentUnknown || turn.Sender != SenderNotStarted || !turn.Open() {
		t.Fatalf("a new turn is queued and holds the slot: %+v", turn)
	}
	for _, s := range []TurnState{TurnQueued, TurnRunning, TurnCancelPending, TurnReconciling} {
		if !s.Open() {
			t.Fatalf("%s holds the slot", s)
		}
	}
	for _, s := range []TurnState{TurnCompleted, TurnFailed, TurnCanceled} {
		if s.Open() {
			t.Fatalf("%s frees the slot", s)
		}
	}
	if turn.OperationCommandID() != "turn:"+turn.ID || ReplyCallID("op_chat_1") != "op_chat_1:reply:1" {
		t.Fatalf("identities: %s %s", turn.OperationCommandID(), ReplyCallID("op_chat_1"))
	}
}

func TestTurnClaimWriter(t *testing.T) {
	_, _, turn, now := turnFixture(t)
	if _, err := turn.ClaimWriter("", 1, "inv_1", now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a claim without an activity key: %v", err)
	}
	existing, err := turn.ClaimWriter("chat/trn/run/act", 1, "inv_1", now)
	if err != nil || existing || turn.WriterEpoch != 1 || turn.State != TurnRunning || turn.Sender != SenderActive {
		t.Fatalf("first claim: existing=%v err=%v %+v", existing, err, turn)
	}
	if existing, err := turn.ClaimWriter("chat/trn/run/act", 1, "inv_1", now); err != nil || !existing || turn.WriterEpoch != 1 {
		t.Fatalf("same invocation re-entering: existing=%v err=%v epoch=%d", existing, err, turn.WriterEpoch)
	}
	if _, err := turn.ClaimWriter("chat/trn/run/act", 1, "inv_2", now); !errors.Is(err, ErrStaleExecution) {
		t.Fatalf("same attempt, other invocation: %v", err)
	}
	if _, err := turn.ClaimWriter("chat/trn/run2/act", 2, "inv_2", now); !errors.Is(err, ErrStaleExecution) {
		t.Fatalf("another activity cannot take over by a higher attempt: %v", err)
	}
	if existing, err := turn.ClaimWriter("chat/trn/run/act", 2, "inv_2", now); err != nil || existing || turn.WriterEpoch != 2 || turn.TemporalAttempt != 2 {
		t.Fatalf("a higher attempt takes over: existing=%v err=%v %+v", existing, err, turn)
	}
	if _, err := turn.ClaimWriter("chat/trn/run/act", 1, "inv_1", now); !errors.Is(err, ErrStaleExecution) {
		t.Fatalf("an older attempt is stale after the takeover: %v", err)
	}
	if err := turn.CheckWriter(1); !errors.Is(err, ErrStaleExecution) {
		t.Fatalf("the old epoch cannot write: %v", err)
	}
	if err := turn.CheckWriter(2); err != nil {
		t.Fatalf("the current epoch writes: %v", err)
	}
	turn.State = TurnCompleted
	if existing, err := turn.ClaimWriter("chat/trn/run/act", 3, "inv_3", now); err != nil || !existing || turn.WriterEpoch != 2 {
		t.Fatalf("a sealed turn is answered as it is, without a takeover: existing=%v err=%v epoch=%d", existing, err, turn.WriterEpoch)
	}
}

func TestTurnClaimKeepsCancelPending(t *testing.T) {
	_, _, turn, now := turnFixture(t)
	turn.RequestStop(false, now)
	if turn.State != TurnCancelPending {
		t.Fatalf("a stop of a queued turn with a live operation waits: %s", turn.State)
	}
	if _, err := turn.ClaimWriter("chat/trn/run/act", 1, "inv_1", now); err != nil || turn.State != TurnCancelPending {
		t.Fatalf("a claim after the stop keeps cancel_pending: %v %s", err, turn.State)
	}
}

func TestTurnRequestStop(t *testing.T) {
	_, _, turn, now := turnFixture(t)
	if !turn.RequestStop(true, now) || turn.State != TurnCanceled || turn.Sender != SenderNotStarted || turn.FailureCode != FailureCanceled || turn.Open() {
		t.Fatalf("an operation canceled at once ends the unstarted turn: %+v", turn)
	}
	if turn.RequestStop(true, now) {
		t.Fatal("a sealed turn is unchanged by a stop")
	}
	_, _, running, _ := turnFixture(t)
	if _, err := running.ClaimWriter("a", 1, "i", now); err != nil {
		t.Fatal(err)
	}
	running.RequestStop(true, now)
	if running.Sender != SenderStopped {
		t.Fatalf("a canceled operation's claimed writer is stopped: %s", running.Sender)
	}
}

func TestTurnEndWithOperation(t *testing.T) {
	_, _, turn, now := turnFixture(t)
	op := &Operation{Lifecycle: LifecycleRunning}
	if turn.EndWithOperation(op, now) {
		t.Fatal("a running operation does not end its turn")
	}
	op.Lifecycle, op.FailureCode = LifecycleFailed, "DEADLINE_EXCEEDED"
	if !turn.EndWithOperation(op, now) || turn.State != TurnFailed || turn.FailureCode != "DEADLINE_EXCEEDED" || turn.Content != ContentPartial || turn.Open() {
		t.Fatalf("a failed operation fails its open turn: %+v", turn)
	}
	_, _, succeeded, _ := turnFixture(t)
	if !succeeded.EndWithOperation(&Operation{Lifecycle: LifecycleSucceeded}, now) || succeeded.State != TurnFailed || succeeded.FailureCode != FailureReplyNotRecorded {
		t.Fatalf("an operation that ended without a recorded reply: %+v", succeeded)
	}
	_, _, canceled, _ := turnFixture(t)
	if !canceled.EndWithOperation(&Operation{Lifecycle: LifecycleCanceled}, now) || canceled.State != TurnCanceled {
		t.Fatalf("a canceled operation cancels its turn: %+v", canceled)
	}
}

func TestReplyRecordCheck(t *testing.T) {
	text := "Yes, the search box fits."
	ok := ReplyRecord{Outcome: ReplyCompleted, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderStopped}
	if err := ok.Check(); err != nil {
		t.Fatalf("a completed reply: %v", err)
	}
	bad := map[string]ReplyRecord{
		"unknown outcome":              {Outcome: "done", HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderStopped},
		"unknown sender":               {Outcome: ReplyCompleted, HasText: true, Text: text, Digest: TextDigest(text), Sender: "gone"},
		"completed without text":       {Outcome: ReplyCompleted, Sender: SenderStopped},
		"completed with sender active": {Outcome: ReplyCompleted, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderActive},
		"completed with a code":        {Outcome: ReplyCompleted, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderStopped, FailureCode: "X"},
		"wrong digest":                 {Outcome: ReplyCompleted, HasText: true, Text: text, Digest: TextDigest("other"), Sender: SenderStopped},
		"digest without text":          {Outcome: ReplyFailed, Digest: TextDigest(text), Sender: SenderStopped, FailureCode: "UPSTREAM_UNREACHABLE"},
		"text above the bound":         {Outcome: ReplyPartialFailed, HasText: true, Text: strings.Repeat("a", MaxReplyBytes+1), Digest: TextDigest(strings.Repeat("a", MaxReplyBytes+1)), Sender: SenderStopped, FailureCode: "UPSTREAM_FINISH_LENGTH"},
		"invalid UTF-8":                {Outcome: ReplyCanceled, HasText: true, Text: "\xff", Digest: TextDigest("\xff"), Sender: SenderStopped, FailureCode: FailureCanceled},
		"NUL in the text":              {Outcome: ReplyCanceled, HasText: true, Text: "a\x00b", Digest: TextDigest("a\x00b"), Sender: SenderStopped, FailureCode: FailureCanceled},
		"failure without a code":       {Outcome: ReplyFailed, Sender: SenderStopped},
		"code above 64 characters":     {Outcome: ReplyFailed, Sender: SenderStopped, FailureCode: strings.Repeat("F", 65)},
		"partial failure without text": {Outcome: ReplyPartialFailed, Sender: SenderStopped, FailureCode: "UPSTREAM_FINISH_LENGTH"},
		"failure with text":            {Outcome: ReplyFailed, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderStopped, FailureCode: "X"},
		"unstarted sender with text":   {Outcome: ReplyCanceled, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderNotStarted, FailureCode: FailureCanceled},
	}
	for name, rec := range bad {
		if err := rec.Check(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := (ReplyRecord{Outcome: ReplyPartialFailed, HasText: true, Text: strings.Repeat("a", MaxReplyBytes), Digest: TextDigest(strings.Repeat("a", MaxReplyBytes)), Sender: SenderStopped, FailureCode: "UPSTREAM_FINISH_LENGTH"}).Check(); err != nil {
		t.Fatalf("a reply of exactly the bound: %v", err)
	}
}

func TestReplyRecordSeals(t *testing.T) {
	cases := []struct {
		rec  ReplyRecord
		want bool
	}{
		{ReplyRecord{Outcome: ReplyCompleted, Sender: SenderStopped}, true},
		{ReplyRecord{Outcome: ReplyFailed, Sender: SenderNotStarted}, true},
		{ReplyRecord{Outcome: ReplyPartialFailed, Sender: SenderStopped}, true},
		{ReplyRecord{Outcome: ReplyCanceled, Sender: SenderActive}, true}, // DEC-068: sealed while the send meters
		{ReplyRecord{Outcome: ReplyPartialFailed, Sender: SenderUnknown}, false},
		{ReplyRecord{Outcome: ReplyFailed, Sender: SenderActive}, false},
		{ReplyRecord{Outcome: ReplyCanceled, Sender: SenderUnknown}, false},
	}
	for _, c := range cases {
		if got := c.rec.Seals(); got != c.want {
			t.Errorf("%s with sender %s seals=%v, want %v", c.rec.Outcome, c.rec.Sender, got, c.want)
		}
	}
}

func TestTurnRecord(t *testing.T) {
	_, _, turn, now := turnFixture(t)
	cmd := CommandIdentity{TenantID: "tenant_a", CommandID: "trn:outcome", ActorID: "anvilkit-agent-workflow", RequestDigest: Digest("sha256:" + strings.Repeat("a", 64))}
	turn.Record(ReplyRecord{Outcome: ReplyPartialFailed, Sender: SenderUnknown, FailureCode: "SENDER_LOST"}, "", cmd, now)
	if turn.State != TurnReconciling || turn.Content != ContentUnknown || !turn.Open() || turn.OutcomeCommandID != "" {
		t.Fatalf("an unknown sender keeps the turn reconciling and unsealed: %+v", turn)
	}
	turn.Record(ReplyRecord{Outcome: ReplyPartialFailed, HasText: true, Sender: SenderStopped, FailureCode: "CONTENT_UNRECOVERABLE"}, "msg_2", cmd, now)
	if turn.State != TurnFailed || turn.Content != ContentPartial || turn.Open() || turn.AssistantMessageID != "msg_2" || turn.OutcomeCommandID != cmd.CommandID {
		t.Fatalf("a stopped sender seals the partial reply: %+v", turn)
	}
	if err := turn.Replays(cmd); err != nil {
		t.Fatalf("the sealing command replays: %v", err)
	}
	other := cmd
	other.RequestDigest = Digest("sha256:" + strings.Repeat("b", 64))
	if err := turn.Replays(other); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("another record for a sealed turn conflicts: %v", err)
	}
	if ReplyCompleted != "completed" || (ReplyRecord{Outcome: ReplyCompleted}).MessageState() != MessageCompleted ||
		(ReplyRecord{Outcome: ReplyCanceled}).MessageState() != MessageCanceled || (ReplyRecord{Outcome: ReplyPartialFailed}).MessageState() != MessagePartialFailed {
		t.Fatal("message states of the outcomes")
	}
}

func TestAppendAssistantMessageKeepsTheRevision(t *testing.T) {
	c, m, _, now := turnFixture(t)
	revision := c.Revision
	reply, err := c.AppendAssistantMessage(MessageCompleted, "Done.", now)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Seq != m.Seq+1 || reply.Role != MessageRoleAssistant || c.Revision != revision || reply.ConversationRevision != revision || c.LastMessageSeq != reply.Seq {
		t.Fatalf("an assistant message takes the next sequence and keeps the revision: %+v, conversation %+v", reply, c)
	}
}

func TestSelectTurnContext(t *testing.T) {
	msg := func(seq uint64, text string) *Message {
		return &Message{ID: "msg_" + string(rune('a'+seq)), Seq: seq, Text: text, ContentDigest: TextDigest(text)}
	}
	newestFirst := []*Message{msg(5, "later"), msg(4, "input"), msg(3, "ccc"), msg(2, "bbbbbbbbbb"), msg(1, "a")}
	got := SelectTurnContext(newestFirst, 4, 9, nil)
	if len(got) != 2 || got[0].Seq != 3 || got[1].Seq != 4 {
		t.Fatalf("newest messages within the budget, ascending, nothing after the input: %+v", got)
	}
	if got := SelectTurnContext(newestFirst, 4, 1, nil); len(got) != 1 || got[0].Seq != 4 {
		t.Fatalf("the input message is always kept: %+v", got)
	}
	all := SelectTurnContext(newestFirst, 4, 1000, nil)
	if len(all) != 4 || all[0].Seq != 1 {
		t.Fatalf("a large budget keeps the whole history up to the input: %+v", all)
	}
	if ContextDigest(all) != ContextDigest(SelectTurnContext(newestFirst, 4, 1000, nil)) || ContextDigest(all) == ContextDigest(got) {
		t.Fatal("the context digest is stable for the same context and differs for another")
	}
}

// TestReplyRecordUnderCancel (B-016, design 0001 §7.4 and §11.3): once the
// turn's operation carries a cancel intent, every sealing record seals the
// turn canceled with its text kept as the canceled message, so a stopped turn
// agrees with its operation; without the intent, and for a record that does
// not seal, the record is unchanged.
func TestReplyRecordUnderCancel(t *testing.T) {
	text := "Black, with a search box on the right."
	completed := ReplyRecord{Outcome: ReplyCompleted, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderStopped}
	if got := completed.UnderCancel(&Operation{Control: ControlNone}); got != completed {
		t.Fatalf("without a cancel intent the record is unchanged: %+v", got)
	}
	stopping := &Operation{Control: ControlCancelPending}
	cases := []struct {
		name string
		rec  ReplyRecord
		want ReplyRecord
	}{
		{"a completed reply after the stop", completed,
			ReplyRecord{Outcome: ReplyCanceled, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderStopped, FailureCode: FailureCanceled}},
		{"a partial failure after the stop", ReplyRecord{Outcome: ReplyPartialFailed, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderStopped, FailureCode: "UPSTREAM_FINISH_LENGTH"},
			ReplyRecord{Outcome: ReplyCanceled, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderStopped, FailureCode: FailureCanceled}},
		{"a refusal before the send", ReplyRecord{Outcome: ReplyFailed, Sender: SenderNotStarted, FailureCode: "BUDGET_EXHAUSTED"},
			ReplyRecord{Outcome: ReplyCanceled, Sender: SenderNotStarted, FailureCode: FailureCanceled}},
		{"the Activity's own stop", ReplyRecord{Outcome: ReplyCanceled, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderActive, FailureCode: FailureCanceled},
			ReplyRecord{Outcome: ReplyCanceled, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderActive, FailureCode: FailureCanceled}},
		{"an unknown sender does not seal", ReplyRecord{Outcome: ReplyPartialFailed, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderUnknown, FailureCode: "SENDER_LOST"},
			ReplyRecord{Outcome: ReplyPartialFailed, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderUnknown, FailureCode: "SENDER_LOST"}},
	}
	for _, c := range cases {
		got := c.rec.UnderCancel(stopping)
		if got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
		if err := got.Check(); err != nil {
			t.Errorf("%s: the record stays valid: %v", c.name, err)
		}
		if got.Seals() != c.rec.Seals() {
			t.Errorf("%s: the stop never changes whether the record seals", c.name)
		}
	}
	if got := completed.UnderCancel(&Operation{Control: ControlCancelApplied}); got.Outcome != ReplyCanceled {
		t.Fatalf("an applied cancel is a cancel intent too: %+v", got)
	}

	_, _, turn, now := turnFixture(t)
	if _, err := turn.ClaimWriter("chat/trn/run/act", 1, "inv_1", now); err != nil {
		t.Fatal(err)
	}
	turn.RequestStop(false, now)
	rec := completed.UnderCancel(stopping)
	cmd := CommandIdentity{TenantID: "tenant_a", CommandID: "trn:outcome", ActorID: "anvilkit-agent-workflow", RequestDigest: Digest("sha256:" + strings.Repeat("a", 64))}
	turn.Record(rec, "msg_2", cmd, now)
	if turn.State != TurnCanceled || turn.Content != ContentPartial || turn.FailureCode != FailureCanceled || turn.Open() || rec.MessageState() != MessageCanceled {
		t.Fatalf("a completed reply after an accepted stop seals the turn canceled with a canceled message: %+v", turn)
	}
}

// TestStoppedMessageLeavesLaterContexts (DEC-069): the input of a turn
// stopped before its send stays out of later contexts and uses no budget;
// a failed turn and a reply stopped after its send began keep their message.
func TestStoppedMessageLeavesLaterContexts(t *testing.T) {
	_, _, queued, now := turnFixture(t)
	queued.RequestStop(true, now)
	_, _, failed, _ := turnFixture(t)
	failed.EndWithOperation(&Operation{Lifecycle: LifecycleFailed, FailureCode: "BUDGET_EXHAUSTED"}, now)
	_, _, midReply, _ := turnFixture(t)
	if _, err := midReply.ClaimWriter("a", 1, "i", now); err != nil {
		t.Fatal(err)
	}
	midReply.RequestStop(false, now)
	text := "Black."
	cmd := CommandIdentity{TenantID: "tenant_a", CommandID: "trn:outcome", ActorID: "anvilkit-agent-workflow", RequestDigest: Digest("sha256:" + strings.Repeat("a", 64))}
	midReply.Record(ReplyRecord{Outcome: ReplyCanceled, HasText: true, Text: text, Digest: TextDigest(text), Sender: SenderActive, FailureCode: FailureCanceled}, "msg_r", cmd, now)
	_, _, claimedNotSent, _ := turnFixture(t)
	if _, err := claimedNotSent.ClaimWriter("a", 1, "i", now); err != nil {
		t.Fatal(err)
	}
	claimedNotSent.Record(ReplyRecord{Outcome: ReplyCanceled, Sender: SenderNotStarted, FailureCode: FailureCanceled}, "", cmd, now)
	for _, c := range []struct {
		name string
		turn *Turn
		want bool
	}{{"stopped while queued", queued, true}, {"failed", failed, false}, {"stopped mid-reply", midReply, false}, {"stopped after the claim, before the call", claimedNotSent, true}} {
		if got := c.turn.StoppedBeforeSend(); got != c.want {
			t.Errorf("%s: StoppedBeforeSend=%v, want %v (%+v)", c.name, got, c.want, c.turn)
		}
	}

	msg := func(seq uint64, text string) *Message {
		return &Message{ID: "msg_" + string(rune('a'+seq)), Seq: seq, Text: text, ContentDigest: TextDigest(text)}
	}
	newestFirst := []*Message{msg(4, "input"), msg(3, "stopped message"), msg(2, "reply"), msg(1, "first")}
	leftOut := map[string]bool{msg(3, "").ID: true}
	got := SelectTurnContext(newestFirst, 4, 15, leftOut)
	if len(got) != 3 || got[0].Seq != 1 || got[1].Seq != 2 || got[2].Seq != 4 {
		t.Fatalf("the stopped message is left out and uses no budget: %+v", got)
	}
	if got := SelectTurnContext(newestFirst, 4, 15, nil); len(got) != 1 || got[0].Seq != 4 {
		t.Fatalf("without the left-out set the stopped message takes the budget and crowds the older ones out: %+v", got)
	}
	if got := SelectTurnContext(newestFirst, 4, 1000, map[string]bool{msg(4, "").ID: true}); len(got) != 4 {
		t.Fatalf("the input message is always kept: %+v", got)
	}
}
