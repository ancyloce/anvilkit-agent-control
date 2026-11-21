package domain

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Assistant replies (design 0001 D3, DEC-068): each accepted user message of
// a conversation with replies enabled opens one turn, answered by one
// chat_turn operation. A conversation has at most one open turn (design
// §4.3); content, sender and financial facts are separate (§7.4), and the
// slot is released only when the turn is sealed. Reply text is user content:
// it is never logged.

// TurnState is the turn's aggregate state (design 0001 §4.2).
type TurnState string

const (
	TurnQueued        TurnState = "queued"
	TurnRunning       TurnState = "running"
	TurnCompleted     TurnState = "completed"
	TurnFailed        TurnState = "failed"
	TurnCancelPending TurnState = "cancel_pending"
	TurnCanceled      TurnState = "canceled"
	TurnReconciling   TurnState = "reconciling"
)

// Open reports whether the turn holds its conversation's one reply slot.
func (s TurnState) Open() bool {
	return s == TurnQueued || s == TurnRunning || s == TurnCancelPending || s == TurnReconciling
}

// ContentStatus is what is known of the reply's text.
type ContentStatus string

const (
	ContentComplete ContentStatus = "complete"
	ContentPartial  ContentStatus = "partial"
	ContentUnknown  ContentStatus = "unknown"
)

// SenderStatus says whether the reply's controlled sending path can still
// send or append; STOPPED never says the provider stopped billing.
type SenderStatus string

const (
	SenderNotStarted SenderStatus = "not_started"
	SenderActive     SenderStatus = "active"
	SenderStopped    SenderStatus = "stopped"
	SenderUnknown    SenderStatus = "unknown"
)

// ReplyOutcome is the outcome the chat Activity records for a reply.
type ReplyOutcome string

const (
	ReplyCompleted     ReplyOutcome = "completed"
	ReplyPartialFailed ReplyOutcome = "partial_failed"
	ReplyCanceled      ReplyOutcome = "canceled"
	ReplyFailed        ReplyOutcome = "failed"
)

// Reply bounds (DEC-068).
const (
	ChatTurnProfileID = "chat-turn-v1"
	ChatTurnStepID    = "reply"
	// MaxReplyBytes is the stored message bound, which a reply shares with
	// user text (DEC-068, amended: the message contract caps text there).
	MaxReplyBytes = MaxUserMessageBytes
	// DefaultTurnContextBytes bounds a turn's context: about 48,000 input
	// units at four bytes each (DEC-068).
	DefaultTurnContextBytes = 192000
	MaxFailureCode          = 64
)

// Failure codes Control itself assigns.
const (
	FailureCanceled         = "CANCELED"
	FailureReplyNotRecorded = "REPLY_NOT_RECORDED"
)

// ErrTurnBusy refuses a message while the conversation's previous turn is
// open (design 0001 §4.3); nothing is stored. It is retryable: the slot
// frees when that turn is sealed.
var ErrTurnBusy = errors.New("turn busy")

// Turn is one bounded assistant reply to one user message.
type Turn struct {
	ID              string
	TenantID        string
	ConversationID  string
	InputMessageID  string
	InputMessageSeq uint64
	// OperationID is the turn's chat_turn operation.
	OperationID string
	State       TurnState
	Content     ContentStatus
	Sender      SenderStatus
	// AssistantMessageID is set once the reply's text is stored.
	AssistantMessageID string
	FailureCode        string
	// The reply writer (design 0001 §7.2): the chat Activity's stable key,
	// the trusted worker's attempt ordinal and invocation, and the epoch a
	// takeover increments. WriterEpoch 0 means unclaimed.
	ActivityKey     string
	TemporalAttempt uint64
	InvocationID    string
	WriterEpoch     uint64
	// The command and request digest that sealed the turn: a retry of it
	// replays, anything else conflicts.
	OutcomeCommandID string
	OutcomeDigest    Digest
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// NewTurn opens the turn answering the stored user message.
func NewTurn(input *Message, now time.Time) *Turn {
	return &Turn{
		ID: NewID("trn"), TenantID: input.TenantID, ConversationID: input.ConversationID, InputMessageID: input.ID,
		InputMessageSeq: input.Seq, State: TurnQueued, Content: ContentUnknown, Sender: SenderNotStarted,
		CreatedAt: now, UpdatedAt: now,
	}
}

// OperationCommandID is the identity the turn's chat_turn operation is
// created under: unique per turn, so it never meets a caller's command.
func (t *Turn) OperationCommandID() string { return "turn:" + t.ID }

// ReplyCallID is the one reply call of a chat_turn operation (design 0001
// §7.2): the chat Activity derives the same identity, and every retry reuses
// it, so a takeover never makes a second physical send.
func ReplyCallID(operationID string) string { return operationID + ":reply:1" }

// Open reports whether the turn holds the conversation's reply slot.
func (t *Turn) Open() bool { return t.State.Open() }

// ClaimWriter applies design 0001 §7.2: the first claim binds the activity
// and starts epoch 1; the same invocation re-entering is answered as it is;
// the same attempt from another invocation, an older attempt or another
// activity is stale; a higher attempt of the same activity takes over and
// increments the epoch. A sealed turn needs no writer and is answered as it
// is, so a late retry learns the outcome.
func (t *Turn) ClaimWriter(activityKey string, attempt uint64, invocation string, now time.Time) (existing bool, err error) {
	if activityKey == "" || attempt == 0 || invocation == "" {
		return false, fmt.Errorf("%w: a reply writer names its activity, attempt and invocation", ErrInvalid)
	}
	if !t.Open() {
		return true, nil
	}
	switch {
	case t.WriterEpoch == 0:
		t.ActivityKey, t.TemporalAttempt, t.InvocationID, t.WriterEpoch = activityKey, attempt, invocation, 1
		if t.State == TurnQueued {
			t.State = TurnRunning
		}
		t.Sender = SenderActive
	case activityKey != t.ActivityKey:
		return false, fmt.Errorf("%w: turn %s is written by another activity", ErrStaleExecution, t.ID)
	case attempt == t.TemporalAttempt && invocation == t.InvocationID:
		return true, nil
	case attempt == t.TemporalAttempt:
		return false, fmt.Errorf("%w: attempt %d of turn %s is claimed by another invocation", ErrStaleExecution, attempt, t.ID)
	case attempt < t.TemporalAttempt:
		return false, fmt.Errorf("%w: attempt %d of turn %s is older than attempt %d", ErrStaleExecution, attempt, t.ID, t.TemporalAttempt)
	default:
		if err := bumpable(t.WriterEpoch); err != nil {
			return false, err
		}
		t.TemporalAttempt, t.InvocationID = attempt, invocation
		t.WriterEpoch++
	}
	t.UpdatedAt = now
	return false, nil
}

// CheckWriter admits a write under the current writer epoch only.
func (t *Turn) CheckWriter(epoch uint64) error {
	if t.WriterEpoch == 0 || epoch != t.WriterEpoch {
		return fmt.Errorf("%w: turn %s is at writer epoch %d, not %d", ErrStaleExecution, t.ID, t.WriterEpoch, epoch)
	}
	return nil
}

// RequestStop records an accepted stop (design 0001 §11.3). When the
// operation was canceled at once nothing could still act, so the turn ends
// canceled without text; otherwise it waits in cancel_pending for the chat
// Activity's outcome. A sealed turn is unchanged.
func (t *Turn) RequestStop(operationCanceled bool, now time.Time) bool {
	if !t.Open() {
		return false
	}
	if operationCanceled {
		t.State, t.Content, t.FailureCode = TurnCanceled, ContentPartial, FailureCanceled
		if t.WriterEpoch == 0 {
			t.Sender = SenderNotStarted
		} else {
			t.Sender = SenderStopped
		}
	} else if t.State == TurnQueued || t.State == TurnRunning {
		t.State = TurnCancelPending
	}
	t.UpdatedAt = now
	return true
}

// EndWithOperation seals an open turn whose operation ended without a
// recorded reply (its deadline, a failure, a cancel): a terminal operation
// has no open attempt, so nothing can still write. It reports whether the
// turn changed.
func (t *Turn) EndWithOperation(op *Operation, now time.Time) bool {
	if !t.Open() || op == nil || !op.Lifecycle.Terminal() {
		return false
	}
	if op.Lifecycle == LifecycleCanceled {
		t.State, t.FailureCode = TurnCanceled, FailureCanceled
	} else {
		t.State, t.FailureCode = TurnFailed, op.FailureCode
		if t.FailureCode == "" {
			t.FailureCode = FailureReplyNotRecorded
		}
	}
	if t.WriterEpoch == 0 {
		t.Sender = SenderNotStarted
	} else {
		t.Sender = SenderStopped
	}
	t.Content = ContentPartial
	t.UpdatedAt = now
	return true
}

// ReplyRecord is what the chat Activity records for a reply.
type ReplyRecord struct {
	Outcome     ReplyOutcome
	HasText     bool
	Text        string
	Digest      Digest
	Sender      SenderStatus
	FailureCode string
}

// Check validates the record against design 0001 §7.4: a completed reply has
// the whole text and a stopped sender; every other outcome names a stable
// failure code; a partial failure keeps a prefix; a failure stores no text;
// the digest is the text's.
func (rec ReplyRecord) Check() error {
	switch rec.Outcome {
	case ReplyCompleted, ReplyPartialFailed, ReplyCanceled, ReplyFailed:
	default:
		return fmt.Errorf("%w: reply outcome %q", ErrInvalid, rec.Outcome)
	}
	switch rec.Sender {
	case SenderNotStarted, SenderActive, SenderStopped, SenderUnknown:
	default:
		return fmt.Errorf("%w: sender status %q", ErrInvalid, rec.Sender)
	}
	if rec.HasText {
		if len(rec.Text) == 0 || len(rec.Text) > MaxReplyBytes || !utf8.ValidString(rec.Text) || strings.ContainsRune(rec.Text, 0) {
			return fmt.Errorf("%w: a reply's text is 1 to %d bytes of UTF-8 without NUL", ErrInvalid, MaxReplyBytes)
		}
		if rec.Digest != TextDigest(rec.Text) {
			return fmt.Errorf("%w: the content digest is not the text's", ErrInvalid)
		}
	} else if rec.Digest != "" {
		return fmt.Errorf("%w: a content digest names text that is absent", ErrInvalid)
	}
	if rec.Outcome == ReplyCompleted {
		if !rec.HasText || rec.Sender != SenderStopped || rec.FailureCode != "" {
			return fmt.Errorf("%w: a completed reply has its whole text, a stopped sender and no failure code", ErrInvalid)
		}
		return nil
	}
	if rec.FailureCode == "" || len(rec.FailureCode) > MaxFailureCode {
		return fmt.Errorf("%w: a %s reply names a failure code of 1 to %d characters", ErrInvalid, rec.Outcome, MaxFailureCode)
	}
	if rec.Outcome == ReplyPartialFailed && !rec.HasText {
		return fmt.Errorf("%w: a partial failure keeps its prefix; without text it is failed", ErrInvalid)
	}
	if rec.Outcome == ReplyFailed && rec.HasText {
		return fmt.Errorf("%w: a failed reply stores no text; a kept prefix is a partial failure", ErrInvalid)
	}
	if rec.Sender == SenderNotStarted && rec.HasText {
		return fmt.Errorf("%w: a sender that never started produced no text", ErrInvalid)
	}
	return nil
}

// Seals reports whether the record ends the turn and frees its slot: a
// completed reply, a stopped or never-started sender, or a stop sealed while
// its send meters to the end (DEC-068: the seal fences every later write,
// and the operation settles when the send does). An unknown sender keeps
// the turn reconciling.
func (rec ReplyRecord) Seals() bool {
	switch {
	case rec.Outcome == ReplyCompleted:
		return true
	case rec.Sender == SenderStopped || rec.Sender == SenderNotStarted:
		return true
	default:
		return rec.Outcome == ReplyCanceled && rec.Sender == SenderActive
	}
}

// UnderCancel applies an accepted stop to a sealing record (design 0001 §7.4
// and §11.3; B-016): once op carries a cancel intent, no completion was
// accepted before the stop, so the record seals the turn canceled and keeps
// its text, if any, as the canceled message. This is the rule the operation
// settles by (a cancel intent dominates any outcome, SettleOperation), so a
// stopped turn and its operation agree. The sender stays the record's own; a
// record that does not seal is unchanged and keeps the turn reconciling.
func (rec ReplyRecord) UnderCancel(op *Operation) ReplyRecord {
	if op.Control != ControlCancelPending && op.Control != ControlCancelApplied {
		return rec
	}
	if !rec.Seals() || rec.Outcome == ReplyCanceled {
		return rec
	}
	rec.Outcome, rec.FailureCode = ReplyCanceled, FailureCanceled
	return rec
}

// MessageState is the stored assistant message's state for a sealed record
// with text.
func (rec ReplyRecord) MessageState() MessageState {
	switch rec.Outcome {
	case ReplyCompleted:
		return MessageCompleted
	case ReplyCanceled:
		return MessageCanceled
	default:
		return MessagePartialFailed
	}
}

// Replays reports whether cmd repeats the command that sealed the turn; any
// other record for a sealed turn conflicts.
func (t *Turn) Replays(cmd CommandIdentity) error {
	if t.OutcomeCommandID == cmd.CommandID && t.OutcomeDigest == cmd.RequestDigest {
		return nil
	}
	return fmt.Errorf("%w: turn %s is sealed under another command", ErrIdempotencyConflict, t.ID)
}

// Record applies a checked reply record. A sealing record ends the turn with
// the outcome's state; assistantMessageID names the stored text, if any. A
// record whose sender is unknown keeps the turn reconciling, holding the
// slot, until a later record seals it.
func (t *Turn) Record(rec ReplyRecord, assistantMessageID string, cmd CommandIdentity, now time.Time) {
	t.Sender, t.FailureCode, t.UpdatedAt = rec.Sender, rec.FailureCode, now
	if !rec.Seals() {
		t.State, t.Content = TurnReconciling, ContentUnknown
		return
	}
	switch rec.Outcome {
	case ReplyCompleted:
		t.State, t.Content = TurnCompleted, ContentComplete
	case ReplyCanceled:
		t.State, t.Content = TurnCanceled, ContentPartial
	default:
		t.State, t.Content = TurnFailed, ContentPartial
	}
	t.AssistantMessageID = assistantMessageID
	t.OutcomeCommandID, t.OutcomeDigest = cmd.CommandID, cmd.RequestDigest
}

// AppendAssistantMessage stores a sealed reply's text as the next message.
// A reply does not move the conversation's business revision (design 0001
// §4.3: assistant text never conflicts with a rename or an archive).
func (c *Conversation) AppendAssistantMessage(state MessageState, text string, now time.Time) (*Message, error) {
	if err := bumpable(c.LastMessageSeq); err != nil {
		return nil, err
	}
	c.LastMessageSeq++
	c.UpdatedAt = now
	return &Message{
		ID: NewID("msg"), TenantID: c.TenantID, ConversationID: c.ID, Seq: c.LastMessageSeq,
		Role: MessageRoleAssistant, State: state, Text: text, ContentDigest: TextDigest(text),
		ConversationRevision: c.Revision, CreatedAt: now,
	}, nil
}

// StoppedBeforeSend reports whether the turn ended canceled before its
// sender started: nothing was sent and no reply exists, so its input message
// stays in the conversation's history but out of later turns' contexts
// (DEC-069). A failed turn's message, and a message whose reply was stopped
// after the send began, stay in the context.
func (t *Turn) StoppedBeforeSend() bool {
	return t.State == TurnCanceled && t.Sender == SenderNotStarted
}

// SelectTurnContext keeps, from messages ordered newest first, the ones up to
// and including the input message whose text fits the budget, and returns
// them in ascending sequence. Messages named in leftOut (the inputs of turns
// stopped before their send) are skipped and use no budget. The input
// message is always kept; earlier turns are sealed before a turn opens, so
// the same turn always yields the same context.
func SelectTurnContext(newestFirst []*Message, inputSeq uint64, budget int, leftOut map[string]bool) []*Message {
	kept := make([]*Message, 0, len(newestFirst))
	bytes := 0
	for _, m := range newestFirst {
		if m.Seq > inputSeq || (m.Seq < inputSeq && leftOut[m.ID]) {
			continue
		}
		if len(kept) > 0 && bytes+len(m.Text) > budget {
			break
		}
		bytes += len(m.Text)
		kept = append(kept, m)
	}
	for i, j := 0, len(kept)-1; i < j; i, j = i+1, j-1 {
		kept[i], kept[j] = kept[j], kept[i]
	}
	return kept
}

// ContextDigest is the SHA-256 over the context's message ids and content
// digests in order: it names the context without its text.
func ContextDigest(messages []*Message) Digest {
	h := sha256.New()
	for _, m := range messages {
		fmt.Fprintf(h, "%s %s\n", m.ID, m.ContentDigest)
	}
	return Digest(fmt.Sprintf("sha256:%x", h.Sum(nil)))
}
