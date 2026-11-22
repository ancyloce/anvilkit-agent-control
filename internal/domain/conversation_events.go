package domain

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// Streaming persistence and the conversation event log (design 0001 §8,
// D4a, DEC-070). A reply's text is committed in batches while it streams:
// each batch is one contiguous range of the Model Proxy's original frames
// (provider sequences from 0), and a batch with text becomes a chunk. The
// reply's assistant message id and sequence are reserved on the turn at its
// first text and its row is written once, at the seal. Each conversation
// keeps a durable event log; event rows hold identities, sequences and
// states, never a body (a message.delta names its chunk). Event, chunk and
// provider sequences are separate domains.

// ConversationEventType names a public conversation event (design 0001 §8.1).
type ConversationEventType string

const (
	EventMessageAccepted     ConversationEventType = "message.accepted"
	EventMessageStarted      ConversationEventType = "message.started"
	EventMessageDelta        ConversationEventType = "message.delta"
	EventMessageCompleted    ConversationEventType = "message.completed"
	EventMessageFailed       ConversationEventType = "message.failed"
	EventMessageCanceled     ConversationEventType = "message.canceled"
	EventTurnChanged         ConversationEventType = "turn.changed"
	EventConversationChanged ConversationEventType = "conversation.changed"
)

// Reasons Control assigns to the reply stream.
const (
	// ReasonStreamInconsistent stops a reply's appends: a replayed range that
	// differs from the committed one, a gap, or a changed original stream.
	ReasonStreamInconsistent = "STREAM_INCONSISTENT"
	// StopSealPrefix marks the outcome command of a turn sealed by its stop;
	// a later reply record is answered with the sealed turn.
	StopSealPrefix = "stop:"
)

// TurnSnapshot is a turn's public state at an event; it carries no text.
type TurnSnapshot struct {
	TurnID             string        `json:"turnId"`
	InputMessageID     string        `json:"inputMessageId"`
	OperationID        string        `json:"operationId"`
	State              TurnState     `json:"state"`
	Content            ContentStatus `json:"contentStatus"`
	Sender             SenderStatus  `json:"senderStatus"`
	Finance            FinanceState  `json:"finance"`
	AssistantMessageID string        `json:"assistantMessageId,omitempty"`
	FailureCode        string        `json:"failureCode,omitempty"`
	CreatedAt          time.Time     `json:"createdAt"`
	UpdatedAt          time.Time     `json:"updatedAt"`
}

// Snapshot is the turn's public state with its operation's finance.
func (t *Turn) Snapshot(finance FinanceState) *TurnSnapshot {
	return &TurnSnapshot{
		TurnID: t.ID, InputMessageID: t.InputMessageID, OperationID: t.OperationID, State: t.State, Content: t.Content,
		Sender: t.Sender, Finance: finance, AssistantMessageID: t.PublicMessageID(), FailureCode: t.FailureCode,
		CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt,
	}
}

// PublicMessageID is the reply's assistant message as a reader sees it: the
// stored message, or the one reserved at the first committed text.
func (t *Turn) PublicMessageID() string {
	if t.AssistantMessageID != "" {
		return t.AssistantMessageID
	}
	return t.ReservedMessageID
}

// EventPayload holds an event's facts other than its identities: sequences,
// states and digests, never a body.
type EventPayload struct {
	MessageSeq           uint64             `json:"messageSeq,omitempty"`
	Role                 MessageRole        `json:"role,omitempty"`
	ContentRevision      uint64             `json:"contentRevision,omitempty"`
	State                MessageState       `json:"state,omitempty"`
	ThroughChunkSeq      uint64             `json:"throughChunkSeq,omitempty"`
	ContentDigest        Digest             `json:"contentDigest,omitempty"`
	TurnState            TurnState          `json:"turnState,omitempty"`
	FailureCode          string             `json:"failureCode,omitempty"`
	Turn                 *TurnSnapshot      `json:"turn,omitempty"`
	ConversationRevision uint64             `json:"conversationRevision,omitempty"`
	Status               ConversationStatus `json:"status,omitempty"`
}

// ConversationEvent is one durable event of a conversation.
type ConversationEvent struct {
	ConversationID string
	TenantID       string
	Seq            uint64
	Type           ConversationEventType
	MessageID      string
	TurnID         string
	// ChunkSeq names a message.delta's chunk.
	ChunkSeq   uint64
	Payload    EventPayload
	OccurredAt time.Time
	// Text is a served message.delta's chunk text; it is never stored in the
	// event row.
	Text string
}

// CoveredEventSeq is the last event the conversation has committed; 0
// before the first.
func (c *Conversation) CoveredEventSeq() uint64 {
	if c.NextEventSeq == 0 {
		return 0
	}
	return c.NextEventSeq - 1
}

func (c *Conversation) newEvent(typ ConversationEventType, now time.Time) (*ConversationEvent, error) {
	if c.NextEventSeq == 0 {
		c.NextEventSeq = 1
	}
	if err := bumpable(c.NextEventSeq); err != nil {
		return nil, err
	}
	ev := &ConversationEvent{ConversationID: c.ID, TenantID: c.TenantID, Seq: c.NextEventSeq, Type: typ, OccurredAt: now}
	c.NextEventSeq++
	return ev, nil
}

// MessageAcceptedEvent records a stored user message and the turn it opened.
func (c *Conversation) MessageAcceptedEvent(m *Message, turnID string, now time.Time) (*ConversationEvent, error) {
	ev, err := c.newEvent(EventMessageAccepted, now)
	if err != nil {
		return nil, err
	}
	ev.MessageID, ev.TurnID = m.ID, turnID
	ev.Payload = EventPayload{MessageSeq: m.Seq, Role: m.Role}
	return ev, nil
}

// TurnChangedEvent records the turn's public state.
func (c *Conversation) TurnChangedEvent(t *Turn, finance FinanceState, now time.Time) (*ConversationEvent, error) {
	ev, err := c.newEvent(EventTurnChanged, now)
	if err != nil {
		return nil, err
	}
	ev.TurnID = t.ID
	ev.Payload = EventPayload{Turn: t.Snapshot(finance)}
	return ev, nil
}

// MessageStartedEvent records the reservation of the reply's message.
func (c *Conversation) MessageStartedEvent(t *Turn, now time.Time) (*ConversationEvent, error) {
	ev, err := c.newEvent(EventMessageStarted, now)
	if err != nil {
		return nil, err
	}
	ev.MessageID, ev.TurnID = t.ReservedMessageID, t.ID
	return ev, nil
}

// MessageDeltaEvent records one committed chunk; its text stays in the chunk.
func (c *Conversation) MessageDeltaEvent(t *Turn, chunk *ReplyChunk, now time.Time) (*ConversationEvent, error) {
	ev, err := c.newEvent(EventMessageDelta, now)
	if err != nil {
		return nil, err
	}
	ev.MessageID, ev.TurnID, ev.ChunkSeq = t.ReservedMessageID, t.ID, chunk.ChunkSeq
	ev.Payload = EventPayload{ContentRevision: chunk.ChunkSeq}
	return ev, nil
}

// MessageSealedEvent records a sealed assistant message: message.completed,
// message.failed or message.canceled by its state.
func (c *Conversation) MessageSealedEvent(t *Turn, m *Message, now time.Time) (*ConversationEvent, error) {
	typ := EventMessageFailed
	switch m.State {
	case MessageCompleted:
		typ = EventMessageCompleted
	case MessageCanceled:
		typ = EventMessageCanceled
	}
	ev, err := c.newEvent(typ, now)
	if err != nil {
		return nil, err
	}
	ev.MessageID, ev.TurnID = m.ID, t.ID
	ev.Payload = EventPayload{State: m.State, ContentRevision: m.ContentRevision, ThroughChunkSeq: m.ThroughChunkSeq,
		ContentDigest: m.ContentDigest, TurnState: t.State, FailureCode: t.FailureCode}
	return ev, nil
}

// ConversationChangedEvent records a rename, an archive or a restore.
func (c *Conversation) ConversationChangedEvent(now time.Time) (*ConversationEvent, error) {
	ev, err := c.newEvent(EventConversationChanged, now)
	if err != nil {
		return nil, err
	}
	ev.Payload = EventPayload{ConversationRevision: c.Revision, Status: c.Status}
	return ev, nil
}

// ConversationEventsContiguous reports whether a page read after the cursor
// starts right after it with no hole: a gap requires a reset, never an
// answer that skips an event.
func ConversationEventsContiguous(after uint64, events []*ConversationEvent) bool {
	next := after + 1
	for _, ev := range events {
		if ev.Seq != next {
			return false
		}
		next++
	}
	return true
}

// ReplyBatch is one range of a reply's original frames as the chat Activity
// submits it (design 0001 §8.2).
type ReplyBatch struct {
	OriginalStreamID   string
	From, Through      uint64
	SourceFramesDigest Digest
	HasText            bool
	Text               string
	TextDigest         Digest
}

// Check validates the batch on its own: a range, the stream identity, the
// digests, and text of whole UTF-8 characters without NUL within the reply
// bound.
func (b ReplyBatch) Check() error {
	if b.OriginalStreamID == "" || len(b.OriginalStreamID) > 128 {
		return fmt.Errorf("%w: a batch names its original stream", ErrInvalid)
	}
	if b.Through < b.From || b.Through >= math.MaxInt64 {
		return fmt.Errorf("%w: provider range %d to %d", ErrInvalid, b.From, b.Through)
	}
	if !digestPattern.MatchString(string(b.SourceFramesDigest)) {
		return fmt.Errorf("%w: the source frames digest is malformed", ErrInvalid)
	}
	if !b.HasText {
		if b.Text != "" || b.TextDigest != "" {
			return fmt.Errorf("%w: a textless batch carries no text digest", ErrInvalid)
		}
		return nil
	}
	if b.Text == "" || len(b.Text) > MaxReplyBytes || !utf8.ValidString(b.Text) || strings.ContainsRune(b.Text, 0) {
		return fmt.Errorf("%w: a batch's text is 1 to %d bytes of UTF-8 without NUL", ErrInvalid, MaxReplyBytes)
	}
	if b.TextDigest != TextDigest(b.Text) {
		return fmt.Errorf("%w: the text digest is not the text's", ErrInvalid)
	}
	return nil
}

// ReplyChunk is one committed batch; ChunkSeq is 0 for a textless one.
type ReplyChunk struct {
	TurnID             string
	TenantID           string
	BatchSeq           uint64
	ChunkSeq           uint64
	From, Through      uint64
	SourceFramesDigest Digest
	Text               string
	TextDigest         Digest
	CreatedAt          time.Time
}

// Same reports whether the batch repeats this committed one exactly.
func (c *ReplyChunk) Same(b ReplyBatch) bool {
	return c.From == b.From && c.Through == b.Through && c.SourceFramesDigest == b.SourceFramesDigest &&
		(c.ChunkSeq > 0) == b.HasText && c.TextDigest == b.TextDigest
}

// BatchClass is how a batch relates to the committed watermark.
type BatchClass int

const (
	// BatchAppend starts at the watermark plus one (0 before any batch).
	BatchAppend BatchClass = iota + 1
	// BatchReplay repeats a committed batch exactly: the first receipt.
	BatchReplay
)

// NextProviderSeq is the provider sequence the next batch starts at.
func (t *Turn) NextProviderSeq() uint64 {
	if t.LastProviderSeq == nil {
		return 0
	}
	return *t.LastProviderSeq + 1
}

// ClassifyBatch applies design 0001 §8.2 to a checked batch. stored is the
// committed batch starting at the batch's first sequence, if any. A
// changed original stream, a gap, or a replay that differs from what was
// committed is ErrIntegrity (STREAM_INCONSISTENT): the caller stops the
// appends and reconciles; text beyond the reply bound is ErrInvalid.
func (t *Turn) ClassifyBatch(b ReplyBatch, stored *ReplyChunk) (BatchClass, error) {
	if t.OriginalStreamID != "" && b.OriginalStreamID != t.OriginalStreamID {
		return 0, fmt.Errorf("%w: %s: turn %s streams %s, not %s", ErrIntegrity, ReasonStreamInconsistent, t.ID, t.OriginalStreamID, b.OriginalStreamID)
	}
	next := t.NextProviderSeq()
	switch {
	case b.From == next:
		if b.HasText && t.CommittedBytes+uint64(len(b.Text)) > MaxReplyBytes {
			return 0, fmt.Errorf("%w: the reply's committed text would exceed %d bytes", ErrInvalid, MaxReplyBytes)
		}
		return BatchAppend, nil
	case b.From < next && stored != nil && stored.Same(b):
		return BatchReplay, nil
	case b.From < next:
		return 0, fmt.Errorf("%w: %s: the range from %d differs from turn %s's committed one", ErrIntegrity, ReasonStreamInconsistent, b.From, t.ID)
	default:
		return 0, fmt.Errorf("%w: %s: turn %s expects provider sequence %d, not %d", ErrIntegrity, ReasonStreamInconsistent, t.ID, next, b.From)
	}
}

// ApplyBatch advances the watermark with an appended batch and returns the
// chunk to store; a batch with text counts against the reply bound.
func (t *Turn) ApplyBatch(b ReplyBatch, now time.Time) *ReplyChunk {
	through := b.Through
	t.OriginalStreamID, t.LastProviderSeq = b.OriginalStreamID, &through
	t.LastBatchSeq++
	c := &ReplyChunk{TurnID: t.ID, TenantID: t.TenantID, BatchSeq: t.LastBatchSeq, From: b.From, Through: b.Through,
		SourceFramesDigest: b.SourceFramesDigest, CreatedAt: now}
	if b.HasText {
		t.LastChunkSeq++
		t.CommittedBytes += uint64(len(b.Text))
		c.ChunkSeq, c.Text, c.TextDigest = t.LastChunkSeq, b.Text, b.TextDigest
	}
	t.UpdatedAt = now
	return c
}

// ReconcileStream stops the turn's appends after an inconsistent batch: the
// turn keeps its slot reconciling until the Activity records an outcome.
func (t *Turn) ReconcileStream(now time.Time) {
	t.State, t.Content, t.FailureCode, t.UpdatedAt = TurnReconciling, ContentUnknown, ReasonStreamInconsistent, now
}

// ReserveReply reserves the reply's assistant message at its first text: a
// new id and the conversation's next message sequence. The slot keeps every
// other message out until the seal, so the sequence stays the reply's. It
// reports whether the reservation is new.
func (c *Conversation) ReserveReply(t *Turn, now time.Time) (bool, error) {
	if t.ReservedMessageID != "" {
		return false, nil
	}
	if err := bumpable(c.LastMessageSeq); err != nil {
		return false, err
	}
	c.LastMessageSeq++
	c.UpdatedAt = now
	t.ReservedMessageID, t.ReservedMessageSeq = NewID("msg"), c.LastMessageSeq
	return true, nil
}

// SealReply is the assistant message a sealing record or a stop stores. A
// reply with a reservation is sealed from its committed chunks under the
// reserved id and sequence; one without (a reply recorded whole, D3) takes
// the next sequence. Neither moves the business revision.
func (c *Conversation) SealReply(t *Turn, state MessageState, text string, now time.Time) (*Message, error) {
	if t.ReservedMessageID == "" {
		return c.AppendAssistantMessage(state, text, now)
	}
	c.UpdatedAt = now
	return &Message{
		ID: t.ReservedMessageID, TenantID: c.TenantID, ConversationID: c.ID, Seq: t.ReservedMessageSeq,
		Role: MessageRoleAssistant, State: state, Text: text, ContentDigest: TextDigest(text),
		ConversationRevision: c.Revision, CreatedAt: now, ContentRevision: t.LastChunkSeq, ThroughChunkSeq: t.LastChunkSeq,
	}, nil
}

// StreamingMessage is the reply's reserved message as a reader sees it while
// it streams: the committed chunks so far.
func StreamingMessage(t *Turn, text string) *Message {
	return &Message{
		ID: t.ReservedMessageID, TenantID: t.TenantID, ConversationID: t.ConversationID, Seq: t.ReservedMessageSeq,
		Role: MessageRoleAssistant, State: MessageStreaming, Text: text, ContentDigest: TextDigest(text),
		CreatedAt: t.UpdatedAt, ContentRevision: t.LastChunkSeq, ThroughChunkSeq: t.LastChunkSeq,
	}
}

// CanSealAtStop reports whether a stop seals the turn at once (DEC-070 Q3
// A): its writer is claimed and the reply is running or waiting on the stop.
// A queued turn waits for its operation's cancel; a reconciling one keeps its
// slot until the sender is proven stopped.
func (t *Turn) CanSealAtStop() bool {
	return t.WriterEpoch > 0 && (t.State == TurnRunning || t.State == TurnCancelPending)
}

// SealStop seals the turn at its stop: canceled, the committed chunks as the
// canceled message (messageID empty without text), the sender as it is (a
// send still active meters to its end, DEC-068 Q3). The seal's command is
// the stop's.
func (t *Turn) SealStop(messageID string, cmd CommandIdentity, now time.Time) {
	t.State, t.Content, t.FailureCode = TurnCanceled, ContentPartial, FailureCanceled
	t.AssistantMessageID = messageID
	t.OutcomeCommandID, t.OutcomeDigest = StopSealPrefix+cmd.CommandID, cmd.RequestDigest
	t.UpdatedAt = now
}

// SealedByStop reports whether the turn was sealed by its stop.
func (t *Turn) SealedByStop() bool {
	return strings.HasPrefix(t.OutcomeCommandID, StopSealPrefix)
}
