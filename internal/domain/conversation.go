package domain

import (
	"crypto/sha256"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Conversations (DEC-067, design 0001 first slice): a personal conversation
// of one verified actor in one tenant and project, holding that actor's text
// messages in sequence. Assistant replies (turns) are later phases; this
// model never produces one.

type ConversationStatus string

const (
	ConversationActive   ConversationStatus = "active"
	ConversationArchived ConversationStatus = "archived"
)

type MessageRole string

const (
	MessageRoleUser      MessageRole = "user"
	MessageRoleAssistant MessageRole = "assistant"
)

// MessageState: a stored user message is completed; the other states
// belong to assistant replies and none of them means a complete reply.
type MessageState string

const (
	MessagePending       MessageState = "pending"
	MessageStreaming     MessageState = "streaming"
	MessageCompleted     MessageState = "completed"
	MessagePartialFailed MessageState = "partial_failed"
	MessageCanceled      MessageState = "canceled"
	MessageReconciling   MessageState = "reconciling"
)

// Conversation bounds (design 0001 §14.1 development defaults).
const (
	MaxConversationTitle    = 200 // characters
	MaxUserMessageBytes     = 32768
	DefaultConversationPage = 20
	DefaultMessagePage      = 30
	MaxHistoryPage          = 100
	// MessagePageTextBudget ends a history page once its text reaches this
	// many bytes; a page always holds at least one message.
	MessagePageTextBudget = 1 << 20
)

// ReasonConversationArchived is the controlled reason a message to an
// archived conversation is refused with.
const ReasonConversationArchived = "conversation archived"

type Conversation struct {
	ID           string
	TenantID     string
	ProjectID    string
	OwnerActorID string
	Status       ConversationStatus
	// Title is empty for an untitled conversation.
	Title          string
	Revision       uint64
	LastMessageSeq uint64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewConversation is a new active conversation owned by the scope's actor
// in the scope's tenant and project.
func NewConversation(scope Scope, title string, now time.Time) (*Conversation, error) {
	if scope.TenantID == "" || scope.ActorID == "" {
		return nil, fmt.Errorf("%w: a conversation needs the caller's tenant and actor", ErrInvalid)
	}
	if err := validTitle(title); err != nil {
		return nil, err
	}
	return &Conversation{
		ID: NewID("cnv"), TenantID: scope.TenantID, ProjectID: scope.ProjectID, OwnerActorID: scope.ActorID,
		Status: ConversationActive, Title: title, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}, nil
}

// OwnedBy reports whether the conversation is the scope's own: the same
// tenant, project and actor. No role widens it (design 0001 §12.1: operating
// a tenant grants no access to its conversations' bodies).
func (c *Conversation) OwnedBy(scope Scope) bool {
	return c.TenantID == scope.TenantID && c.ProjectID == scope.ProjectID && c.OwnerActorID == scope.ActorID
}

// Update renames, archives or restores the conversation at the expected
// revision. title nil and status empty leave that part unchanged; at least
// one of them must be given.
func (c *Conversation) Update(expected uint64, title *string, status ConversationStatus, now time.Time) error {
	if title == nil && status == "" {
		return fmt.Errorf("%w: an update names a title or a status", ErrInvalid)
	}
	if title != nil {
		if *title == "" {
			return fmt.Errorf("%w: a title cannot be removed, only replaced", ErrInvalid)
		}
		if err := validTitle(*title); err != nil {
			return err
		}
	}
	if status != "" && status != ConversationActive && status != ConversationArchived {
		return fmt.Errorf("%w: conversation status %q", ErrInvalid, status)
	}
	if c.Revision != expected {
		return NewRevisionConflict(expected, c.Revision, "conversation %s is at revision %d, not %d", c.ID, c.Revision, expected)
	}
	if err := bumpable(c.Revision); err != nil {
		return err
	}
	if title != nil {
		c.Title = *title
	}
	if status != "" {
		c.Status = status
	}
	c.Revision++
	c.UpdatedAt = now
	return nil
}

// AppendUserMessage stores the owner's text as the next message at the
// expected revision. A stale revision is a conflict before anything else,
// so a caller that missed an archive rereads and sees it.
func (c *Conversation) AppendUserMessage(expected uint64, text string, now time.Time) (*Message, error) {
	if err := validUserText(text); err != nil {
		return nil, err
	}
	if c.Revision != expected {
		return nil, NewRevisionConflict(expected, c.Revision, "conversation %s is at revision %d, not %d", c.ID, c.Revision, expected)
	}
	if c.Status != ConversationActive {
		return nil, fmt.Errorf("%w: %s: restore the conversation before adding a message", ErrInvalid, ReasonConversationArchived)
	}
	if err := bumpable(c.Revision); err != nil {
		return nil, err
	}
	if err := bumpable(c.LastMessageSeq); err != nil {
		return nil, err
	}
	c.LastMessageSeq++
	c.Revision++
	c.UpdatedAt = now
	return &Message{
		ID: NewID("msg"), TenantID: c.TenantID, ConversationID: c.ID, Seq: c.LastMessageSeq,
		Role: MessageRoleUser, State: MessageCompleted, Text: text, ContentDigest: TextDigest(text),
		ConversationRevision: c.Revision, CreatedAt: now,
	}, nil
}

// Message is one stored message; its text never leaves Control's private
// tables except through an owner's read.
type Message struct {
	ID             string
	TenantID       string
	ConversationID string
	Seq            uint64
	Role           MessageRole
	State          MessageState
	Text           string
	ContentDigest  Digest
	// ConversationRevision is the conversation's revision after this
	// message: the receipt a replay of its command answers.
	ConversationRevision uint64
	CreatedAt            time.Time
}

// TextDigest is the SHA-256 of the text's UTF-8 bytes.
func TextDigest(text string) Digest {
	return Digest(fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(text))))
}

type ConversationCommandKind string

const (
	ConversationCommandCreate ConversationCommandKind = "create"
	ConversationCommandUpdate ConversationCommandKind = "update"
	ConversationCommandAppend ConversationCommandKind = "append_message"
)

// ConversationCommand records one accepted conversation command under its
// (tenant, command id), the identity a retry is answered from.
type ConversationCommand struct {
	TenantID       string
	CommandID      string
	Kind           ConversationCommandKind
	ActorID        string
	RequestDigest  Digest
	ConversationID string
	// MessageID is the stored message of an append.
	MessageID  string
	AcceptedAt time.Time
}

// Replays reports whether cmd repeats this recorded command: the same
// actor, kind, conversation and request digest. Anything else reusing the
// command id is an idempotency conflict.
func (c *ConversationCommand) Replays(cmd CommandIdentity, kind ConversationCommandKind, conversationID string) error {
	if c.ActorID != cmd.ActorID || c.Kind != kind || c.RequestDigest != cmd.RequestDigest ||
		(conversationID != "" && c.ConversationID != conversationID) {
		return fmt.Errorf("%w: command %s was accepted with another request", ErrIdempotencyConflict, cmd.CommandID)
	}
	return nil
}

// ConversationCursor is the position after the last conversation of a
// page: the listing is newest first by creation time, then id, so a page
// never moves under later activity.
type ConversationCursor struct {
	CreatedAt time.Time
	ID        string
}

// Encode is the opaque public form.
func (c ConversationCursor) Encode() string {
	return strconv.FormatInt(c.CreatedAt.UnixMicro(), 10) + "." + c.ID
}

// ParseConversationCursor reads a cursor; empty is the first page.
func ParseConversationCursor(s string) (*ConversationCursor, error) {
	if s == "" {
		return nil, nil
	}
	micros, id, ok := strings.Cut(s, ".")
	v, err := strconv.ParseInt(micros, 10, 64)
	if !ok || err != nil || id == "" || micros != strconv.FormatInt(v, 10) {
		return nil, fmt.Errorf("%w: conversation cursor %q", ErrInvalid, s)
	}
	return &ConversationCursor{CreatedAt: time.UnixMicro(v).UTC(), ID: id}, nil
}

// PageLimit applies a page default and the shared maximum.
func PageLimit(limit, fallback int) (int, error) {
	switch {
	case limit < 0 || limit > MaxHistoryPage:
		return 0, fmt.Errorf("%w: page limit %d is outside 1 to %d", ErrInvalid, limit, MaxHistoryPage)
	case limit == 0:
		return fallback, nil
	}
	return limit, nil
}

func validTitle(title string) error {
	if title == "" {
		return nil
	}
	if !utf8.ValidString(title) || strings.ContainsRune(title, 0) || utf8.RuneCountInString(title) > MaxConversationTitle {
		return fmt.Errorf("%w: a title is 1 to %d characters of UTF-8 text without NUL", ErrInvalid, MaxConversationTitle)
	}
	return nil
}

func validUserText(text string) error {
	if text == "" || len(text) > MaxUserMessageBytes || !utf8.ValidString(text) || strings.ContainsRune(text, 0) {
		return fmt.Errorf("%w: a message is 1 to %d bytes of UTF-8 text without NUL", ErrInvalid, MaxUserMessageBytes)
	}
	return nil
}

// bumpable refuses a sequence or revision that would leave PostgreSQL's
// signed BIGINT (design 0001 §5.2): a controlled refusal, never a wrap.
func bumpable(v uint64) error {
	if v >= math.MaxInt64 {
		return fmt.Errorf("%w: sequence exhausted", ErrInvalid)
	}
	return nil
}
