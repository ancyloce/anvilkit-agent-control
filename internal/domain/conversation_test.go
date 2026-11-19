package domain

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

var conversationOwner = Scope{TenantID: "tenant_a", ProjectID: "project_a", ActorID: "user_a"}

func TestConversationOwnership(t *testing.T) {
	now := time.Date(2026, 11, 19, 2, 30, 0, 0, time.UTC)
	c, err := NewConversation(conversationOwner, "Navigation bar", now)
	if err != nil {
		t.Fatal(err)
	}
	if c.Status != ConversationActive || c.Revision != 1 || c.LastMessageSeq != 0 || !strings.HasPrefix(c.ID, "cnv_") {
		t.Fatalf("new conversation: %+v", c)
	}
	if !c.OwnedBy(conversationOwner) {
		t.Fatal("the creator owns the conversation")
	}
	for name, other := range map[string]Scope{
		"another actor":     {TenantID: "tenant_a", ProjectID: "project_a", ActorID: "user_b"},
		"another project":   {TenantID: "tenant_a", ProjectID: "project_b", ActorID: "user_a"},
		"another tenant":    {TenantID: "tenant_b", ProjectID: "project_a", ActorID: "user_a"},
		"a tenant operator": {TenantID: "tenant_a", ProjectID: "project_a", ActorID: "ops", Roles: []string{RoleOperator, RolePlatformOperator}},
	} {
		if c.OwnedBy(other) {
			t.Fatalf("%s must not own the conversation", name)
		}
	}
	if _, err := NewConversation(Scope{TenantID: "tenant_a"}, "", now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a scope without an actor: %v", err)
	}
}

func TestConversationTitles(t *testing.T) {
	now := time.Now().UTC()
	for name, title := range map[string]string{
		"untitled":       "",
		"200 characters": strings.Repeat("é", MaxConversationTitle),
		"emoji":          "导航栏 🚀",
	} {
		if _, err := NewConversation(conversationOwner, title, now); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	for name, title := range map[string]string{
		"201 characters": strings.Repeat("t", MaxConversationTitle+1),
		"NUL":            "a\x00b",
		"invalid UTF-8":  "a\xffb",
	} {
		if _, err := NewConversation(conversationOwner, title, now); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestConversationUpdate(t *testing.T) {
	now := time.Now().UTC()
	c, _ := NewConversation(conversationOwner, "", now)
	if err := c.Update(1, nil, "", now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("an update without a change: %v", err)
	}
	empty := ""
	if err := c.Update(1, &empty, "", now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("removing a title: %v", err)
	}
	if err := c.Update(1, nil, "deleted", now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("an unknown status: %v", err)
	}
	var conflict *RevisionConflict
	if err := c.Update(2, nil, ConversationArchived, now); !errors.As(err, &conflict) || conflict.Expected != 2 || conflict.Current != 1 {
		t.Fatalf("a stale revision: %v", err)
	}
	title := "Search box"
	later := now.Add(time.Minute)
	if err := c.Update(1, &title, ConversationArchived, later); err != nil {
		t.Fatal(err)
	}
	if c.Title != title || c.Status != ConversationArchived || c.Revision != 2 || !c.UpdatedAt.Equal(later) {
		t.Fatalf("after the update: %+v", c)
	}
	if err := c.Update(2, nil, ConversationActive, later); err != nil || c.Status != ConversationActive || c.Revision != 3 {
		t.Fatalf("restore: %v %+v", err, c)
	}
}

func TestConversationAppendUserMessage(t *testing.T) {
	now := time.Now().UTC()
	c, _ := NewConversation(conversationOwner, "", now)
	m, err := c.AppendUserMessage(1, "Make it blue", now)
	if err != nil {
		t.Fatal(err)
	}
	if m.Seq != 1 || m.ConversationRevision != 2 || m.Role != MessageRoleUser || m.State != MessageCompleted ||
		m.ConversationID != c.ID || m.TenantID != "tenant_a" || m.ContentDigest != TextDigest("Make it blue") {
		t.Fatalf("first message: %+v", m)
	}
	if c.LastMessageSeq != 1 || c.Revision != 2 {
		t.Fatalf("conversation after the message: %+v", c)
	}
	var conflict *RevisionConflict
	if _, err := c.AppendUserMessage(1, "again", now); !errors.As(err, &conflict) {
		t.Fatalf("two sends against one revision: the second conflicts, got %v", err)
	}
	if m2, err := c.AppendUserMessage(2, strings.Repeat("a", MaxUserMessageBytes), now); err != nil || m2.Seq != 2 {
		t.Fatalf("32768 bytes: %v", err)
	}
	for name, text := range map[string]string{
		"empty":           "",
		"32769 bytes":     strings.Repeat("a", MaxUserMessageBytes+1),
		"multi-byte over": strings.Repeat("é", MaxUserMessageBytes/2+1),
		"NUL":             "a\x00",
		"invalid UTF-8":   "\xc3",
	} {
		if _, err := c.AppendUserMessage(3, text, now); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if err := c.Update(3, nil, ConversationArchived, now); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AppendUserMessage(3, "late", now); !errors.As(err, &conflict) {
		t.Fatalf("a stale revision on an archived conversation is a conflict first: %v", err)
	}
	if _, err := c.AppendUserMessage(4, "late", now); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), ReasonConversationArchived) {
		t.Fatalf("an archived conversation refuses a message: %v", err)
	}
	if c.LastMessageSeq != 2 || c.Revision != 4 {
		t.Fatalf("a refused message changes nothing: %+v", c)
	}
	c.Status, c.LastMessageSeq = ConversationActive, math.MaxInt64
	if _, err := c.AppendUserMessage(4, "overflow", now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a sequence at the BIGINT bound: %v", err)
	}
}

func TestConversationCommandReplays(t *testing.T) {
	recorded := &ConversationCommand{TenantID: "tenant_a", CommandID: "cmd_1", Kind: ConversationCommandAppend, ActorID: "user_a", RequestDigest: TextDigest("a"), ConversationID: "cnv_1", MessageID: "msg_1"}
	same := CommandIdentity{TenantID: "tenant_a", CommandID: "cmd_1", ActorID: "user_a", RequestDigest: TextDigest("a")}
	if err := recorded.Replays(same, ConversationCommandAppend, "cnv_1"); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		cmd  CommandIdentity
		kind ConversationCommandKind
		conv string
	}{
		"another digest":       {CommandIdentity{TenantID: "tenant_a", CommandID: "cmd_1", ActorID: "user_a", RequestDigest: TextDigest("b")}, ConversationCommandAppend, "cnv_1"},
		"another actor":        {CommandIdentity{TenantID: "tenant_a", CommandID: "cmd_1", ActorID: "user_b", RequestDigest: TextDigest("a")}, ConversationCommandAppend, "cnv_1"},
		"another kind":         {same, ConversationCommandUpdate, "cnv_1"},
		"another conversation": {same, ConversationCommandAppend, "cnv_2"},
	} {
		if err := recorded.Replays(c.cmd, c.kind, c.conv); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestConversationCursor(t *testing.T) {
	at := time.Date(2026, 11, 19, 2, 30, 0, 123456000, time.UTC)
	c := ConversationCursor{CreatedAt: at, ID: "cnv_ABC"}
	back, err := ParseConversationCursor(c.Encode())
	if err != nil || !back.CreatedAt.Equal(at) || back.ID != "cnv_ABC" {
		t.Fatalf("round trip: %v %+v", err, back)
	}
	if first, err := ParseConversationCursor(""); err != nil || first != nil {
		t.Fatalf("empty cursor is the first page: %v %v", first, err)
	}
	for _, bad := range []string{"cnv_ABC", "x.cnv_ABC", "1.", "01.cnv", "99999999999999999999.cnv"} {
		if _, err := ParseConversationCursor(bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%q: %v", bad, err)
		}
	}
	for limit, want := range map[int]int{0: DefaultMessagePage, 1: 1, MaxHistoryPage: MaxHistoryPage} {
		if got, err := PageLimit(limit, DefaultMessagePage); err != nil || got != want {
			t.Fatalf("limit %d: %d %v", limit, got, err)
		}
	}
	for _, bad := range []int{-1, MaxHistoryPage + 1} {
		if _, err := PageLimit(bad, DefaultMessagePage); !errors.Is(err, ErrInvalid) {
			t.Fatalf("limit %d: %v", bad, err)
		}
	}
}
