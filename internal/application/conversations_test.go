package application_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

var (
	convAlice        = domain.Scope{TenantID: "tenant_conv", ProjectID: "project_conv", ActorID: "user_alice"}
	convBob          = domain.Scope{TenantID: "tenant_conv", ProjectID: "project_conv", ActorID: "user_bob"}
	convOtherProject = domain.Scope{TenantID: "tenant_conv", ProjectID: "project_other", ActorID: "user_alice"}
	convOtherTenant  = domain.Scope{TenantID: "tenant_other", ProjectID: "project_conv", ActorID: "user_alice"}
	convOperator     = domain.Scope{TenantID: "tenant_conv", ProjectID: "project_conv", ActorID: "user_ops", Roles: []string{domain.RoleOperator, domain.RolePlatformOperator}}
)

func convCmd(s domain.Scope, id, body string) domain.CommandIdentity {
	return domain.CommandIdentity{TenantID: s.TenantID, CommandID: id, ActorID: s.ActorID, RequestDigest: application.DigestOf([]byte(body))}
}

// TestConversations (DEC-067) proves the first slice's storage rules on a
// real PostgreSQL: reentrant commands, owner-only visibility, one accepted
// send per revision under concurrency, the archive rule and bounded history.
func TestConversations(t *testing.T) {
	inst := testdb.Start(t)
	conversations := application.NewConversations(postgres.NewStore(inst.Pool(t)), domain.SystemClock{}, testLog)
	ctx := context.Background()

	conv, existing, err := conversations.Create(ctx, convCmd(convAlice, "conv_create", "title"), convAlice, "Navigation bar")
	require.NoError(t, err)
	require.False(t, existing)
	require.Equal(t, uint64(1), conv.Revision)
	require.Equal(t, "project_conv", conv.ProjectID)
	again, existing, err := conversations.Create(ctx, convCmd(convAlice, "conv_create", "title"), convAlice, "Navigation bar")
	require.NoError(t, err)
	require.True(t, existing, "the same command answers the existing conversation")
	require.Equal(t, conv.ID, again.ID)
	_, _, err = conversations.Create(ctx, convCmd(convAlice, "conv_create", "other title"), convAlice, "Other")
	require.ErrorIs(t, err, domain.ErrIdempotencyConflict)
	_, _, err = conversations.Create(ctx, convCmd(convBob, "conv_create", "title"), convBob, "Navigation bar")
	require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "another actor reusing the command id learns nothing more")
	_, _, err = conversations.Create(ctx, convCmd(convBob, "conv_forged", "x"), convAlice, "")
	require.ErrorIs(t, err, domain.ErrForbidden, "a command of another actor than the scope")

	t.Run("owner only", func(t *testing.T) {
		got, err := conversations.Get(ctx, convAlice, conv.ID)
		require.NoError(t, err)
		require.Equal(t, "Navigation bar", got.Title)
		for name, other := range map[string]domain.Scope{"another actor": convBob, "another project": convOtherProject, "another tenant": convOtherTenant, "an operator": convOperator} {
			_, err := conversations.Get(ctx, other, conv.ID)
			require.ErrorIs(t, err, domain.ErrNotFound, name)
			_, _, err = conversations.Messages(ctx, other, conv.ID, 0, 0)
			require.ErrorIs(t, err, domain.ErrNotFound, name)
			_, _, err = conversations.Append(ctx, convCmd(other, "conv_intrude_"+other.ActorID+other.ProjectID+other.TenantID, "x"), other, conv.ID, 1, "x")
			require.ErrorIs(t, err, domain.ErrNotFound, name)
			page, _, err := conversations.List(ctx, other, "", "", 0)
			require.NoError(t, err)
			for _, c := range page {
				require.NotEqual(t, conv.ID, c.ID, name)
			}
		}
	})

	t.Run("append and replay", func(t *testing.T) {
		m, existing, err := conversations.Append(ctx, convCmd(convAlice, "conv_msg_1", "blue"), convAlice, conv.ID, 1, "Make it blue")
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, uint64(1), m.Seq)
		require.Equal(t, uint64(2), m.ConversationRevision)
		replay, existing, err := conversations.Append(ctx, convCmd(convAlice, "conv_msg_1", "blue"), convAlice, conv.ID, 1, "Make it blue")
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, m.ID, replay.ID)
		require.Equal(t, uint64(2), replay.ConversationRevision, "the replay answers the original receipt")
		_, _, err = conversations.Append(ctx, convCmd(convAlice, "conv_msg_1", "black"), convAlice, conv.ID, 2, "Make it black")
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict)
		_, _, err = conversations.Append(ctx, convCmd(convAlice, "conv_msg_stale", "stale"), convAlice, conv.ID, 1, "stale")
		var conflict *domain.RevisionConflict
		require.True(t, errors.As(err, &conflict), "%v", err)
		require.Equal(t, uint64(1), conflict.Expected)
		require.Equal(t, uint64(2), conflict.Current)
		page, next, err := conversations.Messages(ctx, convAlice, conv.ID, 0, 0)
		require.NoError(t, err)
		require.Len(t, page, 1)
		require.Zero(t, next)
		require.Equal(t, "Make it blue", page[0].Text)
		require.Equal(t, domain.TextDigest("Make it blue"), page[0].ContentDigest)
	})

	t.Run("one send per revision", func(t *testing.T) {
		c, _, err := conversations.Create(ctx, convCmd(convAlice, "conv_race", "race"), convAlice, "")
		require.NoError(t, err)
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i := range errs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _, errs[i] = conversations.Append(ctx, convCmd(convAlice, fmt.Sprintf("conv_race_%d", i), fmt.Sprint(i)), convAlice, c.ID, 1, fmt.Sprint("send ", i))
			}(i)
		}
		wg.Wait()
		ok, conflicts := 0, 0
		for _, err := range errs {
			switch {
			case err == nil:
				ok++
			case errors.Is(err, domain.ErrRevisionConflict):
				conflicts++
			default:
				t.Fatalf("unexpected: %v", err)
			}
		}
		require.Equal(t, 1, ok, "AT-06: one send is accepted")
		require.Equal(t, 1, conflicts, "the other conflicts and nothing is queued")

		ids := make([]string, 2)
		for i := range ids {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				var m *domain.Message
				if m, _, errs[i] = conversations.Append(ctx, convCmd(convAlice, "conv_race_same", "same"), convAlice, c.ID, 2, "the same command twice"); errs[i] == nil {
					ids[i] = m.ID
				}
			}(i)
		}
		wg.Wait()
		require.NoError(t, errors.Join(errs...))
		require.Equal(t, ids[0], ids[1], "AT-02: the same command from two callers stores one message")
		page, _, err := conversations.Messages(ctx, convAlice, c.ID, 0, 0)
		require.NoError(t, err)
		require.Len(t, page, 2)
	})

	t.Run("archive and restore", func(t *testing.T) {
		current, err := conversations.Get(ctx, convAlice, conv.ID)
		require.NoError(t, err)
		_, _, err = conversations.Update(ctx, convCmd(convAlice, "conv_noop", "noop"), convAlice, conv.ID, current.Revision, nil, "")
		require.ErrorIs(t, err, domain.ErrInvalid, "an update names a change")
		archived, existing, err := conversations.Update(ctx, convCmd(convAlice, "conv_archive", "archive"), convAlice, conv.ID, current.Revision, nil, domain.ConversationArchived)
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.ConversationArchived, archived.Status)
		_, _, err = conversations.Append(ctx, convCmd(convAlice, "conv_msg_archived", "late"), convAlice, conv.ID, archived.Revision, "late")
		require.ErrorIs(t, err, domain.ErrInvalid)
		require.ErrorContains(t, err, domain.ReasonConversationArchived)
		replay, existing, err := conversations.Append(ctx, convCmd(convAlice, "conv_msg_1", "blue"), convAlice, conv.ID, 1, "Make it blue")
		require.NoError(t, err, "a replay after the archive still answers the original receipt")
		require.True(t, existing)
		require.Equal(t, uint64(1), replay.Seq)
		_, _, err = conversations.Update(ctx, convCmd(convBob, "conv_bob_restore", "restore"), convBob, conv.ID, archived.Revision, nil, domain.ConversationActive)
		require.ErrorIs(t, err, domain.ErrNotFound)
		title := "Dark navigation bar"
		restored, _, err := conversations.Update(ctx, convCmd(convAlice, "conv_restore", "restore"), convAlice, conv.ID, archived.Revision, &title, domain.ConversationActive)
		require.NoError(t, err)
		require.Equal(t, domain.ConversationActive, restored.Status)
		require.Equal(t, title, restored.Title)
		again, existing, err := conversations.Update(ctx, convCmd(convAlice, "conv_restore", "restore"), convAlice, conv.ID, archived.Revision, &title, domain.ConversationActive)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, restored.Revision, again.Revision)
		m, _, err := conversations.Append(ctx, convCmd(convAlice, "conv_msg_2", "after"), convAlice, conv.ID, restored.Revision, "And add a search box")
		require.NoError(t, err)
		require.Equal(t, uint64(2), m.Seq)
	})

	t.Run("history pages", func(t *testing.T) {
		c, _, err := conversations.Create(ctx, convCmd(convAlice, "conv_history", "history"), convAlice, "")
		require.NoError(t, err)
		rev := c.Revision
		for i := 1; i <= 35; i++ {
			m, _, err := conversations.Append(ctx, convCmd(convAlice, fmt.Sprintf("conv_history_%d", i), fmt.Sprint(i)), convAlice, c.ID, rev, fmt.Sprintf("message %d", i))
			require.NoError(t, err)
			rev = m.ConversationRevision
		}
		page, next, err := conversations.Messages(ctx, convAlice, c.ID, 0, 0)
		require.NoError(t, err)
		require.Len(t, page, domain.DefaultMessagePage)
		require.Equal(t, uint64(6), page[0].Seq, "the newest page, ascending")
		require.Equal(t, uint64(35), page[len(page)-1].Seq)
		require.Equal(t, uint64(6), next)
		older, next, err := conversations.Messages(ctx, convAlice, c.ID, next, 0)
		require.NoError(t, err)
		require.Len(t, older, 5)
		require.Equal(t, uint64(1), older[0].Seq)
		require.Zero(t, next, "no page before the first message")
		_, _, err = conversations.Messages(ctx, convAlice, c.ID, 0, domain.MaxHistoryPage+1)
		require.ErrorIs(t, err, domain.ErrInvalid)

		big, _, err := conversations.Create(ctx, convCmd(convAlice, "conv_big", "big"), convAlice, "")
		require.NoError(t, err)
		rev = big.Revision
		text := strings.Repeat("a", domain.MaxUserMessageBytes)
		for i := 1; i <= 40; i++ {
			m, _, err := conversations.Append(ctx, convCmd(convAlice, fmt.Sprintf("conv_big_%d", i), fmt.Sprint(i)), convAlice, big.ID, rev, text)
			require.NoError(t, err)
			rev = m.ConversationRevision
		}
		page, next, err = conversations.Messages(ctx, convAlice, big.ID, 0, domain.MaxHistoryPage)
		require.NoError(t, err)
		require.Len(t, page, domain.MessagePageTextBudget/domain.MaxUserMessageBytes, "the page ends at its text budget")
		require.Equal(t, uint64(40), page[len(page)-1].Seq)
		require.Equal(t, page[0].Seq, next)
	})

	t.Run("listing", func(t *testing.T) {
		owner := domain.Scope{TenantID: "tenant_list", ProjectID: "project_list", ActorID: "user_list"}
		made := map[string]bool{}
		for i := 0; i < 5; i++ {
			c, _, err := conversations.Create(ctx, convCmd(owner, fmt.Sprintf("conv_list_%d", i), fmt.Sprint(i)), owner, "")
			require.NoError(t, err)
			made[c.ID] = true
		}
		first, err := conversations.Get(ctx, owner, firstKey(made))
		require.NoError(t, err)
		_, _, err = conversations.Update(ctx, convCmd(owner, "conv_list_archive", "a"), owner, first.ID, first.Revision, nil, domain.ConversationArchived)
		require.NoError(t, err)

		seen := map[string]bool{}
		var last *domain.Conversation
		cursor := ""
		for pages := 0; ; pages++ {
			require.Less(t, pages, 5)
			page, next, err := conversations.List(ctx, owner, "", cursor, 2)
			require.NoError(t, err)
			for _, c := range page {
				require.False(t, seen[c.ID], "a conversation appears once")
				seen[c.ID] = true
				if last != nil {
					require.True(t, c.CreatedAt.Before(last.CreatedAt) || (c.CreatedAt.Equal(last.CreatedAt) && c.ID < last.ID), "newest first")
				}
				last = c
			}
			if next == "" {
				break
			}
			cursor = next
		}
		require.Equal(t, made, seen)
		archived, _, err := conversations.List(ctx, owner, domain.ConversationArchived, "", 0)
		require.NoError(t, err)
		require.Len(t, archived, 1)
		require.Equal(t, first.ID, archived[0].ID)
		_, _, err = conversations.List(ctx, owner, "", "not-a-cursor", 0)
		require.ErrorIs(t, err, domain.ErrInvalid)
	})
}

func firstKey(m map[string]bool) string {
	for k := range m {
		return k
	}
	return ""
}
