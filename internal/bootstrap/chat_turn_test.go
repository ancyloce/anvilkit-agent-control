package bootstrap

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/config"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// TestChatTurnProfile (DEC-068): the chat_turn profile is non-queued like a
// preparation (no execution queue, no permit), funded per turn with no
// credit, bounded by its deadline and attempt window, listed whether or not
// new turns are enabled, and started as the ChatTurnWorkflow.
func TestChatTurnProfile(t *testing.T) {
	var cfg config.Config
	cfg.Profiles.ChatTurn = config.ChatTurnProfile{Deadline: 150 * time.Second, AttemptWindow: 120 * time.Second,
		Funding: config.MoneyValue{Currency: "USD", Amount: "100000"}}
	p := chatTurnProfile(cfg)
	require.Equal(t, domain.KindChatTurn, p.Kind)
	require.Equal(t, domain.ChatTurnStepID, p.StepID)
	require.True(t, p.MultiStep, "settled through SettleOperation, like a preparation")
	require.Empty(t, p.QueuePool, "no execution queue: a chat turn holds no permit")
	require.Zero(t, p.Credits)
	require.NotNil(t, p.Funding)
	require.Equal(t, int64(100000), p.Funding.Amount)
	require.Equal(t, 150*time.Second, p.OperationDeadline)
	require.Equal(t, 120*time.Second, p.AttemptWindow)
	require.Equal(t, ChatTurnWorkflowName, WorkflowNames()[domain.KindChatTurn])
}
