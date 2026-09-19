package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLateControlReceiptPreservesCancellationAndTerminalState(t *testing.T) {
	for _, lifecycle := range []Lifecycle{LifecycleRunning, LifecycleReconciling, LifecycleCanceled, LifecycleSucceeded, LifecycleFailed} {
		for _, kind := range []CommandKind{CommandHold, CommandResume, CommandChangeDefinition} {
			for _, outcome := range []CommandOutcome{OutcomeApplied, OutcomeRejected, OutcomeBlocked} {
				t.Run(string(lifecycle)+"/"+string(kind)+"/"+string(outcome), func(t *testing.T) {
					op := Operation{Lifecycle: lifecycle, Control: ControlCancelPending, Phase: "cancel_pending", DefinitionActivation: "original"}
					before := op
					op.ApplyControl(kind, outcome, "replacement")
					require.Equal(t, before, op)
					if lifecycle.Terminal() {
						op.Control = ControlNone
						before = op
						op.ApplyControl(kind, outcome, "replacement")
						require.Equal(t, before, op)
					}
				})
			}
		}
	}
}
