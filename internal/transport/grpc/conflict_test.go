package grpc

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// RB-A23: a revision conflict that knows both revisions answers Aborted with
// the unchanged message and a google.rpc.ErrorInfo detail naming the expected
// and the current revision as canonical decimal strings; a conflict that
// cannot know the current revision (a concurrent write) carries none.
func TestRevisionConflictCarriesBothRevisions(t *testing.T) {
	info := func(t *testing.T, err error) (*status.Status, *errdetails.ErrorInfo) {
		t.Helper()
		st, ok := status.FromError(toStatus(err))
		require.True(t, ok)
		require.Equal(t, codes.Aborted, st.Code())
		var found *errdetails.ErrorInfo
		for _, d := range st.Details() {
			if ei, ok := d.(*errdetails.ErrorInfo); ok {
				require.Nil(t, found, "one ErrorInfo")
				found = ei
			}
		}
		return st, found
	}

	op := &domain.Operation{ID: "op_1", Revision: 5, Lifecycle: domain.LifecycleWaiting, Control: domain.ControlNone}
	_, cancelErr := op.DecideCancel(3, true)
	_, controlErr := op.DecideControl(domain.Profile{}, domain.CommandHold, 18446744073709551615, "", time.Now())
	qs := &domain.QuestionSet{ID: "qs_1", OperationID: "op_1", Revision: 2}
	_, answerErr := domain.NewAnswer(op, qs, nil, domain.CommandIdentity{}, 1, domain.ArtifactBinding{}, time.Now())

	for _, tc := range []struct {
		name              string
		err               error
		message           string
		expected, current string
	}{
		{"a stale cancel", cancelErr, "REVISION_CONFLICT: revision conflict: expected 3, current 5", "3", "5"},
		{"a stale control command at the uint64 maximum", controlErr, "REVISION_CONFLICT: revision conflict: expected 18446744073709551615, current 5", "18446744073709551615", "5"},
		{"an answer to an older question set", answerErr, "REVISION_CONFLICT: revision conflict: question set qs_1 is at revision 2, the answer names 1", "1", "2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, tc.err, domain.ErrRevisionConflict)
			var rc *domain.RevisionConflict
			require.ErrorAs(t, tc.err, &rc)
			st, ei := info(t, tc.err)
			require.Equal(t, tc.message, st.Message(), "the message is the one the plain error had")
			require.NotNil(t, ei)
			require.Equal(t, ConflictReason, ei.GetReason())
			require.Equal(t, ConflictDomain, ei.GetDomain())
			require.Equal(t, map[string]string{ConflictExpectedKey: tc.expected, ConflictCurrentKey: tc.current}, ei.GetMetadata())
		})
	}

	t.Run("a concurrent conflict knows no current revision and carries no detail", func(t *testing.T) {
		st, ei := info(t, fmt.Errorf("%w: release of op_1 changed concurrently", domain.ErrRevisionConflict))
		require.Equal(t, "REVISION_CONFLICT: revision conflict: release of op_1 changed concurrently", st.Message())
		require.Nil(t, ei)
	})

	t.Run("a wrapped conflict keeps its revisions", func(t *testing.T) {
		_, ei := info(t, fmt.Errorf("submit command: %w", cancelErr))
		require.NotNil(t, ei)
		require.Equal(t, "3", ei.GetMetadata()[ConflictExpectedKey])
		require.True(t, errors.Is(fmt.Errorf("x: %w", cancelErr), domain.ErrRevisionConflict))
	})
}
