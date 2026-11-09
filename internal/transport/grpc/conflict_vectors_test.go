package grpc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// TestRevisionConflictDetailMatchesTheContractVectors is the producer side
// of the conflict-detail contract (RB-A23). testdata/status-details.fixtures.json
// is a verbatim copy of the contracts repository's
// proto/status-details.fixtures.json (the parent's tools/check-contracts.py
// compares the bytes): for every case the contract says Control produces, a
// revision conflict with those revisions answers the contract's code, its
// unchanged message and exactly one google.rpc.ErrorInfo equal to the
// vector's detail, with nothing beside it. The API's consumer test reads the
// same file.
func TestRevisionConflictDetailMatchesTheContractVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "status-details.fixtures.json"))
	require.NoError(t, err)
	var vectors struct {
		ErrorInfo struct {
			Code, MessagePrefix, Reason, Domain, ExpectedKey, CurrentKey string
		} `json:"errorInfo"`
		Cases []struct {
			Name      string            `json:"name"`
			Detail    json.RawMessage   `json:"detail"`
			Revisions map[string]string `json:"revisions"`
			Produced  bool              `json:"produced"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &vectors))
	info := vectors.ErrorInfo
	// The convention's constants are the producer's.
	require.Equal(t, "ABORTED", info.Code, "the code asserted below is codes.Aborted")
	require.Equal(t, ConflictReason+": ", info.MessagePrefix)
	require.Equal(t, ConflictReason, info.Reason)
	require.Equal(t, ConflictDomain, info.Domain)
	require.Equal(t, ConflictExpectedKey, info.ExpectedKey)
	require.Equal(t, ConflictCurrentKey, info.CurrentKey)

	produced := 0
	for _, cs := range vectors.Cases {
		if !cs.Produced {
			continue
		}
		produced++
		t.Run(cs.Name, func(t *testing.T) {
			expected, err := strconv.ParseUint(cs.Revisions[info.ExpectedKey], 10, 64)
			require.NoError(t, err)
			current, err := strconv.ParseUint(cs.Revisions[info.CurrentKey], 10, 64)
			require.NoError(t, err)
			var want errdetails.ErrorInfo
			require.NoError(t, (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(cs.Detail, &want))

			conflict := domain.NewRevisionConflict(expected, current, "expected %d, current %d", expected, current)
			st, ok := status.FromError(toStatus(conflict))
			require.True(t, ok)
			require.Equal(t, codes.Aborted, st.Code())
			// The message is the one a conflict without the detail carries: a
			// peer that ignores the detail reads what it always read.
			plain, _ := status.FromError(toStatus(fmt.Errorf("%w: expected %d, current %d", domain.ErrRevisionConflict, expected, current)))
			require.Equal(t, info.MessagePrefix+conflict.Error(), st.Message())
			require.Equal(t, plain.Message(), st.Message(), "the detail does not change the message")
			require.Equal(t, plain.Code(), st.Code(), "nor the code")
			require.Empty(t, plain.Details(), "a conflict that does not know both revisions carries no detail")

			details := st.Details()
			require.Len(t, details, 1, "exactly one detail")
			got, isInfo := details[0].(*errdetails.ErrorInfo)
			require.True(t, isInfo, "a google.rpc.ErrorInfo, not %T", details[0])
			require.True(t, proto.Equal(&want, got), "produced %v, the contract's vector is %v", got, &want)
		})
	}
	require.Positive(t, produced, "the contract names at least one produced case")
}
