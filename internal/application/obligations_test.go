package application_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// An inventory object is reconciled only as the obligation it was
// published under, with every minimum binding of its class (DD-02 §5): a
// record without its tenant, identity or recording time is an integrity
// failure and never an out-of-scope record that disappears.
func TestDecodeObligationRequiresBindings(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	intake := map[string]any{
		"class": "intake", "commandId": "cmd_1", "operationId": "op_1", "kind": "local_check", "tenantId": "tenant_a", "profileId": "local-check-v1",
		"subjectDigest": string(subject.SubjectDigest), "requestDigest": string(subject.SubjectDigest), "createdAt": now, "executionEpoch": 1,
	}
	body, _ := json.Marshal(intake)
	ob, err := application.DecodeObligation("intake/op_1", body)
	require.NoError(t, err)
	require.Equal(t, "tenant_a", ob.TenantID)
	require.False(t, ob.RecordedAt.IsZero())

	for _, field := range []string{"tenantId", "commandId", "requestDigest", "createdAt"} {
		partial := map[string]any{}
		for k, v := range intake {
			if k != field {
				partial[k] = v
			}
		}
		body, _ := json.Marshal(partial)
		_, err := application.DecodeObligation("intake/op_1", body)
		require.ErrorIs(t, err, domain.ErrIntegrity, field)
		require.Contains(t, err.Error(), field)
		require.NotContains(t, err.Error(), "intake/op_1", "the raw key never enters the diagnostic")
	}
	_, err = application.DecodeObligation("intake/op_2", body)
	require.ErrorIs(t, err, domain.ErrIntegrity, "an identity other than the key's is never reconciled as the key")
	_, err = application.DecodeObligation("outcome/model-dispatch/dsp_1", body)
	require.ErrorIs(t, err, domain.ErrInvalid, "attestations are not obligations")

	launch := map[string]any{
		"class": "job-launch", "launchId": "lch_1", "attemptId": "att_1", "operationId": "op_1", "tenantId": "tenant_a", "launchKey": "lc-1", "backend": "kind",
		"profileId": "local-check-v1", "imageDigest": string(imageDig), "executionEpoch": 1, "launchEpoch": 1, "deadline": now, "createdAt": now,
	}
	body, _ = json.Marshal(launch)
	_, err = application.DecodeObligation("job-launch/lch_1", body)
	require.NoError(t, err)
	delete(launch, "launchEpoch")
	body, _ = json.Marshal(launch)
	_, err = application.DecodeObligation("job-launch/lch_1", body)
	require.ErrorIs(t, err, domain.ErrIntegrity)
	require.Contains(t, err.Error(), "launchEpoch")

	dispatch := map[string]any{
		"class": "tool-dispatch", "dispatchId": "dsp_1", "callId": "call_1", "owner": "mcp", "tenantId": "tenant_a", "operationId": "op_1", "attemptId": "att_1",
		"requestDigest": string(subject.SubjectDigest), "currency": "USD", "exposure": "10000", "meterRevision": "fx-tool-1", "executionEpoch": 1, "deadline": now, "admittedAt": now,
	}
	body, _ = json.Marshal(dispatch)
	_, err = application.DecodeObligation("tool-dispatch/dsp_1", body)
	require.ErrorIs(t, err, domain.ErrIntegrity, "a tool dispatch binds its grant")
	require.Contains(t, err.Error(), "grantId")
	dispatch["grantId"] = "grant_1"
	body, _ = json.Marshal(dispatch)
	_, err = application.DecodeObligation("tool-dispatch/dsp_1", body)
	require.NoError(t, err)

	require.Equal(t, "intake object op_1", application.DescribeKey("intake/op_1"))
	require.Equal(t, "outcome object dsp_1", application.DescribeKey("outcome/model-dispatch/dsp_1"))
	require.Equal(t, "object of class qualification", application.DescribeKey("qualification"))
}
