package modelproxy_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/modelproxy"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

type innerQuery struct{ calls atomic.Int32 }

func (i *innerQuery) QueryDispatch(context.Context, *domain.Dispatch) (application.DispatchOutcomeReport, error) {
	i.calls.Add(1)
	return application.DispatchOutcomeReport{Known: true, Source: "query:double", Sequence: 1, Outcome: domain.DispatchFailed, ObservedAt: time.Now()}, nil
}

func (i *innerQuery) QueryEffect(context.Context, *domain.Effect) (application.EffectOutcomeReport, error) {
	i.calls.Add(1)
	return application.EffectOutcomeReport{}, nil
}

func TestQueryDispatchReadsTheProxyRecord(t *testing.T) {
	var auth atomic.Value
	records := map[string]string{
		"call_ok":      `{"callId":"call_ok","routeId":"r","state":"succeeded","dispatchId":"dsp_ok","usage":{"inputUnits":"12","outputUnits":"5","reasoningUnits":"0","cachedInputUnits":"2"},"nativeReference":"chatcmpl-1","createdAt":"2026-09-16T12:00:00Z","updatedAt":"2026-09-16T12:00:09.500123Z"}`,
		"call_unknown": `{"callId":"call_unknown","routeId":"r","state":"unknown","dispatchId":"dsp_unknown","errorCode":"CANCELED","createdAt":"2026-09-16T12:00:00Z","updatedAt":"2026-09-16T12:00:09Z"}`,
		"call_sending": `{"callId":"call_sending","routeId":"r","state":"sending","dispatchId":"dsp_sending","createdAt":"2026-09-16T12:00:00Z","updatedAt":"2026-09-16T12:00:01Z"}`,
		"call_other":   `{"callId":"call_other","routeId":"r","state":"succeeded","dispatchId":"dsp_elsewhere","createdAt":"2026-09-16T12:00:00Z","updatedAt":"2026-09-16T12:00:01Z"}`,
		"call_raw":     `{"callId":"call_raw","routeId":"r","state":"succeeded","dispatchId":"dsp_raw","createdAt":"2026-09-16T12:00:00Z","updatedAt":"2026-09-16T12:00:01Z","raw":{"provider":"body"}}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		if r.Method != http.MethodGet {
			w.WriteHeader(405)
			return
		}
		body, ok := records[r.URL.Path[len("/api/v1/model-calls/"):]]
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":{"code":"NOT_FOUND","message":"no call","requestId":"req_1","retryable":false}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	inner := &innerQuery{}
	q, err := modelproxy.New(modelproxy.Options{BaseURL: srv.URL, Token: "control-token", Owner: "anvilkit-agent-model-proxy"}, inner)
	require.NoError(t, err)
	model := func(callID, dispatchID string) *domain.Dispatch {
		return &domain.Dispatch{ID: dispatchID, Kind: domain.DispatchModel, Owner: "anvilkit-agent-model-proxy", CallID: callID}
	}

	report, err := q.QueryDispatch(t.Context(), model("call_ok", "dsp_ok"))
	require.NoError(t, err)
	require.True(t, report.Known)
	require.Equal(t, modelproxy.Source, report.Source)
	require.Equal(t, domain.DispatchSucceeded, report.Outcome)
	require.Equal(t, &domain.Usage{Input: 12, Output: 5, Reasoning: 0, CachedInput: 2}, report.Usage)
	require.Equal(t, "chatcmpl-1", report.NativeReference)
	require.Equal(t, uint64(time.Date(2026, 9, 16, 12, 0, 9, 500123000, time.UTC).UnixMicro()), report.Sequence, "the sequence is the record's revision stamp")
	require.Equal(t, "Bearer control-token", auth.Load())

	unknown, err := q.QueryDispatch(t.Context(), model("call_unknown", "dsp_unknown"))
	require.NoError(t, err)
	require.True(t, unknown.Known)
	require.Equal(t, domain.DispatchOutcomeUnknown, unknown.Outcome)
	require.Nil(t, unknown.Usage, "no usage stays absent")

	sending, err := q.QueryDispatch(t.Context(), model("call_sending", "dsp_sending"))
	require.NoError(t, err)
	require.False(t, sending.Known, "an in-flight send establishes nothing")
	missing, err := q.QueryDispatch(t.Context(), model("call_missing", "dsp_missing"))
	require.NoError(t, err)
	require.False(t, missing.Known)
	_, err = q.QueryDispatch(t.Context(), model("call_other", "dsp_other"))
	require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "a record of another dispatch is not evidence of this one")
	_, err = q.QueryDispatch(t.Context(), model("call_raw", "dsp_raw"))
	require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "a record outside the contract is not evidence")

	require.Zero(t, inner.calls.Load())
	tool := &domain.Dispatch{ID: "dsp_tool", Kind: domain.DispatchTool, Owner: "mcp", CallID: "call_tool"}
	other, err := q.QueryDispatch(t.Context(), tool)
	require.NoError(t, err)
	require.Equal(t, "query:double", other.Source, "tool dispatches and other owners stay with the inner query")
	_, err = q.QueryEffect(t.Context(), &domain.Effect{ID: "eff_1"})
	require.NoError(t, err)
	require.Equal(t, int32(2), inner.calls.Load())

	srv.Close()
	_, err = q.QueryDispatch(t.Context(), model("call_ok", "dsp_ok"))
	require.Error(t, err, "an unreachable Proxy establishes nothing")
}
