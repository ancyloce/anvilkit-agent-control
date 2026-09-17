// Package modelproxy is Control's original-identity query of model
// dispatches (DD-02 §4 "query original ID", DD-06 §2 query-by-identity;
// delivery.md P11): the Model Proxy's GET /api/v1/model-calls/{callId}
// answers what the sender knows about a call — its terminal state, native
// usage and reference — and never sends. The answer is recorded through the
// dispatch observation path under the source query:model-proxy and a
// sequence derived from the record's own revision stamp, so a repeated
// query is one observation and a moved record a supplemental one. A call
// the Proxy does not know, or one still in flight, establishes nothing.
package modelproxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-agent-contracts/go/modelproxyapi"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Source is the observation source of answers of the Proxy's query.
const Source = "query:model-proxy"

// Options of the query client.
type Options struct {
	BaseURL string
	Token   string
	TLS     *TLSFiles
	Timeout time.Duration
	// Owner is the sender identity the Proxy records on its dispatches.
	Owner string
}

// TLSFiles is the workload identity material.
type TLSFiles struct{ CertFile, KeyFile, CAFile, ServerName string }

// OutcomeQuery answers model dispatches of the Proxy's owner from the Proxy
// and everything else from the inner query (the DEVELOPMENT_ONLY double of
// the Pagix declaration, ENV-07).
type OutcomeQuery struct {
	api     *modelproxyapi.Client
	owner   string
	timeout time.Duration
	inner   application.OutcomeQuery
}

var _ application.OutcomeQuery = (*OutcomeQuery)(nil)

func New(o Options, inner application.OutcomeQuery) (*OutcomeQuery, error) {
	if o.BaseURL == "" || o.Owner == "" {
		return nil, errors.New("model proxy query: base url and owner are required")
	}
	if (o.Token == "") == (o.TLS == nil) {
		return nil, errors.New("model proxy query: exactly one of a bearer token (development) or the mtls identity is required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if o.TLS != nil {
		cert, err := tls.LoadX509KeyPair(o.TLS.CertFile, o.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("model proxy query: client certificate: %w", err)
		}
		ca, err := os.ReadFile(o.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("model proxy query: ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, errors.New("model proxy query: ca file holds no certificate")
		}
		transport.TLSClientConfig = &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: o.TLS.ServerName, MinVersion: tls.VersionTLS13}
	}
	httpClient := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	opts := []modelproxyapi.ClientOption{modelproxyapi.WithHTTPClient(httpClient)}
	if o.Token != "" {
		token := o.Token
		opts = append(opts, modelproxyapi.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
			req.Header.Set("Authorization", "Bearer "+token)
			return nil
		}))
	}
	api, err := modelproxyapi.NewClient(strings.TrimRight(o.BaseURL, "/")+"/api/v1", opts...)
	if err != nil {
		return nil, err
	}
	timeout := o.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &OutcomeQuery{api: api, owner: o.Owner, timeout: timeout, inner: inner}, nil
}

// QueryDispatch asks the Proxy about a model dispatch it owns.
func (q *OutcomeQuery) QueryDispatch(ctx context.Context, d *domain.Dispatch) (application.DispatchOutcomeReport, error) {
	if d.Kind != domain.DispatchModel || d.Owner != q.owner {
		return q.inner.QueryDispatch(ctx, d)
	}
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()
	resp, err := q.api.GetModelCall(ctx, d.CallID)
	if err != nil {
		return application.DispatchOutcomeReport{}, fmt.Errorf("model proxy query of call %s: %w", d.CallID, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return application.DispatchOutcomeReport{}, nil // the Proxy has no record: nothing is established
	default:
		return application.DispatchOutcomeReport{}, fmt.Errorf("model proxy query of call %s answered %d", d.CallID, resp.StatusCode)
	}
	var m modelproxyapi.ModelCall
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return application.DispatchOutcomeReport{}, fmt.Errorf("%w: model proxy answered a call record outside the contract: %v", domain.ErrEvidenceInsufficient, err)
	}
	if m.CallId != d.CallID || m.DispatchId != d.ID {
		return application.DispatchOutcomeReport{}, fmt.Errorf("%w: model proxy answered call %s of dispatch %s for call %s of dispatch %s", domain.ErrEvidenceInsufficient, m.CallId, m.DispatchId, d.CallID, d.ID)
	}
	var outcome domain.DispatchOutcome
	switch m.State {
	case modelproxyapi.ModelCallStateSucceeded:
		outcome = domain.DispatchSucceeded
	case modelproxyapi.ModelCallStateFailed:
		outcome = domain.DispatchFailed
	case modelproxyapi.ModelCallStateCanceled:
		outcome = domain.DispatchCanceled
	case modelproxyapi.ModelCallStateUnknown:
		outcome = domain.DispatchOutcomeUnknown
	default:
		// admitting, sending or denied: no outcome the sender can vouch for yet.
		return application.DispatchOutcomeReport{}, nil
	}
	observedAt, err := time.Parse(time.RFC3339Nano, m.UpdatedAt)
	if err != nil {
		return application.DispatchOutcomeReport{}, fmt.Errorf("%w: model proxy record of call %s has no valid updatedAt", domain.ErrEvidenceInsufficient, d.CallID)
	}
	report := application.DispatchOutcomeReport{Known: true, Source: Source, Sequence: uint64(observedAt.UnixMicro()), Outcome: outcome, ObservedAt: observedAt}
	if m.NativeReference != nil {
		report.NativeReference = *m.NativeReference
	}
	if m.Usage != nil {
		u := &domain.Usage{}
		for _, c := range []struct {
			raw string
			dst *uint64
		}{{m.Usage.InputUnits, &u.Input}, {m.Usage.OutputUnits, &u.Output}, {m.Usage.ReasoningUnits, &u.Reasoning}, {m.Usage.CachedInputUnits, &u.CachedInput}} {
			rev, err := domain.ParseRevision(c.raw)
			if err != nil {
				return application.DispatchOutcomeReport{}, fmt.Errorf("%w: model proxy usage of call %s: %v", domain.ErrEvidenceInsufficient, d.CallID, err)
			}
			*c.dst = uint64(rev)
		}
		report.Usage = u
	}
	return report, nil
}

// QueryEffect is not the Proxy's to answer.
func (q *OutcomeQuery) QueryEffect(ctx context.Context, e *domain.Effect) (application.EffectOutcomeReport, error) {
	return q.inner.QueryEffect(ctx, e)
}
