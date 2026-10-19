// Package authority is the production commercial authority source of
// Control (P2.1, DD-02 §3, DD-06 AuthorizationPort): a guarded client of the
// IdP/Pagix decision endpoint the deployment declares (ENV-07). Control is
// the sole reader of that evidence; admission and AuthorizationService both
// ask through it.
//
// The request and response are this port's contract, not a claim about an
// upstream API: the endpoint URL comes from the ENV-07 declaration and the
// conformance of the real endpoint is recorded separately. A decision is
// cached by principal, resource, action and kind until its absolute
// freshUntil — the smaller of the source's and Control's freshness bound
// (at most 30 s) — and a hit never extends it. Any failure to obtain a
// current decision (transport error, non-200, malformed or oversized body,
// a redirect) is domain.ErrUnavailable: it denies new admission and
// disclosure and never grants.
//
// Every decision Control relies on — admission's and AuthorizationService's
// for Knowledge and MCP, which use the answer at once and cache nothing —
// passes Check, so the age of a decision at its use is observed there
// (platform.md SLO row 3, the original freshUntil): 0 for a decision just
// fetched, the time since it was fetched for a cache hit.
package authority

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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Options configure the source.
type Options struct {
	// URL is the absolute decision endpoint of the ENV-07 declaration.
	URL string
	// Timeout bounds one decision request.
	Timeout time.Duration
	// Freshness caps the validity Control grants a decision (≤ 30 s).
	Freshness time.Duration
	// CAFile verifies the endpoint; CertFile/KeyFile present the workload
	// certificate (mTLS); TokenFile, when set, is a bearer token (an OAuth
	// client-credentials token the platform keeps current in the file).
	CAFile, CertFile, KeyFile, ServerName, TokenFile string
	// MaxEntries bounds the decision cache.
	MaxEntries int
	// Client overrides the HTTP client (tests).
	Client *http.Client
	// DecisionAge, when set, observes each decision's age at its use by
	// kind (disclosure, execution) and decision (allow, deny).
	DecisionAge *prometheus.HistogramVec
}

// Source is the guarded decision client with its absolute-freshness cache.
type Source struct {
	opts   Options
	client *http.Client
	clock  domain.Clock

	mu    sync.Mutex
	cache map[string]cached
}

// cached is a decision and when it was fetched.
type cached struct {
	decision  domain.Decision
	fetchedAt time.Time
}

// maxBody bounds a decision response.
const maxBody = 64 << 10

// New validates the options and builds the source.
func New(o Options, clock domain.Clock) (*Source, error) {
	if !strings.HasPrefix(o.URL, "https://") && !strings.HasPrefix(o.URL, "http://") {
		return nil, fmt.Errorf("authority: url %q is not an absolute http(s) URL", o.URL)
	}
	if o.Freshness <= 0 || o.Freshness > 30*time.Second {
		return nil, fmt.Errorf("authority: freshness %s outside (0, 30s]", o.Freshness)
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	if o.MaxEntries <= 0 {
		o.MaxEntries = 10000
	}
	client := o.Client
	if client == nil {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: o.ServerName}
		if o.CAFile != "" {
			pem, err := os.ReadFile(o.CAFile)
			if err != nil {
				return nil, fmt.Errorf("authority: ca_file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("authority: ca_file holds no certificate")
			}
			tlsCfg.RootCAs = pool
		}
		if o.CertFile != "" || o.KeyFile != "" {
			certFile, keyFile := o.CertFile, o.KeyFile
			// The certificate is re-read per handshake so a rotated pair
			// is presented without a restart.
			tlsCfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				c, err := tls.LoadX509KeyPair(certFile, keyFile)
				if err != nil {
					return nil, err
				}
				return &c, nil
			}
		}
		client = &http.Client{
			Timeout:   o.Timeout,
			Transport: &http.Transport{TLSClientConfig: tlsCfg, Proxy: nil, MaxIdleConnsPerHost: 16},
		}
	}
	// A redirect is never followed: the declared endpoint answers or the
	// decision is unavailable.
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Source{opts: o, client: &c, clock: clock, cache: map[string]cached{}}, nil
}

type request struct {
	SchemaVersion int      `json:"schemaVersion"`
	TenantID      string   `json:"tenantId"`
	ProjectID     string   `json:"projectId,omitempty"`
	ActorID       string   `json:"actorId"`
	Roles         []string `json:"roles,omitempty"`
	ResourceType  string   `json:"resourceType"`
	ResourceID    string   `json:"resourceId"`
	Action        string   `json:"action"`
	Kind          string   `json:"kind"`
}

type response struct {
	Decision   string `json:"decision"`
	Revision   string `json:"revision"`
	ReasonCode string `json:"reasonCode,omitempty"`
	FreshUntil string `json:"freshUntil"`
}

func kind(q domain.AuthorizationQuery) string {
	if q.Disclosure {
		return "disclosure"
	}
	return "execution"
}

func key(q domain.AuthorizationQuery) string {
	roles := append([]string(nil), q.Scope.Roles...)
	sort.Strings(roles)
	return strings.Join([]string{kind(q), q.Scope.TenantID, q.Scope.ProjectID, q.Scope.ActorID, strings.Join(roles, ","), q.ResourceType, q.ResourceID, q.Action}, "\x00")
}

// CheckExecution is the model-route admission question.
func (s *Source) CheckExecution(ctx context.Context, scope domain.Scope, routeID string) (domain.Decision, error) {
	return s.Check(ctx, domain.ModelRouteQuery(scope, routeID))
}

// Check answers one question from a current cached decision or the source.
func (s *Source) Check(ctx context.Context, q domain.AuthorizationQuery) (domain.Decision, error) {
	k := key(q)
	now := s.clock.Now()
	s.mu.Lock()
	if c, ok := s.cache[k]; ok && now.Before(c.decision.FreshUntil) {
		s.mu.Unlock()
		s.observeAge(q, c.decision, now.Sub(c.fetchedAt))
		return c.decision, nil
	}
	s.mu.Unlock()
	d, err := s.fetch(ctx, q, now)
	if err != nil {
		return domain.Decision{}, fmt.Errorf("%w: AUTHORITY_UNAVAILABLE: %v", domain.ErrUnavailable, err)
	}
	s.mu.Lock()
	if len(s.cache) >= s.opts.MaxEntries {
		for ck, cd := range s.cache {
			if !now.Before(cd.decision.FreshUntil) {
				delete(s.cache, ck)
			}
		}
		if len(s.cache) >= s.opts.MaxEntries {
			s.cache = map[string]cached{}
		}
	}
	s.cache[k] = cached{decision: d, fetchedAt: now}
	s.mu.Unlock()
	s.observeAge(q, d, 0)
	return d, nil
}

func (s *Source) observeAge(q domain.AuthorizationQuery, d domain.Decision, age time.Duration) {
	if s.opts.DecisionAge == nil {
		return
	}
	decision := "deny"
	if d.Allow {
		decision = "allow"
	}
	s.opts.DecisionAge.WithLabelValues(kind(q), decision).Observe(max(age.Seconds(), 0))
}

func (s *Source) fetch(ctx context.Context, q domain.AuthorizationQuery, now time.Time) (domain.Decision, error) {
	body, _ := json.Marshal(request{
		SchemaVersion: 1, TenantID: q.Scope.TenantID, ProjectID: q.Scope.ProjectID, ActorID: q.Scope.ActorID, Roles: q.Scope.Roles,
		ResourceType: q.ResourceType, ResourceID: q.ResourceID, Action: q.Action, Kind: kind(q),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.opts.URL, bytes.NewReader(body))
	if err != nil {
		return domain.Decision{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if s.opts.TokenFile != "" {
		tok, err := os.ReadFile(s.opts.TokenFile)
		if err != nil {
			return domain.Decision{}, errors.New("token file unreadable")
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return domain.Decision{}, errors.New("decision endpoint unreachable")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return domain.Decision{}, errors.New("decision response unreadable")
	}
	if resp.StatusCode != http.StatusOK {
		return domain.Decision{}, fmt.Errorf("decision endpoint answered %d", resp.StatusCode)
	}
	if len(raw) > maxBody {
		return domain.Decision{}, errors.New("decision response exceeds its bound")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var r response
	if err := dec.Decode(&r); err != nil {
		return domain.Decision{}, errors.New("decision response is not the declared shape")
	}
	if r.Revision == "" || len(r.Revision) > 256 {
		return domain.Decision{}, errors.New("decision response carries no revision")
	}
	until, err := time.Parse(time.RFC3339Nano, r.FreshUntil)
	if err != nil {
		return domain.Decision{}, errors.New("decision response carries no valid freshUntil")
	}
	if bound := now.Add(s.opts.Freshness); until.After(bound) {
		until = bound
	}
	switch r.Decision {
	case "allow":
		return domain.Decision{Allow: true, Revision: r.Revision, FreshUntil: until}, nil
	case "deny":
		reason := r.ReasonCode
		if reason == "" {
			reason = "DENIED"
		}
		return domain.Decision{Allow: false, Revision: r.Revision, ReasonCode: reason, FreshUntil: until}, nil
	}
	return domain.Decision{}, errors.New("decision response is neither allow nor deny")
}
