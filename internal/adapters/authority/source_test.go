package authority_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/authority"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *clock) advance(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

// decisionSource is a fake ENV-07 decision endpoint: it answers what the
// test sets, counts requests and can be taken down.
type decisionSource struct {
	srv      *httptest.Server
	requests atomic.Int32
	down     atomic.Bool
	mu       sync.Mutex
	answer   func(req map[string]any) (int, string)
}

func newDecisionSource(t *testing.T) *decisionSource {
	d := &decisionSource{}
	d.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.requests.Add(1)
		if d.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		d.mu.Lock()
		code, body := d.answer(req)
		d.mu.Unlock()
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(d.srv.Close)
	return d
}

func (d *decisionSource) set(fn func(req map[string]any) (int, string)) {
	d.mu.Lock()
	d.answer = fn
	d.mu.Unlock()
}

func allow(until time.Time) func(map[string]any) (int, string) {
	return func(map[string]any) (int, string) {
		return 200, `{"decision":"allow","revision":"rev-7","freshUntil":"` + until.UTC().Format(time.RFC3339Nano) + `"}`
	}
}

var scope = domain.Scope{TenantID: "tenant_a", ProjectID: "project_a", ActorID: "user_a"}

func newSource(t *testing.T, d *decisionSource, c *clock) *authority.Source {
	s, err := authority.New(authority.Options{URL: d.srv.URL + "/decide", Freshness: 30 * time.Second, Timeout: time.Second}, c)
	require.NoError(t, err)
	return s
}

// TestDecisionsAreCachedUntilTheirAbsoluteFreshness: a hit never extends a
// decision, Control's bound caps the source's, and an expired decision is
// asked again (P2.1 AC3).
func TestDecisionsAreCachedUntilTheirAbsoluteFreshness(t *testing.T) {
	d := newDecisionSource(t)
	c := &clock{now: time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)}
	d.set(allow(c.now.Add(10 * time.Minute)))
	s := newSource(t, d, c)
	ctx := context.Background()
	first, err := s.CheckExecution(ctx, scope, "deepseek-chat")
	require.NoError(t, err)
	require.True(t, first.Allow)
	require.Equal(t, "rev-7", first.Revision)
	require.Equal(t, c.now.Add(30*time.Second), first.FreshUntil, "Control's 30 s bound caps the source's ten minutes")
	c.advance(29 * time.Second)
	again, err := s.CheckExecution(ctx, scope, "deepseek-chat")
	require.NoError(t, err)
	require.Equal(t, first.FreshUntil, again.FreshUntil, "a hit never extends the decision")
	require.EqualValues(t, 1, d.requests.Load())
	c.advance(2 * time.Second)
	_, err = s.CheckExecution(ctx, scope, "deepseek-chat")
	require.NoError(t, err)
	require.EqualValues(t, 2, d.requests.Load(), "an expired decision is asked again")

	d.set(func(req map[string]any) (int, string) {
		require.Equal(t, "disclosure", req["kind"])
		require.Equal(t, "knowledge-snapshot", req["resourceType"])
		return 200, `{"decision":"deny","revision":"rev-8","reasonCode":"NO_SUBSCRIPTION","freshUntil":"` + c.Now().Add(5*time.Second).Format(time.RFC3339Nano) + `"}`
	})
	denied, err := s.Check(ctx, domain.AuthorizationQuery{Scope: scope, ResourceType: "knowledge-snapshot", ResourceID: "snap_1", Action: "read", Disclosure: true})
	require.NoError(t, err)
	require.False(t, denied.Allow)
	require.Equal(t, "NO_SUBSCRIPTION", denied.ReasonCode)
}

// TestOutageAndStaleDecisionsNeverGrant: an unreachable, failing,
// malformed or redirecting source is AUTHORITY_UNAVAILABLE; a cached
// decision past its freshness is not served while the source is down
// (P2.1 AC3).
func TestOutageAndStaleDecisionsNeverGrant(t *testing.T) {
	d := newDecisionSource(t)
	c := &clock{now: time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)}
	d.set(allow(c.now.Add(time.Minute)))
	s := newSource(t, d, c)
	ctx := context.Background()
	_, err := s.CheckExecution(ctx, scope, "r")
	require.NoError(t, err)
	d.down.Store(true)
	c.advance(31 * time.Second)
	got, err := s.CheckExecution(ctx, scope, "r")
	require.ErrorIs(t, err, domain.ErrUnavailable)
	require.ErrorContains(t, err, "AUTHORITY_UNAVAILABLE")
	require.False(t, got.Allow, "a stale decision is never served during an outage")

	d.down.Store(false)
	for name, answer := range map[string]func(map[string]any) (int, string){
		"unknown field": func(map[string]any) (int, string) {
			return 200, `{"decision":"allow","revision":"r","freshUntil":"2030-01-01T00:00:00Z","extra":1}`
		},
		"neither allow nor deny": func(map[string]any) (int, string) {
			return 200, `{"decision":"maybe","revision":"r","freshUntil":"2030-01-01T00:00:00Z"}`
		},
		"no revision":  func(map[string]any) (int, string) { return 200, `{"decision":"allow","revision":"","freshUntil":"2030-01-01T00:00:00Z"}` },
		"server error": func(map[string]any) (int, string) { return 500, `{}` },
		"redirect":     func(map[string]any) (int, string) { return 307, `` },
	} {
		d.set(answer)
		_, err := s.CheckExecution(ctx, scope, "route-"+name)
		require.ErrorIs(t, err, domain.ErrUnavailable, name)
	}

	d.srv.Close()
	_, err = s.CheckExecution(ctx, scope, "r-closed")
	require.True(t, errors.Is(err, domain.ErrUnavailable))
}
