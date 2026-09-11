package disclosure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
)

type testStore struct {
	evidence                       storage.Evidence
	readErr, claimErr, completeErr error
	matches, budget                bool
	reads, completions, releases   int
	lease, released                storage.RefreshLease
}

func (s *testStore) MatchesLocalCheckScope(ctx context.Context, scope storage.Scope, id string) (bool, error) {
	return s.MatchesOperationScope(ctx, scope, id)
}

func (s *testStore) MatchesOperationScope(context.Context, storage.Scope, string) (bool, error) {
	return s.matches, nil
}
func (s *testStore) ReadEvidence(context.Context, string) (storage.Evidence, error) {
	s.reads++
	return s.evidence, s.readErr
}
func (s *testStore) AcquireRefresh(context.Context, string, string) (storage.RefreshLease, error) {
	return s.lease, s.claimErr
}
func (s *testStore) CompleteRefresh(_ context.Context, lease storage.RefreshLease, e storage.Evidence) (int64, error) {
	s.completions++
	if lease != s.lease {
		panic("completion changed its acquisition")
	}
	if s.completeErr == nil {
		s.evidence = e
		s.evidence.Revision = 7
		s.readErr = nil
	}
	return 7, s.completeErr
}
func (s *testStore) ReleaseRefresh(_ context.Context, lease storage.RefreshLease) error {
	s.releases++
	s.released = lease
	return nil
}
func (s *testStore) TakeAuthorizationRead(context.Context) (bool, error) { return s.budget, nil }

type testSource func(context.Context, string) (membership, error)

func (f testSource) Read(ctx context.Context, tenant string) (membership, error) {
	return f(ctx, tenant)
}

func decisionFixture() (*Service, *testStore, Principal, *time.Time) {
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	actor := Principal{"fixture-actor-a", "fixture-tenant-a", now.Add(time.Hour)}
	p := profile{sourceCredentialExpiresAt: now.Add(time.Hour), permissions: []permission{{"fixture-reader", "operation", OperationRead}}, bindings: map[string]binding{"op-a": {"op-a", actor.ActorID, actor.TenantID, APIService, true, now.Add(time.Hour)}}}
	store := &testStore{matches: true, budget: true, evidence: storage.Evidence{ObservedAt: now, FreshUntil: now.Add(freshness), Revision: 6, MemberRoles: map[string][]string{actor.ActorID: {"fixture-reader"}}}, lease: storage.RefreshLease{TenantID: actor.TenantID, Owner: "replica-a", ID: "lease-1", Epoch: 1, Until: now.Add(12 * time.Second)}}
	s := newService(store, p, testSource(func(context.Context, string) (membership, error) { panic("fresh cache called source") }))
	s.now = func() time.Time { return now }
	return s, store, actor, &now
}

func TestFourDisclosureFacts(t *testing.T) {
	for _, name := range []string{"owner", "missing member", "unknown role", "no role", "wrong action", "wrong resource kind", "service refused", "wrong service", "wrong actor", "wrong tenant", "missing operation", "expired credential", "expired binding", "expired service authority"} {
		t.Run(name, func(t *testing.T) {
			s, store, actor, now := decisionFixture()
			b := s.profile.bindings["op-a"]
			switch name {
			case "missing member":
				delete(store.evidence.MemberRoles, actor.ActorID)
			case "unknown role":
				store.evidence.MemberRoles[actor.ActorID] = []string{"unmapped"}
			case "no role":
				store.evidence.MemberRoles[actor.ActorID] = []string{}
			case "wrong action":
				s.profile.permissions[0].Action = "operation.cancel"
			case "wrong resource kind":
				s.profile.permissions[0].ResourceKind = "draft"
			case "service refused":
				b.ServiceAuthorized = false
			case "wrong service":
				b.ServiceIdentity = "anvilkit-agent-workflow"
			case "wrong actor":
				actor.ActorID = "fixture-other"
			case "wrong tenant":
				actor.TenantID = "fixture-other"
			case "missing operation":
				store.matches = false
			case "expired credential":
				actor.ExpiresAt = *now
			case "expired binding":
				b.ValidUntil = *now
			case "expired service authority":
				s.profile.sourceCredentialExpiresAt = *now
			}
			s.profile.bindings["op-a"] = b
			decision, err := s.Authorize(t.Context(), actor, "op-a")
			if err != nil || decision.Allowed != (name == "owner") {
				t.Fatalf("decision=%+v err=%v", decision, err)
			}
			if !decision.Allowed && decision != (Decision{}) {
				t.Fatal("denial leaked resource facts")
			}
		})
	}
}

func TestFirstSendAndEarliestDeadlines(t *testing.T) {
	for _, bound := range []string{"first send", "actor credential", "service credential", "declared validity", "binding"} {
		t.Run(bound, func(t *testing.T) {
			s, store, actor, now := decisionFixture()
			start := *now
			store.readErr = storage.ErrLeaseLost
			validUntil := start.Add(time.Hour)
			want := start.Add(30 * time.Second)
			if bound != "first send" {
				want = start.Add(25 * time.Second)
			}
			switch bound {
			case "actor credential":
				actor.ExpiresAt = want
			case "service credential":
				s.profile.sourceCredentialExpiresAt = want
			case "declared validity":
				validUntil = want
			case "binding":
				b := s.profile.bindings["op-a"]
				b.ValidUntil = want
				s.profile.bindings["op-a"] = b
			}
			reads := 0
			s.source = testSource(func(context.Context, string) (membership, error) {
				reads++
				*now = start.Add(6 * time.Second)
				return membership{map[string][]string{actor.ActorID: {"fixture-reader"}}, validUntil}, nil
			})
			decision, err := s.Authorize(t.Context(), actor, "op-a")
			if err != nil || !decision.Allowed || !decision.FreshUntil.Equal(want) || !store.evidence.ObservedAt.Equal(start) || reads != 1 {
				t.Fatalf("origin/deadline changed: %+v %v", decision, err)
			}
			if store.completions != 1 || store.releases != 0 {
				t.Fatal("completed refresh was released or repeated")
			}
		})
	}
}

func TestFailedRefreshNeverGrantsOrRepeatsSource(t *testing.T) {
	for _, failure := range []string{"source failure", "incomplete", "already expired", "lost lease", "read budget", "canceled"} {
		t.Run(failure, func(t *testing.T) {
			s, store, actor, now := decisionFixture()
			store.readErr = storage.ErrLeaseLost
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reads := 0
			s.source = testSource(func(context.Context, string) (membership, error) {
				reads++
				if failure == "source failure" {
					return membership{}, errors.New("private upstream body must not escape")
				}
				if failure == "incomplete" {
					return membership{nil, now.Add(time.Hour)}, nil
				}
				if failure == "canceled" {
					cancel()
				}
				until := now.Add(time.Hour)
				if failure == "already expired" {
					until = *now
				}
				return membership{map[string][]string{actor.ActorID: {"fixture-reader"}}, until}, nil
			})
			if failure == "lost lease" {
				store.completeErr = storage.ErrLeaseLost
			}
			if failure == "read budget" {
				store.budget = false
			}
			decision, err := s.Authorize(ctx, actor, "op-a")
			if !errors.Is(err, ErrUnavailable) || decision.Allowed || reads > 1 {
				t.Fatalf("unsafe failure: %+v %v reads=%d", decision, err, reads)
			}
			if store.releases != 1 || store.released != store.lease {
				t.Fatal("failed holder did not release only its acquisition")
			}
			if failure != "lost lease" && store.completions != 0 {
				t.Fatal("incomplete/expired source reached completion")
			}
		})
	}
}

func TestWaiterHonorsDeadlineWithoutSourceRead(t *testing.T) {
	s, store, actor, _ := decisionFixture()
	store.readErr = storage.ErrLeaseLost
	store.claimErr = storage.ErrLeaseLost
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	decision, err := s.Authorize(ctx, actor, "op-a")
	if !errors.Is(err, ErrUnavailable) || decision.Allowed || store.completions != 0 || store.releases != 0 {
		t.Fatal("waiter changed holder state or granted", err)
	}
}

func TestCacheRenewsBeforeTheAPIExpiryMargin(t *testing.T) {
	s, store, actor, now := decisionFixture()
	store.evidence.FreshUntil = now.Add(clockMargin)
	reads := 0
	s.source = testSource(func(context.Context, string) (membership, error) {
		reads++
		return membership{map[string][]string{actor.ActorID: {"fixture-reader"}}, now.Add(time.Hour)}, nil
	})
	decision, err := s.Authorize(t.Context(), actor, "op-a")
	if err != nil || !decision.Allowed || !decision.FreshUntil.Equal(now.Add(freshness)) || reads != 1 || decision.EvidenceRevision != 7 {
		t.Fatalf("near-expiry cache was not renewed: %+v %v", decision, err)
	}
}
