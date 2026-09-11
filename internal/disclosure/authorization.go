package disclosure

import (
	"context"
	"crypto/rand"
	"errors"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

var ErrUnavailable = errors.New("DEPENDENCY_UNAVAILABLE")

const freshness = 30 * time.Second
const readTimeout = 10 * time.Second
const waiterInterval = 250 * time.Millisecond
const clockMargin = 2 * time.Second

type evidenceStore interface {
	MatchesOperationScope(context.Context, storage.Scope, string) (bool, error)
	MatchesLocalCheckScope(context.Context, storage.Scope, string) (bool, error)
	ReadEvidence(context.Context, string) (storage.Evidence, error)
	AcquireRefresh(context.Context, string, string) (storage.RefreshLease, error)
	CompleteRefresh(context.Context, storage.RefreshLease, storage.Evidence) (int64, error)
	ReleaseRefresh(context.Context, storage.RefreshLease) error
	TakeAuthorizationRead(context.Context) (bool, error)
}
type Service struct {
	store    evidenceStore
	profile  profile
	source   membershipSource
	owner    string
	now      func() time.Time
	idSchema *jsonschema.Schema
}
type Decision struct {
	Allowed            bool
	BoundResourceScope string
	FreshUntil         time.Time
	EvidenceRevision   uint64
}

func newService(store evidenceStore, p profile, source membershipSource) *Service {
	return &Service{store: store, profile: p, source: source, owner: "disclosure-" + rand.Text(), now: time.Now}
}

// Authorize evaluates service authority and exact binding independently from
// membership and reviewed role permissions. Denials contain no resource facts.
func (s *Service) Authorize(ctx context.Context, actor Principal, operationID string) (Decision, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	b, bound := s.profile.bindings[operationID]
	if !bound {
		return s.AuthorizeLocal(ctx, actor, operationID, OperationRead)
	}
	if !bound || b.ActorID != actor.ActorID || b.TenantID != actor.TenantID || b.ServiceIdentity != APIService || !b.ServiceAuthorized {
		return Decision{}, nil
	}
	deadline := earliest(actor.ExpiresAt, b.ValidUntil, s.profile.sourceCredentialExpiresAt)
	if !s.now().Add(clockMargin).Before(deadline) {
		return Decision{}, nil
	}
	matched, err := s.store.MatchesOperationScope(ctx, storage.Scope{TenantID: actor.TenantID, ActorID: actor.ActorID}, operationID)
	if err != nil {
		return Decision{}, ErrUnavailable
	}
	if !matched {
		return Decision{}, nil
	}
	return s.authorizePermission(ctx, actor, deadline, operationID, "operation", OperationRead)
}

func (s *Service) authorizePermission(ctx context.Context, actor Principal, deadline time.Time, scope, resource, action string) (Decision, error) {
	evidence, err := s.currentEvidence(ctx, actor.TenantID)
	if err != nil {
		return Decision{}, err
	}
	deadline = earliest(deadline, evidence.FreshUntil, evidence.ObservedAt.Add(freshness))
	if !s.now().Add(clockMargin).Before(deadline) || evidence.Revision < 1 || ctx.Err() != nil {
		return Decision{}, ErrUnavailable
	}
	roles, member := evidence.MemberRoles[actor.ActorID]
	if !member {
		return Decision{}, nil
	}
	for _, role := range roles {
		for _, permission := range s.profile.permissions {
			if permission.Role == role && permission.Action == action && permission.ResourceKind == resource {
				return Decision{true, scope, deadline, uint64(evidence.Revision)}, nil
			}
		}
	}
	return Decision{}, nil
}

func (s *Service) readUsableEvidence(ctx context.Context, tenant string) (storage.Evidence, error) {
	evidence, err := s.store.ReadEvidence(ctx, tenant)
	if err == nil && !s.now().Add(clockMargin).Before(evidence.FreshUntil) {
		return storage.Evidence{}, storage.ErrLeaseLost
	}
	return evidence, err
}

func (s *Service) currentEvidence(ctx context.Context, tenant string) (storage.Evidence, error) {
	if evidence, err := s.readUsableEvidence(ctx, tenant); err == nil {
		return evidence, nil
	} else if !errors.Is(err, storage.ErrLeaseLost) {
		return storage.Evidence{}, ErrUnavailable
	}
	lease, err := s.store.AcquireRefresh(ctx, tenant, s.owner)
	if errors.Is(err, storage.ErrLeaseLost) {
		// Waiters never read the source or serve expired evidence.
		ticker := time.NewTicker(waiterInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return storage.Evidence{}, ErrUnavailable
			case <-ticker.C:
				evidence, err := s.readUsableEvidence(ctx, tenant)
				if err == nil {
					return evidence, nil
				}
				if !errors.Is(err, storage.ErrLeaseLost) {
					return storage.Evidence{}, ErrUnavailable
				}
			}
		}
	}
	if err != nil {
		return storage.Evidence{}, ErrUnavailable
	}
	completed := false
	defer func() {
		if !completed {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer cancel()
			_ = s.store.ReleaseRefresh(cleanup, lease)
		}
	}()
	// A previous holder may have committed between our first read and claim.
	if evidence, err := s.readUsableEvidence(ctx, tenant); err == nil {
		return evidence, nil
	} else if !errors.Is(err, storage.ErrLeaseLost) {
		return storage.Evidence{}, ErrUnavailable
	}
	allowed, err := s.store.TakeAuthorizationRead(ctx)
	if err != nil || !allowed {
		return storage.Evidence{}, ErrUnavailable
	}
	// Capture the first source-send origin once. Database transaction retries
	// reuse this evidence and never repeat the source read or reset its clock.
	observed := s.now()
	read, err := s.source.Read(ctx, tenant)
	if err != nil || ctx.Err() != nil || read.MemberRoles == nil || !s.now().Before(observed.Add(readTimeout)) {
		return storage.Evidence{}, ErrUnavailable
	}
	evidence := storage.Evidence{ObservedAt: observed, FreshUntil: earliest(observed.Add(freshness), s.profile.sourceCredentialExpiresAt, read.ValidUntil), MemberRoles: read.MemberRoles}
	if !s.now().Before(evidence.FreshUntil) {
		return storage.Evidence{}, ErrUnavailable
	}
	evidence.Revision, err = s.store.CompleteRefresh(ctx, lease, evidence)
	if err != nil {
		return storage.Evidence{}, ErrUnavailable
	}
	completed = true
	return evidence, nil
}

func earliest(times ...time.Time) time.Time {
	result := times[0]
	for _, t := range times[1:] {
		if t.Before(result) {
			result = t
		}
	}
	return result
}
