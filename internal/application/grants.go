package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// GrantPolicies is Control's side of the MCP grant barrier (DD-08 §2): the
// registration receipt that lets MCP mark a grant ACTIVE, and the
// revocation fence with its convergence. MCP owns the grant; Control
// never creates one. Every call is one transaction on the policy row (rank
// 1); counting a grant's senders reads dispatches without locking them.
type GrantPolicies struct {
	store Store
	clock domain.Clock
	log   *slog.Logger
}

func NewGrantPolicies(store Store, clock domain.Clock, log *slog.Logger) *GrantPolicies {
	return &GrantPolicies{store: store, clock: clock, log: log}
}

// Registration is one grant revision's policy as MCP registers it.
type Registration struct {
	GrantID      string
	Revision     uint64
	PolicyDigest domain.Digest
	ServerID     string
	Methods      []string
	CostCap      *domain.Money
	ExpiresAt    *time.Time
}

// tombstoneDigest is the policy digest of a revision revoked before any
// registration (the SHA-256 of nothing): it never matches a real policy.
const tombstoneDigest = domain.Digest("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

// Register records the policy of a grant revision and answers its receipt.
// The same command, or another command for the same revision and digest,
// answers the existing receipt (a retry after a crash or a lost answer);
// another digest for a registered revision, a command reused for another
// registration, and any revision whose revocation has begun — including a
// revision revoked before it was ever registered — are refused.
func (g *GrantPolicies) Register(ctx context.Context, cmd domain.CommandIdentity, reg Registration) (policy *domain.GrantPolicy, existing bool, err error) {
	if reg.GrantID == "" || reg.Revision == 0 || reg.ServerID == "" || len(reg.Methods) == 0 {
		return nil, false, fmt.Errorf("%w: grant, revision, server and methods are required", domain.ErrInvalid)
	}
	if _, err := domain.ParseDigest(string(reg.PolicyDigest)); err != nil {
		return nil, false, fmt.Errorf("%w: policy digest", domain.ErrInvalid)
	}
	methods := slices.Clone(reg.Methods)
	slices.Sort(methods)
	if len(slices.Compact(methods)) != len(reg.Methods) {
		return nil, false, fmt.Errorf("%w: duplicate methods", domain.ErrInvalid)
	}
	now := g.clock.Now()
	if reg.ExpiresAt != nil && !now.Before(*reg.ExpiresAt) {
		return nil, false, fmt.Errorf("%w: the grant expired at %s", domain.ErrInvalid, reg.ExpiresAt.UTC().Format(time.RFC3339))
	}
	matches := func(p *domain.GrantPolicy) bool {
		return p.TenantID == cmd.TenantID && p.GrantID == reg.GrantID && p.GrantRevision == reg.Revision && p.PolicyDigest == reg.PolicyDigest
	}
	for attempt := 0; attempt < 2; attempt++ {
		err = g.store.Tx(ctx, func(r Repo) error {
			if byCmd, err := r.GetGrantPolicyByRegisterCommand(ctx, cmd.TenantID, cmd.CommandID); err == nil {
				if !matches(byCmd) || byCmd.RegisterRequestDigest != cmd.RequestDigest {
					return fmt.Errorf("%w: command %s registered another policy", domain.ErrIdempotencyConflict, cmd.CommandID)
				}
				policy, existing = byCmd, true
				return nil
			} else if !errors.Is(err, domain.ErrNotFound) {
				return err
			}
			current, err := r.LockGrantPolicy(ctx, reg.GrantID, reg.Revision)
			switch {
			case err == nil:
				if current.TenantID != cmd.TenantID {
					return fmt.Errorf("%w: grant %s", domain.ErrNotFound, reg.GrantID)
				}
				if !current.Registered || current.RevocationState != "none" {
					return fmt.Errorf("%w: grant %s revision %d is revoked (%s)", domain.ErrForbidden, reg.GrantID, reg.Revision, current.RevocationState)
				}
				if current.PolicyDigest != reg.PolicyDigest {
					return fmt.Errorf("%w: grant %s revision %d is registered with another policy", domain.ErrIdempotencyConflict, reg.GrantID, reg.Revision)
				}
				policy, existing = current, true
				return nil
			case !errors.Is(err, domain.ErrNotFound):
				return err
			}
			policy, err = r.InsertGrantPolicy(ctx, &domain.GrantPolicy{
				GrantID: reg.GrantID, GrantRevision: reg.Revision, TenantID: cmd.TenantID, PolicyDigest: reg.PolicyDigest, ServerID: reg.ServerID,
				Methods: reg.Methods, CostCap: reg.CostCap, ExpiresAt: reg.ExpiresAt, ReceiptID: domain.NewID("rcpt"), RevocationState: "none",
				Registered: true, RegisterCommandID: cmd.CommandID, RegisterRequestDigest: cmd.RequestDigest,
			})
			existing = false
			return err
		})
		if errors.Is(err, ErrDuplicateKey) && attempt == 0 {
			continue // a concurrent registration of the same revision or command committed first
		}
		break
	}
	if err != nil {
		return nil, false, err
	}
	if !existing {
		g.log.Info("grant policy registered", "grantId", reg.GrantID, "revision", reg.Revision, "epoch", policy.PolicyEpoch)
	}
	return policy, existing, nil
}

// BeginRevocation fences new admission for the grant revision in its own
// transaction and records the senders it must wait for. The update waits
// for any tool admission holding the policy's share lock, so no permission
// is consumed after the fence commits. A revision Control never registered
// gets a converged tombstone, so a registration still in flight can never
// make it executable afterwards. Repeating the revocation (any command)
// answers the existing barrier.
func (g *GrantPolicies) BeginRevocation(ctx context.Context, cmd domain.CommandIdentity, grantID string, revision uint64) (policy *domain.GrantPolicy, existing bool, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		err = g.store.Tx(ctx, func(r Repo) error {
			now := g.clock.Now()
			current, err := r.LockGrantPolicy(ctx, grantID, revision)
			if errors.Is(err, domain.ErrNotFound) {
				policy, err = r.InsertGrantPolicy(ctx, &domain.GrantPolicy{
					GrantID: grantID, GrantRevision: revision, TenantID: cmd.TenantID, PolicyDigest: tombstoneDigest, ServerID: "", Methods: []string{},
					ReceiptID: domain.NewID("tomb"), RevocationState: "converged", FencedAt: &now, ConvergedAt: &now, Registered: false,
					RegisterCommandID: "revoke:" + cmd.CommandID, RegisterRequestDigest: cmd.RequestDigest,
					RevocationCommandID: cmd.CommandID, RevocationRequestDigest: cmd.RequestDigest,
				})
				existing = false
				return err
			}
			if err != nil {
				return err
			}
			if current.TenantID != cmd.TenantID {
				return fmt.Errorf("%w: grant %s", domain.ErrNotFound, grantID)
			}
			if current.RevocationState != "none" {
				policy, existing = current, true
				return nil
			}
			inFlight, unknown, err := r.CountGrantSenders(ctx, grantID, revision)
			if err != nil {
				return err
			}
			current.RevocationState, current.FencedAt = "fenced", &now
			current.RevocationCommandID, current.RevocationRequestDigest = cmd.CommandID, cmd.RequestDigest
			current.InFlightCalls, current.UnknownCalls = inFlight, unknown
			policy, existing = current, false
			return r.UpdateGrantRevocation(ctx, current)
		})
		if errors.Is(err, ErrDuplicateKey) && attempt == 0 {
			continue // a concurrent revocation or registration of the revision committed first
		}
		break
	}
	if err != nil {
		return nil, false, err
	}
	if !existing {
		g.log.Info("grant revocation fenced", "grantId", grantID, "revision", revision, "inFlight", policy.InFlightCalls, "unknown", policy.UnknownCalls)
	}
	return policy, existing, nil
}

// Revocation reads the barrier and advances it: a fenced or converging
// barrier is converging while any sender of the revision still holds a
// consumed permission without an observed outcome (authorized) or an
// unknown outcome, and converged — finally — when none does. A revision
// without a revocation is answered as not found.
func (g *GrantPolicies) Revocation(ctx context.Context, grantID string, revision uint64) (policy *domain.GrantPolicy, err error) {
	err = g.store.Tx(ctx, func(r Repo) error {
		current, err := r.LockGrantPolicy(ctx, grantID, revision)
		if err != nil {
			return err
		}
		if current.RevocationState == "none" {
			return fmt.Errorf("%w: grant %s revision %d has no revocation", domain.ErrNotFound, grantID, revision)
		}
		policy = current
		if current.RevocationState == "converged" {
			return nil
		}
		inFlight, unknown, err := r.CountGrantSenders(ctx, grantID, revision)
		if err != nil {
			return err
		}
		current.InFlightCalls, current.UnknownCalls = inFlight, unknown
		if inFlight == 0 && unknown == 0 {
			now := g.clock.Now()
			current.RevocationState, current.ConvergedAt = "converged", &now
		} else {
			current.RevocationState = "converging"
		}
		return r.UpdateGrantRevocation(ctx, current)
	})
	return policy, err
}
