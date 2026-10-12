package application

import (
	"context"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Authorization serves Control's AuthorizationService (P2.1, DD-02 §3):
// Knowledge asks before retrieval discloses content, MCP before a tool
// call executes, through the same source and freshness rule as admission.
// A decision whose validity has already ended answers deny; an unreachable
// source is domain.ErrUnavailable, never a grant.
type Authorization struct {
	source AuthorizationSource
	clock  domain.Clock
}

func NewAuthorization(source AuthorizationSource, clock domain.Clock) *Authorization {
	return &Authorization{source: source, clock: clock}
}

func (a *Authorization) Decide(ctx context.Context, q domain.AuthorizationQuery) (domain.Decision, error) {
	d, err := a.source.Check(ctx, q)
	if err != nil {
		return domain.Decision{}, err
	}
	if d.Allow && !d.Current(a.clock.Now()) {
		d.Allow, d.ReasonCode = false, "AUTHORITY_EXPIRED"
	}
	return d, nil
}

// SenderReported is the production original-identity query of the
// obligations Control holds no reader for (P2.1): a tool dispatch is
// reconciled by MCP's call reconciler and a business write by the
// Workflow's guarded Pagix ports, both reporting through ObserveDispatch
// and ObserveEffect. Asking Control establishes nothing, so the answer is
// an error, never "no record".
type SenderReported struct{}

func (SenderReported) QueryDispatch(_ context.Context, d *domain.Dispatch) (DispatchOutcomeReport, error) {
	return DispatchOutcomeReport{}, domainUnavailable("the outcome of %s dispatch %s is reported by its sender's reconciler, not queried by Control", d.Kind, d.ID)
}

func (SenderReported) QueryEffect(_ context.Context, e *domain.Effect) (EffectOutcomeReport, error) {
	return EffectOutcomeReport{}, domainUnavailable("the outcome of business write %s is queried by the Workflow's guarded Pagix port and reported through ObserveEffect", e.ID)
}
