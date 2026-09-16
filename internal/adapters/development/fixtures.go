// Package development holds the DEVELOPMENT_ONLY sources of pricing,
// commercial authority and not-sent evidence for the dispatch admission
// chain (delivery.md P06, ENV-06/ENV-07). They exist so the admission
// rules can be verified end to end before the real inputs are available;
// with the fixtures disabled every paid route is denied. Nothing here is a
// production adapter: real prices, an IdP/Pagix decision and a qualified
// evidence source replace them, and no test against these fixtures claims a
// provider, authorization or evidence qualification.
package development

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// PriceBook serves reviewed fixture prices by route and by revision.
type PriceBook struct {
	byRoute    map[string][]*domain.Price
	byRevision map[string]*domain.Price
}

// NewPriceBook indexes validated prices; a duplicate revision is an error
// because a revision names exactly one immutable observation.
func NewPriceBook(prices []domain.Price) (*PriceBook, error) {
	b := &PriceBook{byRoute: map[string][]*domain.Price{}, byRevision: map[string]*domain.Price{}}
	for i := range prices {
		p := prices[i]
		if err := p.Validate(); err != nil {
			return nil, err
		}
		if _, dup := b.byRevision[p.Revision]; dup {
			return nil, fmt.Errorf("%w: duplicate price revision %s", domain.ErrInvalid, p.Revision)
		}
		b.byRevision[p.Revision] = &p
		key := string(p.Kind) + "/" + p.Route
		b.byRoute[key] = append(b.byRoute[key], &p)
	}
	return b, nil
}

func (b *PriceBook) Price(kind domain.DispatchKind, route string, at time.Time) (*domain.Price, bool) {
	for _, p := range b.byRoute[string(kind)+"/"+route] {
		if p.EffectiveAt(at) {
			return p, true
		}
	}
	return nil, false
}

func (b *PriceBook) PriceByRevision(revision string) (*domain.Price, bool) {
	p, ok := b.byRevision[revision]
	return p, ok
}

// Authority allows the fixture's (tenant, route) pairs with a fixed
// freshness window and denies everything else. The decision revision is
// the fixture's; a real decision carries the IdP/Pagix revision.
type Authority struct {
	allowed   map[string]bool
	operators map[string]bool
	freshness time.Duration
	clock     domain.Clock
}

// RouteAuthorization is one allowed (tenant, route) pair of the fixture.
type RouteAuthorization struct {
	TenantID string
	RouteID  string
}

func NewAuthority(routes []RouteAuthorization, freshness time.Duration, clock domain.Clock) *Authority {
	a := &Authority{allowed: map[string]bool{}, freshness: freshness, clock: clock}
	for _, r := range routes {
		a.allowed[r.TenantID+"\x00"+r.RouteID] = true
	}
	return a
}

func (a *Authority) CheckExecution(_ context.Context, scope domain.Scope, routeID string) (domain.Decision, error) {
	now := a.clock.Now()
	if a.allowed[scope.TenantID+"\x00"+routeID] {
		return domain.Decision{Allow: true, Revision: "development-fixture", FreshUntil: now.Add(a.freshness)}, nil
	}
	return domain.Decision{Allow: false, Revision: "development-fixture", ReasonCode: "ROUTE_NOT_AUTHORIZED", FreshUntil: now.Add(a.freshness)}, nil
}

// ObjectReader reads published inventory objects (the filesystem inventory
// in development, a qualified object store in production).
type ObjectReader interface {
	Get(ctx context.Context, key string) (body []byte, version string, err error)
}

// NotSentAttestation is the object a trusted issuer publishes when it has
// terminated the original sender's ability to send the call: it names the
// dispatch and call, the issuer and the moment. The caller offering it
// proves nothing by itself; the object must exist, hash to the offered
// digest, name this dispatch and come from a trusted issuer.
type NotSentAttestation struct {
	Class      string `json:"class"`
	DispatchID string `json:"dispatchId"`
	CallID     string `json:"callId"`
	Owner      string `json:"owner"`
	Issuer     string `json:"issuer"`
	SealedAt   string `json:"sealedAt"`
}

// NotSentEvidence verifies attestations published under
// not-sent/<dispatchId> by one of the fixture's trusted issuers.
type NotSentEvidence struct {
	objects ObjectReader
	issuers map[string]bool
}

func NewNotSentEvidence(objects ObjectReader, issuers []string) *NotSentEvidence {
	e := &NotSentEvidence{objects: objects, issuers: map[string]bool{}}
	for _, i := range issuers {
		e.issuers[i] = true
	}
	return e
}

// Key is the inventory key of a dispatch's attestation.
func Key(dispatchID string) string { return "not-sent/" + dispatchID }

func (e *NotSentEvidence) VerifyNotSent(ctx context.Context, d *domain.Dispatch, evidenceRef string, evidenceDigest domain.Digest) error {
	if evidenceRef != Key(d.ID) {
		return fmt.Errorf("%w: evidence %q is not the attestation of dispatch %s", domain.ErrEvidenceInsufficient, evidenceRef, d.ID)
	}
	body, _, err := e.objects.Get(ctx, evidenceRef)
	if err != nil {
		return fmt.Errorf("%w: attestation %s cannot be read: %v", domain.ErrEvidenceInsufficient, evidenceRef, err)
	}
	if actual := domain.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(body))); actual != evidenceDigest {
		return fmt.Errorf("%w: attestation %s hashes to %s, not the offered %s", domain.ErrEvidenceInsufficient, evidenceRef, actual, evidenceDigest)
	}
	var att NotSentAttestation
	if err := json.Unmarshal(body, &att); err != nil {
		return fmt.Errorf("%w: attestation %s is not readable: %v", domain.ErrEvidenceInsufficient, evidenceRef, err)
	}
	if att.Class != "not-sent" || att.DispatchID != d.ID || att.CallID != d.CallID || att.Owner != d.Owner {
		return fmt.Errorf("%w: attestation %s does not name dispatch %s (call %s of %s)", domain.ErrEvidenceInsufficient, evidenceRef, d.ID, d.CallID, d.Owner)
	}
	if !e.issuers[att.Issuer] {
		return fmt.Errorf("%w: attestation %s issuer %q is not trusted", domain.ErrEvidenceInsufficient, evidenceRef, att.Issuer)
	}
	if _, err := time.Parse(time.RFC3339, att.SealedAt); err != nil {
		return fmt.Errorf("%w: attestation %s has no valid sealing time", domain.ErrEvidenceInsufficient, evidenceRef)
	}
	return nil
}

// OperatorAuthorization is one allowed (tenant, actor) pair of the fixture
// for platform-owned operator actions; tenant "*" allows every scope.
type OperatorAuthorization struct {
	TenantID string
	ActorID  string
}

// WithOperators adds the fixture's operator authorizations.
func (a *Authority) WithOperators(operators []OperatorAuthorization) *Authority {
	if a.operators == nil {
		a.operators = map[string]bool{}
	}
	for _, o := range operators {
		a.operators[o.TenantID+"\x00"+o.ActorID] = true
	}
	return a
}

func (a *Authority) CheckOperator(_ context.Context, scope domain.Scope, action string) (domain.Decision, error) {
	now := a.clock.Now()
	if a.operators[scope.TenantID+"\x00"+scope.ActorID] || a.operators["*\x00"+scope.ActorID] {
		return domain.Decision{Allow: true, Revision: "development-fixture", FreshUntil: now.Add(a.freshness)}, nil
	}
	return domain.Decision{Allow: false, Revision: "development-fixture", ReasonCode: "OPERATOR_NOT_AUTHORIZED:" + action, FreshUntil: now.Add(a.freshness)}, nil
}

// OutcomeAttestation is the object a controlled upstream double publishes
// under outcome/<class>/<obligationId> to answer the original-identity
// query of an obligation. It stands in for the Model Proxy (P11) and Pagix
// (ENV-07) query-by-identity declarations, which do not exist yet; nothing
// read from it is a claim about a real upstream.
type OutcomeAttestation struct {
	Class           string            `json:"class"`
	ObligationClass string            `json:"obligationClass"`
	ObligationID    string            `json:"obligationId"`
	Issuer          string            `json:"issuer"`
	Sequence        uint64            `json:"sequence"`
	Outcome         string            `json:"outcome"`
	Usage           map[string]uint64 `json:"usage,omitempty"`
	ReceiptRef      string            `json:"receiptRef,omitempty"`
	NativeReference string            `json:"nativeReference,omitempty"`
	ObservedAt      string            `json:"observedAt"`
}

// OutcomeKey is the inventory key of an obligation's outcome attestation.
func OutcomeKey(class, obligationID string) string { return "outcome/" + class + "/" + obligationID }

// OutcomeQuery is the DEVELOPMENT_ONLY controlled double of the
// original-identity query ports: it reads outcome attestations from the
// object reader. A missing attestation is "no record" (Known false); an
// unreadable store is an error; an attestation from an untrusted issuer or
// naming another obligation is refused as insufficient evidence.
type OutcomeQuery struct {
	objects ObjectReader
	issuers map[string]bool
}

func NewOutcomeQuery(objects ObjectReader, issuers []string) *OutcomeQuery {
	q := &OutcomeQuery{objects: objects, issuers: map[string]bool{}}
	for _, i := range issuers {
		q.issuers[i] = true
	}
	return q
}

func (q *OutcomeQuery) read(ctx context.Context, class, obligationID string) (*OutcomeAttestation, error) {
	body, _, err := q.objects.Get(ctx, OutcomeKey(class, obligationID))
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var att OutcomeAttestation
	if err := json.Unmarshal(body, &att); err != nil {
		return nil, fmt.Errorf("%w: outcome attestation of %s/%s is not readable: %v", domain.ErrEvidenceInsufficient, class, obligationID, err)
	}
	if att.Class != "outcome" || att.ObligationClass != class || att.ObligationID != obligationID {
		return nil, fmt.Errorf("%w: outcome attestation does not name %s/%s", domain.ErrEvidenceInsufficient, class, obligationID)
	}
	if !q.issuers[att.Issuer] {
		return nil, fmt.Errorf("%w: outcome attestation issuer %q is not trusted", domain.ErrEvidenceInsufficient, att.Issuer)
	}
	if _, err := time.Parse(time.RFC3339, att.ObservedAt); err != nil {
		return nil, fmt.Errorf("%w: outcome attestation has no valid observation time", domain.ErrEvidenceInsufficient)
	}
	return &att, nil
}

func (q *OutcomeQuery) QueryDispatch(ctx context.Context, d *domain.Dispatch) (application.DispatchOutcomeReport, error) {
	class := "model-dispatch"
	if d.Kind == domain.DispatchTool {
		class = "tool-dispatch"
	}
	att, err := q.read(ctx, class, d.ID)
	if err != nil || att == nil {
		return application.DispatchOutcomeReport{}, err
	}
	at, _ := time.Parse(time.RFC3339, att.ObservedAt)
	report := application.DispatchOutcomeReport{Known: true, Source: "query:" + att.Issuer, Sequence: att.Sequence, Outcome: domain.DispatchOutcome(att.Outcome), NativeReference: att.NativeReference, ObservedAt: at}
	if att.Usage != nil {
		report.Usage = &domain.Usage{Input: att.Usage["input_units"], Output: att.Usage["output_units"], Reasoning: att.Usage["reasoning_units"], CachedInput: att.Usage["cached_input_units"]}
	}
	return report, nil
}

func (q *OutcomeQuery) QueryEffect(ctx context.Context, e *domain.Effect) (application.EffectOutcomeReport, error) {
	att, err := q.read(ctx, "business-write", e.ID)
	if err != nil || att == nil {
		return application.EffectOutcomeReport{}, err
	}
	at, _ := time.Parse(time.RFC3339, att.ObservedAt)
	return application.EffectOutcomeReport{Known: true, Source: "query:" + att.Issuer, Sequence: att.Sequence, Outcome: domain.EffectOutcome(att.Outcome), ReceiptRef: att.ReceiptRef, NativeReference: att.NativeReference, ObservedAt: at}, nil
}

// DispositionAttestation is the object a trusted issuer publishes under
// disposition/<class>/<obligationId> to back an operator disposition.
type DispositionAttestation struct {
	Class           string `json:"class"`
	ObligationClass string `json:"obligationClass"`
	ObligationID    string `json:"obligationId"`
	Decision        string `json:"decision"`
	Issuer          string `json:"issuer"`
	SealedAt        string `json:"sealedAt"`
}

// DispositionKey is the inventory key of an obligation's disposition
// attestation.
func DispositionKey(class, obligationID string) string {
	return "disposition/" + class + "/" + obligationID
}

// DispositionEvidence verifies disposition attestations from the fixture's
// trusted issuers (DEVELOPMENT_ONLY).
type DispositionEvidence struct {
	objects ObjectReader
	issuers map[string]bool
}

func NewDispositionEvidence(objects ObjectReader, issuers []string) *DispositionEvidence {
	e := &DispositionEvidence{objects: objects, issuers: map[string]bool{}}
	for _, i := range issuers {
		e.issuers[i] = true
	}
	return e
}

func (e *DispositionEvidence) VerifyDisposition(ctx context.Context, class, obligationID string, decision domain.DispositionDecision, evidenceRef string, evidenceDigest domain.Digest) error {
	if evidenceRef != DispositionKey(class, obligationID) {
		return fmt.Errorf("%w: evidence %q is not the disposition attestation of %s/%s", domain.ErrEvidenceInsufficient, evidenceRef, class, obligationID)
	}
	body, _, err := e.objects.Get(ctx, evidenceRef)
	if err != nil {
		return fmt.Errorf("%w: attestation %s cannot be read: %v", domain.ErrEvidenceInsufficient, evidenceRef, err)
	}
	if actual := domain.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(body))); actual != evidenceDigest {
		return fmt.Errorf("%w: attestation %s hashes to %s, not the offered %s", domain.ErrEvidenceInsufficient, evidenceRef, actual, evidenceDigest)
	}
	var att DispositionAttestation
	if err := json.Unmarshal(body, &att); err != nil {
		return fmt.Errorf("%w: attestation %s is not readable: %v", domain.ErrEvidenceInsufficient, evidenceRef, err)
	}
	if att.Class != "disposition" || att.ObligationClass != class || att.ObligationID != obligationID || att.Decision != string(decision) {
		return fmt.Errorf("%w: attestation %s does not name %s/%s with decision %s", domain.ErrEvidenceInsufficient, evidenceRef, class, obligationID, decision)
	}
	if !e.issuers[att.Issuer] {
		return fmt.Errorf("%w: attestation %s issuer %q is not trusted", domain.ErrEvidenceInsufficient, evidenceRef, att.Issuer)
	}
	if _, err := time.Parse(time.RFC3339, att.SealedAt); err != nil {
		return fmt.Errorf("%w: attestation %s has no valid sealing time", domain.ErrEvidenceInsufficient, evidenceRef)
	}
	return nil
}
