package domain

import (
	"errors"
	"math"
	"testing"
	"time"
)

var (
	priceFrom  = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	priceUntil = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	fixture    = Price{Revision: "fx-1", Kind: DispatchModel, Route: "fixture-route", Provider: "fixture", Model: "fixture-model", Currency: "USD", EffectiveFrom: priceFrom, EffectiveUntil: priceUntil,
		PerMillion: map[UsageCategory]int64{UsageInput: 3_000_000, UsageOutput: 15_000_000, UsageReasoning: 15_000_000, UsageCachedInput: 300_000}, MaxExposure: 5_000_000}
)

func TestMoneyIsIntegerWithOverflowChecks(t *testing.T) {
	m, err := ParseMoney("USD", "1234567")
	if err != nil || m.Amount != 1234567 || m.AmountString() != "1234567" {
		t.Fatalf("parse: %+v %v", m, err)
	}
	for _, bad := range []string{"01", "1.5", "", "99999999999999999999", "-", "1e3"} {
		if _, err := ParseMoney("USD", bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("amount %q accepted: %v", bad, err)
		}
	}
	if _, err := ParseMoney("usd", "1"); !errors.Is(err, ErrInvalid) {
		t.Fatal("lowercase currency accepted")
	}
	big := Money{Currency: "USD", Amount: math.MaxInt64}
	one := Money{Currency: "USD", Amount: 1}
	if _, err := big.Add(one); !errors.Is(err, ErrInvalid) {
		t.Fatalf("overflow not detected: %v", err)
	}
	if _, err := big.Add(Money{Currency: "EUR", Amount: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("currency mismatch accepted")
	}
	neg := Money{Currency: "USD", Amount: math.MinInt64}
	zero := Money{Currency: "USD"}
	if _, err := zero.Sub(neg); !errors.Is(err, ErrInvalid) {
		t.Fatal("negation overflow accepted")
	}
	five := Money{Currency: "USD", Amount: 5}
	sum, err := five.Sub(Money{Currency: "USD", Amount: 7})
	if err != nil || sum.Amount != -2 {
		t.Fatalf("sub: %+v %v", sum, err)
	}
}

func TestPriceCostRoundsUpPerCategoryAndRejectsOverflow(t *testing.T) {
	if err := fixture.Validate(); err != nil {
		t.Fatal(err)
	}
	// Unit prices are scale-6 amounts per one million units: one input unit
	// at 3000000 per million is exactly 3, 1000 output units at 15000000
	// per million exactly 15000, and one cached input unit at 300000 per
	// million is 0.3, which rounds up to 1 (never down to a free unit).
	cost, err := fixture.Cost(Usage{Input: 1, Output: 1000, CachedInput: 1})
	if err != nil || cost != (Money{Currency: "USD", Amount: 3 + 15000 + 1}) {
		t.Fatalf("cost: %+v %v", cost, err)
	}
	if c, _ := fixture.Cost(Usage{}); c.Amount != 0 {
		t.Fatalf("zero usage costs %d", c.Amount)
	}
	if _, err := fixture.Cost(Usage{Output: math.MaxUint64}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("overflowing cost accepted: %v", err)
	}
	if fixture.EffectiveAt(priceUntil) || !fixture.EffectiveAt(priceFrom) {
		t.Fatal("effective interval is [from, until)")
	}
	incomplete := fixture
	incomplete.PerMillion = map[UsageCategory]int64{UsageInput: 1}
	if err := incomplete.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatal("a price that does not meter every category validates")
	}
	unbound := fixture
	unbound.Model = ""
	if err := unbound.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatal("a model price without its provider/model binding validates")
	}
	tool := fixture
	tool.Kind, tool.Route = DispatchTool, "srv/read"
	if err := tool.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatal("a tool price carrying a provider/model validates")
	}
	tool.Provider, tool.Model = "", ""
	if err := tool.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPoolsAndAllocationsEnforceSharedLimits(t *testing.T) {
	now := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	pool := &BudgetPool{ID: "pool", Level: LevelTenant, PeriodStart: now.Add(-time.Hour), PeriodEnd: now.Add(time.Hour), Currency: "USD", Cap: 10}
	if err := pool.Allocate(Money{Currency: "USD", Amount: 6}, now); err != nil {
		t.Fatal(err)
	}
	if err := pool.Allocate(Money{Currency: "USD", Amount: 5}, now); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("cap exceeded but allocated: %v", err)
	}
	if err := pool.Allocate(Money{Currency: "USD", Amount: 1}, now.Add(2*time.Hour)); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatal("a pool outside its period allocated")
	}
	if err := pool.Allocate(Money{Currency: "EUR", Amount: 1}, now); !errors.Is(err, ErrInvalid) {
		t.Fatal("currency mismatch allocated")
	}
	a := NewAllocation(pool, "op", Money{Currency: "USD", Amount: 6}, now)
	if err := a.Reserve(Money{Currency: "USD", Amount: 4}); err != nil {
		t.Fatal(err)
	}
	if err := a.Reserve(Money{Currency: "USD", Amount: 3}); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("reservation above the allocation accepted: %v", err)
	}
	// Actual cost above the reservation is retained, never erased.
	if err := a.Charge(Money{Currency: "USD", Amount: 5}, Money{Currency: "USD", Amount: 4}, true); err != nil {
		t.Fatal(err)
	}
	if a.Reserved != 0 || a.Consumed != 5 {
		t.Fatalf("after settlement reserved %d consumed %d", a.Reserved, a.Consumed)
	}
	if err := a.Reserve(Money{Currency: "USD", Amount: 2}); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatal("headroom after overspend was invented")
	}
	if err := a.Release(Money{Currency: "USD", Amount: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("released more than reserved")
	}
}

func TestPriceMetersInclusiveNativeUsageWithoutCountingSubsetsTwice(t *testing.T) {
	p := fixture
	p.InputIncludesCached, p.OutputIncludesReasoning = true, true
	p.PerMillion = map[UsageCategory]int64{UsageInput: 150000, UsageOutput: 600000, UsageReasoning: 600000, UsageCachedInput: 3000}
	u := Usage{Input: 120000, Output: 34000, Reasoning: 4000, CachedInput: 20000}
	cost, err := p.Cost(u)
	if err != nil || cost.Amount != 35460 {
		t.Fatalf("inclusive usage cost: %+v %v", cost, err)
	}
	if u.Input != 120000 || u.Output != 34000 {
		t.Fatal("metering changed the native observation")
	}
	for _, invalid := range []Usage{{Input: 1, CachedInput: 2}, {Output: 1, Reasoning: 2}} {
		if _, err := p.Cost(invalid); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid inclusive usage accepted: %+v %v", invalid, err)
		}
	}
	// Existing price revisions continue to meter independent categories.
	p.InputIncludesCached, p.OutputIncludesReasoning = false, false
	cost, err = p.Cost(u)
	if err != nil || cost.Amount != 40860 {
		t.Fatalf("independent usage changed: %+v %v", cost, err)
	}
}

func TestGrantPolicyFencesRevocationExpiryMethodAndCap(t *testing.T) {
	now := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	exp := now.Add(time.Hour)
	g := &GrantPolicy{GrantID: "g", GrantRevision: 2, TenantID: "tenant_a", ServerID: "srv", Methods: []string{"read"}, CostCap: &Money{Currency: "USD", Amount: 100}, ExpiresAt: &exp, RevocationState: "none"}
	ok := Money{Currency: "USD", Amount: 50}
	if err := g.Permits("tenant_a", "srv", "read", ok, now); err != nil {
		t.Fatal(err)
	}
	for name, probe := range map[string]func() error{
		"other tenant": func() error { return g.Permits("tenant_b", "srv", "read", ok, now) },
		"other server": func() error { return g.Permits("tenant_a", "srv2", "read", ok, now) },
		"other method": func() error { return g.Permits("tenant_a", "srv", "write", ok, now) },
		"expired":      func() error { return g.Permits("tenant_a", "srv", "read", ok, exp) },
		"above cap":    func() error { return g.Permits("tenant_a", "srv", "read", Money{Currency: "USD", Amount: 101}, now) },
		"fenced": func() error {
			fenced := *g
			fenced.RevocationState = "fenced"
			return fenced.Permits("tenant_a", "srv", "read", ok, now)
		},
	} {
		if err := probe(); !errors.Is(err, ErrForbidden) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func admissionFixture(t *testing.T, now time.Time) (AdmissionRequest, AdmissionContext) {
	t.Helper()
	op := openOperation(t, now)
	op.Lifecycle = LifecycleRunning
	at, err := NewAttempt(op, testCmd, "local-check", 0, 1, testProfile.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	price := fixture
	req := AdmissionRequest{Kind: DispatchModel, CallID: "call-1", Owner: "proxy-a", RequestDigest: testCmd.RequestDigest, OperationID: op.ID, AttemptID: at.ID,
		ExecutionEpoch: op.ExecutionEpoch, RouteID: "fixture-route", Provider: "fixture", Model: "fixture-model", MaxExposure: Money{Currency: "USD", Amount: 1000}, Deadline: now.Add(time.Minute)}
	pool := &BudgetPool{ID: "pool", Level: LevelTenant, Currency: "USD", Cap: 10_000}
	ctx := AdmissionContext{Now: now, Operation: op, Attempt: at, Price: &price,
		Authority:   Decision{Allow: true, Revision: "r1", FreshUntil: now.Add(30 * time.Second)},
		Allocations: []*Allocation{NewAllocation(pool, op.ID, Money{Currency: "USD", Amount: 5000}, now)}}
	return req, ctx
}

func TestCheckAdmissionDeniesEveryMissingOrStaleInput(t *testing.T) {
	now := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	req, ctx := admissionFixture(t, now)
	if err := CheckAdmission(req, ctx, true); err != nil {
		t.Fatalf("eligible request denied: %v", err)
	}
	denialOf := func(t *testing.T, err error) string {
		t.Helper()
		var d *Denial
		if !errors.As(err, &d) {
			t.Fatalf("expected a denial, got %v", err)
		}
		return d.Code
	}
	cases := map[string]struct {
		mutate func(*AdmissionRequest, *AdmissionContext)
		code   string
	}{
		"stale epoch":          {func(r *AdmissionRequest, c *AdmissionContext) { r.ExecutionEpoch = 2 }, DenyStaleExecution},
		"fenced operation":     {func(r *AdmissionRequest, c *AdmissionContext) { c.Operation.Control = ControlCancelPending }, DenyStaleExecution},
		"closed attempt":       {func(r *AdmissionRequest, c *AdmissionContext) { c.Attempt.State = AttemptClosed }, DenyStaleExecution},
		"attempt deadline":     {func(r *AdmissionRequest, c *AdmissionContext) { c.Now = c.Attempt.Deadline }, DenyStaleExecution},
		"call deadline passed": {func(r *AdmissionRequest, c *AdmissionContext) { r.Deadline = now }, DenyStaleExecution},
		"call deadline extends": {func(r *AdmissionRequest, c *AdmissionContext) {
			r.Deadline = c.Attempt.Deadline.Add(time.Second)
		}, DenyStaleExecution},
		"non-current instance": {func(r *AdmissionRequest, c *AdmissionContext) {
			r.InstanceID = "inst"
			c.Instance = &Instance{ID: "inst", AttemptID: c.Attempt.ID, Current: false}
		}, DenyStaleExecution},
		"unknown instance": {func(r *AdmissionRequest, c *AdmissionContext) { r.InstanceID = "inst" }, DenyStaleExecution},
		"missing price":    {func(r *AdmissionRequest, c *AdmissionContext) { c.Price = nil }, DenyProfileUnqualified},
		"price not effective": {func(r *AdmissionRequest, c *AdmissionContext) {
			p := *c.Price
			p.EffectiveUntil = now
			c.Price = &p
		}, DenyProfileUnqualified},
		"price currency":     {func(r *AdmissionRequest, c *AdmissionContext) { r.MaxExposure.Currency = "EUR" }, DenyProfileUnqualified},
		"unbounded exposure": {func(r *AdmissionRequest, c *AdmissionContext) { r.MaxExposure.Amount = c.Price.MaxExposure + 1 }, DenyProfileUnqualified},
		"authority denied":   {func(r *AdmissionRequest, c *AdmissionContext) { c.Authority.Allow = false }, DenyForbidden},
		"authority expired":  {func(r *AdmissionRequest, c *AdmissionContext) { c.Authority.FreshUntil = now }, DenyForbidden},
		"no allocation":      {func(r *AdmissionRequest, c *AdmissionContext) { c.Allocations = nil }, DenyBudgetExhausted},
		"allocation short":   {func(r *AdmissionRequest, c *AdmissionContext) { c.Allocations[0].Amount = 999 }, DenyBudgetExhausted},
		"overspent":          {func(r *AdmissionRequest, c *AdmissionContext) { c.Overspent = true }, DenyBudgetExhausted},
		"replacement of an unconfirmed call": {func(r *AdmissionRequest, c *AdmissionContext) {
			r.SupersedesCallID, r.EvidenceRef = "call-0", "not-sent/x"
			c.Original = &Dispatch{CallID: "call-0", Owner: r.Owner, TenantID: c.Operation.TenantID, State: DispatchUnknown}
		}, DenyStaleExecution},
		"replacement without the confirming evidence": {func(r *AdmissionRequest, c *AdmissionContext) {
			r.SupersedesCallID = "call-0"
			c.Original = &Dispatch{CallID: "call-0", Owner: r.Owner, TenantID: c.Operation.TenantID, State: DispatchConfirmedNotSent, EvidenceRef: "not-sent/x"}
		}, DenyStaleExecution},
		"tool without grant": {func(r *AdmissionRequest, c *AdmissionContext) {
			r.Kind = DispatchTool
			c.Price.Kind, c.Price.Provider, c.Price.Model = DispatchTool, "", ""
		}, DenyForbidden},
		"non-positive exposure":     {func(r *AdmissionRequest, c *AdmissionContext) { r.MaxExposure.Amount = 0 }, DenyInvalidArgument},
		"provider not the route's":  {func(r *AdmissionRequest, c *AdmissionContext) { r.Provider = "other-provider" }, DenyProfileUnqualified},
		"model not the route's":     {func(r *AdmissionRequest, c *AdmissionContext) { r.Model = "other-model" }, DenyProfileUnqualified},
		"provider/model incomplete": {func(r *AdmissionRequest, c *AdmissionContext) { r.Provider = "" }, DenyInvalidArgument},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			req, ctx := admissionFixture(t, now)
			tc.mutate(&req, &ctx)
			if got := denialOf(t, CheckAdmission(req, ctx, true)); got != tc.code {
				t.Fatalf("code %s, want %s", got, tc.code)
			}
		})
	}
	// The second transaction skips the headroom probe (the exposure is
	// already reserved) but still applies every fence.
	req, ctx = admissionFixture(t, now)
	ctx.Allocations[0].Reserved = ctx.Allocations[0].Amount
	if err := CheckAdmission(req, ctx, false); err != nil {
		t.Fatalf("recheck with the reservation held: %v", err)
	}
	ctx.Operation.Control = ControlCancelPending
	if got := denialOf(t, CheckAdmission(req, ctx, false)); got != DenyStaleExecution {
		t.Fatalf("recheck ignores the fence: %s", got)
	}
	// A confirmed-not-sent original with matching evidence admits the replacement.
	req, ctx = admissionFixture(t, now)
	req.SupersedesCallID, req.EvidenceRef = "call-0", "not-sent/x"
	ctx.Original = &Dispatch{CallID: "call-0", Owner: req.Owner, TenantID: ctx.Operation.TenantID, State: DispatchConfirmedNotSent, EvidenceRef: "not-sent/x"}
	if err := CheckAdmission(req, ctx, true); err != nil {
		t.Fatalf("replacement of a confirmed-not-sent call denied: %v", err)
	}
}

func TestDispatchIssuesPermissionOnce(t *testing.T) {
	now := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	req, ctx := admissionFixture(t, now)
	d := NewDispatch(req, ctx)
	if d.State != DispatchPrepared || d.Terminal() {
		t.Fatalf("fresh dispatch %s", d.State)
	}
	if err := d.Consume("v1"); err != nil || d.State != DispatchAuthorized {
		t.Fatalf("consume: %v %s", err, d.State)
	}
	if err := d.Consume("v1"); !errors.Is(err, ErrStaleExecution) {
		t.Fatal("permission consumed twice")
	}
	if err := d.Deny(DenyForbidden); !errors.Is(err, ErrStaleExecution) {
		t.Fatal("an authorized call was denied after the fact")
	}
	// A definite outcome without usage is not settlement evidence: the
	// exposure it would release is unknown, so the report is refused and the
	// call is left as it is. An unknown outcome needs no usage.
	for _, outcome := range []DispatchOutcome{DispatchSucceeded, DispatchFailed, DispatchCanceled} {
		if settled, err := d.Observe(outcome, false, now); !errors.Is(err, ErrEvidenceInsufficient) || settled || d.State != DispatchAuthorized {
			t.Fatalf("%s without usage: %v %t %s", outcome, err, settled, d.State)
		}
	}
	settled, err := d.Observe(DispatchOutcomeUnknown, false, now)
	if err != nil || settled || d.State != DispatchUnknown {
		t.Fatalf("unknown observation: %v %t %s", err, settled, d.State)
	}
	if err := d.ConfirmNotSent("not-sent/x", Money{Currency: "USD", Amount: 1}, false); !errors.Is(err, ErrStaleExecution) {
		t.Fatal("a call with observed cost confirmed not sent")
	}
	if err := d.ConfirmNotSent("not-sent/x", Money{Currency: "USD"}, true); !errors.Is(err, ErrStaleExecution) {
		t.Fatal("a call with explicitly reported zero usage confirmed not sent")
	}
	if settled, err := d.Observe(DispatchSucceeded, false, now); !errors.Is(err, ErrEvidenceInsufficient) || settled || d.State != DispatchUnknown {
		t.Fatalf("succeeded without usage after unknown: %v %t %s", err, settled, d.State)
	}
	settled, err = d.Observe(DispatchSucceeded, true, now)
	if err != nil || !settled || d.State != DispatchObserved {
		t.Fatalf("definite observation with usage after unknown: %v %t %s", err, settled, d.State)
	}
	if settled, err := d.Observe(DispatchFailed, true, now); err != nil || settled || d.Outcome != DispatchSucceeded {
		t.Fatalf("an observed call keeps its first outcome: %v %t %s", err, settled, d.Outcome)
	}
	if settled, err := d.Observe(DispatchFailed, false, now); err != nil || settled {
		t.Fatalf("a later report without usage changes nothing on an observed call: %v %t", err, settled)
	}
	if err := d.ConfirmNotSent("not-sent/x", Money{Currency: "USD"}, false); !errors.Is(err, ErrStaleExecution) {
		t.Fatal("an observed call confirmed not sent")
	}
	denied := DeniedDispatch(req, ctx, deny(DenyBudgetExhausted, "x"))
	if _, err := denied.Observe(DispatchSucceeded, true, now); !errors.Is(err, ErrStaleExecution) {
		t.Fatal("usage accepted for a denied call")
	}
	fresh := NewDispatch(req, ctx)
	if err := fresh.ConfirmNotSent("not-sent/y", Money{Currency: "USD"}, false); err != nil || fresh.State != DispatchConfirmedNotSent {
		t.Fatalf("never-consumed call not confirmed: %v", err)
	}
	if err := fresh.ConfirmNotSent("not-sent/z", Money{Currency: "USD"}, false); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatal("a second, different evidence accepted")
	}
}

// TestDispatchBindsOnlyTheRecordedRequest proves that a request reentering
// a persisted call must be that call: the identity alone (owner, call,
// digest) does not let it name another operation, attempt, instance,
// route, grant, exposure, deadline, replacement or provider/model.
func TestDispatchBindsOnlyTheRecordedRequest(t *testing.T) {
	now := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	req, ctx := admissionFixture(t, now)
	d := NewDispatch(req, ctx)
	tenant := ctx.Operation.TenantID
	if err := d.Binds(tenant, req, ctx.Price); err != nil {
		t.Fatalf("the recorded request does not bind its own record: %v", err)
	}
	if err := d.Binds("tenant_b", req, ctx.Price); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another tenant: %v", err)
	}
	for name, mutate := range map[string]func(*AdmissionRequest){
		"digest": func(r *AdmissionRequest) {
			r.RequestDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
		},
		"kind":            func(r *AdmissionRequest) { r.Kind = DispatchTool },
		"operation":       func(r *AdmissionRequest) { r.OperationID = "op_other" },
		"attempt":         func(r *AdmissionRequest) { r.AttemptID = "att_other" },
		"instance":        func(r *AdmissionRequest) { r.InstanceID = "inst_other" },
		"epoch":           func(r *AdmissionRequest) { r.ExecutionEpoch++ },
		"route":           func(r *AdmissionRequest) { r.RouteID = "other-route" },
		"grant":           func(r *AdmissionRequest) { r.GrantID = "grant-x" },
		"grant revision":  func(r *AdmissionRequest) { r.GrantRevision = 2 },
		"currency":        func(r *AdmissionRequest) { r.MaxExposure.Currency = "EUR" },
		"exposure":        func(r *AdmissionRequest) { r.MaxExposure.Amount++ },
		"deadline":        func(r *AdmissionRequest) { r.Deadline = r.Deadline.Add(time.Second) },
		"superseded call": func(r *AdmissionRequest) { r.SupersedesCallID = "call-0" },
		"evidence":        func(r *AdmissionRequest) { r.EvidenceRef = "not-sent/x" },
		"provider":        func(r *AdmissionRequest) { r.Provider = "other-provider" },
		"model":           func(r *AdmissionRequest) { r.Model = "other-model" },
	} {
		other := req
		mutate(&other)
		if err := d.Binds(tenant, other, ctx.Price); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("changed %s accepted as a reentry: %v", name, err)
		}
	}
	// A denial records no exposure and was not bound to the route's
	// observation: the amount and the provider/model are not compared
	// against it, everything else is.
	denied := DeniedDispatch(req, ctx, deny(DenyBudgetExhausted, "x"))
	larger := req
	larger.MaxExposure.Amount++
	larger.Model = "other-model"
	if err := denied.Binds(tenant, larger, ctx.Price); err != nil {
		t.Fatalf("a denial compared an exposure or binding it never recorded: %v", err)
	}
	larger.OperationID = "op_other"
	if err := denied.Binds(tenant, larger, nil); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("a denial let another operation reenter it: %v", err)
	}
	// Once the call is confirmed not sent its evidence names the
	// attestation, not the request's replacement evidence.
	confirmed := NewDispatch(req, ctx)
	if err := confirmed.ConfirmNotSent("not-sent/self", Money{Currency: "USD"}, false); err != nil {
		t.Fatal(err)
	}
	if err := confirmed.Binds(tenant, req, ctx.Price); err != nil {
		t.Fatalf("the original request no longer binds its confirmed call: %v", err)
	}
}
