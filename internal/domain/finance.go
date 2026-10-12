package domain

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
)

// DenyCapExceeded refuses an admission that would take the reserved
// exposure plus the consumed cost of a tenant, project or operation above
// its actual-cost cap (ENV-06), or that has no cap at a required level.
const DenyCapExceeded = "CAP_EXCEEDED"

// CapLevel is the scope of an actual-cost cap.
type CapLevel string

const (
	CapTenant    CapLevel = "tenant"
	CapProject   CapLevel = "project"
	CapOperation CapLevel = "operation"
)

// CapLevels in the order admission reports them.
var CapLevels = []CapLevel{CapTenant, CapProject, CapOperation}

// CostCap is an actual-cost cap of one scope (P2.1, DD-02 §3): at
// admission the scope's reserved exposure plus its consumed cost plus the
// new exposure never exceeds Cap. Reserved and Consumed are the tenant's or
// the project's running counters; an operation cap keeps none, it is
// checked against the operation's own allocation. Lowering Cap fences new
// admission only: issued permissions and incurred cost are never revoked.
type CostCap struct {
	ID        string
	Level     CapLevel
	TenantID  string
	ProjectID string
	Currency  string
	Cap       int64
	Reserved  int64
	Consumed  int64
	Revision  uint64
	UpdatedBy string
	UpdatedAt time.Time
}

// Counted reports whether the cap keeps running counters.
func (c *CostCap) Counted() bool { return c.Level != CapOperation }

// headroomFor checks spend+exposure against the cap.
func (c *CostCap) headroomFor(spend, exposure int64) error {
	next, ok := addInt64(spend, exposure)
	if !ok || next > c.Cap {
		project := ""
		if c.Level != CapTenant && c.ProjectID != "" {
			project = " project " + c.ProjectID
		}
		return deny(DenyCapExceeded, "%s cost cap of tenant %s%s: %d spent or reserved, %d more would exceed %d %s", c.Level, c.TenantID, project, spend, exposure, c.Cap, c.Currency)
	}
	return nil
}

// Reserve, Release, Charge and Restore follow the allocation they shadow;
// they never check the cap (admission does) and never go negative.
func (c *CostCap) Reserve(amount int64) error {
	next, ok := addInt64(c.Reserved, amount)
	if !ok || amount < 0 {
		return fmt.Errorf("%w: cost cap %s cannot reserve %d", ErrInvalid, c.ID, amount)
	}
	c.Reserved = next
	c.Revision++
	return nil
}

func (c *CostCap) Release(amount int64) {
	c.Reserved = max(0, c.Reserved-amount)
	c.Revision++
}

func (c *CostCap) Charge(delta int64) error {
	next, ok := addInt64(c.Consumed, delta)
	if !ok || delta < 0 {
		return fmt.Errorf("%w: cost cap %s cannot charge %d", ErrInvalid, c.ID, delta)
	}
	c.Consumed = next
	c.Revision++
	return nil
}

// CheckCaps applies the cost caps of one admission. exposure is the new
// reservation (zero at step 4, where the call's own exposure is already
// counted); required denies a paid route whose tenant, project or
// operation has no cap (outside development, "missing caps deny the paid
// route"). The operation's spend is its allocation's reserved plus
// consumed.
func CheckCaps(caps []*CostCap, allocations []*Allocation, currency string, exposure int64, required bool) error {
	byLevel := map[CapLevel]*CostCap{}
	for _, c := range caps {
		if c.Currency != currency {
			continue
		}
		// An operation cap of the operation's own project wins over the
		// tenant-wide one.
		if prev := byLevel[c.Level]; prev != nil && c.Level == CapOperation && prev.ProjectID != "" {
			continue
		}
		byLevel[c.Level] = c
	}
	var opSpend int64
	for _, a := range allocations {
		if held, ok := addInt64(a.Reserved, a.Consumed); ok && held > opSpend {
			opSpend = held
		}
	}
	for _, level := range CapLevels {
		c := byLevel[level]
		if c == nil {
			if required {
				return deny(DenyCapExceeded, "no %s cost cap in %s for the operation's scope", level, currency)
			}
			continue
		}
		spend := opSpend
		if c.Counted() {
			held, ok := addInt64(c.Reserved, c.Consumed)
			if !ok {
				return fmt.Errorf("%w: cost cap %s overflow", ErrInvalid, c.ID)
			}
			spend = held
		}
		if err := c.headroomFor(spend, exposure); err != nil {
			return err
		}
	}
	return nil
}

// PriceRecord is a published price revision (P2.1): the immutable
// observation, its canonical digest and its publication facts. A withdrawn
// revision admits nothing new; PriceByRevision still serves it to the
// dispatches prepared under it.
type PriceRecord struct {
	Price
	Digest      Digest
	PublishedBy string
	CommandID   string
	PublishedAt time.Time
	WithdrawnAt *time.Time
	WithdrawnBy string
}

// PriceDigest is the digest of a revision's canonical content: the same
// revision published twice with the same content is the same record.
func PriceDigest(p Price) Digest {
	canonical := struct {
		Revision, Kind, Route, Provider, Model, Currency string
		From, Until                                      string
		Input, Output, Reasoning, CachedInput            int64
		MaxExposure                                      int64
		InputIncludesCached, OutputIncludesReasoning     bool
	}{
		p.Revision, string(p.Kind), p.Route, p.Provider, p.Model, p.Currency,
		p.EffectiveFrom.UTC().Format(time.RFC3339Nano), p.EffectiveUntil.UTC().Format(time.RFC3339Nano),
		p.PerMillion[UsageInput], p.PerMillion[UsageOutput], p.PerMillion[UsageReasoning], p.PerMillion[UsageCachedInput],
		p.MaxExposure, p.InputIncludesCached, p.OutputIncludesReasoning,
	}
	body, _ := json.Marshal(canonical)
	return Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(body)))
}

// ReturnUnspent is the part of an allocation a terminal operation gives
// back to its pool: the amount not consumed (never negative; an overspend
// returns nothing). A reservation still held means the operation is not
// settled and nothing returns.
func (a *Allocation) ReturnUnspent() (int64, error) {
	if a.Reserved != 0 {
		return 0, fmt.Errorf("%w: allocation %s still reserves %d", ErrInvalid, a.ID, a.Reserved)
	}
	return max(0, a.Amount-a.Consumed), nil
}

// Return gives an amount of a settled allocation back to the pool.
func (p *BudgetPool) Return(amount int64) error {
	if amount < 0 || amount > p.Allocated {
		return fmt.Errorf("%w: pool %s cannot take back %d of %d allocated", ErrInvalid, p.ID, amount, p.Allocated)
	}
	p.Allocated -= amount
	p.Revision++
	return nil
}

// AuthorizationQuery is one commercial authorization question (DD-02 §3,
// DD-06 AuthorizationPort): may the principal of the scope perform the
// action on the resource now. Disclosure questions come from Knowledge
// before retrieval reveals content, execution questions from admission and
// from MCP before a tool call.
type AuthorizationQuery struct {
	Scope        Scope
	ResourceType string
	ResourceID   string
	Action       string
	Disclosure   bool
}

// ModelRouteQuery is the execution question of a model route admission.
func ModelRouteQuery(scope Scope, routeID string) AuthorizationQuery {
	return AuthorizationQuery{Scope: scope, ResourceType: "model-route", ResourceID: routeID, Action: "execute"}
}
