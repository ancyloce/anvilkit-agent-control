package domain

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"time"
)

// Money is a fixed-scale (scale 6) integer quantity in one currency
// (contracts.md §4). It is never a floating point number: every operation
// is integer arithmetic with an explicit overflow check, and the wire form
// is the canonical decimal string of the scaled amount.
type Money struct {
	Currency string
	Amount   int64
}

// Scale is the fixed number of decimal places of every amount.
const Scale = 6

var (
	currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
	amountPattern   = regexp.MustCompile(`^-?(0|[1-9][0-9]{0,29})$`)
)

// ParseMoney accepts the wire form: a three-letter currency and a canonical
// decimal amount that fits int64. The contract allows up to 30 digits so a
// runtime may reject an amount it cannot represent losslessly; this one
// rejects anything outside int64 rather than truncating.
func ParseMoney(currency, amount string) (Money, error) {
	if !currencyPattern.MatchString(currency) {
		return Money{}, fmt.Errorf("%w: currency %q", ErrInvalid, currency)
	}
	if !amountPattern.MatchString(amount) {
		return Money{}, fmt.Errorf("%w: amount %q is not a canonical decimal", ErrInvalid, amount)
	}
	v, err := strconv.ParseInt(amount, 10, 64)
	if err != nil {
		return Money{}, fmt.Errorf("%w: amount %q outside the representable range", ErrInvalid, amount)
	}
	return Money{Currency: currency, Amount: v}, nil
}

// AmountString is the canonical decimal wire form of the scaled amount.
func (m Money) AmountString() string { return strconv.FormatInt(m.Amount, 10) }

func (m Money) String() string { return m.AmountString() + " " + m.Currency }

func (m Money) sameCurrency(o Money) error {
	if m.Currency != o.Currency {
		return fmt.Errorf("%w: currency %s differs from %s", ErrInvalid, o.Currency, m.Currency)
	}
	return nil
}

// Add returns m+o or ErrInvalid on a currency mismatch or overflow.
func (m Money) Add(o Money) (Money, error) {
	if err := m.sameCurrency(o); err != nil {
		return Money{}, err
	}
	sum, ok := addInt64(m.Amount, o.Amount)
	if !ok {
		return Money{}, fmt.Errorf("%w: money overflow adding %s to %s", ErrInvalid, o, m)
	}
	return Money{Currency: m.Currency, Amount: sum}, nil
}

// Sub returns m-o or ErrInvalid on a currency mismatch or overflow.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.sameCurrency(o); err != nil {
		return Money{}, err
	}
	if o.Amount == math.MinInt64 {
		return Money{}, fmt.Errorf("%w: money overflow subtracting %s", ErrInvalid, o)
	}
	return m.Add(Money{Currency: o.Currency, Amount: -o.Amount})
}

func addInt64(a, b int64) (int64, bool) {
	c := a + b
	if (c > a) != (b > 0) {
		return 0, false
	}
	return c, true
}

// UsageCategory names a metered counter of a dispatch.
type UsageCategory string

const (
	UsageInput       UsageCategory = "input_units"
	UsageOutput      UsageCategory = "output_units"
	UsageReasoning   UsageCategory = "reasoning_units"
	UsageCachedInput UsageCategory = "cached_input_units"
)

// UsageCategories is the fixed metering vocabulary, in canonical order.
var UsageCategories = []UsageCategory{UsageInput, UsageOutput, UsageReasoning, UsageCachedInput}

// Usage holds cumulative counters reported by a sender. Cumulative means a
// later report of the same source is never smaller than an earlier one;
// Control applies the deltas of the running maximum, never a sum of
// reports. A report carries either an explicit Usage (zero counters are a
// statement that the provider metered nothing) or no Usage at all (the
// sender does not know what was metered); the application keeps the two
// apart, because only the first is settlement evidence.
type Usage struct {
	Input       uint64
	Output      uint64
	Reasoning   uint64
	CachedInput uint64
}

func (u Usage) Get(c UsageCategory) uint64 {
	switch c {
	case UsageInput:
		return u.Input
	case UsageOutput:
		return u.Output
	case UsageReasoning:
		return u.Reasoning
	case UsageCachedInput:
		return u.CachedInput
	}
	return 0
}

// Max is the per-category running maximum of two cumulative reports.
func (u Usage) Max(o Usage) Usage {
	return Usage{Input: max(u.Input, o.Input), Output: max(u.Output, o.Output), Reasoning: max(u.Reasoning, o.Reasoning), CachedInput: max(u.CachedInput, o.CachedInput)}
}

// Price is one immutable pricing observation: it binds a route of a
// dispatch kind — for a model route the trusted provider and model it
// reaches — a currency, an effective interval and the price of every
// metering category (per one million units, scale 6) under one revision.
// A route without a price effective at admission time cannot be admitted,
// a model call naming another provider or model than the route's
// observation has no reviewed price, and a category the price does not
// name cannot be metered.
type Price struct {
	Revision string
	Kind     DispatchKind
	Route    string
	// Provider and Model are the trusted binding of a model route; they are
	// empty for a tool route (server/method is the route itself).
	Provider       string
	Model          string
	Currency       string
	EffectiveFrom  time.Time
	EffectiveUntil time.Time
	PerMillion     map[UsageCategory]int64
	// Inclusion rules belong to this immutable price revision. Native
	// observations remain unchanged; only Go metering separates subsets.
	InputIncludesCached     bool
	OutputIncludesReasoning bool
	// MaxExposure bounds the exposure one call may reserve on this route;
	// a request above it is an unbounded possible exposure and is denied.
	MaxExposure int64
}

const million = 1_000_000

func (p Price) Validate() error {
	if p.Revision == "" || p.Route == "" {
		return fmt.Errorf("%w: price needs a revision and a route", ErrInvalid)
	}
	switch p.Kind {
	case DispatchModel:
		if p.Provider == "" || p.Model == "" {
			return fmt.Errorf("%w: price %s for model route %s must bind a provider and a model", ErrInvalid, p.Revision, p.Route)
		}
	case DispatchTool:
		if p.Provider != "" || p.Model != "" {
			return fmt.Errorf("%w: price %s for tool route %s binds a provider or model", ErrInvalid, p.Revision, p.Route)
		}
	default:
		return fmt.Errorf("%w: price kind %q", ErrInvalid, p.Kind)
	}
	if !currencyPattern.MatchString(p.Currency) {
		return fmt.Errorf("%w: price currency %q", ErrInvalid, p.Currency)
	}
	if !p.EffectiveUntil.After(p.EffectiveFrom) {
		return fmt.Errorf("%w: price %s effective interval is empty", ErrInvalid, p.Revision)
	}
	if p.MaxExposure <= 0 {
		return fmt.Errorf("%w: price %s has no positive exposure bound", ErrInvalid, p.Revision)
	}
	for _, c := range UsageCategories {
		v, ok := p.PerMillion[c]
		if !ok {
			return fmt.Errorf("%w: price %s does not meter %s", ErrInvalid, p.Revision, c)
		}
		if v < 0 {
			return fmt.Errorf("%w: price %s has a negative unit price for %s", ErrInvalid, p.Revision, c)
		}
	}
	return nil
}

// EffectiveAt reports whether the observation covers the instant.
func (p Price) EffectiveAt(t time.Time) bool {
	return !t.Before(p.EffectiveFrom) && t.Before(p.EffectiveUntil)
}

// Cost prices cumulative usage: for every category ceil(units * perMillion
// / 1e6), summed, in integer arithmetic with an overflow check (a cost
// that does not fit the ledger is an error, never a truncated amount).
func (p Price) Cost(u Usage) (Money, error) {
	if p.InputIncludesCached {
		if u.CachedInput > u.Input {
			return Money{}, fmt.Errorf("%w: cached usage exceeds inclusive input", ErrInvalid)
		}
		u.Input -= u.CachedInput
	}
	if p.OutputIncludesReasoning {
		if u.Reasoning > u.Output {
			return Money{}, fmt.Errorf("%w: reasoning usage exceeds inclusive output", ErrInvalid)
		}
		u.Output -= u.Reasoning
	}
	total := new(big.Int)
	m := big.NewInt(million)
	for _, c := range UsageCategories {
		units := new(big.Int).SetUint64(u.Get(c))
		cost := new(big.Int).Mul(units, big.NewInt(p.PerMillion[c]))
		q, r := new(big.Int).QuoRem(cost, m, new(big.Int))
		if r.Sign() > 0 {
			q.Add(q, big.NewInt(1))
		}
		total.Add(total, q)
	}
	if !total.IsInt64() {
		return Money{}, fmt.Errorf("%w: cost of usage under price %s overflows the ledger", ErrInvalid, p.Revision)
	}
	return Money{Currency: p.Currency, Amount: total.Int64()}, nil
}

// BudgetLevel is the allocation level of a pool.
type BudgetLevel string

const (
	LevelPlatform BudgetLevel = "platform"
	LevelTenant   BudgetLevel = "tenant"
	LevelActor    BudgetLevel = "actor"
)

// BudgetLevels in the order allocations are taken.
var BudgetLevels = []BudgetLevel{LevelPlatform, LevelTenant, LevelActor}

// BudgetPool is a shared limit of one level for one period: allocations to
// operations draw from it and can never exceed its cap together.
type BudgetPool struct {
	ID          string
	Level       BudgetLevel
	TenantID    string
	ActorID     string
	PeriodStart time.Time
	PeriodEnd   time.Time
	Currency    string
	Cap         int64
	Allocated   int64
	Revision    uint64
}

// Covers reports whether the pool is the current pool of the scope level.
func (p *BudgetPool) Covers(now time.Time) bool {
	return !now.Before(p.PeriodStart) && now.Before(p.PeriodEnd)
}

// Allocate draws amount from the pool for an operation.
func (p *BudgetPool) Allocate(amount Money, now time.Time) error {
	if amount.Currency != p.Currency {
		return fmt.Errorf("%w: pool %s is in %s, allocation in %s", ErrInvalid, p.ID, p.Currency, amount.Currency)
	}
	if amount.Amount <= 0 {
		return fmt.Errorf("%w: allocation must be positive", ErrInvalid)
	}
	if !p.Covers(now) {
		return fmt.Errorf("%w: pool %s does not cover %s", ErrBudgetExhausted, p.ID, now.UTC().Format(time.RFC3339))
	}
	next, ok := addInt64(p.Allocated, amount.Amount)
	if !ok || next > p.Cap {
		return fmt.Errorf("%w: pool %s (%s) cannot allocate %s: %d of %d allocated", ErrBudgetExhausted, p.ID, p.Level, amount, p.Allocated, p.Cap)
	}
	p.Allocated = next
	p.Revision++
	return nil
}

// Allocation is one operation's share of one pool: the run total that
// every dispatch of the operation reserves from and settles against.
// Consumed may exceed Amount after an overspend; it is never reduced to
// hide that.
type Allocation struct {
	ID          string
	PoolID      string
	OperationID string
	Currency    string
	Amount      int64
	Reserved    int64
	Consumed    int64
	Revision    uint64
	CreatedAt   time.Time
}

func NewAllocation(pool *BudgetPool, operationID string, amount Money, now time.Time) *Allocation {
	return &Allocation{ID: NewID("alloc"), PoolID: pool.ID, OperationID: operationID, Currency: amount.Currency, Amount: amount.Amount, Revision: 1, CreatedAt: now}
}

// Reserve holds exposure for a dispatch: reserved plus consumed plus the
// new exposure must fit the allocation.
func (a *Allocation) Reserve(exposure Money) error {
	if exposure.Currency != a.Currency {
		return fmt.Errorf("%w: allocation %s is in %s, exposure in %s", ErrInvalid, a.ID, a.Currency, exposure.Currency)
	}
	held, ok := addInt64(a.Reserved, a.Consumed)
	if !ok {
		return fmt.Errorf("%w: allocation %s overflow", ErrInvalid, a.ID)
	}
	next, ok := addInt64(held, exposure.Amount)
	if !ok || next > a.Amount {
		return fmt.Errorf("%w: allocation %s cannot reserve %s: %d reserved, %d consumed of %d", ErrBudgetExhausted, a.ID, exposure, a.Reserved, a.Consumed, a.Amount)
	}
	a.Reserved += exposure.Amount
	a.Revision++
	return nil
}

// Release returns a reservation that will never be spent (a confirmed
// not-sent call, a denied second transaction).
func (a *Allocation) Release(exposure Money) error {
	if exposure.Currency != a.Currency || exposure.Amount > a.Reserved || exposure.Amount < 0 {
		return fmt.Errorf("%w: allocation %s cannot release %s of %d reserved", ErrInvalid, a.ID, exposure, a.Reserved)
	}
	a.Reserved -= exposure.Amount
	a.Revision++
	return nil
}

// Charge records actual cost. The delta is appended to consumed even when
// it exceeds what was reserved (overspend is retained, never erased);
// releaseReservation converts the reservation of a settled dispatch back
// to headroom, which never happens for an unknown outcome.
func (a *Allocation) Charge(delta Money, reservation Money, releaseReservation bool) error {
	if delta.Currency != a.Currency || delta.Amount < 0 {
		return fmt.Errorf("%w: allocation %s cannot charge %s", ErrInvalid, a.ID, delta)
	}
	next, ok := addInt64(a.Consumed, delta.Amount)
	if !ok {
		return fmt.Errorf("%w: allocation %s consumed overflow", ErrInvalid, a.ID)
	}
	a.Consumed = next
	if releaseReservation {
		if err := a.Release(reservation); err != nil {
			return err
		}
	}
	a.Revision++
	return nil
}

// Exhausted reports whether the allocation has no headroom left; consumed
// beyond the amount (overspend) is exhausted too.
func (a *Allocation) Exhausted() bool {
	held, ok := addInt64(a.Reserved, a.Consumed)
	return !ok || held >= a.Amount
}

// UsageObservation is one immutable report from one source under one
// sequence number; the same (dispatch, source, sequence) is never applied
// twice. Usage is nil when the report carried no counters: such a report
// is kept for deduplication but never enters the running maximum and never
// settles the call.
type UsageObservation struct {
	ID              string
	DispatchID      string
	Source          string
	Sequence        uint64
	NativeReference string
	Usage           *Usage
	CostRevision    string
	ObservedAt      time.Time
}

// CostEntryKind classifies append-only ledger entries.
type CostEntryKind string

const (
	CostEstimate   CostEntryKind = "estimate"
	CostActual     CostEntryKind = "actual"
	CostCorrection CostEntryKind = "correction"
	CostCredit     CostEntryKind = "credit"
)

// CostEntry is one append-only ledger line of an operation.
type CostEntry struct {
	ID            string
	OperationID   string
	DispatchID    string
	ObservationID string
	Kind          CostEntryKind
	Amount        Money
	CorrectionOf  string
	CreatedAt     time.Time
}

// GrantPolicy is Control's projection of an MCP grant revision (DD-08 §2):
// the fence every tool admission checks. MCP owns the grant; Control owns
// the projection, its receipt and the revocation barrier.
type GrantPolicy struct {
	GrantID         string
	GrantRevision   uint64
	TenantID        string
	PolicyDigest    Digest
	ServerID        string
	Methods         []string
	CostCap         *Money
	ExpiresAt       *time.Time
	PolicyEpoch     uint64
	ReceiptID       string
	RevocationState string // none | fenced | converging | converged
}

// Permits reports whether the exact grant revision still authorizes the
// method on the server for the tenant with the exposure at now: any
// revocation state other than none fences new admission at once.
func (g *GrantPolicy) Permits(tenantID, serverID, method string, exposure Money, now time.Time) error {
	if g.TenantID != tenantID {
		return fmt.Errorf("%w: grant %s is not the caller's", ErrForbidden, g.GrantID)
	}
	if g.RevocationState != "none" {
		return fmt.Errorf("%w: grant %s revision %d is %s", ErrForbidden, g.GrantID, g.GrantRevision, g.RevocationState)
	}
	if g.ServerID != serverID {
		return fmt.Errorf("%w: grant %s is for server %s", ErrForbidden, g.GrantID, g.ServerID)
	}
	if g.ExpiresAt != nil && !now.Before(*g.ExpiresAt) {
		return fmt.Errorf("%w: grant %s expired at %s", ErrForbidden, g.GrantID, g.ExpiresAt.UTC().Format(time.RFC3339))
	}
	granted := false
	for _, m := range g.Methods {
		if m == method {
			granted = true
		}
	}
	if !granted {
		return fmt.Errorf("%w: grant %s does not include method %s", ErrForbidden, g.GrantID, method)
	}
	if g.CostCap != nil && (g.CostCap.Currency != exposure.Currency || exposure.Amount > g.CostCap.Amount) {
		return fmt.Errorf("%w: exposure %s exceeds the grant's cost cap %s", ErrForbidden, exposure, *g.CostCap)
	}
	return nil
}

// Decision is current authority evidence with an absolute freshness bound
// (DD-02 §3): a hit never refreshes it, and expired evidence denies.
type Decision struct {
	Allow      bool
	Revision   string
	ReasonCode string
	FreshUntil time.Time
}

// Current reports whether the decision may still be relied on at now.
func (d Decision) Current(now time.Time) bool { return d.Allow && now.Before(d.FreshUntil) }
