package application

import (
	"context"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// budget is the rank-1 accounting state of one operation's dispatches
// (P2.1): the actual-cost caps of its tenant, project and operation, locked
// first in id order, then its allocations. Every change of a reservation or
// of consumed cost is applied to both, so the tenant and project caps count
// exactly what the allocations of their operations hold.
type budget struct {
	caps   []*domain.CostCap
	allocs []*domain.Allocation
}

func lockBudget(ctx context.Context, r Repo, operationID string) (*budget, error) {
	caps, err := r.LockOperationCostCaps(ctx, operationID)
	if err != nil {
		return nil, err
	}
	allocs, err := r.LockAllocations(ctx, operationID)
	if err != nil {
		return nil, err
	}
	return &budget{caps: caps, allocs: allocs}, nil
}

// counted are the caps that keep counters in the currency.
func (b *budget) counted(currency string) []*domain.CostCap {
	var out []*domain.CostCap
	for _, c := range b.caps {
		if c.Counted() && c.Currency == currency {
			out = append(out, c)
		}
	}
	return out
}

func (b *budget) reserve(m domain.Money) error {
	for _, a := range b.allocs {
		if err := a.Reserve(m); err != nil {
			return err
		}
	}
	for _, c := range b.counted(m.Currency) {
		if err := c.Reserve(m.Amount); err != nil {
			return err
		}
	}
	return nil
}

func (b *budget) release(m domain.Money) error {
	for _, a := range b.allocs {
		if err := a.Release(m); err != nil {
			return err
		}
	}
	for _, c := range b.counted(m.Currency) {
		c.Release(m.Amount)
	}
	return nil
}

func (b *budget) charge(delta, reservation domain.Money, releaseReservation bool) error {
	for _, a := range b.allocs {
		if err := a.Charge(delta, reservation, releaseReservation); err != nil {
			return err
		}
	}
	for _, c := range b.counted(delta.Currency) {
		if err := c.Charge(delta.Amount); err != nil {
			return err
		}
		if releaseReservation {
			c.Release(reservation.Amount)
		}
	}
	return nil
}

func (b *budget) restore(m domain.Money) error {
	for _, a := range b.allocs {
		if err := a.Restore(m); err != nil {
			return err
		}
	}
	for _, c := range b.counted(m.Currency) {
		if err := c.Reserve(m.Amount); err != nil {
			return err
		}
	}
	return nil
}

func (b *budget) save(ctx context.Context, r Repo) error {
	for _, c := range b.caps {
		if c.Counted() {
			if err := r.UpdateCostCapCounters(ctx, c); err != nil {
				return err
			}
		}
	}
	for _, a := range b.allocs {
		if err := r.UpdateAllocation(ctx, a); err != nil {
			return err
		}
	}
	return nil
}
