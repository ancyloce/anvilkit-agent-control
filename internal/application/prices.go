package application

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// StoredPrices is the production price book (P2.1, ENV-06): the price
// revisions published through PriceAdminService, held as a snapshot that
// every replica refreshes at its interval and at once after a publication
// of its own. Revisions are immutable rows, so a snapshot can be late but
// never wrong: a revision another replica published moments ago is read
// from the database before PriceByRevision reports it missing, and a
// withdrawn revision admits nothing new while it keeps metering the
// dispatches prepared under it.
type StoredPrices struct {
	store Store
	log   *slog.Logger

	mu         sync.RWMutex
	byRoute    map[string][]*domain.PriceRecord
	byRevision map[string]*domain.PriceRecord
}

func NewStoredPrices(store Store, log *slog.Logger) *StoredPrices {
	return &StoredPrices{store: store, log: log, byRoute: map[string][]*domain.PriceRecord{}, byRevision: map[string]*domain.PriceRecord{}}
}

// Refresh replaces the snapshot with the database's revisions.
func (p *StoredPrices) Refresh(ctx context.Context) error {
	var rows []*domain.PriceRecord
	if err := p.store.Read(ctx, func(r Repo) error {
		var err error
		rows, err = r.ListPriceRevisions(ctx)
		return err
	}); err != nil {
		return err
	}
	byRoute := map[string][]*domain.PriceRecord{}
	byRevision := make(map[string]*domain.PriceRecord, len(rows))
	for _, rec := range rows {
		byRevision[rec.Revision] = rec
		if rec.WithdrawnAt == nil {
			key := string(rec.Kind) + "/" + rec.Route
			byRoute[key] = append(byRoute[key], rec)
		}
	}
	// The most specific observation wins: the latest effective start, then
	// the latest publication.
	for _, list := range byRoute {
		sort.SliceStable(list, func(i, j int) bool {
			if !list[i].EffectiveFrom.Equal(list[j].EffectiveFrom) {
				return list[i].EffectiveFrom.After(list[j].EffectiveFrom)
			}
			return list[i].PublishedAt.After(list[j].PublishedAt)
		})
	}
	p.mu.Lock()
	p.byRoute, p.byRevision = byRoute, byRevision
	p.mu.Unlock()
	return nil
}

// Run refreshes the snapshot every interval until ctx ends.
func (p *StoredPrices) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := p.Refresh(ctx); err != nil && ctx.Err() == nil {
			p.log.Warn("price book refresh failed; the previous snapshot keeps serving", "error", err)
		}
	}
}

// Count is the number of revisions in the snapshot.
func (p *StoredPrices) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.byRevision)
}

func (p *StoredPrices) Price(kind domain.DispatchKind, route string, at time.Time) (*domain.Price, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, rec := range p.byRoute[string(kind)+"/"+route] {
		if rec.EffectiveAt(at) {
			price := rec.Price
			return &price, true
		}
	}
	return nil, false
}

func (p *StoredPrices) PriceByRevision(revision string) (*domain.Price, bool) {
	if revision == "" {
		return nil, false
	}
	p.mu.RLock()
	rec, ok := p.byRevision[revision]
	p.mu.RUnlock()
	if ok {
		price := rec.Price
		return &price, true
	}
	// Published by another replica after this snapshot: read it once.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := p.store.Read(ctx, func(r Repo) error {
		var err error
		rec, err = r.GetPriceRevision(ctx, revision)
		return err
	})
	if err != nil {
		if !errors.Is(err, domain.ErrNotFound) {
			p.log.Warn("price revision read failed", "revision", revision, "error", err)
		}
		return nil, false
	}
	p.mu.Lock()
	p.byRevision[revision] = rec
	p.mu.Unlock()
	price := rec.Price
	return &price, true
}
