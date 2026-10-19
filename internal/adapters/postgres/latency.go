package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// LifecycleLatency is platform.md SLO row 2 (P3.6): the seconds from an
// operation's acceptance (created_at, its acceptedAt) to its first
// progress — the transition that takes it out of the accepted lifecycle —
// and to its terminal lifecycle (for a Generation, succeeded is the
// accepted candidate). Both are labelled by kind, profile and the
// lifecycle reached; no identifier reaches a label.
//
// The store observes a milestone after the commit of the transaction whose
// event reaches it, on the replica that committed it. Every transition
// commits once (its event sequence and transition id are unique and it is
// written under the operation lock), so a rolled-back transaction, a
// retried command that answers the recorded outcome and a racing replica
// that loses the lock all observe nothing. Operations Control re-created
// from the inventory in a recovery (whose first event is not their
// acceptance) are not observed.
type LifecycleLatency struct {
	FirstProgress *prometheus.HistogramVec
	Completion    *prometheus.HistogramVec
}

// WithLifecycleLatency makes the store observe the lifecycle latencies.
func (s *Store) WithLifecycleLatency(l *LifecycleLatency) *Store {
	s.latency = l
	return s
}

// stageLatency records the milestone an inserted event reaches; Tx observes
// it once the transaction committed.
func (r *repo) stageLatency(ctx context.Context, ev domain.Event, kind, profileID string, createdAt time.Time) error {
	if r.latency == nil || (!ev.FirstProgress() && !ev.Completes()) {
		return nil
	}
	if ev.Completes() {
		var accepted bool
		err := r.db.QueryRow(ctx, "SELECT payload->>'lifecycle' = $2 FROM operation_events WHERE operation_id = $1 AND event_seq = 1",
			ev.OperationID, string(domain.LifecycleAccepted)).Scan(&accepted)
		// An operation without its first event (one a recovery restored) is
		// not observed; a metric never fails the transition it measures.
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && !accepted) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	seconds := max(ev.OccurredAt.Sub(createdAt).Seconds(), 0)
	labels := prometheus.Labels{"kind": kind, "profile": profileID, "lifecycle": string(ev.Payload.Lifecycle)}
	if ev.FirstProgress() {
		r.committed = append(r.committed, func() { r.latency.FirstProgress.With(labels).Observe(seconds) })
	}
	if ev.Completes() {
		r.committed = append(r.committed, func() { r.latency.Completion.With(labels).Observe(seconds) })
	}
	return nil
}
