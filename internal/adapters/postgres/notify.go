package postgres

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EventChannel is the LISTEN/NOTIFY channel of operation events: InsertEvent
// notifies it at commit with the operation id as the payload.
const EventChannel = "anvilkit_control_events"

// Notifier fans operation-event notifications out in process (P1.4): one
// dedicated connection listens on EventChannel and every notification
// wakes the subscribers of its operation. A subscriber's channel holds one
// pending wake (coalesced); a lost connection is re-established with
// backoff, and meanwhile the streams fall back to their poll. It is a
// wake-up, never a source of events: the stream re-reads committed events
// after its cursor.
type Notifier struct {
	pool *pgxpool.Pool
	log  *slog.Logger

	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

func NewNotifier(pool *pgxpool.Pool, log *slog.Logger) *Notifier {
	return &Notifier{pool: pool, log: log, subs: map[string]map[chan struct{}]struct{}{}}
}

// Subscribe registers a wake channel for the operation until cancel.
func (n *Notifier) Subscribe(operationID string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	set, ok := n.subs[operationID]
	if !ok {
		set = map[chan struct{}]struct{}{}
		n.subs[operationID] = set
	}
	set[ch] = struct{}{}
	n.mu.Unlock()
	return ch, func() {
		n.mu.Lock()
		if set, ok := n.subs[operationID]; ok {
			delete(set, ch)
			if len(set) == 0 {
				delete(n.subs, operationID)
			}
		}
		n.mu.Unlock()
	}
}

// Wake signals every subscriber of the operation.
func (n *Notifier) Wake(operationID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs[operationID] {
		select {
		case ch <- struct{}{}:
		default: // a wake is already pending
		}
	}
}

// Subscribers reports the open subscriptions.
func (n *Notifier) Subscribers() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	total := 0
	for _, set := range n.subs {
		total += len(set)
	}
	return total
}

// Run holds the listening connection until ctx ends, reconnecting with
// backoff after a failure.
func (n *Notifier) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := n.listen(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			n.log.Warn("event notifier connection lost; streams poll until it returns", "error", err, "retryIn", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (n *Notifier) listen(ctx context.Context) error {
	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN "+EventChannel); err != nil {
		return err
	}
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		n.Wake(notification.Payload)
	}
}
