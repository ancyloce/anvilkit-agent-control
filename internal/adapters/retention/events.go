// Package retention deletes operation events past the retention period
// (RB-A27) under the retention identity (anvilkit_control_retention, granted
// by migration 00019), never the app role, which only appends them.
package retention

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PruneOperationEvents deletes the event history of up to batch operations
// that are terminal, settled, with their cleanup resolved and unchanged
// since before. Each operation keeps its newest attempt-bound event (the
// progress its snapshot projects) and every later one; without such an
// event it keeps none. One statement deletes an operation's whole prefix,
// so what remains of a history is always a suffix of it: a cursor before
// the suffix finds the next sequence missing and is reset
// (domain.EventGap), never served a history with a hole. An operation is a
// candidate while its first event exists; the operations are taken oldest
// change first. It reports the events deleted and the operations pruned.
func PruneOperationEvents(ctx context.Context, pool *pgxpool.Pool, before time.Time, batch int) (events int64, operations int, err error) {
	rows, err := pool.Query(ctx, `WITH candidates AS (
		SELECT o.operation_id, o.updated_at,
		       COALESCE((SELECT max(s.event_seq) FROM operation_events s WHERE s.operation_id = o.operation_id AND s.payload ? 'step'),
		                o.next_event_seq) AS keep_from
		FROM operations o
		WHERE o.lifecycle IN ('succeeded', 'failed', 'canceled') AND o.finance_state = 'settled'
		  AND o.cleanup_state IN ('not_required', 'complete') AND o.updated_at < $1
		  AND EXISTS (SELECT 1 FROM operation_events f WHERE f.operation_id = o.operation_id AND f.event_seq = 1)
	), batch AS (
		SELECT operation_id, keep_from FROM candidates WHERE keep_from > 1 ORDER BY updated_at, operation_id LIMIT $2
	)
	DELETE FROM operation_events e USING batch b
	WHERE e.operation_id = b.operation_id AND e.event_seq < b.keep_from
	RETURNING e.operation_id`, before, batch)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	pruned := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, 0, err
		}
		events++
		pruned[id] = true
	}
	return events, len(pruned), rows.Err()
}

// ErrIdentity is a retention connection that is not the retention identity.
var ErrIdentity = errors.New("retention identity")

// VerifyIdentity checks that the pool connects as the retention identity
// and nothing broader: another role than the app role (appUser), allowed to
// delete operation events, and allowed neither to append them nor to change
// an operation, which rules out the app role and the table owner.
func VerifyIdentity(ctx context.Context, pool *pgxpool.Pool, appUser string) error {
	var user string
	var deletes, inserts, updates bool
	if err := pool.QueryRow(ctx, `SELECT current_user,
		has_table_privilege('operation_events', 'DELETE'),
		has_table_privilege('operation_events', 'INSERT'),
		has_table_privilege('operations', 'UPDATE')`).Scan(&user, &deletes, &inserts, &updates); err != nil {
		return err
	}
	switch {
	case user == appUser:
		return fmt.Errorf("%w: connected as the app role %s", ErrIdentity, user)
	case !deletes:
		return fmt.Errorf("%w: %s may not delete operation events (migration 00019 not applied, or another role)", ErrIdentity, user)
	case inserts || updates:
		return fmt.Errorf("%w: %s may append events or change operations; the retention identity deletes events only", ErrIdentity, user)
	}
	return nil
}
