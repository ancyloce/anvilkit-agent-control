package domain_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// TestEventGap (RB-A27): a page honors its cursor only while it continues
// the cursor without a skipped sequence and, short of limit, reaches the
// covered sequence; anything retention deleted answers RESET_REQUIRED.
func TestEventGap(t *testing.T) {
	seqs := func(s ...uint64) []domain.Event {
		evs := make([]domain.Event, 0, len(s))
		for _, n := range s {
			evs = append(evs, domain.Event{EventSeq: n})
		}
		return evs
	}
	for _, tc := range []struct {
		name           string
		after, covered uint64
		events         []domain.Event
		limit          int
		gap            bool
	}{
		{"complete history", 0, 3, seqs(1, 2, 3), 200, false},
		{"cursor at the covered sequence", 3, 3, nil, 200, false},
		{"no event yet", 0, 0, nil, 200, false},
		{"a full page continues later", 0, 5, seqs(1, 2), 2, false},
		{"mid-history cursor", 2, 4, seqs(3, 4), 200, false},
		{"every event deleted", 0, 7, nil, 200, true},
		{"the first events deleted", 0, 7, seqs(6), 200, true},
		{"the kept snapshot event follows the cursor, later ones deleted", 5, 7, seqs(6), 200, true},
		{"a sequence skipped inside the page", 0, 3, seqs(1, 3), 200, true},
		{"a full page that does not continue the cursor", 0, 9, seqs(4, 5), 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.gap, domain.EventGap(tc.after, tc.covered, tc.events, tc.limit))
		})
	}
}
