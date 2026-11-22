package domain

import (
	"errors"
	"testing"
)

// TestRetainExposureClosesOnlyAnUnknownCall: an operator's retained
// exposure (DEC-071) closes an unknown call as observed with its outcome
// still unknown and the evidence named; every other state is refused and
// left as it was.
func TestRetainExposureClosesOnlyAnUnknownCall(t *testing.T) {
	d := &Dispatch{ID: "dsp_1", State: DispatchUnknown, Outcome: DispatchOutcomeUnknown, Reserved: Money{Currency: "USD", Amount: 1_000_000}}
	if err := d.RetainExposure("disposition/model-dispatch/dsp_1"); err != nil {
		t.Fatalf("unknown call: %v", err)
	}
	if d.State != DispatchObserved || d.Outcome != DispatchOutcomeUnknown || d.EvidenceRef != "disposition/model-dispatch/dsp_1" {
		t.Fatalf("retained call: state %s outcome %s evidence %q", d.State, d.Outcome, d.EvidenceRef)
	}
	if !d.Settled() || d.Reserved.Amount != 1_000_000 {
		t.Fatalf("retained call settled=%v reserved=%d: the reservation is its cost, kept on the record", d.Settled(), d.Reserved.Amount)
	}
	for _, state := range []DispatchState{DispatchPrepared, DispatchAuthorized, DispatchObserved, DispatchConfirmedNotSent, DispatchDenied} {
		other := &Dispatch{ID: "dsp_2", State: state, EvidenceRef: "before"}
		if err := other.RetainExposure("after"); !errors.Is(err, ErrStaleExecution) {
			t.Fatalf("%s: want ErrStaleExecution, got %v", state, err)
		}
		if other.State != state || other.EvidenceRef != "before" {
			t.Fatalf("%s: a refused retention changed the call to %s %q", state, other.State, other.EvidenceRef)
		}
	}
}
