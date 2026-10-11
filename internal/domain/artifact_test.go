package domain

import (
	"errors"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

// The fixed result of a qualification fixture profile: what a certified
// manifest of the profile must name, and the only output that may be
// embedded without a finalized object.
func TestFixtureManifestDeliversItsFixedResult(t *testing.T) {
	fixed := FixedResult{Digest: "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3", SizeBytes: 24}
	fixture := JobProfile{ID: "local-check-v1", JobKind: "validator", ExpectedResult: &fixed}
	wiring := JobProfile{ID: "harness-wiring-dev-v1", JobKind: "codegen"}
	candidate := JobProfile{ID: "codegen-fixed-v1", JobKind: "codegen", CandidateCode: true, ExpectedResult: &fixed}
	other := Digest("sha256:1111111111111111111111111111111111111111111111111111111111111111")
	result := ManifestOutput{Class: "result", Digest: fixed.Digest, SizeBytes: 24}
	evidence := ManifestOutput{Class: "evidence", Digest: other, SizeBytes: 10, Handle: "hdl_ev"}
	cases := []struct {
		name    string
		profile JobProfile
		verdict Verdict
		outputs []ManifestOutput
		refused bool
	}{
		{"certified fixture result names the fixed result", fixture, VerdictCertified, []ManifestOutput{result}, false},
		{"certified fixture result with the fixed result and a bound evidence", fixture, VerdictCertified, []ManifestOutput{result, evidence}, false},
		{"certified fixture result without outputs is refused", fixture, VerdictCertified, nil, true},
		{"another output does not substitute for the fixed result", fixture, VerdictCertified, []ManifestOutput{evidence}, true},
		{"a result of another digest does not substitute", fixture, VerdictCertified, []ManifestOutput{{Class: "result", Digest: other, SizeBytes: 24}}, true},
		{"the reviewed digest with another size does not substitute", fixture, VerdictCertified, []ManifestOutput{{Class: "result", Digest: fixed.Digest, SizeBytes: 25}}, true},
		{"the fixed result named twice is refused", fixture, VerdictCertified, []ManifestOutput{result, result}, true},
		{"a failed fixture run prescribes no output", fixture, VerdictInvalid, nil, false},
		{"a profile without a fixed result prescribes nothing", wiring, VerdictCertified, nil, false},
		{"a candidate-code profile prescribes nothing here", candidate, VerdictCertified, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := ResultManifest{ProfileID: c.profile.ID, Verdict: c.verdict, Outputs: c.outputs}
			err := m.Delivers(c.profile)
			if c.refused != (err != nil) || (err != nil && !errors.Is(err, ErrInvalid)) {
				t.Fatalf("Delivers: refused=%v err=%v", c.refused, err)
			}
		})
	}

	// EmbeddedOutput binds the embedded fixed result to the reviewed digest
	// and size together; a missing fixed result embeds nothing.
	if err := fixture.EmbeddedOutput(result, VerdictCertified); err != nil {
		t.Fatalf("the fixed result embeds: %v", err)
	}
	for name, out := range map[string]ManifestOutput{
		"another size":   {Class: "result", Digest: fixed.Digest, SizeBytes: 25},
		"another digest": {Class: "result", Digest: other, SizeBytes: 24},
		"an evidence":    {Class: "evidence", Digest: other, SizeBytes: 10},
	} {
		if err := fixture.EmbeddedOutput(out, VerdictCertified); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s embedded: %v", name, err)
		}
	}
	if err := fixture.EmbeddedOutput(result, VerdictInvalid); !errors.Is(err, ErrInvalid) {
		t.Fatalf("the fixed result under a failed verdict: %v", err)
	}
	if err := wiring.EmbeddedOutput(result, VerdictCertified); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a profile without a fixed result embeds: %v", err)
	}
	if err := candidate.EmbeddedOutput(result, VerdictCertified); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a candidate-code profile embeds: %v", err)
	}
}

// P1.5 (CTL-APP-03): a transfer is bounded by the operation's effective
// deadline — the active deadline the first permit set — not by the queue
// deadline: a Generation granted its permit late in the queue window can
// still transfer in the tail of its execution window.
func TestTransferBoundByTheEffectiveDeadline(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Microsecond)
	active := start.Add(25 * time.Hour) // the permit came 23h into a 24h queue window, with a 2h active window
	op := &Operation{ID: "op_g", TenantID: "tenant_a", Lifecycle: LifecycleRunning, Control: ControlNone, ExecutionEpoch: 1, RecoveryEpoch: 1,
		Deadline: start.Add(24 * time.Hour), ActiveDeadline: &active}
	now := start.Add(24*time.Hour + 30*time.Minute)
	req := TransferRequest{Class: ArtifactSource, MediaType: "application/zip", ExpectedDigest: Digest("sha256:" + strings.Repeat("0", 64)), ExpectedSize: 10,
		OperationID: op.ID, Deadline: now.Add(15 * time.Minute)}
	tr, err := NewTransfer(CommandIdentity{TenantID: "tenant_a", CommandID: "x", RequestDigest: Digest("sha256:" + strings.Repeat("1", 64))}, Scope{TenantID: "tenant_a", ActorID: "u"}, req,
		TransferContext{Now: now, Operation: op, Limits: TransferLimits{MaxObjectBytes: 1 << 20, MaxWindow: time.Hour}})
	require.NoError(t, err, "a transfer at hour 24.5 is admitted under the active deadline of hour 25")
	require.Equal(t, now.Add(15*time.Minute), tr.Deadline)
	// The active deadline still bounds it.
	req.Deadline = active.Add(time.Hour)
	tr, err = NewTransfer(CommandIdentity{TenantID: "tenant_a", CommandID: "y", RequestDigest: Digest("sha256:" + strings.Repeat("1", 64))}, Scope{TenantID: "tenant_a", ActorID: "u"}, req,
		TransferContext{Now: now, Operation: op, Limits: TransferLimits{MaxObjectBytes: 1 << 20, MaxWindow: 2 * time.Hour}})
	require.NoError(t, err)
	require.Equal(t, active, tr.Deadline, "the transfer never outlives the effective deadline")
	// Without a permit the queue deadline is the bound, as before.
	op.ActiveDeadline = nil
	_, err = NewTransfer(CommandIdentity{TenantID: "tenant_a", CommandID: "z", RequestDigest: Digest("sha256:" + strings.Repeat("1", 64))}, Scope{TenantID: "tenant_a", ActorID: "u"}, req,
		TransferContext{Now: now, Operation: op, Limits: TransferLimits{MaxObjectBytes: 1 << 20, MaxWindow: 2 * time.Hour}})
	require.ErrorIs(t, err, ErrStaleExecution)
}
