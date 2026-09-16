package domain

import (
	"errors"
	"testing"
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
