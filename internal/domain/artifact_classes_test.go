package domain

import (
	"errors"
	"testing"
	"time"
)

// The build deliverables of a certified component (P10) are artifact
// classes of their own: a validator manifest binds an npm tarball, a
// browser module and a stylesheet to finalized transfers of exactly those
// classes, never to a transfer of another class, and never without a
// finalized object.
func TestBuildDeliverableClassesBind(t *testing.T) {
	for _, c := range []string{"npm", "browser", "css"} {
		if _, err := ParseArtifactClass(c); err != nil {
			t.Fatalf("%s is not an artifact class: %v", c, err)
		}
	}
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	op := &Operation{ID: "op_1", TenantID: "tenant_a", ExecutionEpoch: 1, RecoveryEpoch: 1}
	at := &Attempt{ID: "att_1", TenantID: "tenant_a", OperationID: "op_1", ExecutionEpoch: 1}
	digest := Digest("sha256:6666666666666666666666666666666666666666666666666666666666666666")
	finalized := func(class ArtifactClass) *Transfer {
		return &Transfer{ID: "xfer_" + string(class), TenantID: "tenant_a", OperationID: "op_1", AttemptID: "att_1", Class: class, State: TransferFinalized,
			ActualDigest: digest, ActualSize: 2637, ObjectVersion: "v1", Handle: "hdl_" + string(class), ExecutionEpoch: 1, RecoveryEpoch: 1, FinalizedAt: &now}
	}
	cases := []struct {
		name    string
		out     ManifestOutput
		t       *Transfer
		refused bool
	}{
		{"npm output binds a finalized npm transfer", ManifestOutput{Class: "npm", Digest: digest, SizeBytes: 2637, Handle: "hdl_npm"}, finalized(ArtifactNpm), false},
		{"browser output binds a finalized browser transfer", ManifestOutput{Class: "browser", Digest: digest, SizeBytes: 2637, Handle: "hdl_browser"}, finalized(ArtifactBrowser), false},
		{"css output binds a finalized css transfer", ManifestOutput{Class: "css", Digest: digest, SizeBytes: 2637, Handle: "hdl_css"}, finalized(ArtifactCSS), false},
		{"an npm output does not bind a source transfer", ManifestOutput{Class: "npm", Digest: digest, SizeBytes: 2637, Handle: "hdl_source"}, finalized(ArtifactSource), true},
		{"a browser output does not bind a begun transfer", ManifestOutput{Class: "browser", Digest: digest, SizeBytes: 2637, Handle: "hdl_browser"}, func() *Transfer { x := finalized(ArtifactBrowser); x.State = TransferBegun; return x }(), true},
		{"a css output of another digest is refused", ManifestOutput{Class: "css", Digest: "sha256:7777777777777777777777777777777777777777777777777777777777777777", SizeBytes: 2637, Handle: "hdl_css"}, finalized(ArtifactCSS), true},
		{"a chunks output has no class on this surface", ManifestOutput{Class: "chunks", Digest: digest, SizeBytes: 1, Handle: "hdl_x"}, finalized(ArtifactCSS), true},
		// The joint stage of a codegen team attempt (P12) binds a finalized stage transfer and nothing else.
		{"a stage output binds a finalized stage transfer", ManifestOutput{Class: "stage", Digest: digest, SizeBytes: 2637, Handle: "hdl_stage"}, finalized(ArtifactStage), false},
		{"a stage output does not bind an evidence transfer", ManifestOutput{Class: "stage", Digest: digest, SizeBytes: 2637, Handle: "hdl_evidence"}, finalized(ArtifactEvidence), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a, err := BindArtifact(c.out, c.t, op, at)
			if c.refused {
				if err == nil {
					t.Fatalf("bound %+v", a)
				}
				if !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrStaleExecution) {
					t.Fatalf("unexpected error class: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if a.Class != c.t.Class || a.ObjectVersion != "v1" || a.Digest != digest {
				t.Fatalf("bound %+v", a)
			}
		})
	}
	// A validator profile embeds nothing: a deliverable without a handle is refused.
	profile := JobProfile{ID: "validator-fixed-dev-v1", JobKind: "validator"}
	if err := profile.EmbeddedOutput(ManifestOutput{Class: "npm", Digest: digest, SizeBytes: 2637}, VerdictCertified); err == nil {
		t.Fatal("an npm output without a finalized object was accepted")
	}
}
