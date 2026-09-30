package domain

import (
	"strings"
	"testing"
	"time"
)

func h(c string) Digest { return Digest("sha256:" + strings.Repeat(c, 64)) }

func releaseFixture(t *testing.T) (*Operation, *ReleaseSubject, *ReleaseCertification) {
	t.Helper()
	cert := &ReleaseCertification{EvidenceDigest: h("9"), Npm: ArtifactDigestRef{h("6"), "20480"}, Browser: ArtifactDigestRef{h("7"), "8192"}, CSS: []ArtifactDigestRef{{h("8"), "512"}}}
	s := &ReleaseSubject{
		SchemaVersion: 1, ComponentID: "cmp_hero", PuckType: "Hero", SourceRevision: "3", SourceDigest: h("4"), PackageName: "@anvilkit/hero", Version: "1.0.0",
		Npm: cert.Npm, Browser: cert.Browser, CSS: cert.CSS, BuildProfileID: "build-support-v1", BuildProfileDigest: h("5"),
		ValidatorProfileID: "validator-v1", ValidatorProfileDigest: h("b"), HostAbi: "host-abi-v1", HostAbiDigest: h("c"),
		Destinations:                ReleaseDestinations{NpmRegistry: "https://registry.example.invalid/", BrowserOrigin: "https://components.example.invalid"},
		CertificationEvidenceDigest: cert.EvidenceDigest,
	}
	d, err := ComputeSubjectDigest(*s)
	if err != nil {
		t.Fatal(err)
	}
	s.SubjectDigest = d
	op := &Operation{ID: "op_rel", TenantID: "tenant_a", Kind: KindRelease, Subject: Subject{SubjectDigest: h("0"), SourceRevision: "3", PackageVersion: "1.0.0"}}
	return op, s, cert
}

func TestSubjectDigestContractVector(t *testing.T) {
	_, s, _ := releaseFixture(t)
	if want := Digest("sha256:ba0108acc45e339e7a8041e35a67fd50191f044216e4f6b8a46796554d5be75b"); s.SubjectDigest != want {
		t.Fatalf("subject digest %s, contract vector %s", s.SubjectDigest, want)
	}
}

func TestApplyReleaseBindings(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(24 * time.Hour)
	op, s, cert := releaseFixture(t)
	pending := ReleaseTarget{State: TargetPending}
	must := func(cur *Release, r ReleaseRecord) *Release {
		t.Helper()
		next, _, err := ApplyRelease(op, cur, r, cert, now)
		if err != nil {
			t.Fatalf("%s: %v", r.State, err)
		}
		return next
	}
	refuse := func(cur *Release, r ReleaseRecord, why string) {
		t.Helper()
		if _, _, err := ApplyRelease(op, cur, r, cert, now); err == nil {
			t.Fatalf("accepted: %s", why)
		}
	}
	cur := must(nil, ReleaseRecord{State: ReleaseCertifying, Npm: pending, Browser: pending, Activation: pending})
	awaiting := ReleaseRecord{ExpectedRevision: 1, State: ReleaseAwaitingApproval, Subject: s, ReleaseID: "rel_1", ReviewEffectID: "eff_rev",
		Approval: &Approval{State: ApprovalPending, SubjectDigest: s.SubjectDigest}, ApprovalDeadline: &deadline, Npm: pending, Browser: pending, Activation: pending}

	forged := *s
	forged.Version = "1.0.1"
	bad := awaiting
	bad.Subject = &forged
	refuse(cur, bad, "a subject whose digest does not cover its fields")
	forged = *s
	forged.Npm = ArtifactDigestRef{h("e"), "1"}
	forged.SubjectDigest, _ = ComputeSubjectDigest(forged)
	bad.Subject = &forged
	refuse(cur, bad, "artifact digests other than the accepted certification")
	forged = *s
	forged.SourceRevision = "2"
	forged.SubjectDigest, _ = ComputeSubjectDigest(forged)
	bad.Subject = &forged
	refuse(cur, bad, "another revision than the operation's")
	if _, _, err := ApplyRelease(op, cur, awaiting, nil, now); err == nil {
		t.Fatal("a subject without an accepted certified stage")
	}
	cur = must(cur, awaiting)

	// Stale approval: a decision for another digest authorizes nothing.
	sent := ReleaseTarget{State: TargetUnknown, EffectID: "eff_npm"}
	publishing := ReleaseRecord{ExpectedRevision: 2, State: ReleasePublishing, Subject: s, ReleaseID: "rel_1", ReviewEffectID: "eff_rev",
		Approval: &Approval{State: ApprovalApproved, SubjectDigest: h("a")}, ApprovalDeadline: &deadline, Npm: pending, Browser: pending, Activation: pending}
	refuse(cur, publishing, "an approval of another subject")
	publishing.Approval = &Approval{State: ApprovalApproved, SubjectDigest: s.SubjectDigest, ApproverID: "maintainer_a"}
	changed := *s
	changed.Destinations.NpmRegistry = "https://other.example.invalid/"
	changed.SubjectDigest, _ = ComputeSubjectDigest(changed)
	moved := publishing
	moved.Subject = &changed
	refuse(cur, moved, "a changed subject inherits the approval")
	cur = must(cur, publishing)

	// Receipts bind the subject, the version and the destination.
	npmOK := ReleaseTarget{State: TargetSucceeded, EffectID: "eff_npm", ReceiptID: "r1", ReceiptDigest: string(h("1")), SubjectDigest: s.SubjectDigest, Destination: s.Destinations.NpmRegistry, Version: "1.0.0"}
	wrongDest := npmOK
	wrongDest.Destination = "https://elsewhere.invalid/"
	rec := publishing
	rec.ExpectedRevision = 3
	rec.Npm = wrongDest
	refuse(cur, rec, "a receipt naming another destination")
	rec.Npm = npmOK
	rec.Browser = sent
	rec.Browser.EffectID = "eff_browser"
	rec.State = ReleaseReconciling
	cur = must(cur, rec) // npm-only success with browser unknown: reconciling, never published

	early := rec
	early.ExpectedRevision = 4
	early.Activation = ReleaseTarget{State: TargetUnknown, EffectID: "eff_act"}
	refuse(cur, early, "activation before both receipts")
	pub := rec
	pub.ExpectedRevision = 4
	pub.State = ReleasePublished
	refuse(cur, pub, "published with the browser target unknown")
	resend := rec
	resend.ExpectedRevision = 4
	resend.Npm = ReleaseTarget{State: TargetUnknown, EffectID: "eff_npm"}
	refuse(cur, resend, "a succeeded target changes")
	partial := rec
	partial.ExpectedRevision = 4
	partial.State = ReleasePartiallyPublished
	partial.Browser = ReleaseTarget{State: TargetFailed, EffectID: "eff_browser", FailureCode: "REGISTRY_REJECTED"}
	p := must(cur, partial)
	if p.Published() {
		t.Fatal("npm-only success is never published")
	}
	browserOK := ReleaseTarget{State: TargetSucceeded, EffectID: "eff_browser", ReceiptID: "r2", ReceiptDigest: string(h("2")), SubjectDigest: s.SubjectDigest,
		Destination: s.Destinations.BrowserOrigin, Version: "1.0.0", ManifestDigest: string(h("d"))}
	pub.Browser = browserOK
	cur = must(cur, pub)
	act := pub
	act.ExpectedRevision = 5
	act.State = ReleaseActivated
	act.Activation = ReleaseTarget{State: TargetSucceeded, EffectID: "eff_act", ReceiptID: "r3", ReceiptDigest: string(h("3")), SubjectDigest: s.SubjectDigest}
	refuse(cur, act, "activated without the catalog revision")
	act.CatalogRevision = "4"
	cur = must(cur, act)
	again, existing, err := ApplyRelease(op, cur, act, cert, now)
	if err != nil || !existing || again.Revision != 6 {
		t.Fatalf("the same transition repeated: %v %v", existing, err)
	}
}

func TestCheckReleaseEffect(t *testing.T) {
	op, s, _ := releaseFixture(t)
	req := func(kind EffectKind, target string) EffectRequest {
		return EffectRequest{Kind: kind, CanonicalSubject: ReleaseEffectSubject(s.SubjectDigest, target)}
	}
	code := func(err error) string {
		if d, ok := err.(*Denial); ok {
			return d.Code
		}
		return ""
	}
	awaiting := &Release{Subject: s, Approval: &Approval{State: ApprovalPending, SubjectDigest: s.SubjectDigest}}
	if code(CheckReleaseEffect(req(EffectPublication, "npm"), op, awaiting)) != DenyApprovalRequired {
		t.Fatal("publication before the approval")
	}
	stale := &Release{Subject: s, Approval: &Approval{State: ApprovalApproved, SubjectDigest: h("a")}}
	if code(CheckReleaseEffect(req(EffectPublication, "npm"), op, stale)) != DenyApprovalRequired {
		t.Fatal("publication under an approval of another subject")
	}
	approved := &Release{Subject: s, Approval: &Approval{State: ApprovalApproved, SubjectDigest: s.SubjectDigest}, Npm: ReleaseTarget{State: TargetSucceeded}, Browser: ReleaseTarget{State: TargetUnknown}}
	if err := CheckReleaseEffect(req(EffectPublication, "browser"), op, approved); err != nil {
		t.Fatal(err)
	}
	if code(CheckReleaseEffect(EffectRequest{Kind: EffectPublication, CanonicalSubject: ReleaseEffectSubject(h("a"), "npm")}, op, approved)) != DenyApprovalRequired {
		t.Fatal("publication of another subject")
	}
	if code(CheckReleaseEffect(req(EffectActivation, "activation"), op, approved)) != DenyReceiptsRequired {
		t.Fatal("activation with the browser target unknown")
	}
	approved.Browser.State = TargetSucceeded
	if err := CheckReleaseEffect(req(EffectActivation, "activation"), op, approved); err != nil {
		t.Fatal(err)
	}
	other := &Operation{ID: "op_gen", Kind: KindGeneration}
	if code(CheckReleaseEffect(req(EffectPublication, "npm"), other, nil)) != DenyInvalidArgument {
		t.Fatal("a publication effect of a non-release operation")
	}
}
