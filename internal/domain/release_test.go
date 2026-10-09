package domain

import (
	"errors"
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

// ledger is the effect rows of the fixture release as a sender observed
// them: the registered review and the succeeded npm, browser and
// activation effects with their receipts.
func ledger(op *Operation, s *ReleaseSubject) ReleaseEffects {
	e := func(id string, kind EffectKind, target, ref string) *Effect {
		subject := ReleaseEffectSubject(s.SubjectDigest, target)
		return &Effect{ID: id, OperationID: op.ID, TenantID: op.TenantID, Kind: kind, CanonicalSubject: subject, State: EffectSucceeded, Outcome: EffectOutcomeSucceeded, OutcomeRef: ref}
	}
	return ReleaseEffects{
		"eff_rev":     e("eff_rev", EffectReview, "review", string(s.SubjectDigest)),
		"eff_npm":     e("eff_npm", EffectPublication, "npm", string(h("1"))),
		"eff_browser": e("eff_browser", EffectPublication, "browser", string(h("2"))),
		"eff_act":     e("eff_act", EffectActivation, "activation", string(h("3"))),
	}
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
	effects := ledger(op, s)
	pending := ReleaseTarget{State: TargetPending}
	must := func(cur *Release, r ReleaseRecord) *Release {
		t.Helper()
		next, _, err := ApplyRelease(op, cur, r, cert, effects, now)
		if err != nil {
			t.Fatalf("%s: %v", r.State, err)
		}
		return next
	}
	refuse := func(cur *Release, r ReleaseRecord, why string) {
		t.Helper()
		if _, _, err := ApplyRelease(op, cur, r, cert, effects, now); err == nil {
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
	if _, _, err := ApplyRelease(op, cur, awaiting, nil, effects, now); err == nil {
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
	again, existing, err := ApplyRelease(op, cur, act, cert, effects, now)
	if err != nil || !existing || again.Revision != 6 {
		t.Fatalf("the same transition repeated: %v %v", existing, err)
	}
}

func TestCheckReleaseEffect(t *testing.T) {
	op, s, _ := releaseFixture(t)
	effects := ledger(op, s)
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
	if code(CheckReleaseEffect(req(EffectPublication, "npm"), op, awaiting, effects)) != DenyApprovalRequired {
		t.Fatal("publication before the approval")
	}
	stale := &Release{Subject: s, Approval: &Approval{State: ApprovalApproved, SubjectDigest: h("a")}}
	if code(CheckReleaseEffect(req(EffectPublication, "npm"), op, stale, effects)) != DenyApprovalRequired {
		t.Fatal("publication under an approval of another subject")
	}
	approved := &Release{Subject: s, ReleaseID: "rel_1", ReviewEffectID: "eff_rev", Approval: &Approval{State: ApprovalApproved, SubjectDigest: s.SubjectDigest},
		Npm: ReleaseTarget{State: TargetSucceeded, EffectID: "eff_npm", ReceiptID: "r1", ReceiptDigest: string(h("1"))}, Browser: ReleaseTarget{State: TargetUnknown, EffectID: "eff_browser"}}
	if err := CheckReleaseEffect(req(EffectPublication, "browser"), op, approved, effects); err != nil {
		t.Fatal(err)
	}
	if code(CheckReleaseEffect(EffectRequest{Kind: EffectPublication, CanonicalSubject: ReleaseEffectSubject(h("a"), "npm")}, op, approved, effects)) != DenyApprovalRequired {
		t.Fatal("publication of another subject")
	}
	if code(CheckReleaseEffect(req(EffectActivation, "activation"), op, approved, effects)) != DenyReceiptsRequired {
		t.Fatal("activation with the browser target unknown")
	}
	approved.Browser.State, approved.Browser.ReceiptID, approved.Browser.ReceiptDigest = TargetSucceeded, "r2", string(h("2"))
	if err := CheckReleaseEffect(req(EffectActivation, "activation"), op, approved, effects); err != nil {
		t.Fatal(err)
	}
	// B-38: the permit guard reads the ledger, not the projection alone.
	if code(CheckReleaseEffect(req(EffectActivation, "activation"), op, approved, ReleaseEffects{"eff_rev": effects["eff_rev"], "eff_npm": effects["eff_npm"]})) != DenyReceiptsRequired {
		t.Fatal("activation with a browser receipt the ledger does not hold")
	}
	unknownNpm := *effects["eff_npm"]
	unknownNpm.State = EffectUnknown
	if code(CheckReleaseEffect(req(EffectActivation, "activation"), op, approved, ReleaseEffects{"eff_rev": effects["eff_rev"], "eff_npm": &unknownNpm, "eff_browser": effects["eff_browser"]})) != DenyReceiptsRequired {
		t.Fatal("activation with the npm effect unknown in the ledger")
	}
	if code(CheckReleaseEffect(req(EffectPublication, "npm"), op, approved, ReleaseEffects{})) != DenyApprovalRequired {
		t.Fatal("publication under an approval without the registered review")
	}
	other := &Operation{ID: "op_gen", Kind: KindGeneration}
	if code(CheckReleaseEffect(req(EffectPublication, "npm"), other, nil, nil)) != DenyInvalidArgument {
		t.Fatal("a publication effect of a non-release operation")
	}
}

// B-38: a succeeded target and an approval are accepted only when the
// effect ledger of this operation holds them.
func TestReleaseRecordNeedsTheLedger(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(24 * time.Hour)
	op, s, cert := releaseFixture(t)
	effects := ledger(op, s)
	pending := ReleaseTarget{State: TargetPending}
	approved := &Approval{State: ApprovalApproved, SubjectDigest: s.SubjectDigest, ApproverID: "maintainer_a"}
	awaiting := &Release{OperationID: op.ID, TenantID: op.TenantID, State: ReleaseAwaitingApproval, Subject: s, ReleaseID: "rel_1", ReviewEffectID: "eff_rev",
		Approval: &Approval{State: ApprovalPending, SubjectDigest: s.SubjectDigest}, ApprovalDeadline: &deadline, Npm: pending, Browser: pending, Activation: pending, Revision: 2}
	publishing := ReleaseRecord{ExpectedRevision: 2, State: ReleasePublishing, Subject: s, ReleaseID: "rel_1", ReviewEffectID: "eff_rev", Approval: approved, ApprovalDeadline: &deadline,
		Npm: pending, Browser: pending, Activation: pending}
	if _, _, err := ApplyRelease(op, awaiting, publishing, cert, ReleaseEffects{}, now); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("an approval without any ledger row: %v, want RESULT_INTEGRITY", err)
	}
	cur, _, err := ApplyRelease(op, awaiting, publishing, cert, effects, now)
	if err != nil {
		t.Fatal(err)
	}
	npmOK := ReleaseTarget{State: TargetSucceeded, EffectID: "eff_npm", ReceiptID: "r1", ReceiptDigest: string(h("1")), SubjectDigest: s.SubjectDigest, Destination: s.Destinations.NpmRegistry, Version: "1.0.0"}
	rec := ReleaseRecord{ExpectedRevision: 3, State: ReleaseReconciling, Subject: s, ReleaseID: "rel_1", ReviewEffectID: "eff_rev", Approval: approved, ApprovalDeadline: &deadline,
		Npm: npmOK, Browser: ReleaseTarget{State: TargetUnknown, EffectID: "eff_browser"}, Activation: pending}
	if _, _, err := ApplyRelease(op, cur, rec, cert, effects, now); err != nil {
		t.Fatalf("a target the ledger holds: %v", err)
	}
	refuse := func(e ReleaseEffects, r ReleaseRecord, why string) {
		t.Helper()
		_, _, err := ApplyRelease(op, cur, r, cert, e, now)
		if !errors.Is(err, ErrIntegrity) {
			t.Fatalf("%s: %v, want RESULT_INTEGRITY", why, err)
		}
	}
	without := func(id string) ReleaseEffects {
		out := ReleaseEffects{}
		for k, v := range effects {
			if k != id {
				out[k] = v
			}
		}
		return out
	}
	with := func(id string, change func(*Effect)) ReleaseEffects {
		out := without(id)
		e := *effects[id]
		change(&e)
		out[id] = &e
		return out
	}
	refuse(without("eff_npm"), rec, "a succeeded target whose effect has no row")
	refuse(with("eff_npm", func(e *Effect) { e.OperationID = "op_other" }), rec, "an effect of another operation")
	refuse(with("eff_npm", func(e *Effect) { e.State = EffectPermitted }), rec, "an effect not observed succeeded")
	refuse(with("eff_npm", func(e *Effect) { e.OutcomeRef = string(h("9")) }), rec, "an effect with another receipt")
	refuse(with("eff_npm", func(e *Effect) { e.Kind = EffectBusinessWrite }), rec, "an effect of another kind")
	refuse(with("eff_npm", func(e *Effect) { e.CanonicalSubject = ReleaseEffectSubject(s.SubjectDigest, "browser") }), rec, "the browser effect named as npm")
	refuse(without("eff_rev"), rec, "an approval without the registered review")
	refuse(with("eff_rev", func(e *Effect) { e.OutcomeRef = string(h("a")) }), rec, "an approval under the review of another subject")
}
