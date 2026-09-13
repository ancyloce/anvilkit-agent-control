//go:build integration

package storage

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
)

func testRef(kind, refID, digestByte string) Ref {
	digest := "sha256:"
	for range 64 {
		digest += digestByte
	}
	return Ref{Kind: kind, RefID: refID, SubjectDigest: "sha256:3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855e", ContentDigest: digest, SizeBytes: "512", ObjectVersion: digest}
}

// TestPreparationTransactions covers the S2 preparation records on the real
// DDL: intake and replay, the start/run binding, a posed question set, the
// single accepted answer set per revision with its delivery intent, the round
// bound, the frozen brief, terminal acceptance, cancellation and expiry.
func TestPreparationTransactions(t *testing.T) {
	s := openTestStore(t)
	admin := testConnection(t, "ADMIN")
	ctx := t.Context()
	scope := Scope{"fixture-local-tenant", "fixture-local-actor"}
	until := time.Now().Add(time.Hour)
	profile, limits, billing, err := contracts.Preparation()
	if err != nil {
		t.Fatal(err)
	}
	intake := func(command string, ref Ref) Preparation {
		t.Helper()
		p, err := s.PreparePreparation(ctx, PreparationIntake{Scope: scope, OperationID: NewPreparationID(), CommandID: command, IntakeSource: "ui", Namespace: "workflow02", Environment: "fixture-sql-only", InputRef: ref, Profile: profile, Limits: limits, Billing: billing}, until)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	confirm := func(p Preparation) Preparation {
		t.Helper()
		p, err := s.ConfirmPreparationIntake(ctx, p, p.ObjectDigest, until)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	establish := func(p Preparation) Preparation {
		t.Helper()
		p, send, err := s.MarkPreparationStart(ctx, p, until)
		if err != nil || !send {
			t.Fatal("start marker", err)
		}
		if err := s.EstablishPreparationRun(ctx, p, "fixture-run-"+p.ID, p.Input); err != nil {
			t.Fatal(err)
		}
		if err := s.ObservePreparationRunning(ctx, Preparation{ID: p.ID, RunID: "fixture-run-" + p.ID}); err != nil {
			// ObservePreparationRunning reloads under the lock; pass the run identity.
			t.Fatal(err)
		}
		p, err = s.ReadPreparation(ctx, p.ID)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	input := testRef("preparation-input", "input-1", "a")

	t.Run("intake binds the test quote and replays by content", func(t *testing.T) {
		p := intake("fixture-prep-intake", input)
		if p.FundingAuthority != "test" || p.AuthorizedFundingRef != "tq-"+p.ID || p.FundingPolicyRevision != billing.PolicyRevision || p.QuotedCredits != 0 || p.Stage != "admission_pending" || p.Status != "pending" || p.Recorded || p.WorkflowID != "preparation:"+p.ID {
			t.Fatalf("unexpected intake %+v", p)
		}
		var fundingState *string
		if err := admin.QueryRow(ctx, `SELECT funding_state FROM agent_control.operations WHERE operation_id=$1`, p.ID).Scan(&fundingState); err != nil || fundingState != nil {
			t.Fatalf("a bound quote awaiting its bootstrap reservation carries no funding decision: %v %v", fundingState, err)
		}
		again, err := s.PreparePreparation(ctx, PreparationIntake{Scope: scope, OperationID: NewPreparationID(), CommandID: "fixture-prep-intake", IntakeSource: "ui", Namespace: "workflow02", Environment: "fixture-sql-only", InputRef: input, Profile: profile, Limits: limits, Billing: billing}, until)
		if err != nil || !again.Existing || again.ID != p.ID {
			t.Fatalf("same command and content must replay: %v %+v", err, again)
		}
		_, err = s.PreparePreparation(ctx, PreparationIntake{Scope: scope, OperationID: NewPreparationID(), CommandID: "fixture-prep-intake", IntakeSource: "ui", Namespace: "workflow02", Environment: "fixture-sql-only", InputRef: testRef("preparation-input", "input-1", "b"), Profile: profile, Limits: limits, Billing: billing}, until)
		wantError(t, err, ErrConflict)
		p = confirm(p)
		if !p.Recorded || p.Stage != "queued" || p.Revision != 1 || p.NextSequence != 2 {
			t.Fatalf("confirmed intake %+v", p)
		}
		found, err := s.FindPreparationCommand(ctx, scope, "fixture-prep-intake")
		if err != nil || found.ID != p.ID || !found.Existing {
			t.Fatal("find by command", err)
		}
		matched, err := s.MatchesLocalCheckScope(ctx, scope, p.ID)
		if err != nil || !matched {
			t.Fatal("preparation must match the local-profile scope")
		}
		matched, err = s.MatchesLocalCheckScope(ctx, Scope{"fixture-other-tenant", "fixture-other-actor"}, p.ID)
		if err != nil || matched {
			t.Fatal("another tenant must not match")
		}
	})

	t.Run("question set, one answer set per revision, delivery, brief and terminal", func(t *testing.T) {
		p := establish(confirm(intake("fixture-prep-rounds", input)))
		if p.Stage != "analyzing" || p.Status != "running" || p.RunID == "" {
			t.Fatalf("established %+v", p)
		}
		question := testRef("evidence", "question-set-1", "c")
		out, err := s.RecordPreparationRound(ctx, RoundRecord{OperationID: p.ID, ExecutionGeneration: 1, Ordinal: 1, QuestionSetRef: &question, QuestionSetRevision: 1}, limits)
		if err != nil || out.Duplicate || out.Preparation.Stage != "awaiting_input" || out.Preparation.Status != "pending" || out.Round.ExpiresAt == nil || out.Round.ExpiresAt.Sub(*out.Round.AskedAt) != time.Duration(limits.AwaitingInputExpirySeconds)*time.Second {
			t.Fatalf("question set round: %v %+v", err, out)
		}
		projection := out.Preparation.Projection()
		if projection["round"] != "1" || projection["questionSetRevision"] != "1" || projection["questionSetRef"].(map[string]any)["contentDigest"] != question.ContentDigest {
			t.Fatalf("projection %+v", projection)
		}
		again, err := s.RecordPreparationRound(ctx, RoundRecord{OperationID: p.ID, ExecutionGeneration: 1, Ordinal: 1, QuestionSetRef: &question, QuestionSetRevision: 1}, limits)
		if err != nil || !again.Duplicate || again.Preparation.Revision != out.Preparation.Revision {
			t.Fatalf("a retried round appends nothing: %v %+v", err, again)
		}
		other := testRef("evidence", "question-set-1", "d")
		_, err = s.RecordPreparationRound(ctx, RoundRecord{OperationID: p.ID, ExecutionGeneration: 1, Ordinal: 1, QuestionSetRef: &other, QuestionSetRevision: 1}, limits)
		wantError(t, err, ErrConflict)
		_, err = s.RecordPreparationRound(ctx, RoundRecord{OperationID: p.ID, ExecutionGeneration: 2, Ordinal: 2, QuestionSetRef: &other, QuestionSetRevision: 2}, limits)
		wantError(t, err, ErrRevision)
		brief := testRef("evidence", "brief-1", "e")
		_, err = s.RecordPreparationRound(ctx, RoundRecord{OperationID: p.ID, ExecutionGeneration: 1, Ordinal: 2, BriefRef: &brief, BriefRevision: 1}, limits)
		wantError(t, err, ErrRevision) // an unanswered question set is never completed over

		revision := uint64(out.Preparation.Revision)
		answer := testRef("evidence", "answer-set-1", "f")
		_, err = s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: scope, OperationID: p.ID, CommandID: "fixture-answers-1", ExpectedRevision: revision - 1, QuestionSetRef: question, AnswerRef: answer, QuestionSetRevision: 1}, until)
		wantError(t, err, ErrRevision)
		_, err = s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: scope, OperationID: p.ID, CommandID: "fixture-answers-1", ExpectedRevision: revision, QuestionSetRef: other, AnswerRef: answer, QuestionSetRevision: 1}, until)
		wantError(t, err, ErrRevision)
		_, err = s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: Scope{"fixture-other-tenant", "fixture-other-actor"}, OperationID: p.ID, CommandID: "fixture-answers-1", ExpectedRevision: revision, QuestionSetRef: question, AnswerRef: answer, QuestionSetRevision: 1}, until)
		wantError(t, err, ErrNotFound)
		accepted, err := s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: scope, OperationID: p.ID, CommandID: "fixture-answers-1", ExpectedRevision: revision, QuestionSetRef: question, AnswerRef: answer, QuestionSetRevision: 1}, until)
		if err != nil || accepted.Decision != AnswerAccepted || accepted.Round.DeliveryState != "pending" || accepted.Round.DeliveryIntentID == "" || accepted.Preparation.Revision != int64(revision)+1 || accepted.Preparation.Stage != "awaiting_input" {
			t.Fatalf("accepted answers: %v %+v", err, accepted)
		}
		duplicate, err := s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: scope, OperationID: p.ID, CommandID: "fixture-answers-1", ExpectedRevision: revision, QuestionSetRef: question, AnswerRef: answer, QuestionSetRevision: 1}, until)
		if err != nil || duplicate.Decision != AnswerDuplicate || duplicate.Preparation.Revision != accepted.Preparation.Revision {
			t.Fatalf("duplicate command returns the acceptance: %v %+v", err, duplicate)
		}
		_, err = s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: scope, OperationID: p.ID, CommandID: "fixture-answers-1", ExpectedRevision: revision + 1, QuestionSetRef: question, AnswerRef: answer, QuestionSetRevision: 1}, until)
		wantError(t, err, ErrConflict)
		stale, err := s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: scope, OperationID: p.ID, CommandID: "fixture-answers-2", ExpectedRevision: revision + 1, QuestionSetRef: question, AnswerRef: testRef("evidence", "answer-set-1", "9"), QuestionSetRevision: 1}, until)
		if err != nil || stale.Decision != AnswerStale {
			t.Fatalf("a second answer set for the same revision is stale: %v %+v", err, stale)
		}
		pending, err := s.PendingPreparationDeliveries(ctx)
		if err != nil || len(pending) == 0 || pending[len(pending)-1].Preparation.ID != p.ID {
			t.Fatalf("pending delivery: %v %+v", err, pending)
		}
		if err := s.MarkPreparationDelivered(ctx, p.ID, 1, accepted.Round.DeliveryIntentID); err != nil {
			t.Fatal(err)
		}
		p, _ = s.ReadPreparation(ctx, p.ID)
		if p.Rounds[0].DeliveryState != "delivered" || p.Rounds[0].DeliveredAt == nil {
			t.Fatalf("delivered %+v", p.Rounds[0])
		}
		var events int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM agent_control.operation_events WHERE operation_id=$1`, p.ID).Scan(&events); err != nil || int64(events) != p.NextSequence-1 {
			t.Fatalf("events %d next %d", events, p.NextSequence)
		}

		second := testRef("evidence", "question-set-2", "1")
		round2, err := s.RecordPreparationRound(ctx, RoundRecord{OperationID: p.ID, ExecutionGeneration: 1, Ordinal: 2, QuestionSetRef: &second, QuestionSetRevision: 2}, limits)
		if err != nil || round2.Preparation.Stage != "awaiting_input" || round2.Round.QuestionSetRevision != 2 {
			t.Fatalf("second round: %v %+v", err, round2)
		}
		if got := round2.Preparation.Projection(); got["questionSetRevision"] != "2" || got["acceptedAnswerSetRef"].(map[string]any)["contentDigest"] != answer.ContentDigest {
			t.Fatalf("projection after round 2 %+v", got)
		}
		_, err = s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: scope, OperationID: p.ID, CommandID: "fixture-answers-3", ExpectedRevision: uint64(round2.Preparation.Revision), QuestionSetRef: question, AnswerRef: answer, QuestionSetRevision: 1}, until)
		wantError(t, err, ErrRevision) // the superseded revision is rejected without advancing
		answer2 := testRef("evidence", "answer-set-2", "2")
		accepted2, err := s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: scope, OperationID: p.ID, CommandID: "fixture-answers-3", ExpectedRevision: uint64(round2.Preparation.Revision), QuestionSetRef: second, AnswerRef: answer2, QuestionSetRevision: 2}, until)
		if err != nil || accepted2.Decision != AnswerAccepted {
			t.Fatal("second answers", err)
		}
		third := testRef("evidence", "question-set-3", "3")
		_, err = s.RecordPreparationRound(ctx, RoundRecord{OperationID: p.ID, ExecutionGeneration: 1, Ordinal: 3, QuestionSetRef: &third, QuestionSetRevision: 3}, limits)
		wantError(t, err, ErrRevision) // the round bound (2) holds in Control, never in prompt text
		done, err := s.RecordPreparationRound(ctx, RoundRecord{OperationID: p.ID, ExecutionGeneration: 1, Ordinal: 3, BriefRef: &brief, BriefRevision: 1}, limits)
		if err != nil || done.Preparation.Stage != "brief_ready" || done.Preparation.Status != "succeeded" || done.Preparation.CleanupState != "complete" || done.Round.BriefRevision != 1 {
			t.Fatalf("brief: %v %+v", err, done)
		}
		if got := done.Preparation.Projection(); got["briefRevision"] != "1" || got["briefRef"].(map[string]any)["contentDigest"] != brief.ContentDigest || got["round"] != "3" || got["questionSetRef"] != nil {
			t.Fatalf("projection after brief %+v", got)
		}
		_, err = s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: scope, OperationID: p.ID, CommandID: "fixture-answers-4", ExpectedRevision: uint64(done.Preparation.Revision), QuestionSetRef: second, AnswerRef: answer2, QuestionSetRevision: 2}, until)
		wantError(t, err, ErrPreparationTerminal)

		p, _ = s.ReadPreparation(ctx, p.ID)
		result, _ := json.Marshal(contracts.PreparationWorkflowResultV1{SchemaVersion: 1, OperationID: p.ID, Outcome: "brief_ready", Round: "3", BriefRevision: "1"})
		terminal := PreparationTerminal{WorkflowID: p.WorkflowID, RunID: p.RunID, Input: p.Input, Type: "completed", EventID: 40, At: time.Now().UTC(), Result: result}
		if err := s.AcceptPreparationTerminal(ctx, terminal); err != nil {
			t.Fatal(err)
		}
		before, _ := s.ReadPreparation(ctx, p.ID)
		if !before.Resolved || before.Stage != "brief_ready" || before.Status != "succeeded" {
			t.Fatalf("resolved %+v", before)
		}
		if err := s.AcceptPreparationTerminal(ctx, terminal); err != nil {
			t.Fatal("identical terminal evidence replays", err)
		}
		after, _ := s.ReadPreparation(ctx, p.ID)
		if after.NextSequence != before.NextSequence {
			t.Fatal("a replayed terminal appends no event")
		}
		terminal.EventID = 41
		wantError(t, s.AcceptPreparationTerminal(ctx, terminal), ErrConflict)
		cancelled, coalesced, err := s.CancelPreparation(ctx, scope, p.ID, "fixture-cancel-late", "", uint64(after.Revision), until)
		if err != nil || !coalesced || cancelled.Stage != "brief_ready" {
			t.Fatalf("a frozen brief is never canceled: %v %+v", err, cancelled)
		}
	})

	t.Run("expiry ends the wait once and refuses later answers", func(t *testing.T) {
		p := establish(confirm(intake("fixture-prep-expiry", input)))
		question := testRef("evidence", "question-set-1", "c")
		out, err := s.RecordPreparationRound(ctx, RoundRecord{OperationID: p.ID, ExecutionGeneration: 1, Ordinal: 1, QuestionSetRef: &question, QuestionSetRevision: 1}, limits)
		if err != nil {
			t.Fatal(err)
		}
		expired, err := s.ExpireAwaitingInput(ctx, p.ID)
		if err != nil || expired {
			t.Fatal("an unexpired clock does not expire", err)
		}
		// The clock is immutable to Control; the test moves it as the superuser.
		mustExec(t, admin, `ALTER TABLE agent_control.preparation_rounds DISABLE TRIGGER preparation_rounds_guard`)
		mustExec(t, admin, `UPDATE agent_control.preparation_rounds SET asked_at=clock_timestamp()-interval '2 seconds',expires_at=clock_timestamp()-interval '1 second' WHERE operation_id=$1 AND round_ordinal=1`, p.ID)
		mustExec(t, admin, `ALTER TABLE agent_control.preparation_rounds ENABLE TRIGGER preparation_rounds_guard`)
		expired, err = s.ExpireAwaitingInput(ctx, p.ID)
		if err != nil || !expired {
			t.Fatal("expired clock", err)
		}
		again, err := s.ExpireAwaitingInput(ctx, p.ID)
		if err != nil || again {
			t.Fatal("expiry records once", err)
		}
		p, _ = s.ReadPreparation(ctx, p.ID)
		if p.Stage != "expired" || p.Status != "expired" || p.ExpiryReason != "PREPARATION_INPUT_EXPIRED" {
			t.Fatalf("expired %+v", p)
		}
		answer := testRef("evidence", "answer-set-1", "f")
		_, err = s.RecordPreparationAnswers(ctx, AnswerRecord{Scope: scope, OperationID: p.ID, CommandID: "fixture-answers-late", ExpectedRevision: uint64(p.Revision), QuestionSetRef: question, AnswerRef: answer, QuestionSetRevision: 1}, until)
		wantError(t, err, ErrPreparationTerminal)
		_ = out
		result, _ := json.Marshal(contracts.PreparationWorkflowResultV1{SchemaVersion: 1, OperationID: p.ID, Outcome: "expired", Round: "1"})
		if err := s.AcceptPreparationTerminal(ctx, PreparationTerminal{WorkflowID: p.WorkflowID, RunID: p.RunID, Input: p.Input, Type: "completed", EventID: 12, At: time.Now().UTC(), Result: result}); err != nil {
			t.Fatal(err)
		}
		p, _ = s.ReadPreparation(ctx, p.ID)
		if !p.Resolved || p.Stage != "expired" {
			t.Fatalf("terminal after expiry %+v", p)
		}
	})

	t.Run("cancel fences an unstarted and a running preparation", func(t *testing.T) {
		p := confirm(intake("fixture-prep-cancel-unstarted", input))
		cancelled, coalesced, err := s.CancelPreparation(ctx, scope, p.ID, "fixture-cancel-1", "USER_REQUESTED", uint64(p.Revision), until)
		if err != nil || coalesced || !cancelled.Resolved || cancelled.Stage != "canceled" || cancelled.TerminalType != "unattempted_canceled" {
			t.Fatalf("unstarted cancel: %v %+v", err, cancelled)
		}
		running := establish(confirm(intake("fixture-prep-cancel-running", input)))
		_, _, err = s.CancelPreparation(ctx, scope, running.ID, "fixture-cancel-2", "", uint64(running.Revision)+5, until)
		wantError(t, err, ErrRevision)
		cancelled, coalesced, err = s.CancelPreparation(ctx, scope, running.ID, "fixture-cancel-2", "", uint64(running.Revision), until)
		if err != nil || coalesced || !cancelled.CancelRequested || cancelled.Resolved || cancelled.CleanupState != "pending" {
			t.Fatalf("running cancel: %v %+v", err, cancelled)
		}
		question := testRef("evidence", "question-set-1", "c")
		_, err = s.RecordPreparationRound(ctx, RoundRecord{OperationID: running.ID, ExecutionGeneration: 1, Ordinal: 1, QuestionSetRef: &question, QuestionSetRevision: 1}, limits)
		wantError(t, err, ErrPreparationTerminal)
		if err := s.AcceptPreparationTerminal(ctx, PreparationTerminal{WorkflowID: cancelled.WorkflowID, RunID: cancelled.RunID, Input: cancelled.Input, Type: "canceled", EventID: 8, At: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
		final, _ := s.ReadPreparation(ctx, running.ID)
		if !final.Resolved || final.Stage != "canceled" || final.Status != "canceled" || final.CleanupState != "complete" {
			t.Fatalf("canceled terminal %+v", final)
		}
		if _, _, err := s.CancelPreparation(ctx, Scope{"fixture-other-tenant", "fixture-other-actor"}, running.ID, "fixture-cancel-3", "", 1, until); !errors.Is(err, ErrNotFound) {
			t.Fatal("cross-tenant cancel must not disclose the operation")
		}
	})
}
