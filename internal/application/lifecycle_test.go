package application_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	artifactstore "github.com/ancyloce/anvilkit-agent-control/internal/adapters/artifacts"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/development"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// manualClock is a clock the test moves: the clarification expiry and the
// lease validity are decided against it, never against a sleep.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// relayDouble records the Updates the relay issues and answers them as
// scripted: a lost receipt (an error) once, then the outcome.
type relayDouble struct {
	fakeWorkflows
	mu       sync.Mutex
	answers  map[string]int // update id -> issue count
	commands map[string]int
	loseOnce map[string]bool
	outcome  application.RelayOutcome
}

func (r *relayDouble) UpdateAnswer(_ context.Context, _ string, a *domain.Answer) (application.RelayOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.answers[a.UpdateID]++
	if r.loseOnce[a.UpdateID] {
		delete(r.loseOnce, a.UpdateID)
		return application.RelayOutcome{}, context.DeadlineExceeded
	}
	return r.outcome, nil
}

func (r *relayDouble) UpdateCommand(_ context.Context, _ string, c *domain.Command) (application.RelayOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands[c.CommandID]++
	return r.outcome, nil
}

// lifecycleProfiles are the reviewed profiles of this test: short waits so
// the manual clock can pass them.
func lifecycleProfiles() []domain.Profile {
	prepFunding := domain.Money{Currency: "USD", Amount: 500_000}
	genFunding := domain.Money{Currency: "USD", Amount: 1_000_000}
	return []domain.Profile{
		profile,
		{
			ID: "preparation-v1", Kind: domain.KindPreparation, OperationDeadline: 10 * time.Hour, StepID: "analysis", MultiStep: true,
			Clarification: domain.ClarificationBounds{MaxRounds: 2, MaxQuestions: 3, Wait: time.Hour}, Funding: &prepFunding,
		},
		{
			ID: "generation-v1", Kind: domain.KindGeneration, OperationDeadline: 2 * time.Hour, StepID: "codegen", MultiStep: true,
			Funding: &genFunding, QueuePool: "generation", ActiveWindow: time.Hour, Definitions: []string{"generation-v1:def-1", "generation-v1:def-2"}, SupportsControl: true,
			MaxRepairs: 1, CodegenProfileID: "codegen-team-dev-v1", ValidatorProfileID: "validator-fixed-dev-v1", JobProfileID: "codegen-team-dev-v1",
		},
	}
}

type lifecycleProcess struct {
	ops          *application.Operations
	exec         *application.Execution
	preparations *application.Preparations
	generations  *application.Generations
	artifacts    *application.Artifacts
	dispatch     *application.Dispatch
	relay        *application.Relay
	wf           *relayDouble
}

func newLifecycleProcess(t *testing.T, inst *testdb.Instance, inv application.Inventory, objects application.ArtifactStore, clock domain.Clock) *lifecycleProcess {
	t.Helper()
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	profiles := lifecycleProfiles()
	prices, err := development.NewPriceBook(nil)
	require.NoError(t, err)
	dispatch := application.NewDispatch(store, inv, prices, development.NewAuthority(nil, 30*time.Second, clock), development.NewNotSentEvidence(inv, nil), clock, testLog).WithProfiles(profiles)
	limits := application.ArtifactLimits{MaxObjectBytes: 1 << 20, MaxWindow: time.Hour, CapabilityTTL: 5 * time.Minute}
	p := &lifecycleProcess{
		ops:          application.NewOperations(store, inv, profiles, clock, testLog),
		exec:         application.NewExecution(store, inv, manifests, profiles, clock, testLog),
		preparations: application.NewPreparations(store, profiles, clock, testLog),
		generations:  application.NewGenerations(store, dispatch, profiles, []domain.ResourcePool{{ID: "generation", Class: "generation", Capacity: 1}}, clock, testLog),
		artifacts:    application.NewArtifacts(store, objects, limits, clock, testLog),
		dispatch:     dispatch,
		wf:           &relayDouble{fakeWorkflows: fakeWorkflows{starts: map[string]int{}, cancels: map[string]int{}}, answers: map[string]int{}, commands: map[string]int{}, loseOnce: map[string]bool{}, outcome: application.RelayOutcome{Outcome: domain.OutcomeApplied}},
	}
	p.relay = application.NewRelay(store, p.wf, p.ops, nil, p.preparations, p.generations, clock, testLog, time.Second)
	require.NoError(t, p.generations.InstallPools(context.Background()))
	return p
}

// finalizedArtifact uploads bytes as a finalized transfer of the class,
// bound to the operation when given, and returns it.
func finalizedArtifact(t *testing.T, ctx context.Context, a *application.Artifacts, tenant, id, class, operationID string, body []byte, deadline time.Time) *domain.Transfer {
	t.Helper()
	req := domain.TransferRequest{Class: domain.ArtifactClass(class), MediaType: "application/json", ExpectedDigest: digestOf(body), ExpectedSize: int64(len(body)), OperationID: operationID, Deadline: deadline}
	tr, _, cap, err := a.Begin(ctx, transferCmd(tenant, id), domain.Scope{TenantID: tenant, ActorID: "user_" + tenant}, req)
	require.NoError(t, err)
	require.NotNil(t, cap)
	version, err := artifactstore.Upload(ctx, *cap, body)
	require.NoError(t, err)
	got, _, err := a.Finalize(ctx, transferCmd(tenant, id+":fin"), tr.ID, "", version, "")
	require.NoError(t, err)
	require.Equal(t, domain.TransferFinalized, got.State)
	return got
}

// TestLifecycle runs the Preparation and Generation records of P13 on two
// Control replicas over one database and one versioned store: the intake
// with its prompt, question sets with their immutable expiry, answers
// (idempotent, stale, expired, relayed as Updates whose lost receipt
// re-issues the same identity), the frozen and superseded brief, the
// generation's permit (active deadline once, bounded queue), funding,
// lease facts (confirmed only, loss fences), the supported control
// commands and the settlement.
func TestLifecycle(t *testing.T) {
	inst := testdb.Start(t)
	objects := startArtifactStore(t)
	fsInv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	inv := &flakyInventory{inner: fsInv}
	clock := &manualClock{now: time.Now().UTC().Truncate(time.Microsecond)}
	p1, p2 := newLifecycleProcess(t, inst, inv, objects, clock), newLifecycleProcess(t, inst, inv, objects, clock)
	ctx := context.Background()
	conn := inst.Pool(t)
	for _, row := range []struct {
		level         string
		tenant, actor *string
		cap           int64
	}{{"platform", nil, nil, 1_000_000_000}, {"tenant", ptr("tenant_a"), nil, 100_000_000}, {"actor", ptr("tenant_a"), ptr("user_a"), 10_000_000}} {
		_, err := conn.Exec(ctx, "INSERT INTO budget_pools (pool_id, level, tenant_id, actor_id, period_start, period_end, currency, cap_amount) VALUES ($1, $2, $3, $4, now() - interval '1 hour', now() + interval '30 day', 'USD', $5)",
			"pool-life-"+row.level, row.level, row.tenant, row.actor, row.cap)
		require.NoError(t, err)
	}
	deadline := clock.Now().Add(time.Hour)
	prompt := []byte(`{"text":"A hero section with a headline and a call to action."}`)
	prepSubject := domain.Subject{ProfileID: "preparation-v1", SubjectDigest: application.DigestOf([]byte("prep-subject"))}

	var prep *domain.Operation
	var promptTransfer *domain.Transfer
	t.Run("intake binds a finalized prompt artifact of the tenant", func(t *testing.T) {
		promptTransfer = finalizedArtifact(t, ctx, p1.artifacts, "tenant_a", "prompt1", "prompt", "", prompt, deadline)
		_, _, err := p1.ops.Create(ctx, cmd("tenant_a", "prep_bad", "body"), scopeA, domain.KindPreparation, prepSubject, &domain.PreparationIntake{Prompt: domain.ArtifactBinding{TransferID: promptTransfer.ID, Digest: application.DigestOf([]byte("other"))}})
		require.ErrorIs(t, err, domain.ErrInvalid, "a digest the transfer does not hold is refused")
		_, _, err = p1.ops.Create(ctx, cmd("tenant_a", "prep_none", "body"), scopeA, domain.KindPreparation, prepSubject, nil)
		require.ErrorIs(t, err, domain.ErrInvalid, "a preparation needs its intake")
		_, _, err = p1.ops.Create(ctx, cmd("tenant_b", "prep_b", "body"), scopeB, domain.KindPreparation, prepSubject, &domain.PreparationIntake{Prompt: domain.ArtifactBinding{TransferID: promptTransfer.ID, Digest: promptTransfer.ActualDigest}})
		require.ErrorIs(t, err, domain.ErrInvalid, "another tenant's prompt is not an input")
		intake := &domain.PreparationIntake{Prompt: domain.ArtifactBinding{TransferID: promptTransfer.ID, Digest: promptTransfer.ActualDigest}, BrandReferences: []domain.SourceReference{{SourceID: "brand_1", Revision: 3}}}
		var err1 error
		prep, _, err1 = p1.ops.Create(ctx, cmd("tenant_a", "prep_1", "body"), scopeA, domain.KindPreparation, prepSubject, intake)
		require.NoError(t, err1)
		require.Equal(t, domain.IntakeConfirmed, prep.Intake)
		again, existing, err := p2.ops.Create(ctx, cmd("tenant_a", "prep_1", "body"), scopeA, domain.KindPreparation, prepSubject, intake)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, prep.ID, again.ID)
		require.NotNil(t, again.Preparation)
		require.Equal(t, "brand_1", again.Preparation.BrandReferences[0].SourceID)
		// The Workflow reads the prompt under the operation's relationship; a stranger transfer is no input.
		_, cap, err := p2.artifacts.Read(ctx, "", promptTransfer.ID, prep.ID, "")
		require.NoError(t, err)
		require.Equal(t, "GET", cap.Method)
		stranger := finalizedArtifact(t, ctx, p1.artifacts, "tenant_a", "stranger", "prompt", "", []byte("other prompt"), deadline)
		_, _, err = p2.artifacts.Read(ctx, "", stranger.ID, prep.ID, "")
		require.ErrorIs(t, err, domain.ErrStaleExecution)
	})

	var qs *domain.QuestionSet
	questions := []domain.Question{{ID: "q1", Text: "Which call to action?"}, {ID: "q2", Text: "Which tone?"}}
	t.Run("a question round is recorded once with an immutable expiry and bounded", func(t *testing.T) {
		_, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", prep.ID+":analysis:1:open", "body"), prep.ID, "analysis", 0, "preparation-v1")
		require.NoError(t, err, "the analysis attempt opens without a permit or a lease (no queue on the preparation profile)")
		var err2 error
		qs, _, err2 = p1.preparations.RecordQuestionSet(ctx, cmd("tenant_a", prep.ID+":questions:1", "q"), prep.ID, 1, questions)
		require.NoError(t, err2)
		require.Equal(t, clock.Now().Add(time.Hour), qs.ExpiresAt)
		clock.Advance(time.Minute)
		again, existing, err := p2.preparations.RecordQuestionSet(ctx, cmd("tenant_a", prep.ID+":questions:1", "q"), prep.ID, 1, questions)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, qs.ExpiresAt, again.ExpiresAt, "reentry never extends the wait")
		_, _, err = p2.preparations.RecordQuestionSet(ctx, cmd("tenant_a", prep.ID+":questions:2", "q"), prep.ID, 2, questions)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "one open round at a time")
		view, err := p2.ops.Get(ctx, scopeA, prep.ID)
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleWaiting, view.Lifecycle)
		require.Equal(t, "awaiting_input", view.Phase)
		four := append(append([]domain.Question(nil), questions...), domain.Question{ID: "q3", Text: "3"}, domain.Question{ID: "q4", Text: "4"})
		_, _, err = p2.preparations.RecordQuestionSet(ctx, cmd("tenant_a", "too-many", "q"), prep.ID, 1, four)
		require.ErrorIs(t, err, domain.ErrInvalid, "more than three questions are refused")
	})

	var answer *domain.Answer
	t.Run("answers are idempotent, stale revisions and repeats under another command are refused", func(t *testing.T) {
		body := []byte(`{"q1":"Book a demo","q2":"confident"}`)
		tr := finalizedArtifact(t, ctx, p1.artifacts, "tenant_a", "answer1", "answer", "", body, deadline)
		binding := domain.ArtifactBinding{TransferID: tr.ID, Digest: tr.ActualDigest}
		_, _, err := p1.preparations.SubmitAnswer(ctx, cmd("tenant_a", "ans_stale", "a"), scopeA, prep.ID, qs.ID, 2, binding)
		require.ErrorIs(t, err, domain.ErrRevisionConflict)
		var err2 error
		answer, _, err2 = p1.preparations.SubmitAnswer(ctx, cmd("tenant_a", "ans_1", "a"), scopeA, prep.ID, qs.ID, 1, binding)
		require.NoError(t, err2)
		require.Equal(t, domain.AnswerRelayPending, answer.Relay)
		require.Equal(t, answer.ID, answer.UpdateID)
		again, existing, err := p2.preparations.SubmitAnswer(ctx, cmd("tenant_a", "ans_1", "a"), scopeA, prep.ID, qs.ID, 1, binding)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, answer.ID, again.ID)
		_, _, err = p2.preparations.SubmitAnswer(ctx, cmd("tenant_a", "ans_2", "a"), scopeA, prep.ID, qs.ID, 1, binding)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "the revision is answered once")
		got, err := p2.preparations.GetAnswer(ctx, "tenant_a", prep.ID, "", qs.ID)
		require.NoError(t, err)
		require.Equal(t, answer.ID, got.ID)
		_, _, err = p2.artifacts.Read(ctx, "", tr.ID, prep.ID, "")
		require.NoError(t, err, "an accepted answer is an input of the preparation")
		view, err := p1.ops.Get(ctx, scopeA, prep.ID)
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleRunning, view.Lifecycle)
	})

	t.Run("the relay issues the answer Update under its identity; a lost receipt re-issues the same one", func(t *testing.T) {
		p1.wf.loseOnce[answer.UpdateID] = true
		require.NoError(t, p1.relay.Tick(ctx))
		a, err := p1.preparations.GetAnswer(ctx, "tenant_a", prep.ID, answer.ID, "")
		require.NoError(t, err)
		require.Equal(t, domain.AnswerRelaySent, a.Relay, "a lost receipt leaves the intent sent")
		require.NoError(t, p2.relay.Tick(ctx))
		a, err = p2.preparations.GetAnswer(ctx, "tenant_a", prep.ID, answer.ID, "")
		require.NoError(t, err)
		require.Equal(t, domain.AnswerRelayApplied, a.Relay)
		require.Equal(t, 1, p1.wf.answers[answer.UpdateID]+p2.wf.answers[answer.UpdateID]-1, "the same update id was issued twice, never a second identity")
		require.NoError(t, p1.relay.Tick(ctx))
		require.Equal(t, 2, p1.wf.answers[answer.UpdateID]+p2.wf.answers[answer.UpdateID], "a settled answer is not re-issued")
	})

	t.Run("an expired round refuses answers", func(t *testing.T) {
		qs2, _, err := p1.preparations.RecordQuestionSet(ctx, cmd("tenant_a", prep.ID+":questions:2", "q"), prep.ID, 2, questions[:1])
		require.NoError(t, err)
		clock.Advance(time.Hour + time.Second)
		body := []byte(`{"q1":"late"}`)
		tr := finalizedArtifact(t, ctx, p1.artifacts, "tenant_a", "answer-late", "answer", "", body, clock.Now().Add(time.Hour))
		_, _, err = p1.preparations.SubmitAnswer(ctx, cmd("tenant_a", "ans_late", "a"), scopeA, prep.ID, qs2.ID, 1, domain.ArtifactBinding{TransferID: tr.ID, Digest: tr.ActualDigest})
		require.ErrorIs(t, err, domain.ErrStaleExecution)
		_, _, err = p1.preparations.RecordQuestionSet(ctx, cmd("tenant_a", prep.ID+":questions:3", "q"), prep.ID, 3, questions[:1])
		require.ErrorIs(t, err, domain.ErrInvalid, "a third round is beyond the bound")
	})

	var brief *domain.Brief
	t.Run("the brief freezes the exact inputs; a new brief supersedes; a superseded brief starts nothing", func(t *testing.T) {
		body := []byte(`{"schemaVersion":1,"componentId":"cmp_1","puckType":"Hero","packageName":"@acme/hero","version":"0.1.0","requirements":{"purpose":"hero","content":"headline"}}`)
		tr := finalizedArtifact(t, ctx, p1.artifacts, "tenant_a", "brief1", "brief", prep.ID, body, clock.Now().Add(time.Hour))
		binding := domain.ArtifactBinding{TransferID: tr.ID, Digest: tr.ActualDigest}
		requirements := application.DigestOf([]byte("requirements"))
		_, _, err := p1.preparations.RecordBrief(ctx, cmd("tenant_a", prep.ID+":brief", "b"), prep.ID, binding, requirements, nil, nil, nil)
		require.ErrorIs(t, err, domain.ErrInvalid, "the intake's brand reference must be frozen with a digest")
		brands := []domain.ContentDigest{{SourceID: "brand_1", Revision: 3, Digest: application.DigestOf([]byte("brand"))}}
		var err2 error
		brief, _, err2 = p1.preparations.RecordBrief(ctx, cmd("tenant_a", prep.ID+":brief", "b"), prep.ID, binding, requirements, nil, brands, nil)
		require.NoError(t, err2)
		again, existing, err := p2.preparations.RecordBrief(ctx, cmd("tenant_a", prep.ID+":brief", "b"), prep.ID, binding, requirements, nil, brands, nil)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, brief.ID, again.ID)
		genSubject := domain.Subject{ProfileID: "generation-v1", SubjectDigest: application.DigestOf([]byte("gen-subject")), BriefID: brief.ID}
		_, _, err = p1.ops.Create(ctx, cmd("tenant_a", "gen_early", "body"), scopeA, domain.KindGeneration, genSubject, nil)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "the preparation has not succeeded")
		// A second brief (a re-analysis before the preparation settles) supersedes the first.
		body2 := append(append([]byte(nil), body...), '\n')
		tr2 := finalizedArtifact(t, ctx, p1.artifacts, "tenant_a", "brief2", "brief", prep.ID, body2, clock.Now().Add(time.Hour))
		brief2, _, err := p1.preparations.RecordBrief(ctx, cmd("tenant_a", prep.ID+":brief2", "b"), prep.ID, domain.ArtifactBinding{TransferID: tr2.ID, Digest: tr2.ActualDigest}, requirements, nil, brands, nil)
		require.NoError(t, err)
		superseded, err := p2.preparations.GetBrief(ctx, "tenant_a", brief.ID)
		require.NoError(t, err)
		require.Equal(t, domain.BriefSuperseded, superseded.State)
		// Settle the preparation: the analysis attempt closes first, then the business outcome.
		var attempts []string
		rows, err := conn.Query(ctx, "SELECT attempt_id FROM attempts WHERE operation_id = $1", prep.ID)
		require.NoError(t, err)
		for rows.Next() {
			var id string
			require.NoError(t, rows.Scan(&id))
			attempts = append(attempts, id)
		}
		rows.Close()
		for _, id := range attempts {
			_, _, _, err := p1.exec.CloseAttempt(ctx, cmd("tenant_a", id+":close", "c"), id, domain.OutcomeCompleted, domain.CleanupNotRequired, "")
			require.NoError(t, err)
		}
		op, _, err := p1.exec.SettleOperation(ctx, cmd("tenant_a", prep.ID+":settle", "s"), prep.ID, domain.OperationSucceeded, "", "brief_frozen")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleSucceeded, op.Lifecycle)
		require.Equal(t, brief2.ID, op.Subject.BriefID)
		settled, existing, err := p2.exec.SettleOperation(ctx, cmd("tenant_a", prep.ID+":settle", "s"), prep.ID, domain.OperationSucceeded, "", "brief_frozen")
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, domain.LifecycleSucceeded, settled.Lifecycle)
		_, _, err = p1.ops.Create(ctx, cmd("tenant_a", "gen_stale", "body"), scopeA, domain.KindGeneration, genSubject, nil)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a superseded brief starts no generation")
		_, _, err = p1.preparations.RecordBrief(ctx, cmd("tenant_a", prep.ID+":brief3", "b"), prep.ID, domain.ArtifactBinding{TransferID: tr2.ID, Digest: tr2.ActualDigest}, requirements, nil, brands, nil)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a settled preparation freezes nothing more")
		brief = brief2
	})

	var gen, gen2 *domain.Operation
	genSubject := domain.Subject{ProfileID: "generation-v1", SubjectDigest: application.DigestOf([]byte("gen-subject"))}
	t.Run("the first permit sets the active deadline once; the pool queues the second generation", func(t *testing.T) {
		genSubject.BriefID = brief.ID
		var err error
		gen, _, err = p1.ops.Create(ctx, cmd("tenant_a", "gen_1", "body"), scopeA, domain.KindGeneration, genSubject, nil)
		require.NoError(t, err)
		require.Equal(t, "generation-v1:def-1", gen.DefinitionActivation)
		gen2, _, err = p1.ops.Create(ctx, cmd("tenant_a", "gen_2", "body"), scopeA, domain.KindGeneration, genSubject, nil)
		require.NoError(t, err)
		_, _, err = p1.exec.OpenAttempt(ctx, cmd("tenant_a", gen.ID+":codegen:0:1:open", "o"), gen.ID, "codegen", 0, "codegen-team-dev-v1")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "no attempt before the permit")
		first, err := p1.generations.RequestExecutionPermit(ctx, cmd("tenant_a", gen.ID+":permit", "p"), gen.ID)
		require.NoError(t, err)
		require.True(t, first.Granted)
		require.NotNil(t, first.ActiveDeadline)
		require.Equal(t, clock.Now().Add(time.Hour), *first.ActiveDeadline)
		clock.Advance(5 * time.Minute)
		again, err := p2.generations.RequestExecutionPermit(ctx, cmd("tenant_a", gen.ID+":permit", "p"), gen.ID)
		require.NoError(t, err)
		require.True(t, again.Granted)
		require.Equal(t, first.Permit.ID, again.Permit.ID)
		require.Equal(t, *first.ActiveDeadline, *again.ActiveDeadline, "a repeated request never resets the active deadline")
		queued, err := p2.generations.RequestExecutionPermit(ctx, cmd("tenant_a", gen2.ID+":permit", "p"), gen2.ID)
		require.NoError(t, err)
		require.False(t, queued.Granted)
		require.Nil(t, queued.ActiveDeadline)
		require.Equal(t, gen2.Deadline, queued.QueueDeadline, "the original queue deadline is answered, never extended")
		v, err := p1.ops.Get(ctx, scopeA, gen2.ID)
		require.NoError(t, err)
		require.Equal(t, "queued", v.Phase)
		_, _, err = p1.exec.OpenAttempt(ctx, cmd("tenant_a", gen.ID+":codegen:0:1:open", "o"), gen.ID, "codegen", 0, "codegen-team-dev-v1")
		require.ErrorIs(t, err, domain.ErrBudgetExhausted, "no attempt before the funding")
	})

	t.Run("funding once, confirmed lease facts only; loss fences for good", func(t *testing.T) {
		f, existing, err := p1.generations.RecordFunding(ctx, cmd("tenant_a", gen.ID+":funding", "f"), gen.ID)
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, int64(1_000_000), f.Amount.Amount)
		f2, existing, err := p2.generations.RecordFunding(ctx, cmd("tenant_a", gen.ID+":funding", "f"), gen.ID)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, f.FundedAt, f2.FundedAt)
		var allocations int
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM allocations WHERE operation_id = $1", gen.ID).Scan(&allocations))
		require.Equal(t, 3, allocations, "one allocation per pool level, never a second")
		_, _, err = p1.exec.OpenAttempt(ctx, cmd("tenant_a", gen.ID+":codegen:0:1:open", "o"), gen.ID, "codegen", 0, "codegen-team-dev-v1")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "no attempt before a confirmed lease")
		exp1 := clock.Now().Add(10 * time.Minute)
		op, existing, err := p1.generations.RecordLease(ctx, cmd("tenant_a", gen.ID+":lease:1", "l"), gen.ID, 1, domain.LeaseHeld, "lease_x", 7, &exp1)
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.LeaseHeld, op.Lease.State)
		require.Equal(t, exp1, *op.Lease.ExpiresAt)
		op, existing, err = p2.generations.RecordLease(ctx, cmd("tenant_a", gen.ID+":lease:1", "l"), gen.ID, 1, domain.LeaseHeld, "lease_x", 7, &exp1)
		require.NoError(t, err)
		require.True(t, existing)
		at, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", gen.ID+":codegen:0:1:open", "o"), gen.ID, "codegen", 0, "codegen-team-dev-v1")
		require.NoError(t, err)
		require.Equal(t, clock.Now().Add(55*time.Minute), at.Deadline, "the attempt inherits the active deadline")
		exp2 := clock.Now().Add(20 * time.Minute)
		op, _, err = p1.generations.RecordLease(ctx, cmd("tenant_a", gen.ID+":lease:2", "l"), gen.ID, 2, domain.LeaseHeld, "lease_x", 7, &exp2)
		require.NoError(t, err)
		require.Equal(t, exp2, *op.Lease.ExpiresAt, "a confirmed renewal moves the known expiry")
		earlier := clock.Now().Add(time.Minute)
		op, _, err = p2.generations.RecordLease(ctx, cmd("tenant_a", gen.ID+":lease:3", "l"), gen.ID, 3, domain.LeaseHeld, "lease_x", 7, &earlier)
		require.NoError(t, err)
		require.Equal(t, exp2, *op.Lease.ExpiresAt, "a confirmed expiry never moves back")
		_, _, err = p2.generations.RecordLease(ctx, cmd("tenant_a", gen.ID+":lease:4", "l"), gen.ID, 4, domain.LeaseHeld, "lease_other", 8, &exp2)
		require.ErrorIs(t, err, domain.ErrInvalid, "another lease identity is not this operation's")
		_, _, _, err = p1.exec.CloseAttempt(ctx, cmd("tenant_a", at.ID+":close", "c"), at.ID, domain.OutcomeInfrastructureFailed, domain.CleanupNotRequired, "OBSERVER_FAILED")
		require.NoError(t, err)
		v, err := p1.ops.Get(ctx, scopeA, gen.ID)
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleRunning, v.Lifecycle, "a definite close of a multi-step profile keeps the operation running")
		op, _, err = p1.generations.RecordLease(ctx, cmd("tenant_a", gen.ID+":lease:5", "l"), gen.ID, 5, domain.LeaseLost, "", 0, nil)
		require.NoError(t, err)
		require.Equal(t, domain.LeaseLost, op.Lease.State)
		_, _, err = p2.exec.OpenAttempt(ctx, cmd("tenant_a", gen.ID+":codegen:0:2:open", "o"), gen.ID, "codegen", 0, "codegen-team-dev-v1")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a lost lease fences new dispatch")
		_, _, err = p2.generations.RecordLease(ctx, cmd("tenant_a", gen.ID+":lease:6", "l"), gen.ID, 6, domain.LeaseHeld, "lease_x", 7, &exp2)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a lost lease never returns to held")
		settled, _, err := p1.exec.SettleOperation(ctx, cmd("tenant_a", gen.ID+":settle", "s"), gen.ID, domain.OperationFailed, "LEASE_LOST", "")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleFailed, settled.Lifecycle)
		require.Equal(t, "LEASE_LOST", settled.FailureCode)
		granted, err := p1.generations.RequestExecutionPermit(ctx, cmd("tenant_a", gen2.ID+":permit", "p"), gen2.ID)
		require.NoError(t, err)
		require.True(t, granted.Granted, "the settled operation released the pool's permit")
	})

	t.Run("hold, resume and definition changes are tracked Updates applied only as the Workflow answers", func(t *testing.T) {
		v, err := p1.ops.Get(ctx, scopeA, gen2.ID)
		require.NoError(t, err)
		c, _, err := p1.ops.SubmitCommand(ctx, cmd("tenant_a", "hold_1", "h"), scopeA, gen2.ID, domain.CommandHold, v.Revision, "")
		require.NoError(t, err)
		require.Equal(t, domain.OutcomePending, c.Outcome)
		require.Equal(t, domain.CommandRelayPending, c.Relay)
		v, err = p1.ops.Get(ctx, scopeA, gen2.ID)
		require.NoError(t, err)
		require.Equal(t, domain.ControlHoldPending, v.Control, "the fence is installed first")
		require.NotEqual(t, c.OperationRevision, c.ExpectedRevision, "the fence moved the revision")
		v, err = p1.ops.Get(ctx, scopeA, gen2.ID)
		require.NoError(t, err)
		_, _, err = p1.ops.SubmitCommand(ctx, cmd("tenant_a", "resume_early", "r"), scopeA, gen2.ID, domain.CommandResume, v.Revision, "")
		require.NoError(t, err)
		rc, err := p1.ops.GetCommand(ctx, scopeA, gen2.ID, "resume_early")
		require.NoError(t, err)
		require.Equal(t, domain.OutcomeBlocked, rc.Outcome, "a resume while the hold is pending is blocked")
		require.NoError(t, p2.relay.Tick(ctx))
		hc, err := p2.ops.GetCommand(ctx, scopeA, gen2.ID, "hold_1")
		require.NoError(t, err)
		require.Equal(t, domain.OutcomeApplied, hc.Outcome)
		v, err = p2.ops.Get(ctx, scopeA, gen2.ID)
		require.NoError(t, err)
		require.Equal(t, domain.ControlHoldApplied, v.Control)
		require.Equal(t, domain.LifecycleSuspended, v.Lifecycle)
		require.Equal(t, 1, p2.wf.commands["hold_1"])
		_, _, err = p1.ops.SubmitCommand(ctx, cmd("tenant_a", "chg_bad", "c"), scopeA, gen2.ID, domain.CommandChangeDefinition, v.Revision, "generation-v1:def-9")
		require.NoError(t, err)
		bad, err := p1.ops.GetCommand(ctx, scopeA, gen2.ID, "chg_bad")
		require.NoError(t, err)
		require.Equal(t, domain.OutcomeRejected, bad.Outcome)
		require.Equal(t, "DEFINITION_UNKNOWN", bad.ReasonCode)
		_, _, err = p1.ops.SubmitCommand(ctx, cmd("tenant_a", "chg_1", "c"), scopeA, gen2.ID, domain.CommandChangeDefinition, v.Revision, "generation-v1:def-2")
		require.NoError(t, err)
		_, _, err = p1.ops.SubmitCommand(ctx, cmd("tenant_a", "resume_stale", "r"), scopeA, gen2.ID, domain.CommandResume, v.Revision, "")
		require.ErrorIs(t, err, domain.ErrRevisionConflict, "a tracked command is a revision; a stale expected revision conflicts")
		v, err = p1.ops.Get(ctx, scopeA, gen2.ID)
		require.NoError(t, err)
		_, _, err = p1.ops.SubmitCommand(ctx, cmd("tenant_a", "resume_1", "r"), scopeA, gen2.ID, domain.CommandResume, v.Revision, "")
		require.NoError(t, err)
		require.NoError(t, p1.relay.Tick(ctx))
		v, err = p1.ops.Get(ctx, scopeA, gen2.ID)
		require.NoError(t, err)
		require.Equal(t, "generation-v1:def-2", v.DefinitionActivation)
		require.Equal(t, domain.ControlNone, v.Control)
		require.Equal(t, domain.LifecycleRunning, v.Lifecycle)
		require.NotNil(t, v.ActiveDeadline, "hold and resume keep the active deadline")
		// An unreconciled exposure of the operation permits no new attempt
		// (a recovery starts from reconciled costs) and keeps a settlement
		// reconciling rather than terminal.
		_, err = conn.Exec(ctx, "UPDATE operations SET finance_state = 'exposure_unknown' WHERE operation_id = $1", gen2.ID)
		require.NoError(t, err)
		_, _, err = p2.exec.OpenAttempt(ctx, cmd("tenant_a", gen2.ID+":codegen:0:1:open", "o"), gen2.ID, "codegen", 0, "codegen-team-dev-v1")
		require.ErrorIs(t, err, domain.ErrEffectUncertain, "an unknown dispatch never permits a new attempt")
		reconciling, _, err := p2.exec.SettleOperation(ctx, cmd("tenant_a", gen2.ID+":settle", "s"), gen2.ID, domain.OperationSucceeded, "", "candidate_ready")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleReconciling, reconciling.Lifecycle, "an unresolved obligation keeps the operation reconciling, never behind a completed flag")
		require.Equal(t, "EFFECT_UNCERTAIN", reconciling.FailureCode)
		_, _, err = p1.ops.SubmitCommand(ctx, cmd("tenant_a", "hold_prep", "h"), scopeA, prep.ID, domain.CommandHold, domain.Revision(1), "")
		require.ErrorIs(t, err, domain.ErrRevisionConflict)
		pv, err := p1.ops.Get(ctx, scopeA, prep.ID)
		require.NoError(t, err)
		_, _, err = p1.ops.SubmitCommand(ctx, cmd("tenant_a", "hold_prep2", "h"), scopeA, prep.ID, domain.CommandHold, pv.Revision, "")
		require.NoError(t, err)
		pc, err := p1.ops.GetCommand(ctx, scopeA, prep.ID, "hold_prep2")
		require.NoError(t, err)
		require.Equal(t, domain.OutcomeRejected, pc.Outcome)
		require.Equal(t, "UNSUPPORTED_BY_PROFILE", pc.ReasonCode, "the preparation profile supports cancellation only")
	})
}

func ptr(s string) *string { return &s }
