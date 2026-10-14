package outbox_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/outbox"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

var (
	testLog = slog.New(slog.NewTextHandler(io.Discard, nil))
	profile = domain.Profile{ID: "local-check-v1", Kind: domain.KindLocalCheck, OperationDeadline: 15 * time.Minute, StepID: "local-check", JobProfileID: "local-check-v1"}
	subject = domain.Subject{ProfileID: "local-check-v1", SubjectDigest: application.DigestOf([]byte("subject"))}
)

// newOperation commits an operation of the tenant (its creation event is
// left to the test).
func newOperation(t *testing.T, store *postgres.Store, tenant, command string) *domain.Operation {
	t.Helper()
	op, err := domain.NewOperation(domain.CommandIdentity{TenantID: tenant, CommandID: command, ActorID: "user", RequestDigest: application.DigestOf([]byte(command))},
		domain.Scope{TenantID: tenant, ProjectID: "proj", ActorID: "user"}, domain.KindLocalCheck, subject, profile, time.Now().UTC())
	require.NoError(t, err)
	require.NoError(t, store.Tx(context.Background(), func(r application.Repo) error { return r.InsertOperation(context.Background(), op) }))
	return op
}

func event(op *domain.Operation, seq uint64, lifecycle domain.Lifecycle) domain.Event {
	return domain.Event{
		OperationID: op.ID, EventSeq: seq, TransitionID: fmt.Sprintf("%s:t%d", op.ID, seq), EventType: domain.EventOperationChanged,
		Revision: domain.Revision(seq + 1), OccurredAt: time.Now().UTC(),
		Payload: domain.ChangedPayload{Lifecycle: lifecycle, Phase: "launching", Control: domain.ControlNone, Cleanup: domain.CleanupPending, Finance: domain.FinanceNotFunded},
	}
}

// outboxEnvelopes reads every stored row as its event envelope.
func outboxEnvelopes(t *testing.T, pool *pgxpool.Pool) []domain.Envelope {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT payload::text FROM outbox ORDER BY transaction_id, "offset"`)
	require.NoError(t, err)
	defer rows.Close()
	var out []domain.Envelope
	for rows.Next() {
		var raw string
		require.NoError(t, rows.Scan(&raw))
		env, dest, err := outbox.Unwrap([]byte(raw))
		require.NoError(t, err)
		require.Equal(t, env.Subject, dest, "the forwarder publishes to the envelope's subject")
		out = append(out, env)
	}
	require.NoError(t, rows.Err())
	return out
}

// envelopeSchema compiles the contracts' envelope definition when the
// contracts checkout is reachable from this module (the parent workspace
// or ANVILKIT_CONTRACTS_DIR); it answers nil otherwise and the caller
// reports the schema check UNEXECUTED.
func envelopeSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	dir := os.Getenv("ANVILKIT_CONTRACTS_DIR")
	if dir == "" {
		wd, _ := os.Getwd()
		for d := wd; d != filepath.Dir(d); d = filepath.Dir(d) {
			if _, err := os.Stat(filepath.Join(d, "contracts", "events", "events.schema.json")); err == nil {
				dir = filepath.Join(d, "contracts")
				break
			}
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "events", "events.schema.json"))
	if dir == "" || err != nil {
		t.Logf("UNEXECUTED: the contracts' events schema is not reachable (set ANVILKIT_CONTRACTS_DIR)")
		return nil
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	require.NoError(t, err)
	c := jsonschema.NewCompiler()
	require.NoError(t, c.AddResource("urn:anvilkit:events:v1", doc))
	s, err := c.Compile("urn:anvilkit:events:v1#/$defs/envelope")
	require.NoError(t, err)
	return s
}

func validate(t *testing.T, s *jsonschema.Schema, env domain.Envelope) {
	t.Helper()
	if s == nil {
		return
	}
	raw, err := jsonMarshal(env)
	require.NoError(t, err)
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	require.NoError(t, err)
	require.NoError(t, s.Validate(inst), "the envelope satisfies contracts/events/events.schema.json")
}

// P2.8 AC1: an operation event and its operation.changed envelope commit in
// the same transaction: a rolled-back transaction leaves neither, a
// committed one leaves exactly one of each; aggregateRevision is the
// event's sequence, the payload carries the operation revision, and an
// event outside a transaction is refused.
func TestOperationEventCommitsWithItsEnvelope(t *testing.T) {
	inst := testdb.Start(t)
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	ctx := context.Background()
	schema := envelopeSchema(t)
	op := newOperation(t, store, "tenant_a", "cmd_ac1")
	count := func() (events, envelopes int) {
		require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM operation_events WHERE operation_id = $1", op.ID).Scan(&events))
		require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&envelopes))
		return
	}

	abort := errors.New("abort after the event")
	err := store.Tx(ctx, func(r application.Repo) error {
		if err := r.InsertEvent(ctx, event(op, 1, domain.LifecycleAccepted)); err != nil {
			return err
		}
		return abort
	})
	require.ErrorIs(t, err, abort)
	events, envelopes := count()
	require.Zero(t, events, "the rolled-back event is gone")
	require.Zero(t, envelopes, "and so is its envelope")

	require.NoError(t, store.Tx(ctx, func(r application.Repo) error { return r.InsertEvent(ctx, event(op, 1, domain.LifecycleAccepted)) }))
	events, envelopes = count()
	require.Equal(t, 1, events)
	require.Equal(t, 1, envelopes, "one committed event, one envelope")
	require.NoError(t, store.Tx(ctx, func(r application.Repo) error { return r.InsertEvent(ctx, event(op, 2, domain.LifecycleRunning)) }))
	events, envelopes = count()
	require.Equal(t, 2, events)
	require.Equal(t, 2, envelopes)

	envs := outboxEnvelopes(t, pool)
	for i, env := range envs {
		require.Equal(t, domain.EventOperationChanged, env.EventType)
		require.Equal(t, domain.SubjectOperationChanged, env.Subject)
		require.Equal(t, "tenant_a", env.TenantID)
		require.Equal(t, op.ID, env.AggregateID)
		require.Equal(t, fmt.Sprintf("%d", i+1), env.AggregateRevision, "aggregateRevision is the event sequence")
		require.Contains(t, string(env.Payload), fmt.Sprintf(`"revision":"%d"`, i+2), "the payload carries the operation revision")
		validate(t, schema, env)
	}
	require.Contains(t, string(envs[1].Payload), `"lifecycle":"running"`)

	err = store.Read(ctx, func(r application.Repo) error { return r.InsertEvent(ctx, event(op, 3, domain.LifecycleRunning)) })
	require.ErrorContains(t, err, "inside a transaction")
	events, envelopes = count()
	require.Equal(t, 2, events)
	require.Equal(t, 2, envelopes)
}

func startNATS(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "nats:2.12.4-alpine", Cmd: []string{"-js"}, ExposedPorts: []string{"4222/tcp"}, WaitingFor: wait.ForLog("Server is ready"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	testcontainers.CleanupContainer(t, c)
	host, err := c.Host(ctx)
	require.NoError(t, err)
	port, err := c.MappedPort(ctx, "4222/tcp")
	require.NoError(t, err)
	return fmt.Sprintf("nats://%s:%s", host, port.Port())
}

// P2.8 AC2: a committed operation event reaches JetStream through the
// forwarder (Nats-Msg-Id = eventId) and wakes the event streams of its
// operation through the waker, with the LISTEN wake not running and no
// database poll: the measured commit-to-wake latency stays under the 2 s
// stream poll. Retention then deletes acknowledged rows only, and the lag
// reports the unforwarded one.
func TestForwarderPublishesAndWakesStreams(t *testing.T) {
	if os.Getenv("ANVILKIT_SKIP_DOCKER_TESTS") != "" {
		t.Skip("ANVILKIT_SKIP_DOCKER_TESTS set")
	}
	natsURL := startNATS(t)
	inst := testdb.Start(t)
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	nc, err := outbox.Connect(outbox.NATSConfig{URL: natsURL, Name: "control-test", PublishTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: outbox.Stream, Subjects: []string{"anvilkit.control.>"}, Duplicates: 2 * time.Minute})
	require.NoError(t, err)

	counter := func(name string) prometheus.Counter { return prometheus.NewCounter(prometheus.CounterOpts{Name: name}) }
	forwarded, failures, wakes, rejected := counter("f"), counter("x"), counter("w"), counter("r")
	fwd, err := outbox.NewForwarder(pool, nc, outbox.ForwarderConfig{
		ConsumerGroup: "anvilkit-agent-control-forwarder", PollInterval: 200 * time.Millisecond, AckDeadline: 30 * time.Second,
		ResendInterval: time.Second, BatchSize: 100, CloseTimeout: 5 * time.Second,
	}, forwarded, failures)
	require.NoError(t, err)
	// The notifier's LISTEN loop is deliberately not run: every wake below
	// arrives through JetStream.
	notifier := postgres.NewNotifier(pool, testLog)
	waker, err := outbox.NewWaker(nc, notifier.Wake, testLog, wakes, rejected)
	require.NoError(t, err)
	runCtx, stop := context.WithCancel(ctx)
	fwdDone, wakerDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(fwdDone); _ = fwd.Run(runCtx) }()
	go func() { defer close(wakerDone); waker.Run(runCtx) }()
	<-fwd.Running()
	require.Eventually(t, func() bool {
		info, err := js.Stream(ctx, outbox.Stream)
		if err != nil {
			return false
		}
		si, err := info.Info(ctx)
		return err == nil && si.State.Consumers >= 1
	}, 10*time.Second, 50*time.Millisecond, "the waker's ordered consumer is in place")

	op := newOperation(t, store, "tenant_a", "cmd_ac2")
	other := newOperation(t, store, "tenant_a", "cmd_ac2_other")
	wake, unsubscribe := notifier.Subscribe(op.ID)
	defer unsubscribe()
	otherWake, unsubscribeOther := notifier.Subscribe(other.ID)
	defer unsubscribeOther()

	const samples = 20
	latencies := make([]time.Duration, 0, samples)
	for seq := uint64(1); seq <= samples; seq++ {
		start := time.Now()
		require.NoError(t, store.Tx(ctx, func(r application.Repo) error { return r.InsertEvent(ctx, event(op, seq, domain.LifecycleRunning)) }))
		select {
		case <-wake:
			latencies = append(latencies, time.Since(start))
		case <-time.After(2 * time.Second):
			t.Fatalf("event %d did not wake its stream within the 2 s poll bound", seq)
		}
	}
	select {
	case <-otherWake:
		t.Fatal("another operation's stream was woken")
	case <-time.After(300 * time.Millisecond):
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50, p95, worst := latencies[samples/2], latencies[samples*95/100-1], latencies[samples-1]
	t.Logf("commit-to-wake latency over %d events (forwarder poll 200ms, LISTEN off): p50 %s p95 %s max %s", samples, p50, p95, worst)
	require.Less(t, worst, 2*time.Second)
	if dir := os.Getenv("ANVILKIT_EVIDENCE_DIR"); dir != "" {
		line := fmt.Sprintf("P2.8 AC2 commit-to-wake latency (JetStream path, LISTEN off, forwarder poll 200ms) samples=%d p50=%s p95=%s max=%s\n", samples, p50, p95, worst)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "control-event-latency.txt"), []byte(line), 0o644))
	}
	require.Zero(t, testutilValue(rejected))

	// Every event is on the stream once, under its eventId.
	check, err := js.OrderedConsumer(ctx, outbox.Stream, jetstream.OrderedConsumerConfig{FilterSubjects: []string{domain.SubjectOperationChanged}})
	require.NoError(t, err)
	batch, err := check.Fetch(samples, jetstream.FetchMaxWait(5*time.Second))
	require.NoError(t, err)
	seen := 0
	for m := range batch.Messages() {
		target, err := outbox.WakeTarget(m.Data())
		require.NoError(t, err)
		require.Equal(t, op.ID, target)
		var env domain.Envelope
		require.NoError(t, jsonUnmarshal(m.Data(), &env))
		require.Equal(t, env.EventID, m.Headers().Get(nats.MsgIdHdr), "Nats-Msg-Id is the eventId")
		seen++
		require.Equal(t, fmt.Sprintf("%d", seen), env.AggregateRevision, "the stream holds the events in order")
	}
	require.Equal(t, samples, seen)

	// Retention: stop forwarding, commit one more event; only the
	// acknowledged rows are deleted, and the lag reports the unforwarded one.
	stop()
	require.NoError(t, fwd.Close())
	<-fwdDone
	<-wakerDone
	require.NoError(t, store.Tx(ctx, func(r application.Repo) error {
		return r.InsertEvent(ctx, event(op, samples+1, domain.LifecycleSucceeded))
	}))
	n, err := outbox.Prune(ctx, pool, "anvilkit-agent-control-forwarder", time.Now().Add(time.Hour), 1000)
	require.NoError(t, err)
	require.EqualValues(t, samples, n, "every acknowledged row past the bound is deleted")
	remaining := outboxEnvelopes(t, pool)
	require.Len(t, remaining, 1)
	require.Equal(t, fmt.Sprintf("%d", samples+1), remaining[0].AggregateRevision, "the unforwarded row stays")
	time.Sleep(1100 * time.Millisecond)
	lag, err := outbox.Oldest(ctx, pool, "anvilkit-agent-control-forwarder")
	require.NoError(t, err)
	require.GreaterOrEqual(t, lag, time.Second, "the lag reports the unforwarded row")
	n, err = outbox.Prune(ctx, pool, "anvilkit-agent-control-forwarder", time.Now().Add(-time.Hour), 1000)
	require.NoError(t, err)
	require.Zero(t, n, "nothing younger than the retention is deleted")
}

// The waker wakes only for a well-formed operation.changed of Control.
func TestWakeTargetRejectsForeignMessages(t *testing.T) {
	op := &domain.Operation{ID: "op_WAKE1"}
	env, err := domain.OperationChangedEnvelope(event(op, 3, domain.LifecycleRunning), "tenant_a")
	require.NoError(t, err)
	raw, err := jsonMarshal(env)
	require.NoError(t, err)
	target, err := outbox.WakeTarget(raw)
	require.NoError(t, err)
	require.Equal(t, "op_WAKE1", target)
	for name, mutate := range map[string]func(e *domain.Envelope){
		"another type":      func(e *domain.Envelope) { e.EventType = "mcp.grant-revoked" },
		"another producer":  func(e *domain.Envelope) { e.Producer = "anvilkit-agent-mcp" },
		"another subject":   func(e *domain.Envelope) { e.Subject = "anvilkit.mcp.mcp.grant-revoked" },
		"another aggregate": func(e *domain.Envelope) { e.AggregateType = "grant" },
		"schema version":    func(e *domain.Envelope) { e.SchemaVersion = 2 },
		"malformed id":      func(e *domain.Envelope) { e.AggregateID = "op 1; drop" },
		"empty tenant":      func(e *domain.Envelope) { e.TenantID = "" },
	} {
		e := env
		mutate(&e)
		raw, err := jsonMarshal(e)
		require.NoError(t, err)
		_, err = outbox.WakeTarget(raw)
		require.Error(t, err, name)
	}
	_, err = outbox.WakeTarget([]byte("not json"))
	require.Error(t, err)
	_, err = outbox.WakeTarget([]byte(`{"pad":"` + strings.Repeat("x", 70<<10) + `"}`))
	require.ErrorContains(t, err, "over the bound")
	// Only operation.changed is a Control event (attempt.result-accepted was
	// removed from the catalog in P2.8).
	_, err = domain.OperationChangedEnvelope(domain.Event{EventType: "attempt.result-accepted"}, "tenant_a")
	require.Error(t, err)
}
