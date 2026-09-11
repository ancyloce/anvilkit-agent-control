//go:build integration

package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMain(m *testing.M) {
	for _, name := range []string{"ANVILKIT_CONTROL_TEST_DATABASE_URL", "ANVILKIT_CONTROL_TEST_ADMIN_DATABASE_URL", "ANVILKIT_CONTROL_TEST_MIGRATOR_DATABASE_URL", "ANVILKIT_CONTROL_TEST_API_DATABASE_URL", "ANVILKIT_CONTROL_TEST_RESTART_FILE"} {
		if os.Getenv(name) == "" {
			fmt.Println("UNEXECUTED: integration tests require the parent CONTROL-02 driver")
			os.Exit(2)
		}
	}
	os.Exit(m.Run())
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s, err := Open(ctx, os.Getenv("ANVILKIT_CONTROL_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func testConnection(t *testing.T, role string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), os.Getenv("ANVILKIT_CONTROL_TEST_"+role+"_DATABASE_URL"))
	if err != nil {
		t.Fatal("test connection failed")
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func mustExec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func wantError(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("wanted %v; got %v", want, err)
	}
}

func wantSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != code {
		t.Fatalf("wanted SQLSTATE %s; got %v", code, err)
	}
}

func fixtureActivation(t *testing.T, s *Store, name string) (Activation, ActivationCommand) {
	t.Helper()
	raw, _ := canonical(map[string]any{"fixture": "CONTROL-02 synthetic storage only", "name": name})
	if err := s.PutImmutable(t.Context(), ImmutableRecord{hash(raw), "definition", raw}); err != nil {
		t.Fatal(err)
	}
	command := ActivationCommand{Family: "component", DefinitionID: name, CommandID: "fixture-initial", DefinitionDigest: hash(raw), RuntimeProfileRef: "fixture-unqualified", ValidatorReportRef: "fixture-storage-proof", ActivatedBy: "fixture-developer"}
	a, err := s.CompareAndSwapActivation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	return a, command
}

func fixtureOperation(t *testing.T, s *Store) (Operation, OperationCommand) {
	t.Helper()
	a, _ := fixtureActivation(t, s, "fixture-"+rand.Text())
	command := OperationCommand{Scope: Scope{"fixture-tenant-" + rand.Text(), "fixture-actor"}, Kind: "generation", IntakeSource: "api", CommandID: "fixture-command", ActivationID: a.ID, Request: json.RawMessage(`{"fixture":"CONTROL-02","paid":false}`)}
	op, err := s.PersistOperation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	return op, command
}

func startedEvent(t *testing.T, s *Store, op Operation, command OperationCommand) StepEvent {
	t.Helper()
	var digest string
	if err := s.pool.QueryRow(t.Context(), `SELECT definition_digest FROM definition_contract.activations WHERE activation_id=$1`, command.ActivationID).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	body, _ := canonical(map[string]any{"stepExecutionId": "fixture-step-" + rand.Text(), "definitionSegment": "1", "definitionDigest": digest, "stepId": "code", "visit": "1", "actionId": "component.code", "actionVersion": "1.0.0", "inputRefs": map[string]any{}})
	return StepEvent{Scope: command.Scope, OperationID: op.ID, TransitionID: "fixture-transition-" + rand.Text(), StepID: "code", ActionID: "component.code", ActionVersion: "1.0.0", Type: "step.started", DefinitionSegment: 1, Visit: 1, Payload: body}
}

func counters(t *testing.T, s *Store, id string) (int64, int64, int64) {
	t.Helper()
	var next, revision, count int64
	if err := s.pool.QueryRow(t.Context(), `SELECT next_event_seq,operation_revision,(SELECT count(*) FROM agent_control.operation_events e WHERE e.operation_id=o.operation_id) FROM agent_control.operations o WHERE operation_id=$1`, id).Scan(&next, &revision, &count); err != nil {
		t.Fatal(err)
	}
	return next, revision, count
}

func TestPostgresTransactions(t *testing.T) {
	t.Run("immutable and scoped CAS", testImmutableCAS)
	t.Run("command identity", testOperationIdentity)
	t.Run("atomic projections and transitions", testEvents)
	t.Run("rollback and bounded retry", testRollbackRetry)
	t.Run("restricted roles and tenant reuse", testRoles)
	t.Run("authorization lease takeover", testLeases)
	t.Run("monotonicity is separate from deduplication", testMonotonicity)
	t.Run("canceled database wait", testCanceledWait)
}

func TestPostgresDisclosureStorage(t *testing.T) {
	s, other := openTestStore(t), openTestStore(t)
	op, command := fixtureOperation(t, s)
	for _, scope := range []Scope{command.Scope, {TenantID: "fixture-foreign", ActorID: command.ActorID}, {TenantID: command.TenantID, ActorID: "fixture-foreign"}} {
		matches, err := s.MatchesOperationScope(t.Context(), scope, op.ID)
		if err != nil || matches != (scope == command.Scope) {
			t.Fatal("exact operation scope check failed", err)
		}
	}
	if matches, err := s.MatchesOperationScope(t.Context(), command.Scope, "fixture-absent"); err != nil || matches {
		t.Fatal("absent operation matched", err)
	}
	if allowed, err := s.TakeAuthorizationRead(t.Context()); err != nil || !allowed {
		t.Fatal("initial read budget unavailable", err)
	}
	admin := testConnection(t, "ADMIN")
	// Freeze refill with a future timestamp to measure the burst independently
	// of scheduler speed. This task-owned database contains no live budgets.
	mustExec(t, admin, `UPDATE agent_control.authorization_read_budget SET tokens=40,refilled_at=clock_timestamp()+interval '1 hour'`)
	var wg sync.WaitGroup
	start := make(chan struct{})
	allowed, errs := make([]bool, 80), make([]error, 80)
	for i := range allowed {
		wg.Go(func() {
			<-start
			store := s
			if i%2 == 1 {
				store = other
			}
			allowed[i], errs[i] = store.TakeAuthorizationRead(t.Context())
		})
	}
	close(start)
	wg.Wait()
	count := 0
	for i, value := range allowed {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if value {
			count++
		}
	}
	if count != 40 {
		t.Fatalf("shared burst granted %d reads, wanted 40", count)
	}
	var empty bool
	if err := admin.QueryRow(t.Context(), `SELECT tokens=0 FROM agent_control.authorization_read_budget`).Scan(&empty); err != nil || !empty {
		t.Fatal("bucket did not retain exact consumption", err)
	}
	mustExec(t, admin, `UPDATE agent_control.authorization_read_budget SET refilled_at=clock_timestamp()-interval '1 second'`)
	if allowed, err := s.TakeAuthorizationRead(t.Context()); err != nil || !allowed {
		t.Fatal("database clock did not refill", err)
	}
	mustExec(t, admin, `UPDATE agent_control.authorization_read_budget SET capacity=41`)
	allowedOne, err := s.TakeAuthorizationRead(t.Context())
	wantError(t, err, ErrInvalid)
	if allowedOne {
		t.Fatal("unreviewed budget configuration granted a read")
	}
	mustExec(t, admin, `UPDATE agent_control.authorization_read_budget SET capacity=40`)
}

func testImmutableCAS(t *testing.T) {
	s, other := openTestStore(t), openTestStore(t)
	name := "fixture-cas-" + rand.Text()
	raw := []byte(`{"fixture":"CONTROL-02 immutable"}`)
	record := ImmutableRecord{hash(raw), "definition", raw}
	if err := s.PutImmutable(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err := other.PutImmutable(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	changed := record
	changed.Kind = "policy"
	wantError(t, s.PutImmutable(t.Context(), changed), ErrConflict)
	changed = record
	changed.CanonicalBytes = []byte(`{"fixture":"changed"}`)
	wantError(t, s.PutImmutable(t.Context(), changed), ErrInvalid)
	var original []byte
	if err := s.pool.QueryRow(t.Context(), `SELECT canonical_bytes FROM definition_contract.immutable_records WHERE digest=$1`, record.Digest).Scan(&original); err != nil || string(original) != string(raw) {
		t.Fatal("original immutable bytes changed")
	}
	base := ActivationCommand{Family: "component", DefinitionID: name, DefinitionDigest: record.Digest, RuntimeProfileRef: "fixture-unqualified", ValidatorReportRef: "fixture-report", ActivatedBy: "fixture-developer"}
	results := make([]Activation, 8)
	failures := make([]error, 8)
	commands := make([]ActivationCommand, 8)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range results {
		commands[i] = base
		commands[i].CommandID = fmt.Sprintf("fixture-command-%d", i)
		wg.Go(func() {
			<-start
			store := s
			if i%2 == 1 {
				store = other
			}
			results[i], failures[i] = store.CompareAndSwapActivation(t.Context(), commands[i])
		})
	}
	close(start)
	wg.Wait()
	winner := -1
	for i, err := range failures {
		if err == nil {
			if winner != -1 {
				t.Fatal("multiple first-CAS winners")
			}
			winner = i
		} else {
			wantError(t, err, ErrRevision)
		}
	}
	if winner < 0 {
		t.Fatal("no CAS winner")
	}
	advance := base
	advance.CommandID = "fixture-advance"
	advance.ExpectedCurrentActivationID = &results[winner].ID
	latest, err := s.CompareAndSwapActivation(t.Context(), advance)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := other.CompareAndSwapActivation(t.Context(), commands[winner])
	if err != nil || !replay.Existing || replay.ID != results[winner].ID || replay.ID == latest.ID {
		t.Fatal("replay did not return the original activation", err)
	}
	changedCommand := commands[winner]
	changedCommand.RuntimeProfileRef = "fixture-changed"
	_, err = s.CompareAndSwapActivation(t.Context(), changedCommand)
	wantError(t, err, ErrConflict)
	base.CommandID = "fixture-new-command"
	_, err = s.CompareAndSwapActivation(t.Context(), base)
	wantError(t, err, ErrRevision)
	admin := testConnection(t, "ADMIN")
	_, err = admin.Exec(t.Context(), `UPDATE definition_contract.immutable_records SET canonical_bytes='{}' WHERE digest=$1`, record.Digest)
	wantSQLState(t, err, "23000")
	_, err = admin.Exec(t.Context(), `DELETE FROM definition_contract.activations WHERE activation_id=$1`, latest.ID)
	wantSQLState(t, err, "23000")
	// A failure at the pointer update must roll back the preceding activation insert.
	advance.ExpectedCurrentActivationID = &latest.ID
	advance.CommandID = "fixture-rollback"
	mustExec(t, admin, `CREATE FUNCTION definition_contract.control02_reject_pointer() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic rollback' USING ERRCODE='23514'; END $$;
		CREATE TRIGGER control02_reject_pointer BEFORE UPDATE ON definition_contract.activation_pointers FOR EACH ROW EXECUTE FUNCTION definition_contract.control02_reject_pointer()`)
	_, err = s.CompareAndSwapActivation(t.Context(), advance)
	mustExec(t, admin, `DROP TRIGGER control02_reject_pointer ON definition_contract.activation_pointers; DROP FUNCTION definition_contract.control02_reject_pointer()`)
	wantSQLState(t, err, "23514")
	var count int
	if err = s.pool.QueryRow(t.Context(), `SELECT count(*) FROM definition_contract.activations WHERE definition_id=$1 AND command_id=$2`, name, advance.CommandID).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed CAS retained its activation")
	}
}

func testOperationIdentity(t *testing.T) {
	s, other := openTestStore(t), openTestStore(t)
	a, _ := fixtureActivation(t, s, "fixture-command-"+rand.Text())
	command := OperationCommand{Scope: Scope{"fixture-tenant-" + rand.Text(), "fixture-actor"}, Kind: "generation", IntakeSource: "api", CommandID: "fixture-command", ActivationID: a.ID, Request: []byte(`{"b":2,"a":1}`)}
	results := make([]Operation, 12)
	failures := make([]error, 12)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() {
			<-start
			store := s
			if i%2 == 1 {
				store = other
			}
			results[i], failures[i] = store.PersistOperation(t.Context(), command)
		})
	}
	close(start)
	wg.Wait()
	created := 0
	for i, result := range results {
		if failures[i] != nil {
			t.Fatal(failures[i])
		}
		if result.ID != results[0].ID {
			t.Fatal("duplicate command minted another operation")
		}
		if !result.Existing {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created %d operations", created)
	}
	command.Request = []byte(`{ "a":1, "b":2 }`)
	op, err := s.PersistOperation(t.Context(), command)
	if err != nil || !op.Existing || op.ID != results[0].ID {
		t.Fatal("semantic replay changed identity", err)
	}
	n, r, c := counters(t, s, op.ID)
	if n != 2 || r != 1 || c != 1 {
		t.Fatalf("duplicate intake changed projection/event: %d %d %d", n, r, c)
	}
	command.Request = []byte(`{"a":2}`)
	_, err = s.PersistOperation(t.Context(), command)
	wantError(t, err, ErrConflict)
	command.ActorID = "fixture-other-actor"
	different, err := s.PersistOperation(t.Context(), command)
	if err != nil || different.ID == op.ID {
		t.Fatal("actor command scope collapsed", err)
	}
	command.TenantID = "fixture-other-tenant"
	different, err = s.PersistOperation(t.Context(), command)
	if err != nil || different.ID == op.ID {
		t.Fatal("tenant command scope collapsed", err)
	}
}

func testEvents(t *testing.T) {
	s, other := openTestStore(t), openTestStore(t)
	op, command := fixtureOperation(t, s)
	event := startedEvent(t, s, op, command)
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]RecordedEvent, 10)
	failures := make([]error, 10)
	for i := range results {
		wg.Go(func() {
			<-start
			store := s
			if i%2 == 1 {
				store = other
			}
			results[i], failures[i] = store.RecordStepEvent(t.Context(), event)
		})
	}
	close(start)
	wg.Wait()
	created := 0
	for i, result := range results {
		if failures[i] != nil {
			t.Fatal(failures[i])
		}
		if result.Sequence != 2 {
			t.Fatal("duplicate consumed a sequence")
		}
		if !result.Existing {
			created++
		}
	}
	if created != 1 {
		t.Fatal("transition was applied more than once")
	}
	wait := event
	wait.Type = "step.waiting"
	wait.TransitionID = "fixture-wait"
	wait.Payload, _ = canonical(map[string]any{"stepExecutionId": results[0].StepExecutionID, "definitionSegment": "1", "observationSequence": "1", "waitReason": "review"})
	if result, err := s.RecordStepEvent(t.Context(), wait); err != nil || result.Sequence != 3 {
		t.Fatal("waiting projection failed", err)
	}
	finish := event
	finish.Type = "step.finished"
	finish.TransitionID = "fixture-finish"
	finish.Payload, _ = canonical(map[string]any{"stepExecutionId": results[0].StepExecutionID, "definitionSegment": "1", "outcome": "ready", "outputRefs": map[string]any{}, "endedAt": time.Now().UTC().Format(time.RFC3339Nano)})
	if result, err := s.RecordStepEvent(t.Context(), finish); err != nil || result.Sequence != 4 {
		t.Fatal("finished projection failed", err)
	}
	if result, err := s.RecordStepEvent(t.Context(), event); err != nil || !result.Existing || result.Sequence != 2 {
		t.Fatal("old transition replay lost original result", err)
	}
	changed := finish
	changed.Payload = []byte(strings.Replace(string(finish.Payload), `"ready"`, `"failed"`, 1))
	_, err := s.RecordStepEvent(t.Context(), changed)
	wantError(t, err, ErrConflict)
	changed = wait
	changed.StepID = "other-step"
	_, err = s.RecordStepEvent(t.Context(), changed)
	wantError(t, err, ErrConflict)
	changed = wait
	changed.Scope = Scope{"foreign-tenant", command.ActorID}
	_, err = s.RecordStepEvent(t.Context(), changed)
	wantError(t, err, ErrNotFound)
	n, r, c := counters(t, s, op.ID)
	if n != 5 || r != 4 || c != 4 {
		t.Fatalf("projection/event mismatch: %d %d %d", n, r, c)
	}
	var status, outcome string
	var covered int64
	if err = s.pool.QueryRow(t.Context(), `SELECT status,outcome,covered_seq FROM agent_control.step_executions WHERE step_execution_id=$1`, results[0].StepExecutionID).Scan(&status, &outcome, &covered); err != nil || status != "finished" || outcome != "ready" || covered != 4 {
		t.Fatal("committed step disagrees with its event", err)
	}
	// Distinct transitions from independent pools serialize under the operation lock.
	for i := range results {
		next := startedEvent(t, s, op, command)
		next.StepID = fmt.Sprintf("step-%d", i)
		next.TransitionID = fmt.Sprintf("fixture-parallel-%d", i)
		var payload map[string]any
		_ = json.Unmarshal(next.Payload, &payload)
		payload["stepId"] = next.StepID
		next.Payload, _ = canonical(payload)
		wg.Go(func() { results[i], failures[i] = other.RecordStepEvent(t.Context(), next) })
	}
	wg.Wait()
	seen := map[int64]bool{}
	for i, result := range results {
		if failures[i] != nil {
			t.Fatal(failures[i])
		}
		if result.Sequence < 5 || result.Sequence > 14 || seen[result.Sequence] {
			t.Fatal("sequences are not unique and gapless")
		}
		seen[result.Sequence] = true
	}
	n, r, c = counters(t, s, op.ID)
	if n != 15 || r != 14 || c != 14 {
		t.Fatal("concurrent projection/event totals diverged")
	}
	rows, err := s.pool.Query(t.Context(), `SELECT event_seq,event_type,body,occurred_at FROM agent_control.operation_events WHERE operation_id=$1`, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		var kind string
		var body []byte
		var at time.Time
		if err = rows.Scan(&seq, &kind, &body, &at); err != nil {
			t.Fatal(err)
		}
		var payload any
		_ = json.Unmarshal(body, &payload)
		if err = s.eventSchema.Validate(map[string]any{"schemaVersion": 1, "operationId": op.ID, "eventSeq": fmt.Sprint(seq), "type": kind, "occurredAt": at.UTC().Format(time.RFC3339Nano), "payload": payload}); err != nil {
			t.Fatal(err)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func testRollbackRetry(t *testing.T) {
	s := openTestStore(t)
	admin := testConnection(t, "ADMIN")
	op, command := fixtureOperation(t, s)
	event := startedEvent(t, s, op, command)
	mustExec(t, admin, `CREATE SEQUENCE agent_control.control02_failures;
		GRANT USAGE,SELECT ON SEQUENCE agent_control.control02_failures TO anvilkit_control_rw;
		CREATE FUNCTION agent_control.control02_event_fault() RETURNS trigger LANGUAGE plpgsql AS $$
		DECLARE n bigint; BEGIN n=nextval('agent_control.control02_failures');
		IF NEW.transition_id='fixture-rollback' THEN RAISE EXCEPTION 'synthetic rollback' USING ERRCODE='23514'; END IF;
		IF NEW.transition_id='fixture-exhaust' OR (NEW.transition_id='fixture-retry' AND n<3) THEN RAISE EXCEPTION 'synthetic serialization abort' USING ERRCODE='40001'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER control02_event_fault BEFORE INSERT ON agent_control.operation_events FOR EACH ROW EXECUTE FUNCTION agent_control.control02_event_fault()`)
	defer mustExec(t, admin, `DROP TRIGGER control02_event_fault ON agent_control.operation_events; DROP FUNCTION agent_control.control02_event_fault(); DROP SEQUENCE agent_control.control02_failures`)
	event.TransitionID = "fixture-rollback"
	result, err := s.RecordStepEvent(t.Context(), event)
	wantSQLState(t, err, "23514")
	if result.Sequence != 0 {
		t.Fatal("failed transaction acknowledged an event")
	}
	n, r, c := counters(t, s, op.ID)
	if n != 2 || r != 1 || c != 1 {
		t.Fatal("rollback consumed a sequence or revision")
	}
	var count int
	_ = s.pool.QueryRow(t.Context(), `SELECT count(*) FROM agent_control.step_executions WHERE operation_id=$1`, op.ID).Scan(&count)
	if count != 0 {
		t.Fatal("rollback retained projection")
	}
	mustExec(t, admin, `ALTER SEQUENCE agent_control.control02_failures RESTART WITH 1`)
	event.TransitionID = "fixture-retry"
	result, err = s.RecordStepEvent(t.Context(), event)
	if err != nil || result.Sequence != 2 {
		t.Fatal("serialization retry did not commit once", err)
	}
	var attempts int64
	_ = admin.QueryRow(t.Context(), `SELECT last_value FROM agent_control.control02_failures`).Scan(&attempts)
	if attempts != 3 {
		t.Fatalf("got %d attempts", attempts)
	}
	n, r, c = counters(t, s, op.ID)
	if n != 3 || r != 2 || c != 2 {
		t.Fatal("retry applied multiple events")
	}
	other := startedEvent(t, s, op, command)
	other.StepID = "other"
	var payload map[string]any
	_ = json.Unmarshal(other.Payload, &payload)
	payload["stepId"] = "other"
	other.Payload, _ = canonical(payload)
	other.TransitionID = "fixture-exhaust"
	mustExec(t, admin, `ALTER SEQUENCE agent_control.control02_failures RESTART WITH 1`)
	_, err = s.RecordStepEvent(t.Context(), other)
	wantSQLState(t, err, "40001")
	_ = admin.QueryRow(t.Context(), `SELECT last_value FROM agent_control.control02_failures`).Scan(&attempts)
	if attempts != int64(transactionRetryMax+1) {
		t.Fatalf("retry bound exceeded: %d", attempts)
	}
	n, r, c = counters(t, s, op.ID)
	if n != 3 || r != 2 || c != 2 {
		t.Fatal("exhausted retry changed durable state")
	}
}

func testRoles(t *testing.T) {
	s := openTestStore(t)
	op, command := fixtureOperation(t, s)
	other, _ := fixtureOperation(t, s)
	event := startedEvent(t, s, op, command)
	if _, err := s.RecordStepEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	api := testConnection(t, "API")
	admin := testConnection(t, "ADMIN")
	var activationID string
	if err := s.pool.QueryRow(t.Context(), `SELECT activation_id FROM definition_contract.activations WHERE activation_id=$1 FOR KEY SHARE`, command.ActivationID).Scan(&activationID); err != nil || activationID != command.ActivationID {
		t.Fatal("Control cannot lock its rank-1 activation", err)
	}
	_, err := s.pool.Exec(t.Context(), `UPDATE definition_contract.activations SET activation_id=activation_id WHERE activation_id=$1`, command.ActivationID)
	wantSQLState(t, err, "23000")
	_, err = s.pool.Exec(t.Context(), `UPDATE definition_contract.activations SET definition_id=definition_id WHERE activation_id=$1`, command.ActivationID)
	wantSQLState(t, err, "42501")
	var updateColumns int
	if err = s.pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_attribute
		WHERE attrelid='definition_contract.activations'::regclass AND attnum>0 AND NOT attisdropped
		AND has_column_privilege(current_user,attrelid,attnum,'UPDATE')`).Scan(&updateColumns); err != nil || updateColumns != 1 {
		t.Fatal("activation locking grant extends beyond its ID column", err)
	}
	_, err = api.Exec(t.Context(), `SELECT activation_id FROM definition_contract.activations WHERE activation_id=$1 FOR KEY SHARE`, command.ActivationID)
	wantSQLState(t, err, "42501")
	_, err = api.Exec(t.Context(), `UPDATE definition_contract.activations SET activation_id=activation_id WHERE activation_id=$1`, command.ActivationID)
	wantSQLState(t, err, "42501")
	for _, scope := range []struct {
		tenant       string
		own, foreign int
	}{{command.TenantID, 1, 0}, {"foreign-tenant", 0, 0}, {command.TenantID, 1, 0}} {
		tx, err := api.BeginTx(t.Context(), pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(t.Context(), `SELECT set_config('anvilkit.tenant_id',$1,true)`, scope.tenant)
		if err != nil {
			t.Fatal(err)
		}
		var own, foreign, events, steps int
		if err = tx.QueryRow(t.Context(), `SELECT count(*) FILTER (WHERE operation_id=$1),count(*) FILTER (WHERE operation_id=$2) FROM agent_control.operations`, op.ID, other.ID).Scan(&own, &foreign); err != nil {
			t.Fatal(err)
		}
		if own != scope.own || foreign != scope.foreign {
			t.Fatal("tenant read leaked")
		}
		_ = tx.QueryRow(t.Context(), `SELECT count(*) FROM agent_control.operation_events WHERE operation_id=$1`, op.ID).Scan(&events)
		_ = tx.QueryRow(t.Context(), `SELECT count(*) FROM agent_control.step_executions WHERE operation_id=$1`, op.ID).Scan(&steps)
		if (own == 0 && (events != 0 || steps != 0)) || (own == 1 && (events != 2 || steps != 1)) {
			t.Fatal("child RLS disagrees with tenant")
		}
		if err = tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
		var visible int
		if err = api.QueryRow(t.Context(), `SELECT count(*) FROM agent_control.operations`).Scan(&visible); err != nil || visible != 0 {
			t.Fatal("transaction-local tenant leaked through connection reuse", err)
		}
	}
	for _, sql := range []string{`UPDATE agent_control.operations SET public_status='succeeded'`, `TRUNCATE agent_control.operations CASCADE`, `SELECT * FROM definition_contract.activations`, `SET ROLE anvilkit_control_migrator`, `SET ROLE anvilkit_control_rw`} {
		_, err := api.Exec(t.Context(), sql)
		wantSQLState(t, err, "42501")
	}
	mustExec(t, api, `SET anvilkit.service_context='control'`)
	var count int
	_ = api.QueryRow(t.Context(), `SELECT count(*) FROM agent_control.operations`).Scan(&count)
	if count != 0 {
		t.Fatal("API invented Control authority via a setting")
	}
	for _, role := range []string{"ADMIN", "MIGRATOR", "API"} {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		wrong, err := Open(ctx, os.Getenv("ANVILKIT_CONTROL_TEST_"+role+"_DATABASE_URL"))
		cancel()
		if wrong != nil {
			wrong.Close()
		}
		if err == nil {
			t.Fatal("storage accepted privileged/wrong login", role)
		}
	}
	_, err = s.pool.Exec(t.Context(), `SET ROLE anvilkit_control_migrator`)
	wantSQLState(t, err, "42501")
	var owners bool
	err = admin.QueryRow(t.Context(), `SELECT bool_and(pg_get_userbyid(c.relowner)='anvilkit_control_migrator') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('agent_control','definition_contract') AND c.relkind IN ('r','i')`).Scan(&owners)
	if err != nil || !owners {
		t.Fatal("migrator does not own schema objects", err)
	}
	// Reapplying a step fails atomically; service logins cannot execute DDL.
	err = Migrate(t.Context(), os.Getenv("ANVILKIT_CONTROL_TEST_MIGRATOR_DATABASE_URL"), "activation")
	wantSQLState(t, err, "42P06")
	err = Migrate(t.Context(), os.Getenv("ANVILKIT_CONTROL_TEST_DATABASE_URL"), "control")
	if err == nil {
		t.Fatal("runtime login migrated schema")
	}
}

func testLeases(t *testing.T) {
	s, other := openTestStore(t), openTestStore(t)
	admin := testConnection(t, "ADMIN")
	tenant := "fixture-lease-" + rand.Text()
	var wg sync.WaitGroup
	start := make(chan struct{})
	leases := make([]RefreshLease, 8)
	errs := make([]error, 8)
	for i := range leases {
		wg.Go(func() {
			<-start
			store := s
			if i%2 == 1 {
				store = other
			}
			leases[i], errs[i] = store.AcquireRefresh(t.Context(), tenant, fmt.Sprintf("replica-%d", i))
		})
	}
	close(start)
	wg.Wait()
	var first RefreshLease
	winners := 0
	for i, err := range errs {
		if err == nil {
			first = leases[i]
			winners++
		} else {
			wantError(t, err, ErrLeaseLost)
		}
	}
	if winners != 1 || first.Epoch != 1 {
		t.Fatal("refresh first-use race did not have exactly one owner")
	}
	mustExec(t, admin, `UPDATE agent_control.authorization_evidence SET refresh_lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1`, tenant)
	second, err := other.AcquireRefresh(t.Context(), tenant, first.Owner)
	if err != nil || second.Epoch != 2 || second.ID == first.ID {
		t.Fatal("same owner takeover reused an acquisition", err)
	}
	now := time.Now().UTC()
	evidence := Evidence{ObservedAt: now, FreshUntil: now.Add(20 * time.Second), MemberRoles: map[string][]string{"fixture-member": {"developer"}}}
	_, err = s.CompleteRefresh(t.Context(), first, evidence)
	wantError(t, err, ErrLeaseLost)
	wantError(t, s.ReleaseRefresh(t.Context(), first), ErrLeaseLost)
	wrong := second
	wrong.Owner = "wrong-owner"
	_, err = s.CompleteRefresh(t.Context(), wrong, evidence)
	wantError(t, err, ErrLeaseLost)
	expired := evidence
	expired.ObservedAt = now.Add(-20 * time.Second)
	expired.FreshUntil = now.Add(-time.Second)
	_, err = s.CompleteRefresh(t.Context(), second, expired)
	wantError(t, err, ErrLeaseLost)
	tooLong := evidence
	tooLong.FreshUntil = now.Add(31 * time.Second)
	_, err = s.CompleteRefresh(t.Context(), second, tooLong)
	wantError(t, err, ErrInvalid)
	revision, err := s.CompleteRefresh(t.Context(), second, evidence)
	if err != nil || revision != 1 {
		t.Fatal("current owner failed to complete", err)
	}
	_, err = other.CompleteRefresh(t.Context(), second, evidence)
	wantError(t, err, ErrLeaseLost)
	read, err := other.ReadEvidence(t.Context(), tenant)
	if err != nil || read.Revision != 1 || len(read.MemberRoles) != 1 || read.FreshUntil.After(evidence.FreshUntil.Add(time.Microsecond)) {
		t.Fatal("evidence did not retain its original deadline", err)
	}
	third, err := s.AcquireRefresh(t.Context(), tenant, first.Owner)
	if err != nil || third.Epoch != 3 {
		t.Fatal("refresh epoch did not advance", err)
	}
	wantError(t, s.ReleaseRefresh(t.Context(), second), ErrLeaseLost)
	if err = s.ReleaseRefresh(t.Context(), third); err != nil {
		t.Fatal(err)
	}
	mustExec(t, admin, `UPDATE agent_control.authorization_evidence SET observed_at=clock_timestamp()-interval '20 seconds',fresh_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1`, tenant)
	_, err = s.ReadEvidence(t.Context(), tenant)
	wantError(t, err, ErrLeaseLost)
}

func testMonotonicity(t *testing.T) {
	s := openTestStore(t)
	op, _ := fixtureOperation(t, s)
	admin := testConnection(t, "ADMIN")
	err := s.transact(t.Context(), func(tx pgx.Tx) error {
		if _, err := tx.Exec(t.Context(), `SELECT operation_id FROM agent_control.operations WHERE operation_id=$1 FOR UPDATE`, op.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(t.Context(), `INSERT INTO agent_control.action_visits(operation_id,action_id,consumed_visits) VALUES ($1,'component.repair',1)`, op.ID); err != nil {
			return err
		}
		_, err := tx.Exec(t.Context(), `INSERT INTO agent_control.action_counters(operation_id,action_id,counter_kind,value) VALUES ($1,'component.repair','runtime_retries',1)`, op.ID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{`UPDATE agent_control.action_visits SET consumed_visits=0 WHERE operation_id=$1`, `UPDATE agent_control.action_counters SET value=0 WHERE operation_id=$1`, `UPDATE agent_control.operations SET next_event_seq=1 WHERE operation_id=$1`, `UPDATE agent_control.operations SET command_id='changed' WHERE operation_id=$1`} {
		_, err = admin.Exec(t.Context(), sql, op.ID)
		wantSQLState(t, err, "23000")
	}
	mustExec(t, admin, `UPDATE agent_control.operations SET first_permit_at=clock_timestamp(),active_deadline=clock_timestamp()+interval '1 hour' WHERE operation_id=$1`, op.ID)
	_, err = admin.Exec(t.Context(), `UPDATE agent_control.operations SET first_permit_at=first_permit_at+interval '1 second' WHERE operation_id=$1`, op.ID)
	wantSQLState(t, err, "23000")
}

func testCanceledWait(t *testing.T) {
	s := openTestStore(t)
	op, command := fixtureOperation(t, s)
	admin := testConnection(t, "ADMIN")
	tx, err := admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), `SELECT operation_id FROM agent_control.operations WHERE operation_id=$1 FOR UPDATE`, op.ID); err != nil {
		t.Fatal(err)
	}
	event := startedEvent(t, s, op, command)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = s.RecordStepEvent(ctx, event)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled wait continued: %v", err)
	}
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	n, r, c := counters(t, s, op.ID)
	if n != 2 || r != 1 || c != 1 {
		t.Fatal("cancellation committed partial state")
	}
}

type restartRecord struct {
	OperationID, ActivationID, TenantID, ActorID, StepID, LeaseID string
	Epoch                                                         int64
}

func TestPostgresRestartWrite(t *testing.T) {
	s := openTestStore(t)
	op, command := fixtureOperation(t, s)
	event := startedEvent(t, s, op, command)
	result, err := s.RecordStepEvent(t.Context(), event)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireRefresh(t.Context(), command.TenantID, "fixture-restart-owner")
	if err != nil {
		t.Fatal(err)
	}
	record := restartRecord{op.ID, command.ActivationID, command.TenantID, command.ActorID, result.StepExecutionID, lease.ID, lease.Epoch}
	raw, _ := json.Marshal(record)
	if err = os.WriteFile(os.Getenv("ANVILKIT_CONTROL_TEST_RESTART_FILE"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresRestartRead(t *testing.T) {
	s := openTestStore(t)
	raw, err := os.ReadFile(os.Getenv("ANVILKIT_CONTROL_TEST_RESTART_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	var record restartRecord
	if err = json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	var activation, step string
	err = s.pool.QueryRow(t.Context(), `SELECT activation_id,current_step_execution_id FROM agent_control.operations WHERE operation_id=$1 AND tenant_id=$2 AND actor_id=$3`, record.OperationID, record.TenantID, record.ActorID).Scan(&activation, &step)
	if err != nil || activation != record.ActivationID || step != record.StepID {
		t.Fatal("operation identity/projection did not survive restart", err)
	}
	n, r, c := counters(t, s, record.OperationID)
	if n != 3 || r != 2 || c != 2 {
		t.Fatal("events/revision did not survive restart")
	}
	var id string
	var epoch int64
	err = s.pool.QueryRow(t.Context(), `SELECT refresh_lease_id,refresh_epoch FROM agent_control.authorization_evidence WHERE tenant_id=$1`, record.TenantID).Scan(&id, &epoch)
	if err != nil || id != record.LeaseID || epoch != record.Epoch {
		t.Fatal("lease identity did not survive restart", err)
	}
}
