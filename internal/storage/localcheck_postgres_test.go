//go:build integration

package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLocalCheckTransactions(t *testing.T) {
	s := openTestStore(t)
	admin := testConnection(t, "ADMIN")
	ctx := t.Context()
	scope := Scope{"fixture-local-tenant", "fixture-local-actor"}
	until := time.Now().Add(time.Hour)
	prepare := func(command string) LocalCheck {
		t.Helper()
		c, err := s.PrepareLocalCheck(ctx, scope, command, "plain-v1", "fixture-sql-only", "workflow02", until)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	confirm := func(c LocalCheck) LocalCheck {
		t.Helper()
		c, err := s.ConfirmLocalIntake(ctx, c, c.ObjectDigest, until)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	establish := func(c LocalCheck) LocalCheck {
		t.Helper()
		c, send, err := s.MarkLocalStart(ctx, c, until)
		if err != nil || !send {
			t.Fatal("start marker", err)
		}
		if err := s.EstablishLocalRun(ctx, c, "fixture-run-"+c.ID, c.Input); err != nil {
			t.Fatal(err)
		}
		c, err = s.ReadLocalCheck(ctx, c.ID)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	terminal := func(c LocalCheck) LocalTerminal {
		fixture, _ := contracts.Fixture(c.Input.FixtureID)
		raw, _ := json.Marshal(contracts.LocalCheckWorkflowResultV1{SchemaVersion: 1, OperationID: c.ID, RequestDigest: c.RequestDigest, FixtureID: c.Input.FixtureID, ProfileDigest: c.Input.ProfileDigest, ExecutionGeneration: c.Input.ExecutionGeneration, RecoveryGeneration: c.Input.RecoveryGeneration, ByteLength: fixture.ByteLength, ContentDigest: fixture.ContentDigest})
		return LocalTerminal{WorkflowID: c.WorkflowID, RunID: c.RunID, Input: c.Input, Type: "completed", EventID: 9, At: time.Now().UTC(), Result: raw}
	}

	t.Run("prepare concurrency and capacity", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make(chan LocalCheck, 8)
		for range 8 {
			wg.Go(func() {
				c, err := s.PrepareLocalCheck(ctx, scope, "fixture-sql-concurrent", "plain-v1", "fixture-sql-only", "workflow02", until)
				if err != nil {
					t.Error(err)
				}
				results <- c
			})
		}
		wg.Wait()
		close(results)
		var c LocalCheck
		for next := range results {
			if c.ID != "" && c.ID != next.ID {
				t.Fatal("duplicate identity")
			}
			c = next
		}
		_, err := s.PrepareLocalCheck(ctx, scope, "fixture-sql-concurrent", "newline-v1", "fixture-sql-only", "workflow02", until)
		wantError(t, err, ErrConflict)
		_, err = s.PrepareLocalCheck(ctx, scope, "fixture-sql-full", "plain-v1", "fixture-sql-only", "workflow02", until)
		wantError(t, err, ErrLocalCapacity)
		if _, _, err := s.MarkLocalStart(ctx, c, until); err != nil {
			t.Fatal(err)
		}
		current, err := s.ReadLocalCheck(ctx, c.ID)
		if err != nil || current.StartState != "start_unattempted" || current.Recorded || current.NextSequence != 1 {
			t.Fatal("unconfirmed intake escaped its gate", err)
		}
		_, err = s.ConfirmLocalIntake(ctx, c, "sha256:"+string(make([]byte, 64)), until)
		wantError(t, err, ErrConflict)
		c, _, err = s.CancelLocalCheck(ctx, scope, c.ID, "fixture-cancel-before-confirm", "", 1, until)
		if err != nil {
			t.Fatal(err)
		}
		if c.Status != "canceled" || !c.Resolved || c.RunID != "" {
			t.Fatal("unattempted cancel did not finish locally")
		}
		_, err = s.ConfirmLocalIntake(ctx, c, c.ObjectDigest, until)
		wantError(t, err, ErrRevision)
	})

	t.Run("intake confirmation rollback and replay", func(t *testing.T) {
		c := prepare("fixture-confirm-rollback")
		mustExec(t, admin, `CREATE FUNCTION agent_control.fixture_fail_local_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture before commit'; END $$;
			CREATE TRIGGER fixture_fail_local_event BEFORE INSERT ON agent_control.operation_events FOR EACH ROW EXECUTE FUNCTION agent_control.fixture_fail_local_event()`)
		_, err := s.ConfirmLocalIntake(ctx, c, c.ObjectDigest, until)
		if err == nil {
			t.Fatal("injected event failure was ignored")
		}
		current, _ := s.ReadLocalCheck(ctx, c.ID)
		if current.Recorded || current.NextSequence != 1 {
			t.Fatal("partial intake confirmation committed")
		}
		mustExec(t, admin, `DROP TRIGGER fixture_fail_local_event ON agent_control.operation_events;DROP FUNCTION agent_control.fixture_fail_local_event()`)
		c = confirm(c)
		again := confirm(c)
		if c.Revision != 1 || c.NextSequence != 2 || again.NextSequence != 2 {
			t.Fatal("initial event or confirmation replay changed identity")
		}
		c = establish(c)
		e := terminal(c)
		mustExec(t, admin, `CREATE FUNCTION agent_control.fixture_fail_terminal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture before result commit'; END $$;
			CREATE TRIGGER fixture_fail_terminal BEFORE INSERT ON agent_control.operation_events FOR EACH ROW EXECUTE FUNCTION agent_control.fixture_fail_terminal()`)
		if err := s.AcceptLocalTerminal(ctx, e); err == nil {
			t.Fatal("terminal failure was ignored")
		}
		current, _ = s.ReadLocalCheck(ctx, c.ID)
		if current.Resolved || current.TerminalDigest != "" || current.NextSequence != 2 {
			t.Fatal("partial terminal commit")
		}
		mustExec(t, admin, `DROP TRIGGER fixture_fail_terminal ON agent_control.operation_events;DROP FUNCTION agent_control.fixture_fail_terminal()`)
		if err := s.AcceptLocalTerminal(ctx, e); err != nil {
			t.Fatal(err)
		}
		current, _ = s.ReadLocalCheck(ctx, c.ID)
		if !current.Resolved || current.Status != "succeeded" || current.NextSequence != 3 {
			t.Fatal("terminal transaction incomplete")
		}
		if err := s.AcceptLocalTerminal(ctx, e); err != nil {
			t.Fatal(err)
		}
		again, _ = s.ReadLocalCheck(ctx, c.ID)
		if again.NextSequence != current.NextSequence {
			t.Fatal("duplicate terminal event")
		}
		e.Result = json.RawMessage(`{"changed":true}`)
		wantError(t, s.AcceptLocalTerminal(ctx, e), ErrConflict)
	})

	t.Run("wrong binding and stale generation", func(t *testing.T) {
		c := establish(confirm(prepare("fixture-stale-result")))
		e := terminal(c)
		wrong := e
		wrong.RunID = "different-run"
		wantError(t, s.AcceptLocalTerminal(ctx, wrong), ErrRevision)
		wrong = e
		wrong.Input.RecoveryGeneration = "2"
		wantError(t, s.AcceptLocalTerminal(ctx, wrong), ErrRevision)
		mustExec(t, admin, `UPDATE agent_control.operations SET recovery_generation=recovery_generation+1 WHERE operation_id=$1`, c.ID)
		wantError(t, s.AcceptLocalTerminal(ctx, e), ErrRevision)
		current, _ := s.ReadLocalCheck(ctx, c.ID)
		if current.Resolved {
			t.Fatal("stale result released capacity")
		}
		// Restore only this synthetic test fence; no runtime recovery endpoint exists.
		mustExec(t, admin, `UPDATE agent_control.operations SET recovery_generation=1 WHERE operation_id=$1`, c.ID)
		e.Result = json.RawMessage(`{"schemaVersion":1}`)
		if err := s.AcceptLocalTerminal(ctx, e); err != nil {
			t.Fatal(err)
		}
		current, _ = s.ReadLocalCheck(ctx, c.ID)
		if current.Status != "failed" || current.CleanupState != "complete" {
			t.Fatal("invalid completed result did not fail locally")
		}
	})
	t.Run("lost PostgreSQL commit reply", func(t *testing.T) {
		c := establish(confirm(prepare("fixture-lost-commit-reply")))
		e := terminal(c)
		var inject, dropped atomic.Bool
		config := s.pool.Config()
		config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &dropCommitReply{Conn: conn, inject: &inject, dropped: &dropped}, nil
		}
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		uncertain := &Store{pool: pool, idSchema: s.idSchema, eventSchema: s.eventSchema}
		inject.Store(true)
		if err := uncertain.AcceptLocalTerminal(ctx, e); err == nil || !dropped.Load() {
			t.Fatal("commit reply was not lost at the actual PostgreSQL boundary", err)
		}
		current, err := s.ReadLocalCheck(ctx, c.ID)
		if err != nil || !current.Resolved || current.Status != "succeeded" {
			t.Fatal("server did not commit terminal evidence before connection loss", err)
		}
		if err := s.AcceptLocalTerminal(ctx, e); err != nil {
			t.Fatal("original terminal evidence could not reconcile unknown commit", err)
		}
		replayed, err := s.ReadLocalCheck(ctx, c.ID)
		if err != nil || replayed.NextSequence != current.NextSequence || replayed.TerminalDigest != current.TerminalDigest {
			t.Fatal("unknown commit reconciliation duplicated the result/event", err)
		}
	})

	t.Run("late completion cannot override cancel", func(t *testing.T) {
		c := establish(confirm(prepare("fixture-cancel-late")))
		e := terminal(c)
		c, _, err := s.CancelLocalCheck(ctx, scope, c.ID, "fixture-first-cancel", "", uint64(c.Revision), until)
		if err != nil {
			t.Fatal(err)
		}
		if c.Resolved || !c.CancelRequested {
			t.Fatal("cancel guessed terminality")
		}
		_, coalesced, err := s.CancelLocalCheck(ctx, scope, c.ID, "fixture-second-cancel", "", 1, until)
		if err != nil || !coalesced {
			t.Fatal("cancel did not coalesce before stale revision", err)
		}
		if err := s.AcceptLocalTerminal(ctx, e); err != nil {
			t.Fatal(err)
		}
		c, _ = s.ReadLocalCheck(ctx, c.ID)
		if c.Status != "canceled" || !c.Resolved {
			t.Fatal("late completion defeated cancel fence")
		}
		var present bool
		if err := s.pool.QueryRow(ctx, `SELECT result_content_digest IS NOT NULL FROM agent_control.local_checks WHERE operation_id=$1`, c.ID).Scan(&present); err != nil || present {
			t.Fatal("accepted canceled result", err)
		}
	})

	for index, kind := range []string{"failed", "timed_out", "canceled", "terminated"} {
		t.Run("terminal "+kind, func(t *testing.T) {
			c := establish(confirm(prepare("fixture-terminal-" + strconv.Itoa(index))))
			e := terminal(c)
			e.Type = kind
			e.Result = nil
			if err := s.AcceptLocalTerminal(ctx, e); err != nil {
				t.Fatal(err)
			}
			if err := s.AcceptLocalTerminal(ctx, e); err != nil {
				t.Fatal("non-success replay", err)
			}
			c, _ = s.ReadLocalCheck(ctx, c.ID)
			expected := map[string]string{"failed": "failed", "timed_out": "expired", "canceled": "canceled", "terminated": "failed"}[kind]
			if c.Status != expected || !c.Resolved || c.TerminalType != kind {
				t.Fatal("incorrect terminal mapping")
			}
		})
	}
	for _, attempted := range []bool{false, true} {
		t.Run("original expired window attempted="+strconv.FormatBool(attempted), func(t *testing.T) {
			// Synthetic SQL boundary fixture fixes acceptedAt in the past; the
			// runtime exposes no clock override or deadline-editing capability.
			c := LocalCheck{Scope: scope, ID: "op-expired-" + rand.Text(), CommandID: "fixture-expired-" + rand.Text(), AcceptedAt: time.Now().UTC().Add(-16 * time.Minute), Namespace: "fixture-sql-only"}
			c.QueueExpiresAt = c.AcceptedAt.Add(15 * time.Minute)
			c.WorkflowID = contracts.LocalCheckWorkflowIDPrefix + c.ID
			semantic, _ := canonical(map[string]any{"schemaVersion": 1, "kind": "local-check", "fixtureId": "plain-v1"})
			c.RequestDigest = hash(semantic)
			fixture, _ := contracts.Fixture("plain-v1")
			c.Input = contracts.LocalCheckInputV1{SchemaVersion: 1, OperationID: c.ID, RequestDigest: c.RequestDigest, FixtureID: fixture.FixtureID, FixtureText: fixture.FixtureText, ProfileRef: contracts.LocalCheckProfileRef, ProfileDigest: contracts.LocalCheckProfileDigest, ExecutionGeneration: "1", RecoveryGeneration: "1"}
			raw, _ := canonical(c.Input)
			body, _ := c.IntakeBody()
			mustExec(t, admin, `INSERT INTO agent_control.operations(operation_id,tenant_id,actor_id,kind,intake_source,command_id,request_digest,funding_state,public_status,business_stage,control_state,cleanup_state,accepted_at,queue_expires_at)
				VALUES($1,$2,$3,'local-check','api',$4,$5,'not_applicable','pending','queued','running','not_required',$6,$7)`, c.ID, scope.TenantID, scope.ActorID, c.CommandID, c.RequestDigest, c.AcceptedAt, c.QueueExpiresAt)
			mustExec(t, admin, `INSERT INTO agent_control.local_checks(operation_id,fixture_id,profile_ref,profile_digest,input_digest,workflow_input,workflow_id,temporal_namespace,admission_execution_generation,admission_recovery_generation,start_state,start_attempted_at,start_window_expires_at)
				VALUES($1,'plain-v1','local-check-v1',$2,$3,$4,$5,'fixture-sql-only',1,1,CASE WHEN $6 THEN 'start_attempted' ELSE 'start_unattempted' END,CASE WHEN $6 THEN $7::timestamptz ELSE NULL END,$8)`, c.ID, c.Input.ProfileDigest, hash(raw), raw, c.WorkflowID, attempted, c.AcceptedAt, c.QueueExpiresAt)
			key := "obligations/workflow02/" + c.AcceptedAt.Format("2006/01/02/15") + "/" + scope.TenantID + "/intake/" + c.ID
			mustExec(t, admin, `INSERT INTO agent_control.obligations(obligation_id,class,identity,tenant_id,operation_id,state,object_key,body_digest,object_version,recorded_at)
				VALUES($1,'intake',$2,$3,$2,'obligation_recorded',$4,$5,$5,$6)`, "intake-"+c.ID, c.ID, scope.TenantID, key, hash(body), c.AcceptedAt)
			c, err := s.ReadLocalCheck(ctx, c.ID)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.pool.Exec(ctx, `UPDATE agent_control.operations SET queue_expires_at=clock_timestamp()+interval '15 minutes' WHERE operation_id=$1`, c.ID)
			wantSQLState(t, err, "23000")
			current, send, err := s.MarkLocalStart(ctx, c, until)
			if err != nil || send {
				t.Fatal("expired start was sent", err)
			}
			if attempted {
				if current.Resolved || current.Status != "blocked" || current.StartState != "start_attempted" {
					t.Fatal("uncertain expired start became absence")
				}
				_, err := s.PrepareLocalCheck(ctx, scope, "fixture-expired-capacity", "plain-v1", "fixture-sql-only", "workflow02", until)
				wantError(t, err, ErrLocalCapacity)
				// Later verified original evidence can still settle an expired
				// attempted start; the window forbids new starts, not observation.
				if err := s.EstablishLocalRun(ctx, current, "fixture-late-original-run", current.Input); err != nil {
					t.Fatal(err)
				}
				current, _ = s.ReadLocalCheck(ctx, c.ID)
				e := terminal(current)
				e.Type = "timed_out"
				e.Result = nil
				if err := s.AcceptLocalTerminal(ctx, e); err != nil {
					t.Fatal(err)
				}
			} else if !current.Resolved || current.Status != "expired" || current.RunID != "" {
				t.Fatal("never-attempted expiry fabricated execution")
			}
		})
	}
	if _, err := s.PendingLocalCheck(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatal("test left an unresolved fixture", err)
	}
}

// dropCommitReply is a test-only connection boundary for pgx's simple-protocol
// COMMIT. It consumes the real CommandComplete/ReadyForQuery before dropping
// the connection, so PostgreSQL committed but the storage caller cannot know.
type dropCommitReply struct {
	net.Conn
	inject, dropped *atomic.Bool
	armed           bool
}

func (c *dropCommitReply) Write(p []byte) (int, error) {
	if len(p) >= 6 && p[0] == 'Q' && int(binary.BigEndian.Uint32(p[1:5])) == len(p)-1 && strings.EqualFold(string(p[5:len(p)-1]), "commit") && c.inject.CompareAndSwap(true, false) {
		c.armed = true
	}
	return c.Conn.Write(p)
}

func (c *dropCommitReply) Read(p []byte) (int, error) {
	if !c.armed {
		return c.Conn.Read(p)
	}
	var response bytes.Buffer
	committed := false
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(c.Conn, header); err != nil {
			return 0, err
		}
		length := int(binary.BigEndian.Uint32(header[1:]))
		if length < 4 || length > 65536 {
			return 0, errors.New("unexpected test COMMIT frame")
		}
		body := make([]byte, length-4)
		if _, err := io.ReadFull(c.Conn, body); err != nil {
			return 0, err
		}
		response.Write(header)
		response.Write(body)
		if header[0] == 'C' && string(body) == "COMMIT\x00" {
			committed = true
		}
		if header[0] == 'Z' {
			break
		}
	}
	c.armed = false
	if committed {
		c.dropped.Store(true)
		_ = c.Conn.Close()
		return 0, io.ErrUnexpectedEOF
	}
	if response.Len() > len(p) {
		return 0, errors.New("unexpected test COMMIT response size")
	}
	return copy(p, response.Bytes()), nil
}
