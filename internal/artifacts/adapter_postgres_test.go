//go:build integration

package artifacts

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/intake"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	"github.com/jackc/pgx/v5"
)

// S1-T05 (development plan 2026-09-12): the artifact-access and recovery-record
// boundaries against a real PostgreSQL through the parent CONTROL-02 driver
// (`tools/verify/control_storage_proof.py --artifacts`). Controlled inputs only:
// no job, no model, no publication.

func TestMain(m *testing.M) {
	for _, name := range []string{"ANVILKIT_CONTROL_TEST_DATABASE_URL", "ANVILKIT_CONTROL_TEST_ADMIN_DATABASE_URL"} {
		if os.Getenv(name) == "" {
			fmt.Println("UNEXECUTED: integration tests require the parent CONTROL-02 driver")
			os.Exit(2)
		}
	}
	os.Exit(m.Run())
}

type fixture struct {
	store        *storage.Store
	admin        *pgx.Conn
	objects      *Store
	inventory    *intake.Filesystem
	adapter      *Adapter
	binding      storage.TransferBinding
	deadline     time.Time
	inventoryDir string
	environment  string
}

func connect(t *testing.T, role string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), os.Getenv("ANVILKIT_CONTROL_TEST_"+role+"DATABASE_URL"))
	if err != nil {
		t.Fatal("test connection failed")
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func exec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func canonicalDigest(value any) string {
	raw, _ := json.Marshal(value)
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
}

// seed creates an activation, a generation operation, one current Attempt and
// one current physical instance with controlled values, exactly the rank-2 and
// rank-3 facts a transfer is bound to.
func seed(t *testing.T, deadline time.Duration) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store, err := storage.Open(ctx, os.Getenv("ANVILKIT_CONTROL_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	definition := []byte(`{"fixture":"S1-T05 synthetic storage only","name":"` + rand.Text() + `"}`)
	if err := store.PutImmutable(t.Context(), storage.ImmutableRecord{Digest: digestOf(definition), Kind: "definition", CanonicalBytes: definition}); err != nil {
		t.Fatal(err)
	}
	activation, err := store.CompareAndSwapActivation(t.Context(), storage.ActivationCommand{Family: "component", DefinitionID: "fixture-" + rand.Text(), CommandID: "fixture-initial", DefinitionDigest: digestOf(definition), RuntimeProfileRef: "fixture-unqualified", ValidatorReportRef: "fixture-storage-proof", ActivatedBy: "fixture-developer"})
	if err != nil {
		t.Fatal(err)
	}
	scope := storage.Scope{TenantID: "fixture-tenant-" + rand.Text(), ActorID: "fixture-actor"}
	op, err := store.PersistOperation(t.Context(), storage.OperationCommand{Scope: scope, Kind: "generation", IntakeSource: "api", CommandID: "fixture-command", ActivationID: activation.ID, Request: json.RawMessage(`{"fixture":"S1-T05","paid":false}`)})
	if err != nil {
		t.Fatal(err)
	}
	control := connect(t, "")
	f := &fixture{store: store, admin: connect(t, "ADMIN_"), environment: "s1t05"}
	f.binding = storage.TransferBinding{Scope: scope, OperationID: op.ID, AttemptID: "attempt-" + rand.Text(), InstanceID: "instance-" + rand.Text(), ExecutionGeneration: 1, RecoveryGeneration: 1}
	f.deadline = time.Now().Add(deadline)
	exec(t, control, `INSERT INTO agent_control.attempts(attempt_id,operation_id,step_execution_id,profile_ref,execution_generation,deadline,state,command_id) VALUES($1,$2,'step-code-1','fixture-profile',1,$3,'running','attempt-command')`, f.binding.AttemptID, op.ID, f.deadline)
	exec(t, control, `INSERT INTO agent_control.physical_instances(instance_id,attempt_id,operation_id,launch_key,job_uid,pod_uid,image_digests,execution_generation,registration_evidence_ref,state) VALUES($1,$2,$3,'launch-1','job-'||$1,'pod-'||$1,'{"job":"sha256:fixture"}',1,'evidence-fixture','running')`, f.binding.InstanceID, f.binding.AttemptID, op.ID)
	f.inventoryDir = t.TempDir()
	f.inventory, err = intake.Open(f.inventoryDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.inventory.Close() })
	f.objects, err = Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.objects.Close() })
	f.adapter, err = NewAdapter(store, f.objects, f.inventory, f.environment)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) request(command, operation, kind, refID string) IssueRequest {
	return IssueRequest{TransferBinding: f.binding, CommandID: command, Operation: operation, Kind: kind, RefID: refID, Purpose: "result-output"}
}

func (f *fixture) effect(t *testing.T, id string) (dispatch, obligation, obligationRow, version string) {
	t.Helper()
	var v *string
	err := f.admin.QueryRow(t.Context(), `SELECT e.dispatch_state,e.obligation_state,b.state,coalesce(b.object_version,'') FROM agent_control.effects e JOIN agent_control.obligations b ON b.class='business-write' AND b.identity=e.effect_id WHERE e.effect_id=$1`, id).Scan(&dispatch, &obligation, &obligationRow, &v)
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		version = *v
	}
	return
}

// recordingInventory wraps the real inventory to inject persistence outcomes
// and to observe that no transaction of the Control login is open while the
// object is written. "rotate-recovery" persists normally but rotates the
// operation's recovery generation while the object is being written, which is
// the window the post-persistence recheck exists for.
type recordingInventory struct {
	t           *testing.T
	real        *intake.Filesystem
	admin       *pgx.Conn
	operationID string
	mode        string // "ok" | "fail" | "unknown" | "list-fail" | "rotate-recovery"
	calls       int
}

func (r *recordingInventory) Persist(key string, body []byte) (string, error) {
	r.calls++
	var open int
	if err := r.admin.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity WHERE usename LIKE 'fixture_control%' AND xact_start IS NOT NULL`).Scan(&open); err != nil {
		r.t.Fatal(err)
	}
	if open != 0 {
		r.t.Fatalf("inventory persistence ran while %d Control transaction(s) were open", open)
	}
	switch r.mode {
	case "fail":
		return "", errors.New("injected: inventory unavailable")
	case "unknown":
		if _, err := r.real.Persist(key, body); err != nil {
			return "", err
		}
		return "", errors.New("injected: outcome unknown after the write")
	case "rotate-recovery":
		if _, err := r.admin.Exec(context.Background(), `UPDATE agent_control.operations SET recovery_generation=recovery_generation+1 WHERE operation_id=$1`, r.operationID); err != nil {
			r.t.Fatal(err)
		}
	}
	return r.real.Persist(key, body)
}

func (r *recordingInventory) List(prefix string) ([]string, error) {
	if r.mode == "list-fail" {
		return []string{"obligations/x"}, errors.New("injected: listing interrupted")
	}
	return r.real.List(prefix)
}

func TestArtifactBoundaries(t *testing.T) {
	f := seed(t, 10*time.Minute)
	inventory := &recordingInventory{t: t, real: f.inventory, admin: f.admin, operationID: f.binding.OperationID, mode: "ok"}
	adapter, err := NewAdapter(f.store, f.objects, inventory, f.environment)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"fixture":"S1-T05 upload","bytes":"controlled"}`)

	t.Run("a write capability is issued only after the obligation record exists, with explicit scope, ceiling and expiry", func(t *testing.T) {
		before := time.Now()
		c, err := adapter.Issue(t.Context(), f.request("transfer-1", "write", "source", "src-1"))
		if err != nil {
			t.Fatal(err)
		}
		if c.Secret == "" || c.Operation != "write" || c.TransferBinding != f.binding || c.ByteCeiling != ByteCeiling("source") || c.State != "in_progress" {
			t.Fatal("capability shape", c)
		}
		if c.ExpiresAt.After(before.Add(transferLifetime+2*time.Second)) || c.ExpiresAt.After(f.deadline) || !c.ExpiresAt.After(before) {
			t.Fatal("capability expiry must be min(deadline, 300 s)", c.ExpiresAt, f.deadline)
		}
		dispatch, obligation, row, version := f.effect(t, c.EffectID)
		if dispatch != "dispatched" || obligation != "obligation_recorded" || row != "obligation_recorded" || version == "" {
			t.Fatal("effect and obligation after issue", dispatch, obligation, row, version)
		}
		keys, err := f.inventory.List("obligations/" + f.environment)
		if err != nil || len(keys) != 1 {
			t.Fatal("inventory object", keys, err)
		}
		object, err := f.inventory.Read(keys[0])
		if err != nil || version != digestOf(object) {
			t.Fatal("recorded object version must be the inventory object's identity")
		}
		var record map[string]any
		if err := json.Unmarshal(object, &record); err != nil || record["effectId"] != c.EffectID || record["class"] != "business-write" || record["kind"] != "artifact-transfer" {
			t.Fatal("obligation body", string(object))
		}
		if _, has := record["secretDigest"]; has {
			t.Fatal("the inventory record must not carry the capability")
		}
		if inventory.calls != 1 {
			t.Fatal("exactly one persistence per issue")
		}
		// Upload, immutability and finalization.
		stored, err := adapter.Upload(t.Context(), c, body)
		if err != nil || stored.SizeBytes != uint64(len(body)) {
			t.Fatal("upload", err)
		}
		if _, err := adapter.Upload(t.Context(), c, []byte(`{"fixture":"replaced"}`)); !errors.Is(err, ErrConflict) {
			t.Fatalf("a stored object is immutable: %v", err)
		}
		subject := digestOf([]byte("subject-1"))
		claim := Reference{Kind: "source", RefID: "src-1", SubjectDigest: subject, ContentDigest: stored.ContentDigest, SizeBytes: stored.SizeBytes, ObjectVersion: stored.ObjectVersion}
		tampered := []struct {
			name string
			ref  Reference
			want error
		}{
			{"content", func() Reference { r := claim; r.ContentDigest = digestOf([]byte("other")); return r }(), ErrContent},
			{"size", func() Reference { r := claim; r.SizeBytes = 1; return r }(), ErrContent},
			{"version", func() Reference { r := claim; r.ObjectVersion = "v-forged"; return r }(), ErrVersion},
			{"identity", func() Reference { r := claim; r.RefID = "src-other"; return r }(), ErrDenied},
		}
		for _, c2 := range tampered {
			if _, err := adapter.Finalize(t.Context(), c, c2.ref); !errors.Is(err, c2.want) {
				t.Fatalf("tampered %s: wanted %v, got %v", c2.name, c2.want, err)
			}
			if dispatch, _, _, _ := f.effect(t, c.EffectID); dispatch != "dispatched" {
				t.Fatalf("a rejected finalization must not change the transfer (%s)", dispatch)
			}
		}
		wrongTenant := c
		wrongTenant.TenantID = "fixture-tenant-other"
		if _, err := adapter.Finalize(t.Context(), wrongTenant, claim); !errors.Is(err, ErrDenied) {
			t.Fatalf("wrong tenant: %v", err)
		}
		wrongSecret := c
		wrongSecret.Secret = rand.Text()
		if _, err := adapter.Upload(t.Context(), wrongSecret, body); !errors.Is(err, ErrDenied) {
			t.Fatalf("unauthorized capability: %v", err)
		}
		ref, err := adapter.Finalize(t.Context(), c, claim)
		if err != nil || ref != claim {
			t.Fatal("finalize", err, ref)
		}
		if again, err := adapter.Finalize(t.Context(), c, claim); err != nil || again != ref {
			t.Fatal("finalization replay", err)
		}
		if _, err := adapter.Finalize(t.Context(), c, func() Reference { r := claim; r.SubjectDigest = digestOf([]byte("other subject")); return r }()); !errors.Is(err, storage.ErrConflict) {
			t.Fatalf("a different receipt after acceptance: %v", err)
		}
		var receipt json.RawMessage
		if err := f.admin.QueryRow(t.Context(), `SELECT receipt_ref FROM agent_control.effects WHERE effect_id=$1 AND dispatch_state='confirmed'`, c.EffectID).Scan(&receipt); err != nil {
			t.Fatal("accepted receipt", err)
		}
		var accepted Reference
		if err := json.Unmarshal(receipt, &struct {
			Kind, RefID, SubjectDigest, ContentDigest, ObjectVersion *string
			SizeBytes                                                *string
		}{&accepted.Kind, &accepted.RefID, &accepted.SubjectDigest, &accepted.ContentDigest, &accepted.ObjectVersion, new(string)}); err != nil {
			t.Fatal(err)
		}
		if accepted.ContentDigest != stored.ContentDigest || accepted.ObjectVersion != stored.ObjectVersion {
			t.Fatal("receipt identity", string(receipt))
		}
		// The capability is spent: no further upload through it.
		if _, err := adapter.Upload(t.Context(), c, body); !errors.Is(err, storage.ErrTransferState) {
			t.Fatalf("finalized transfer must not admit more bytes: %v", err)
		}
		// A read capability delivers only a reference that verifies.
		r, err := adapter.Issue(t.Context(), f.request("transfer-1-read", "read", "source", "src-1"))
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := adapter.Download(t.Context(), r, ref)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatal("download", err)
		}
		if got, _, err := adapter.Download(t.Context(), r, func() Reference { r := ref; r.ContentDigest = digestOf([]byte("x")); return r }()); err == nil || got != nil {
			t.Fatal("a tampered reference returns no bytes")
		}
		if _, _, err := adapter.Download(t.Context(), r, ref); err != nil {
			t.Fatal(err)
		}
		if _, err := adapter.Upload(t.Context(), r, body); !errors.Is(err, ErrDenied) {
			t.Fatalf("a read capability never writes: %v", err)
		}
		if _, err := adapter.Issue(t.Context(), f.request("transfer-1-read-missing", "read", "bundle", "nothing")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("a read capability names existing bytes: %v", err)
		}
	})

	t.Run("failed or unknown obligation persistence hands out no capability", func(t *testing.T) {
		inventory.mode = "fail"
		if _, err := adapter.Issue(t.Context(), f.request("transfer-2", "write", "evidence", "ev-1")); err == nil {
			t.Fatal("a failed persistence must not issue")
		}
		var id string
		if err := f.admin.QueryRow(t.Context(), `SELECT effect_id FROM agent_control.effects WHERE operation_id=$1 AND command_id='transfer-2'`, f.binding.OperationID).Scan(&id); err != nil {
			t.Fatal("the prepared identity survives for reconciliation", err)
		}
		if dispatch, obligation, row, _ := f.effect(t, id); dispatch != "registered" || obligation != "obligation_pending" || row != "obligation_pending" {
			t.Fatal("prepared state after failure", dispatch, obligation, row)
		}
		inventory.mode = "unknown"
		if _, err := adapter.Issue(t.Context(), f.request("transfer-2", "write", "evidence", "ev-1")); err == nil {
			t.Fatal("an unknown persistence outcome must not issue")
		}
		if dispatch, _, row, _ := f.effect(t, id); dispatch != "registered" || row != "obligation_pending" {
			t.Fatal("unknown outcome is not written for permission", dispatch, row)
		}
		// The object exists (possibly written, for reconciliation); the same
		// scoped command reconciles it and issues.
		inventory.mode = "ok"
		c, err := adapter.Issue(t.Context(), f.request("transfer-2", "write", "evidence", "ev-1"))
		if err != nil || c.EffectID != id || c.Secret == "" {
			t.Fatal("replay after unknown persistence", err, c.EffectID, id)
		}
		if dispatch, _, row, _ := f.effect(t, id); dispatch != "dispatched" || row != "obligation_recorded" {
			t.Fatal("reconciled state", dispatch, row)
		}
		if _, err := adapter.Issue(t.Context(), IssueRequest{TransferBinding: f.binding, CommandID: "transfer-2", Operation: "write", Kind: "evidence", RefID: "ev-2"}); !errors.Is(err, storage.ErrConflict) {
			t.Fatalf("same command, different request: %v", err)
		}
		replay, err := adapter.Issue(t.Context(), f.request("transfer-2", "write", "evidence", "ev-1"))
		if err != nil || replay.Secret != "" || replay.State != "in_progress" {
			t.Fatal("a dispatched transfer's replay reports state and no secret", err, replay.State)
		}
	})

	t.Run("a finalization after the binding is fenced is retained as evidence and rejected", func(t *testing.T) {
		c, err := adapter.Issue(t.Context(), f.request("transfer-3", "write", "transcript", "tr-1"))
		if err != nil {
			t.Fatal(err)
		}
		stored, err := adapter.Upload(t.Context(), c, body)
		if err != nil {
			t.Fatal(err)
		}
		exec(t, f.admin, `UPDATE agent_control.operations SET execution_generation=2 WHERE operation_id=$1`, f.binding.OperationID)
		t.Cleanup(func() {
			// The subtest's context is done by now; the reset must still run.
			if _, err := f.admin.Exec(context.Background(), `UPDATE agent_control.operations SET execution_generation=1,cancel_requested=false WHERE operation_id=$1`, f.binding.OperationID); err != nil {
				t.Error(err)
			}
		})
		claim := Reference{Kind: "transcript", RefID: "tr-1", SubjectDigest: digestOf([]byte("s")), ContentDigest: stored.ContentDigest, SizeBytes: stored.SizeBytes, ObjectVersion: stored.ObjectVersion}
		if _, err := adapter.Finalize(t.Context(), c, claim); !errors.Is(err, storage.ErrRevision) {
			t.Fatalf("post-fence finalization: %v", err)
		}
		var dispatch, disposition, evidence string
		if err := f.admin.QueryRow(t.Context(), `SELECT dispatch_state,coalesce(disposition,''),coalesce(disposition_evidence_ref,'') FROM agent_control.effects WHERE effect_id=$1`, c.EffectID).Scan(&dispatch, &disposition, &evidence); err != nil {
			t.Fatal(err)
		}
		if dispatch != "superseded" || disposition != "abandon-unresolved" || evidence == "" {
			t.Fatal("retained evidence", dispatch, disposition, evidence)
		}
		if _, err := adapter.Issue(t.Context(), f.request("transfer-4", "write", "transcript", "tr-2")); !errors.Is(err, storage.ErrRevision) {
			t.Fatalf("a superseded binding is issued nothing: %v", err)
		}
		exec(t, f.admin, `UPDATE agent_control.operations SET execution_generation=1 WHERE operation_id=$1`, f.binding.OperationID)
		exec(t, f.admin, `UPDATE agent_control.operations SET cancel_requested=true WHERE operation_id=$1`, f.binding.OperationID)
		if _, err := adapter.Issue(t.Context(), f.request("transfer-5", "write", "transcript", "tr-3")); !errors.Is(err, storage.ErrRevision) {
			t.Fatalf("a fenced operation is issued nothing: %v", err)
		}
	})

	t.Run("an identity from a superseded recovery generation is denied at issue, at confirmation after persistence and at acceptance, while the current identity operates", func(t *testing.T) {
		// Only recoveryGeneration changes in this subtest: the operation is not
		// fenced, the execution generation, Attempt, instance, lease and deadline
		// stay valid throughout.
		t.Cleanup(func() {
			if _, err := f.admin.Exec(context.Background(), `UPDATE agent_control.operations SET recovery_generation=1 WHERE operation_id=$1`, f.binding.OperationID); err != nil {
				t.Error(err)
			}
		})
		// A capability issued under recovery generation 1 with its bytes uploaded.
		stale, err := adapter.Issue(t.Context(), f.request("transfer-9", "write", "evidence", "ev-9"))
		if err != nil {
			t.Fatal(err)
		}
		staleStored, err := adapter.Upload(t.Context(), stale, body)
		if err != nil {
			t.Fatal(err)
		}
		// A transfer prepared under generation 1 whose obligation persistence
		// straddles the rotation: the object is written, the recheck denies the
		// confirmation and no capability exists.
		inventory.mode = "rotate-recovery"
		if _, err := adapter.Issue(t.Context(), f.request("transfer-10", "write", "evidence", "ev-10")); !errors.Is(err, storage.ErrRevision) {
			t.Fatalf("confirmation after the recovery generation rotated during persistence: %v", err)
		}
		inventory.mode = "ok"
		var rotated int64
		if err := f.admin.QueryRow(t.Context(), `SELECT recovery_generation FROM agent_control.operations WHERE operation_id=$1`, f.binding.OperationID).Scan(&rotated); err != nil || rotated != 2 {
			t.Fatal("recovery generation", rotated, err)
		}
		var prepared string
		if err := f.admin.QueryRow(t.Context(), `SELECT effect_id FROM agent_control.effects WHERE operation_id=$1 AND command_id='transfer-10'`, f.binding.OperationID).Scan(&prepared); err != nil {
			t.Fatal(err)
		}
		if dispatch, obligation, row, _ := f.effect(t, prepared); dispatch != "registered" || obligation != "obligation_pending" || row != "obligation_pending" {
			t.Fatal("the prepared transfer stays prepared", dispatch, obligation, row)
		}
		// The old identity obtains no new capability, ...
		if _, err := adapter.Issue(t.Context(), f.request("transfer-11", "write", "evidence", "ev-11")); !errors.Is(err, storage.ErrRevision) {
			t.Fatalf("issue under a superseded recovery generation: %v", err)
		}
		if err := f.admin.QueryRow(t.Context(), `SELECT effect_id FROM agent_control.effects WHERE operation_id=$1 AND command_id='transfer-11'`, f.binding.OperationID).Scan(new(string)); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal("a denied issue registers nothing", err)
		}
		// ... cannot confirm the prepared transfer by replaying its command, ...
		if _, err := adapter.Issue(t.Context(), f.request("transfer-10", "write", "evidence", "ev-10")); !errors.Is(err, storage.ErrRevision) {
			t.Fatalf("replay of the prepared transfer under a superseded recovery generation: %v", err)
		}
		if dispatch, _, row, _ := f.effect(t, prepared); dispatch != "registered" || row != "obligation_pending" {
			t.Fatal("the replay confirmed a prepared transfer", dispatch, row)
		}
		// ... and completes no acceptance: the finalization is retained as
		// evidence and rejected, the bytes are not accepted.
		claim := Reference{Kind: "evidence", RefID: "ev-9", SubjectDigest: digestOf([]byte("s")), ContentDigest: staleStored.ContentDigest, SizeBytes: staleStored.SizeBytes, ObjectVersion: staleStored.ObjectVersion}
		if _, err := adapter.Finalize(t.Context(), stale, claim); !errors.Is(err, storage.ErrRevision) {
			t.Fatalf("acceptance under a superseded recovery generation: %v", err)
		}
		var dispatch, disposition string
		if err := f.admin.QueryRow(t.Context(), `SELECT dispatch_state,coalesce(disposition,'') FROM agent_control.effects WHERE effect_id=$1`, stale.EffectID).Scan(&dispatch, &disposition); err != nil || dispatch != "superseded" || disposition != "abandon-unresolved" {
			t.Fatal("retained evidence", dispatch, disposition, err)
		}
		// The current identity, differing only in the recovery generation,
		// issues, uploads and finalizes normally.
		current := f.binding
		current.RecoveryGeneration = 2
		c, err := adapter.Issue(t.Context(), IssueRequest{TransferBinding: current, CommandID: "transfer-12", Operation: "write", Kind: "evidence", RefID: "ev-12", Purpose: "result-output"})
		if err != nil || c.Secret == "" || c.RecoveryGeneration != 2 {
			t.Fatal("issue under the current recovery generation", err, c.RecoveryGeneration)
		}
		stored, err := adapter.Upload(t.Context(), c, body)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := adapter.Finalize(t.Context(), c, Reference{Kind: "evidence", RefID: "ev-12", SubjectDigest: digestOf([]byte("s")), ContentDigest: stored.ContentDigest, SizeBytes: stored.SizeBytes, ObjectVersion: stored.ObjectVersion})
		if err != nil || ref.ContentDigest != stored.ContentDigest {
			t.Fatal("finalize under the current recovery generation", err)
		}
		if dispatch, _, _, _ := f.effect(t, c.EffectID); dispatch != "confirmed" {
			t.Fatal("current identity's transfer", dispatch)
		}
		// The current capability presented with the old generation is a
		// different binding and is denied.
		forged := c
		forged.RecoveryGeneration = 1
		if _, err := adapter.Upload(t.Context(), forged, body); !errors.Is(err, ErrDenied) {
			t.Fatalf("capability presented under another recovery generation: %v", err)
		}
	})

	t.Run("an expired capability admits nothing", func(t *testing.T) {
		c, err := adapter.Issue(t.Context(), f.request("transfer-6", "write", "plan", "plan-1"))
		if err != nil {
			t.Fatal(err)
		}
		adapter.now = func() time.Time { return c.ExpiresAt.Add(time.Second) }
		defer func() { adapter.now = time.Now }()
		if _, err := adapter.Upload(t.Context(), c, body); !errors.Is(err, ErrExpired) {
			t.Fatalf("expired upload: %v", err)
		}
		if _, err := adapter.Finalize(t.Context(), c, Reference{Kind: "plan", RefID: "plan-1"}); !errors.Is(err, ErrExpired) {
			t.Fatalf("expired finalize: %v", err)
		}
	})

	t.Run("recovery discovery subtracts surviving rows from the inventory and never concludes from a partial listing", func(t *testing.T) {
		from, to := time.Now().Add(-2*time.Hour), time.Now().Add(time.Hour)
		d, err := adapter.Discover(t.Context(), "business-write", from, to)
		if err != nil || d.Enumerated < 4 || len(d.Missing) != 0 {
			t.Fatal("complete inventory with surviving rows", d, err)
		}
		// A cutoff inside an hour: the listing enumerates the whole hour, so the
		// rows prepared at or after the cutoff are enumerated too and must be
		// subtracted, never reported as lost. The cutoff is the latest row's own
		// prepared_at, which an exact `< cutoff` query would exclude.
		var latest time.Time
		if err := f.admin.QueryRow(t.Context(), `SELECT max(prepared_at) FROM agent_control.obligations WHERE class='business-write' AND operation_id=$1`, f.binding.OperationID).Scan(&latest); err != nil {
			t.Fatal(err)
		}
		intraHour, err := adapter.Discover(t.Context(), "business-write", latest.Add(-2*time.Hour), latest)
		if err != nil || intraHour.Enumerated != d.Enumerated || intraHour.Surviving != d.Surviving || len(intraHour.Missing) != 0 {
			t.Fatal("rows after an intra-hour cutoff are not lost", intraHour, err)
		}
		// A database restore that lost the row: the object remains and is discovered.
		var lost string
		if err := f.admin.QueryRow(t.Context(), `SELECT effect_id FROM agent_control.effects WHERE operation_id=$1 AND command_id='transfer-6'`, f.binding.OperationID).Scan(&lost); err != nil {
			t.Fatal(err)
		}
		exec(t, f.admin, `DELETE FROM agent_control.obligations WHERE class='business-write' AND identity=$1`, lost)
		d, err = adapter.Discover(t.Context(), "business-write", from, to)
		if err != nil || !slices.Equal(d.Missing, []string{lost}) {
			t.Fatal("reconciliation set", d, err)
		}
		if d, err := adapter.Discover(t.Context(), "business-write", latest.Add(-2*time.Hour), latest); err != nil || !slices.Equal(d.Missing, []string{lost}) {
			t.Fatal("the lost row is still discovered with an intra-hour cutoff", d, err)
		}
		inventory.mode = "list-fail"
		if _, err := adapter.Discover(t.Context(), "business-write", from, to); !errors.Is(err, ErrIncomplete) {
			t.Fatalf("partial listing: %v", err)
		}
		inventory.mode = "ok"
		if _, err := adapter.Discover(t.Context(), "intake", to, from); !errors.Is(err, storage.ErrInvalid) {
			t.Fatal("window")
		}
	})

	t.Run("an exhausted attempt deadline denies issue and finalization", func(t *testing.T) {
		short := seed(t, 1500*time.Millisecond)
		a, err := NewAdapter(short.store, short.objects, short.inventory, short.environment)
		if err != nil {
			t.Fatal(err)
		}
		c, err := a.Issue(t.Context(), short.request("transfer-7", "write", "bundle", "bundle-1"))
		if err != nil {
			t.Fatal(err)
		}
		if c.ByteCeiling != ByteCeiling("bundle") || !c.ExpiresAt.Equal(short.deadline.Truncate(time.Microsecond)) && c.ExpiresAt.After(short.deadline) {
			t.Fatal("bundle ceiling and deadline-bound expiry", c.ByteCeiling, c.ExpiresAt, short.deadline)
		}
		stored, err := a.Upload(t.Context(), c, body)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		claim := Reference{Kind: "bundle", RefID: "bundle-1", SubjectDigest: digestOf([]byte("s")), ContentDigest: stored.ContentDigest, SizeBytes: stored.SizeBytes, ObjectVersion: stored.ObjectVersion}
		if _, err := a.Finalize(t.Context(), c, claim); err == nil {
			t.Fatal("finalization after the deadline must be rejected")
		}
		if _, err := a.Issue(t.Context(), short.request("transfer-8", "write", "bundle", "bundle-2")); !errors.Is(err, storage.ErrRevision) {
			t.Fatalf("issue after the deadline: %v", err)
		}
	})
}
