//go:build integration

package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	pb "github.com/ancyloce/anvilkit-agent-control/internal/contracts/controlv1"
	rpc "github.com/ancyloce/anvilkit-agent-control/internal/contracts/controlv1/controlv1connect"
	"github.com/ancyloce/anvilkit-agent-control/internal/disclosure"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	for _, key := range []string{"ANVILKIT_CONTROL_TEST_DATABASE_URL", "ANVILKIT_CONTROL_TEST_ADMIN_DATABASE_URL", "ANVILKIT_CONTROL_TEST_SERVER_BINARY"} {
		if os.Getenv(key) == "" {
			fmt.Println("UNEXECUTED: use the parent CONTROL-03 verification driver")
			os.Exit(2)
		}
	}
	os.Exit(m.Run())
}

type disclosureFixture struct {
	store                               *storage.Store
	admin                               *pgx.Conn
	profilePath, evidencePath           string
	profile, evidence                   map[string]any
	tokens, actors, tenants, operations [2]string
}

func fixtureFiles(t *testing.T) *disclosureFixture {
	t.Helper()
	f := &disclosureFixture{}
	var err error
	f.store, err = storage.Open(t.Context(), os.Getenv("ANVILKIT_CONTROL_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.store.Close)
	f.admin, err = pgx.Connect(t.Context(), os.Getenv("ANVILKIT_CONTROL_TEST_ADMIN_DATABASE_URL"))
	if err != nil {
		t.Fatal("administrator test connection unavailable")
	}
	t.Cleanup(func() { _ = f.admin.Close(context.Background()) })
	root := t.TempDir()
	f.profilePath = filepath.Join(root, "profile.json")
	f.evidencePath = filepath.Join(root, "evidence.json")
	identities, bindings, tenants := []any{}, []any{}, []any{}
	until := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	for i := range f.tokens {
		f.tokens[i] = "fixture-" + rand.Text() + rand.Text()
		f.actors[i] = "fixture-actor-" + rand.Text()
		f.tenants[i] = "fixture-tenant-" + rand.Text()
		raw := []byte(`{"fixture":"CONTROL-03 storage only"}`)
		digest := fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
		if err := f.store.PutImmutable(t.Context(), storage.ImmutableRecord{Digest: digest, Kind: "definition", CanonicalBytes: raw}); err != nil {
			t.Fatal(err)
		}
		a, err := f.store.CompareAndSwapActivation(t.Context(), storage.ActivationCommand{Family: "component", DefinitionID: "fixture-disclosure-" + rand.Text(), CommandID: "fixture-activation", DefinitionDigest: digest, RuntimeProfileRef: "fixture-unqualified", ValidatorReportRef: "fixture-storage-only", ActivatedBy: "fixture-developer"})
		if err != nil {
			t.Fatal(err)
		}
		op, err := f.store.PersistOperation(t.Context(), storage.OperationCommand{Scope: storage.Scope{TenantID: f.tenants[i], ActorID: f.actors[i]}, Kind: "generation", IntakeSource: "api", CommandID: "fixture-operation", ActivationID: a.ID, Request: []byte(`{"fixture":"CONTROL-03","paid":false}`)})
		if err != nil {
			t.Fatal(err)
		}
		f.operations[i] = op.ID
		identities = append(identities, map[string]any{"token": f.tokens[i], "actorId": f.actors[i], "tenantId": f.tenants[i], "expiresAt": until})
		bindings = append(bindings, map[string]any{"operationId": op.ID, "actorId": f.actors[i], "tenantId": f.tenants[i], "serviceIdentity": disclosure.APIService, "serviceAuthorized": true, "validUntil": until})
		tenants = append(tenants, map[string]any{"tenantId": f.tenants[i], "complete": true, "available": true, "validUntil": until, "memberRoles": map[string]any{f.actors[i]: []any{"fixture-reader"}}})
	}
	f.profile = map[string]any{"schemaVersion": 1, "profile": "control-03-fixture", "sourceCredentialExpiresAt": until, "identities": identities, "rolePermissions": []any{map[string]any{"role": "fixture-reader", "resourceKind": "operation", "action": "operation.read"}}, "operationBindings": bindings}
	f.evidence = map[string]any{"schemaVersion": 1, "tenants": tenants}
	f.save(t)
	return f
}

func writeFixture(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path+".next", raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(path+".next", path); err != nil {
		t.Fatal(err)
	}
}
func (f *disclosureFixture) save(t *testing.T) {
	t.Helper()
	writeFixture(t, f.profilePath, f.profile)
	writeFixture(t, f.evidencePath, f.evidence)
}
func (f *disclosureFixture) expire(t *testing.T) {
	t.Helper()
	_, err := f.admin.Exec(t.Context(), `UPDATE agent_control.authorization_evidence SET observed_at=clock_timestamp()-interval '40 seconds',fresh_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1`, f.tenants[0])
	if err != nil {
		t.Fatal(err)
	}
}
func (f *disclosureFixture) revision(t *testing.T) int64 {
	t.Helper()
	var r int64
	if err := f.admin.QueryRow(t.Context(), `SELECT evidence_revision FROM agent_control.authorization_evidence WHERE tenant_id=$1`, f.tenants[0]).Scan(&r); err != nil {
		t.Fatal(err)
	}
	return r
}
func (f *disclosureFixture) request(i int) *connect.Request[pb.GetDisclosureAuthorizationRequest] {
	r := connect.NewRequest(&pb.GetDisclosureAuthorizationRequest{Context: &pb.AuthenticatedContext{ActorId: proto.String(f.actors[i]), TenantId: proto.String(f.tenants[i]), ServiceIdentity: proto.String(disclosure.APIService), DestinationMethod: proto.String(rpc.ControlServiceGetDisclosureAuthorizationProcedure)}, OperationId: proto.String(f.operations[i])})
	r.Header().Set("Authorization", "Bearer "+f.tokens[i])
	return r
}

func grpcClient(t *testing.T, address string) rpc.ControlServiceClient {
	t.Helper()
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: protocols}
	t.Cleanup(transport.CloseIdleConnections)
	return rpc.NewControlServiceClient(&http.Client{Transport: transport, Timeout: 12 * time.Second}, "http://"+address, connect.WithGRPC())
}
func (f *disclosureFixture) server(t *testing.T) (rpc.ControlServiceClient, *logBuffer) {
	t.Helper()
	store, err := storage.Open(t.Context(), os.Getenv("ANVILKIT_CONTROL_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	service, err := disclosure.NewLocal(store, f.profilePath, f.evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	logs := new(logBuffer)
	server, err := NewLocalServer("127.0.0.1:0", developmentToken, logs, service, nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	})
	return grpcClient(t, listener.Addr().String()), logs
}

func allow(t *testing.T, response *connect.Response[pb.GetDisclosureAuthorizationResponse], err error, operationID string) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	m := response.Msg
	if m.GetDecision() != pb.DisclosureDecision_DISCLOSURE_DECISION_ALLOW || m.GetBoundResourceScope() != operationID || m.FreshUntil == nil || !m.FreshUntil.IsValid() || !m.FreshUntil.AsTime().After(time.Now()) || m.GetEvidenceRevision() < 1 {
		t.Fatalf("invalid grant: %v", m)
	}
	raw, _ := protojson.Marshal(m)
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	if len(fields) != 4 {
		t.Fatal("grant returned fields outside the disclosure contract")
	}
}
func denyDecision(t *testing.T, response *connect.Response[pb.GetDisclosureAuthorizationResponse], err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	m := response.Msg
	if m.GetDecision() != pb.DisclosureDecision_DISCLOSURE_DECISION_DENY || m.BoundResourceScope != nil || m.FreshUntil != nil || m.EvidenceRevision != nil {
		t.Fatal("denial disclosed resource facts")
	}
}

func TestPostgresDisclosure(t *testing.T) {
	t.Run("generated RPC identity and four facts", func(t *testing.T) {
		f := fixtureFiles(t)
		client, logs := f.server(t)
		for i := range f.tokens {
			response, err := client.GetDisclosureAuthorization(t.Context(), f.request(i))
			allow(t, response, err, f.operations[i])
		}
		r := f.request(0)
		r.Msg.OperationId = proto.String(f.operations[1])
		response, err := client.GetDisclosureAuthorization(t.Context(), r)
		denyDecision(t, response, err)
		for _, name := range []string{"actor", "tenant", "service", "method", "role", "unknown context", "unknown message", "credential", "validation credential"} {
			t.Run(name, func(t *testing.T) {
				r := f.request(0)
				want := connect.CodePermissionDenied
				switch name {
				case "actor":
					r.Msg.Context.ActorId = proto.String(f.actors[1])
				case "tenant":
					r.Msg.Context.TenantId = proto.String(f.tenants[1])
				case "service":
					r.Msg.Context.ServiceIdentity = proto.String("anvilkit-agent-workflow")
				case "method":
					r.Msg.Context.DestinationMethod = proto.String("operation.cancel")
				case "role":
					r.Msg.Context.ActorRole = pb.ActorRole(1).Enum()
				case "unknown context":
					r.Msg.Context.ProtoReflect().SetUnknown([]byte{0x78, 1})
					want = connect.CodeInvalidArgument
				case "unknown message":
					r.Msg.ProtoReflect().SetUnknown([]byte{0x78, 1})
					want = connect.CodeInvalidArgument
				case "credential":
					r.Header().Set("Authorization", "Bearer fixture-unmapped-credential-0000000")
					want = connect.CodeUnauthenticated
				case "validation credential":
					r.Header().Set("Authorization", "Bearer "+developmentToken)
				}
				_, err := client.GetDisclosureAuthorization(t.Context(), r)
				if connect.CodeOf(err) != want {
					t.Fatalf("wanted %v got %v", want, err)
				}
			})
		}
		checkDisclosureLogs(t, logs.snapshot(), f, 12)
	})
	t.Run("reviewed action and service bindings", func(t *testing.T) {
		for _, kind := range []string{"wrong action", "unmapped role", "wrong resource kind", "wrong service", "service refused", "binding expired", "credential expired"} {
			t.Run(kind, func(t *testing.T) {
				f := fixtureFiles(t)
				permission := f.profile["rolePermissions"].([]any)[0].(map[string]any)
				binding := f.profile["operationBindings"].([]any)[0].(map[string]any)
				switch kind {
				case "wrong action":
					permission["action"] = "operation.cancel"
				case "unmapped role":
					permission["role"] = "fixture-unmapped"
				case "wrong resource kind":
					permission["resourceKind"] = "draft"
				case "wrong service":
					binding["serviceIdentity"] = "anvilkit-agent-workflow"
				case "service refused":
					binding["serviceAuthorized"] = false
				case "binding expired":
					binding["validUntil"] = time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)
				case "credential expired":
					f.profile["identities"].([]any)[0].(map[string]any)["expiresAt"] = time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)
				}
				f.save(t)
				client, _ := f.server(t)
				response, err := client.GetDisclosureAuthorization(t.Context(), f.request(0))
				if kind == "credential expired" {
					if connect.CodeOf(err) != connect.CodeUnauthenticated {
						t.Fatal(err)
					}
					return
				}
				denyDecision(t, response, err)
			})
		}
	})
	t.Run("cached evidence revocation and unavailable renewal", func(t *testing.T) {
		f := fixtureFiles(t)
		client, logs := f.server(t)
		response, err := client.GetDisclosureAuthorization(t.Context(), f.request(0))
		allow(t, response, err, f.operations[0])
		original := f.revision(t)
		response, err = client.GetDisclosureAuthorization(t.Context(), f.request(0))
		allow(t, response, err, f.operations[0])
		if f.revision(t) != original {
			t.Fatal("fresh cache was refreshed")
		}
		// API renews at freshUntil minus the shared two-second margin. A cache
		// still valid by database time must refresh before returning to API.
		if _, err = f.admin.Exec(t.Context(), `UPDATE agent_control.authorization_evidence SET fresh_until=clock_timestamp()+interval '1 second' WHERE tenant_id=$1`, f.tenants[0]); err != nil {
			t.Fatal(err)
		}
		response, err = client.GetDisclosureAuthorization(t.Context(), f.request(0))
		allow(t, response, err, f.operations[0])
		if f.revision(t) != original+1 || !response.Msg.FreshUntil.AsTime().After(time.Now().Add(2*time.Second)) {
			t.Fatal("API renewal received expiring cache")
		}
		original = f.revision(t)
		tenant := f.evidence["tenants"].([]any)[0].(map[string]any)
		tenant["memberRoles"] = map[string]any{}
		writeFixture(t, f.evidencePath, f.evidence)
		f.expire(t)
		response, err = client.GetDisclosureAuthorization(t.Context(), f.request(0))
		denyDecision(t, response, err)
		if f.revision(t) != original+1 {
			t.Fatal("revocation did not advance evidence")
		}
		tenant["memberRoles"] = map[string]any{f.actors[0]: []any{"fixture-reader"}}
		for _, state := range []string{"incomplete", "unavailable", "invalid", "unknown tenant", "expired validity"} {
			f.expire(t)
			before := f.revision(t)
			tenant["complete"] = true
			tenant["available"] = true
			tenant["tenantId"] = f.tenants[0]
			tenant["validUntil"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
			switch state {
			case "incomplete":
				tenant["complete"] = false
			case "unavailable":
				tenant["available"] = false
			case "unknown tenant":
				tenant["tenantId"] = "fixture-other"
			case "expired validity":
				tenant["validUntil"] = time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)
			}
			writeFixture(t, f.evidencePath, f.evidence)
			if state == "invalid" {
				if err := os.WriteFile(f.evidencePath, []byte(`{"schemaVersion":1,"schemaVersion":1}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := client.GetDisclosureAuthorization(t.Context(), f.request(0))
			if connect.CodeOf(err) != connect.CodeUnavailable {
				t.Fatalf("%s: %v", state, err)
			}
			if f.revision(t) != before {
				t.Fatal("failed source changed cached evidence")
			}
		}
		checkDisclosureLogs(t, logs.snapshot(), f, 9)
	})
	t.Run("two replicas share one refresh", func(t *testing.T) {
		f := fixtureFiles(t)
		first, _ := f.server(t)
		second, _ := f.server(t)
		response, err := first.GetDisclosureAuthorization(t.Context(), f.request(0))
		allow(t, response, err, f.operations[0])
		before := f.revision(t)
		f.expire(t)
		_, err = f.admin.Exec(t.Context(), `UPDATE agent_control.authorization_read_budget SET tokens=40,refilled_at=clock_timestamp()+interval '1 hour'`)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, 12)
		revisions := make([]uint64, 12)
		for i := range errs {
			wg.Go(func() {
				<-start
				client := first
				if i%2 == 1 {
					client = second
				}
				response, err := client.GetDisclosureAuthorization(t.Context(), f.request(0))
				errs[i] = err
				if err == nil {
					revisions[i] = response.Msg.GetEvidenceRevision()
				}
			})
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			if err != nil || revisions[i] != uint64(before+1) {
				t.Fatalf("replica refresh disagrees: revision=%d err=%v", revisions[i], err)
			}
		}
		var consumed bool
		if err = f.admin.QueryRow(t.Context(), `SELECT tokens=39 FROM agent_control.authorization_read_budget`).Scan(&consumed); err != nil || !consumed {
			t.Fatal("concurrent requests read the source more than once", err)
		}
	})
	t.Run("canonical process and absolute source deadline", func(t *testing.T) {
		f := fixtureFiles(t)
		deadline := time.Now().Add(8 * time.Second).UTC()
		f.profile["sourceCredentialExpiresAt"] = deadline.Format(time.RFC3339Nano)
		f.save(t)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		_ = listener.Close()
		logs, stderr := new(logBuffer), new(logBuffer)
		command := exec.Command(os.Getenv("ANVILKIT_CONTROL_TEST_SERVER_BINARY"))
		command.Env = append(os.Environ(), "ANVILKIT_CONTROL_DATABASE_URL="+os.Getenv("ANVILKIT_CONTROL_TEST_DATABASE_URL"), "ANVILKIT_CONTROL_DISCLOSURE_PROFILE="+f.profilePath, "ANVILKIT_CONTROL_DISCLOSURE_EVIDENCE="+f.evidencePath, "ANVILKIT_CONTROL_LISTEN_ADDR="+address, "ANVILKIT_CONTROL_DEVELOPMENT_TOKEN="+developmentToken)
		command.Stdout = logs
		command.Stderr = stderr
		if err = command.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if command.ProcessState == nil {
				_ = command.Process.Kill()
				_ = command.Wait()
			}
		})
		ready := time.Now().Add(4 * time.Second)
		for {
			conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				break
			}
			if time.Now().After(ready) {
				t.Fatal("canonical Control did not start", string(stderr.snapshot()))
			}
			time.Sleep(10 * time.Millisecond)
		}
		client := grpcClient(t, address)
		response, err := client.GetDisclosureAuthorization(t.Context(), f.request(0))
		allow(t, response, err, f.operations[0])
		if response.Msg.FreshUntil.AsTime().After(deadline) {
			t.Fatal("credential deadline was extended")
		}
		_ = command.Process.Signal(syscall.SIGTERM)
		if err = command.Wait(); err != nil {
			t.Fatal("canonical Control did not stop cleanly")
		}
		if len(stderr.snapshot()) != 0 {
			t.Fatal("unexpected canonical process error")
		}
		checkDisclosureLogs(t, logs.snapshot(), f, 1)
	})
}

func checkDisclosureLogs(t *testing.T, data []byte, f *disclosureFixture, want int) {
	t.Helper()
	for _, secret := range append(f.tokens[:], "fixture-reader", "memberRoles", developmentToken) {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatal("private fixture material entered logs")
		}
	}
	compiler := jsonschema.NewCompiler()
	for _, name := range []string{"common-v1.schema.json", "log-record-v1.schema.json"} {
		raw, err := contracts.Retained.ReadFile("retained/" + name)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		_ = json.Unmarshal(raw, &document)
		if err = compiler.AddResource(document["$id"].(string), document); err != nil {
			t.Fatal(err)
		}
	}
	schema, err := compiler.Compile("urn:anvilkit:log-record:v1")
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	seen := map[string]bool{}
	count := 0
	for {
		var record map[string]any
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = schema.Validate(record); err != nil {
			t.Fatal(err)
		}
		id, _ := record["requestId"].(string)
		if seen[id] {
			t.Fatal("duplicate RPC summary")
		}
		seen[id] = true
		count++
		if record["routeTemplate"] != rpc.ControlServiceGetDisclosureAuthorizationProcedure {
			t.Fatal("wrong RPC route in summary")
		}
		if record["caller"] != "unauthenticated" && record["rpc.code"] != "invalid_argument" && record["actorId"] != "fixture-platform-developer" {
			if _, ok := record["operationId"]; !ok {
				t.Fatal("operation summary omitted operationId")
			}
		}
		if code, _ := record["rpc.code"].(string); code != "ok" && record["outcome"] == "ok" {
			t.Fatal("RPC failure logged as success")
		}
	}
	if count != want {
		t.Fatalf("got %d RPC summaries, wanted %d: %s", count, want, strings.TrimSpace(string(data)))
	}
}
