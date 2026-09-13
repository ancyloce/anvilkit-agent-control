package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	pb "github.com/ancyloce/anvilkit-agent-control/internal/contracts/definitionvalidationv1"
	rpc "github.com/ancyloce/anvilkit-agent-control/internal/contracts/definitionvalidationv1/definitionvalidationv1connect"
	"github.com/ancyloce/anvilkit-agent-control/internal/definition"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const developmentToken = "control-test-credential-0123456789abcdef"

type logBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (b *logBuffer) Write(data []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(data)
}

func (b *logBuffer) snapshot() []byte {
	b.Lock()
	defer b.Unlock()
	return bytes.Clone(b.Buffer.Bytes())
}

func startServer(t *testing.T) (rpc.DefinitionValidationClient, *logBuffer, *http.Server) {
	t.Helper()
	logs := new(logBuffer)
	server, err := NewLocalServer("127.0.0.1:0", developmentToken, logs, nil, nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: protocols}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Error(err)
			_ = server.Close()
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("server shutdown: %v", err)
		}
	})
	return rpc.NewDefinitionValidationClient(client, "http://"+listener.Addr().String(), connect.WithGRPC()), logs, server
}

type validationCase struct {
	ID      string
	Request struct {
		Definition        json.RawMessage
		DescriptorDigest  string
		RuntimeProfileRef string
		PolicyRefs        []string
	}
	Response json.RawMessage
}

func cases(t *testing.T) []validationCase {
	t.Helper()
	data, err := contracts.Retained.ReadFile("retained/validation-positive-v1.fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct{ Cases []validationCase }
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture.Cases
}

func clientRequest(c validationCase) *connect.Request[pb.ValidateDefinitionRequest] {
	r := connect.NewRequest(&pb.ValidateDefinitionRequest{DefinitionJson: bytes.Clone(c.Request.Definition), DescriptorDigest: proto.String(c.Request.DescriptorDigest), RuntimeProfileRef: proto.String(c.Request.RuntimeProfileRef), PolicyRefs: append([]string(nil), c.Request.PolicyRefs...)})
	r.Header().Set("Authorization", "Bearer "+developmentToken)
	return r
}

func TestGeneratedClientReportsAndConcurrentCalls(t *testing.T) {
	client, logs, _ := startServer(t)
	for _, c := range cases(t) {
		t.Run(c.ID, func(t *testing.T) {
			response, err := client.ValidateDefinition(context.Background(), clientRequest(c))
			if err != nil {
				t.Fatal(err)
			}
			var expected pb.ValidateDefinitionResponse
			if err := protojson.Unmarshal(c.Response, &expected); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(response.Msg, &expected) || response.Header().Get("X-Request-Id") == "" {
				t.Fatalf("incorrect report or missing request ID: %v", response.Msg)
			}
		})
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			response, err := client.ValidateDefinition(context.Background(), clientRequest(cases(t)[0]))
			if err != nil || !response.Msg.GetValid() {
				t.Errorf("concurrent validation: %v", err)
			}
		})
	}
	wg.Wait()
	assertLogs(t, logs, 18)
}

func TestPrivateGRPCRejectionsAndLimits(t *testing.T) {
	client, logs, _ := startServer(t)
	base := cases(t)[0]
	cases := []struct {
		name   string
		change func(*connect.Request[pb.ValidateDefinitionRequest])
		code   connect.Code
	}{
		{"no credential", func(r *connect.Request[pb.ValidateDefinitionRequest]) { r.Header().Del("Authorization") }, connect.CodeUnauthenticated},
		{"wrong credential", func(r *connect.Request[pb.ValidateDefinitionRequest]) {
			r.Header().Set("Authorization", "Bearer forged-identity-do-not-log")
		}, connect.CodeUnauthenticated},
		{"repeated credential", func(r *connect.Request[pb.ValidateDefinitionRequest]) {
			r.Header().Add("Authorization", "Bearer "+developmentToken)
		}, connect.CodeUnauthenticated},
		{"missing presence", func(r *connect.Request[pb.ValidateDefinitionRequest]) { r.Msg.DescriptorDigest = nil }, connect.CodeInvalidArgument},
		{"unknown authority", func(r *connect.Request[pb.ValidateDefinitionRequest]) {
			r.Msg.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 50, protowire.VarintType), 1))
		}, connect.CodeInvalidArgument},
		{"duplicate JSON", func(r *connect.Request[pb.ValidateDefinitionRequest]) {
			r.Msg.DefinitionJson = []byte(`{"schemaVersion":1,"schemaVersion":1}`)
		}, connect.CodeInvalidArgument},
		{"null JSON", func(r *connect.Request[pb.ValidateDefinitionRequest]) { r.Msg.DefinitionJson = []byte(`null`) }, connect.CodeInvalidArgument},
		{"invalid Unicode", func(r *connect.Request[pb.ValidateDefinitionRequest]) {
			r.Msg.DefinitionJson = []byte(`{"id":"\ud800\u0000"}`)
		}, connect.CodeInvalidArgument},
		{"wire size", func(r *connect.Request[pb.ValidateDefinitionRequest]) {
			r.Msg.DefinitionJson = bytes.Repeat([]byte(" "), definition.RequestMaxBytes+1)
		}, connect.CodeResourceExhausted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := clientRequest(base)
			r.Header().Set("X-Actor-Id", "forged-actor-do-not-log")
			tc.change(r)
			response, err := client.ValidateDefinition(context.Background(), r)
			if response != nil || connect.CodeOf(err) != tc.code {
				t.Fatalf("wanted %v, got %v", tc.code, err)
			}
		})
	}
	r := clientRequest(base)
	metadata, _ := json.Marshal(map[string]any{"descriptorDigest": r.Msg.GetDescriptorDigest(), "runtimeProfileRef": r.Msg.GetRuntimeProfileRef(), "policyRefs": r.Msg.PolicyRefs})
	padding := definition.RequestMaxBytes - len(`{"definition":`) - len(metadata) - len(r.Msg.DefinitionJson)
	r.Msg.DefinitionJson = append(r.Msg.DefinitionJson, bytes.Repeat([]byte(" "), padding)...)
	if response, err := client.ValidateDefinition(context.Background(), r); err != nil || !response.Msg.GetValid() {
		t.Fatalf("exact byte boundary rejected: %v", err)
	}
	r.Msg.DefinitionJson = append(r.Msg.DefinitionJson, ' ')
	if _, err := client.ValidateDefinition(context.Background(), r); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("byte boundary plus one accepted: %v", err)
	}
	codes := assertLogs(t, logs, len(cases)+2)
	if expected := map[string]int{"ok": 1, "unauthenticated": 3, "invalid_argument": 6, "resource_exhausted": 1}; !reflect.DeepEqual(codes, expected) {
		t.Fatalf("log outcomes do not match actual calls: %v", codes)
	}
}

func assertLogs(t *testing.T, logs *logBuffer, count int) map[string]int {
	t.Helper()
	data := logs.snapshot()
	for _, forbidden := range []string{developmentToken, "forged-identity", "forged-actor", "definition_json", "component-generation-pilot-v1"} {
		if bytes.Contains(data, []byte(forbidden)) {
			t.Fatalf("sensitive content in log: %s", forbidden)
		}
	}
	compiler := jsonschema.NewCompiler()
	for _, name := range []string{"common-v1.schema.json", "log-record-v1.schema.json"} {
		raw, err := contracts.Retained.ReadFile("retained/" + name)
		if err != nil {
			t.Fatal(err)
		}
		var schema map[string]any
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatal(err)
		}
		if err := compiler.AddResource(schema["$id"].(string), schema); err != nil {
			t.Fatal(err)
		}
	}
	schema, err := compiler.Compile("urn:anvilkit:log-record:v1")
	if err != nil {
		t.Fatal(err)
	}
	decoder, actual := json.NewDecoder(bytes.NewReader(data)), 0
	ids := map[string]bool{}
	codes := map[string]int{}
	for {
		var record map[string]any
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(record); err != nil {
			t.Fatalf("log schema: %v; record: %v", err, record)
		}
		id := record["requestId"].(string)
		if ids[id] {
			t.Fatal("duplicated request summary")
		}
		ids[id] = true
		if record["rpc.code"] != "ok" && record["outcome"] == "ok" {
			t.Fatal("failed call logged as success")
		}
		actual++
		codes[record["rpc.code"].(string)]++
	}
	if actual != count {
		t.Fatalf("got %d call summaries, want %d", actual, count)
	}
	return codes
}

func TestPrivateSemanticReportLimit(t *testing.T) {
	client, logs, _ := startServer(t)
	r := clientRequest(cases(t)[0])
	var document map[string]any
	if err := json.Unmarshal(r.Msg.DefinitionJson, &document); err != nil {
		t.Fatal(err)
	}
	for _, item := range document["steps"].([]any) {
		step := item.(map[string]any)
		for _, side := range []string{"inputs", "outputs"} {
			ports := map[string]any{}
			for index := range 16 {
				ports[string(rune('a'+index))] = "absent"
			}
			step[side] = ports
		}
	}
	r.Msg.DefinitionJson, _ = json.Marshal(document)
	first, err := client.ValidateDefinition(context.Background(), r)
	if err != nil || first.Msg.GetValid() || first.Msg.DefinitionDigest != nil || len(first.Msg.Issues) != 100 {
		t.Fatalf("issue cap failure: %v", err)
	}
	second, err := client.ValidateDefinition(context.Background(), r)
	if err != nil || !proto.Equal(first.Msg, second.Msg) {
		t.Fatalf("nondeterministic issues: %v", err)
	}
	for _, issue := range first.Msg.Issues {
		if issue.Code == nil || issue.Path == nil || issue.Message == nil || len(issue.GetCode()) > 128 || len(issue.GetPath()) > 1024 || len(issue.GetMessage()) > 512 {
			t.Fatal("issue shape or bounds violated")
		}
	}
	assertLogs(t, logs, 2)
}

func TestPrivateMethodAndProtocolAreRestricted(t *testing.T) {
	server, err := NewLocalServer("127.0.0.1:0", developmentToken, io.Discard, nil, nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path, contentType, code string
		major                   int
	}{
		{"/unregistered", "application/grpc", "7", 2},
		{rpc.DefinitionValidationValidateDefinitionProcedure, "application/grpc", "3", 1},
		{rpc.DefinitionValidationValidateDefinitionProcedure, "application/grpc+json", "3", 2},
	} {
		r := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader("{}"))
		r.Header.Set("Authorization", "Bearer "+developmentToken)
		r.Header.Set("Content-Type", tc.contentType)
		r.ProtoMajor = tc.major
		w := httptest.NewRecorder()
		server.Handler.ServeHTTP(w, r)
		if w.Header().Get("Grpc-Status") != tc.code {
			t.Fatalf("unexpected transport rejection: %v", w.Header())
		}
	}
}

func TestLocalServerRejectsUnsafeConfiguration(t *testing.T) {
	for _, address := range []string{":8081", "0.0.0.0:8081", "localhost:8081", "[::]:8081", "127.0.0.1:65536"} {
		if _, err := NewLocalServer(address, developmentToken, io.Discard, nil, nil, nil, ""); err == nil {
			t.Fatalf("accepted %s", address)
		}
	}
	for _, token := range []string{"", "short", strings.Repeat("x", 257), developmentToken + "\n"} {
		if _, err := NewLocalServer("127.0.0.1:0", token, io.Discard, nil, nil, nil, ""); err == nil {
			t.Fatal("accepted invalid credential configuration")
		}
	}
}

func TestCanceledRequestDoesNotValidate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response, err := (definitionHandler{}).ValidateDefinition(ctx, connect.NewRequest(&pb.ValidateDefinitionRequest{}))
	if response != nil || connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("cancellation lost: %v", err)
	}
}
