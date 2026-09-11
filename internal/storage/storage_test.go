package storage

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/jackc/pgx/v5/pgconn"
)

func schemaStore(t *testing.T) *Store {
	t.Helper()
	id, event, err := schemas()
	if err != nil {
		t.Fatal(err)
	}
	return &Store{idSchema: id, eventSchema: event}
}

func TestRetainedEventPayloads(t *testing.T) {
	s := schemaStore(t)
	raw, err := contracts.Retained.ReadFile("retained/operation-event-v1.fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct{ Records []map[string]any }
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, event := range fixtures.Records {
		if err = s.eventSchema.Validate(event); err != nil {
			t.Fatalf("%s: %v", event["type"], err)
		}
	}
}

func TestOnlyExplicitDatabaseAbortsAreRetried(t *testing.T) {
	for _, code := range []string{"40001", "40P01", "23505", "23514", "42501", "08006", "57014", "57P01"} {
		want := code == "40001" || code == "40P01"
		if retryable(&pgconn.PgError{Code: code}) != want {
			t.Fatalf("wrong retry policy for %s", code)
		}
	}
	if retryable(nil) || retryable(errors.New("connection lost during commit")) {
		t.Fatal("unknown outcome became retry authority")
	}
}

func TestStrictStoredEventBoundary(t *testing.T) {
	s := schemaStore(t)
	base := StepEvent{Scope: Scope{"fixture-tenant", "fixture-actor"}, OperationID: "fixture-op", TransitionID: "fixture-transition", StepID: "code", ActionID: "component.code", ActionVersion: "1.0.0", Type: "step.started", DefinitionSegment: 1, Visit: 1}
	good := map[string]any{"stepExecutionId": "fixture-step", "definitionSegment": "1", "definitionDigest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "stepId": "code", "visit": "1", "actionId": "component.code", "actionVersion": "1.0.0", "inputRefs": map[string]any{}}
	base.Payload, _ = json.Marshal(good)
	if _, _, err := s.checkStepEvent(base); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{}`, `{"stepExecutionId":null}`, `{"a":1,"a":2}`, `{"a":"\ud800"}`} {
		bad := base
		bad.Payload = []byte(raw)
		if _, _, err := s.checkStepEvent(bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("unsafe event accepted: %v", err)
		}
	}
	bad := base
	bad.Visit = 2
	if _, _, err := s.checkStepEvent(bad); !errors.Is(err, ErrInvalid) {
		t.Fatal("payload/header disagreement accepted")
	}
	if boundedCounters(map[string]any{"sizeBytes": "18446744073709551616"}) {
		t.Fatal("uint64 overflow accepted")
	}
}
