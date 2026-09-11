package definition

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
)

func fixture(t *testing.T, name string) map[string]any {
	t.Helper()
	data, err := contracts.Retained.ReadFile("retained/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

func step(document map[string]any, id string) map[string]any {
	for _, item := range array(document["steps"]) {
		if object(item)["id"] == id {
			return object(item)
		}
	}
	panic("test step not found")
}

func TestRetainedGraphs(t *testing.T) {
	for _, name := range []string{"component-generation.example.json", "component-release.example.json"} {
		t.Run(name, func(t *testing.T) {
			issues := validateGraph(fixture(t, name), fixture(t, "registry.json"), 1)
			if len(issues) != 0 {
				t.Fatalf("retained graph rejected: %+v", issues)
			}
		})
	}
}

func TestRetainedGraphMutations(t *testing.T) {
	cases := []struct {
		name, family, code string
		change             func(map[string]any)
	}{
		{"duplicate step", "generation", "STEP_ID_DUPLICATE", func(d map[string]any) { step(d, "code")["id"] = "plan" }},
		{"unknown entry", "generation", "ENTRY_UNKNOWN", func(d map[string]any) { d["entry"] = "absent" }},
		{"exact version", "generation", "ACTION_UNREGISTERED", func(d map[string]any) { object(step(d, "code")["action"])["version"] = "2.0.0" }},
		{"family", "generation", "FAMILY_NOT_ALLOWED", func(d map[string]any) { d["family"] = "image" }},
		{"protected initial result", "release", "PROTECTED_GATE_MISSING", func(d map[string]any) { object(d["inputSlots"])["approval"] = "approval" }},
		{"missing validation", "generation", "PROTECTED_GATE_MISSING", func(d map[string]any) {
			d["steps"] = append(array(d["steps"])[:2], array(d["steps"])[3:]...)
		}},
		{"unreachable step", "generation", "STEP_UNREACHABLE", func(d map[string]any) {
			object(step(d, "validate")["on"])["repairable"] = map[string]any{"terminal": "needs_changes"}
		}},
		{"missing edge", "generation", "EDGE_COVERAGE", func(d map[string]any) { delete(object(step(d, "validate")["on"]), "invalid") }},
		{"extra edge", "generation", "EDGE_UNKNOWN_OUTCOME", func(d map[string]any) {
			object(step(d, "validate")["on"])["invented"] = map[string]any{"terminal": "failed"}
		}},
		{"control outcome edge", "release", "EDGE_PENDING_OR_CONTROL", func(d map[string]any) {
			object(step(d, "await-publication")["on"])["partial"] = map[string]any{"step": "activate"}
		}},
		{"pending outcome edge", "release", "EDGE_PENDING_OR_CONTROL", func(d map[string]any) {
			object(step(d, "await-review")["on"])["pending"] = map[string]any{"step": "publish"}
		}},
		{"unknown port", "generation", "PORT_UNKNOWN", func(d map[string]any) { object(step(d, "code")["outputs"])["invented"] = "source" }},
		{"wrong port kind", "generation", "PORT_KIND_MISMATCH", func(d map[string]any) { object(step(d, "code")["inputs"])["plan"] = "request" }},
		{"missing required port", "generation", "INPUT_UNAVAILABLE", func(d map[string]any) { delete(object(step(d, "code")["inputs"]), "plan") }},
		{"unavailable predecessor input", "generation", "INPUT_UNAVAILABLE", func(d map[string]any) {
			object(step(d, "plan")["on"])["failed"] = map[string]any{"step": "code"}
		}},
		{"failed review cannot publish", "release", "INPUT_UNAVAILABLE", func(d map[string]any) {
			object(step(d, "await-review")["on"])["rejected"] = map[string]any{"step": "publish"}
		}},
		{"repair input cannot bootstrap itself", "generation", "INPUT_UNAVAILABLE", func(d map[string]any) { d["entry"] = "repair" }},
		{"unknown target", "generation", "STEP_TARGET_UNKNOWN", func(d map[string]any) {
			object(step(d, "code")["on"])["ready"] = map[string]any{"step": "absent"}
		}},
		{"unknown terminal", "generation", "TERMINAL_UNKNOWN", func(d map[string]any) {
			object(step(d, "code")["on"])["failed"] = map[string]any{"terminal": "invented"}
		}},
		{"failure cannot become success", "generation", "PROTECTED_GATE_MISSING", func(d map[string]any) {
			object(step(d, "register")["on"])["conflict"] = map[string]any{"terminal": "candidate_ready"}
		}},
		{"unbounded repair", "generation", "REPAIR_CYCLE_INVALID", func(d map[string]any) { delete(step(d, "repair"), "visitLimit") }},
		{"repair exceeds policy", "generation", "REPAIR_CYCLE_INVALID", func(d map[string]any) { object(step(d, "repair")["visitLimit"])["max"] = float64(2) }},
		{"repair exhaustion cannot succeed", "generation", "REPAIR_CYCLE_INVALID", func(d map[string]any) {
			object(step(d, "repair")["visitLimit"])["onExhausted"] = map[string]any{"terminal": "candidate_ready"}
		}},
		{"other cycle", "generation", "REPAIR_CYCLE_INVALID", func(d map[string]any) {
			object(step(d, "code")["on"])["ready"] = map[string]any{"step": "plan"}
		}},
		{"wait mutation", "generation", "WAIT_NOT_READONLY", func(d map[string]any) {
			s := step(d, "code")
			s["kind"], s["wait"] = "wait", map[string]any{"pendingOutcome": "ready", "pollPolicyRef": "review-poll-v1"}
		}},
		{"validation bypass", "generation", "PROTECTED_GATE_MISSING", func(d map[string]any) {
			object(step(d, "code")["on"])["ready"] = map[string]any{"step": "register"}
		}},
		{"invalid report cannot register", "generation", "PROTECTED_GATE_MISSING", func(d map[string]any) {
			object(step(d, "validate")["on"])["invalid"] = map[string]any{"step": "register"}
		}},
		{"repair result requires revalidation", "generation", "PROTECTED_GATE_MISSING", func(d map[string]any) {
			object(step(d, "repair")["on"])["ready"] = map[string]any{"step": "register"}
		}},
		{"longer repair cycle", "generation", "REPAIR_CYCLE_INVALID", func(d map[string]any) {
			object(step(d, "validate")["on"])["invalid"] = map[string]any{"step": "code"}
			object(step(d, "code")["on"])["ready"] = map[string]any{"step": "repair"}
		}},
		{"invalid outcome cannot enter repair", "generation", "REPAIR_CYCLE_INVALID", func(d map[string]any) {
			object(step(d, "validate")["on"])["invalid"] = map[string]any{"step": "repair"}
		}},
		{"pending outcome cannot be renamed", "release", "WAIT_NOT_READONLY", func(d map[string]any) {
			object(step(d, "await-review")["wait"])["pendingOutcome"] = "rejected"
		}},
		{"new source invalidates prior report", "generation", "PROTECTED_GATE_MISSING", func(d map[string]any) {
			data, _ := json.Marshal(step(d, "code"))
			var replacement map[string]any
			_ = json.Unmarshal(data, &replacement)
			replacement["id"] = "rewrite"
			object(replacement["on"])["ready"] = map[string]any{"step": "register"}
			object(step(d, "validate")["on"])["valid"] = map[string]any{"step": "rewrite"}
			d["steps"] = append(array(d["steps"]), replacement)
		}},
		{"report cannot certify another source slot", "generation", "PROTECTED_GATE_MISSING", func(d map[string]any) {
			data, _ := json.Marshal(step(d, "code"))
			var other map[string]any
			_ = json.Unmarshal(data, &other)
			other["id"] = "other-code"
			object(other["outputs"])["source"] = "other-source"
			object(step(d, "code")["on"])["ready"] = map[string]any{"step": "other-code"}
			object(step(d, "register")["inputs"])["source"] = "other-source"
			d["steps"] = append(array(d["steps"]), other)
		}},
	}
	registry := fixture(t, "registry.json")
	validator := newValidator(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			document := fixture(t, "component-"+tc.family+".example.json")
			tc.change(document)
			issues := validateGraph(document, registry, 1)
			found := false
			for _, issue := range issues {
				found = found || issue.code == tc.code
			}
			if !found {
				t.Fatalf("wanted %s, got %+v", tc.code, issues)
			}
			request := positiveRequest(t)
			request.DefinitionJson, _ = json.Marshal(document)
			request.PolicyRefs = []string{stringValue(document["policyRef"])}
			if response, err := validator.Validate(request); err != nil || response.GetValid() || response.DefinitionDigest != nil || len(response.Issues) == 0 {
				t.Fatalf("private validator must reject the graph with bounded issues: %v", err)
			}
			for range 10 {
				if repeated := validateGraph(document, registry, 1); !reflect.DeepEqual(issues, repeated) {
					t.Fatalf("report order changed: %+v versus %+v", issues, repeated)
				}
			}
		})
	}
}

func TestRepairCanBeRemovedOrDisabled(t *testing.T) {
	for _, remove := range []bool{false, true} {
		document := fixture(t, "component-generation.example.json")
		if remove {
			object(step(document, "validate")["on"])["repairable"] = map[string]any{"terminal": "needs_changes"}
			document["steps"] = append(array(document["steps"])[:3], array(document["steps"])[4:]...)
		} else {
			object(step(document, "repair")["visitLimit"])["max"] = float64(0)
		}
		if issues := validateGraph(document, fixture(t, "registry.json"), 1); len(issues) != 0 {
			t.Fatalf("bounded repair option rejected: %+v", issues)
		}
	}
}

func TestGraphIssueLimit(t *testing.T) {
	document := fixture(t, "component-generation.example.json")
	for _, item := range array(document["steps"]) {
		s := object(item)
		for index := range 16 {
			object(s["inputs"])[string(rune('a'+index))] = "absent"
			object(s["outputs"])[string(rune('a'+index))] = "absent"
		}
	}
	if issues := validateGraph(document, fixture(t, "registry.json"), 1); len(issues) != maxIssues {
		t.Fatalf("got %d issues, want %d", len(issues), maxIssues)
	}
}
