package definition

import (
	"fmt"
	"slices"
	"strings"
)

const maxIssues = 100

// graphIssue is an internal diagnostic, translated to the generated RPC report
// after structural validation and retained-reference checks.
type graphIssue struct {
	code, path, message, stepID string
}

type graphEdge struct {
	from, to int
	outcome  string
}

// validateGraph consumes documents already checked against the parent-owned
// schemas. It neither resolves runtime authority nor produces a digest.
func validateGraph(document, registry map[string]any, maximumRepairVisits int) []graphIssue {
	var issues []graphIssue
	add := func(code, path, message, stepID string) {
		if len(issues) < maxIssues {
			issues = append(issues, graphIssue{code, path, message, stepID})
		}
	}
	steps := array(document["steps"])
	if len(steps) == 0 || len(steps) > 32 {
		add("STEP_COUNT_INVALID", "/steps", "The definition must contain between 1 and 32 steps", "")
		return issues
	}
	descriptors := make(map[string]map[string]any)
	for _, item := range array(registry["descriptors"]) {
		descriptor := object(item)
		descriptors[stringValue(descriptor["id"])+"@"+stringValue(descriptor["version"])] = descriptor
	}
	indices := make(map[string]int)
	actions := make(map[string][]int)
	resolved := make([]map[string]any, len(steps))
	for index, item := range steps {
		step := object(item)
		id := stringValue(step["id"])
		path := fmt.Sprintf("/steps/%d", index)
		if _, exists := indices[id]; exists {
			add("STEP_ID_DUPLICATE", path+"/id", "Step IDs must be unique", id)
		}
		indices[id] = index
		action := object(step["action"])
		actionID := stringValue(action["id"])
		actions[actionID] = append(actions[actionID], index)
		descriptor, exists := descriptors[actionID+"@"+stringValue(action["version"])]
		if !exists {
			add("ACTION_UNREGISTERED", path+"/action", "The exact action and version must be retained", id)
			continue
		}
		resolved[index] = descriptor
		if !contains(descriptor["families"], stringValue(document["family"])) {
			add("FAMILY_NOT_ALLOWED", path+"/action", "The action does not support this family", id)
		}
		for _, output := range sortedKeys(object(descriptor["outputs"])) {
			for _, outcome := range array(object(object(descriptor["outputs"])[output])["availableOn"]) {
				if !contains(descriptor["outcomes"], stringValue(outcome)) {
					add("PORT_UNKNOWN", path+"/outputs", "Output availability must name a registered outcome", id)
				}
			}
		}
	}
	entry, entryExists := indices[stringValue(document["entry"])]
	if !entryExists {
		add("ENTRY_UNKNOWN", "/entry", "The entry must identify a defined step", "")
	}
	if len(issues) != 0 {
		return issues
	}

	// Retained component workflows begin with unprivileged request references.
	// Allowing an approval/report/receipt as an initial slot would bypass its
	// protected producer even when port kinds and graph reachability agree.
	release := len(actions["component.certify-release"]) != 0 || len(actions["component.request-publication"]) != 0 || len(actions["component.request-activation"]) != 0
	allowedInputs := map[string]bool{"generation-request": true, "build-support": true}
	allowedTerminals := []string{"candidate_ready", "needs_changes", "failed"}
	required := []string{"component.validate", "component.register-candidate"}
	if release {
		allowedInputs = map[string]bool{"release-request": true}
		allowedTerminals = []string{"active", "needs_changes", "rejected", "expired", "failed"}
		required = []string{"component.certify-release", "component.submit-review", "component.review-status", "component.request-publication", "component.publication-status", "component.request-activation", "component.activation-status"}
	}
	for _, action := range required {
		if len(actions[action]) != 1 {
			add("PROTECTED_GATE_MISSING", "/steps", "Each required protected action must occur exactly once", "")
		}
	}
	for _, terminal := range array(document["terminals"]) {
		if !slices.Contains(allowedTerminals, stringValue(terminal)) {
			add("TERMINAL_UNKNOWN", "/terminals", "The terminal is not supported by this retained workflow", "")
		}
	}
	slotKinds := make(map[string]string)
	initialSlots := make(map[string]bool)
	for _, slot := range sortedKeys(object(document["inputSlots"])) {
		kind := stringValue(object(document["inputSlots"])[slot])
		slotKinds[slot], initialSlots[slot] = kind, true
		if !allowedInputs[kind] {
			add("PROTECTED_GATE_MISSING", "/inputSlots", "Initial slots cannot supply protected action results", "")
		}
	}
	for index, item := range steps {
		step := object(item)
		for _, port := range sortedKeys(object(step["outputs"])) {
			slot := stringValue(object(step["outputs"])[port])
			output, exists := object(resolved[index]["outputs"])[port]
			if !exists {
				continue
			}
			kind := stringValue(object(output)["kind"])
			if previous, exists := slotKinds[slot]; exists && previous != kind {
				add("PORT_KIND_MISMATCH", fmt.Sprintf("/steps/%d/outputs", index), "All writers of a slot must agree on its reference kind", stringValue(step["id"]))
			}
			slotKinds[slot] = kind
		}
	}
	var edges []graphEdge
	for index, item := range steps {
		step, descriptor := object(item), resolved[index]
		id, path := stringValue(step["id"]), fmt.Sprintf("/steps/%d", index)
		pending := stringValue(object(step["wait"])["pendingOutcome"])
		if step["kind"] == "wait" {
			if !slices.Equal(array(descriptor["effects"]), []any{"read"}) || pending != "pending" || !contains(descriptor["outcomes"], pending) || contains(descriptor["controlOutcomes"], pending) {
				add("WAIT_NOT_READONLY", path+"/wait", "A wait requires a read-only action and an ordinary pending outcome", id)
			}
		}
		for _, side := range []string{"inputs", "outputs"} {
			ports, declared := object(step[side]), object(descriptor[side])
			for _, name := range sortedKeys(ports) {
				port, exists := declared[name]
				if !exists {
					add("PORT_UNKNOWN", path+"/"+side, "The port must exist in the retained descriptor", id)
					continue
				}
				if slotKinds[stringValue(ports[name])] != stringValue(object(port)["kind"]) {
					add("PORT_KIND_MISMATCH", path+"/"+side, "The slot must match the declared reference kind", id)
				}
			}
			if side == "inputs" {
				for _, name := range sortedKeys(declared) {
					if object(declared[name])["required"] == true {
						if _, exists := ports[name]; !exists {
							add("INPUT_UNAVAILABLE", path+"/inputs", "Every required input port must be bound", id)
						}
					}
				}
			}
		}
		on := object(step["on"])
		for _, outcome := range array(descriptor["outcomes"]) {
			name := stringValue(outcome)
			if contains(descriptor["controlOutcomes"], name) || name == pending {
				continue
			}
			if _, exists := on[name]; !exists {
				add("EDGE_COVERAGE", path+"/on", "Every ordinary outcome must have an edge", id)
			}
		}
		checkTarget := func(value any, outcome, targetPath string) {
			target := object(value)
			if next, exists := target["step"]; exists {
				to, known := indices[stringValue(next)]
				if !known {
					add("STEP_TARGET_UNKNOWN", targetPath, "The target step must exist", id)
				} else {
					edges = append(edges, graphEdge{index, to, outcome})
				}
			} else if terminal := stringValue(target["terminal"]); !contains(document["terminals"], terminal) {
				add("TERMINAL_UNKNOWN", targetPath, "The target terminal must be declared", id)
			} else if (terminal == "candidate_ready" && (descriptor["id"] != "component.register-candidate" || outcome != "registered")) || (terminal == "active" && (descriptor["id"] != "component.activation-status" || outcome != "active")) {
				add("PROTECTED_GATE_MISSING", targetPath, "Success requires the matching protected final outcome", id)
			}
		}
		for _, name := range sortedKeys(on) {
			if !contains(descriptor["outcomes"], name) {
				add("EDGE_UNKNOWN_OUTCOME", path+"/on", "The outcome must exist in the retained descriptor", id)
			} else if contains(descriptor["controlOutcomes"], name) || name == pending {
				add("EDGE_PENDING_OR_CONTROL", path+"/on", "Pending and protected control outcomes cannot have definition edges", id)
			}
			checkTarget(on[name], name, path+"/on")
		}
		if limit, exists := step["visitLimit"]; exists {
			maximum := object(limit)["max"]
			bounded := false
			// The authoritative schema admits only integer values 0 through 2.
			for allowed := 0; allowed <= maximumRepairVisits && allowed <= 2; allowed++ {
				if maximum == float64(allowed) {
					bounded = true
				}
			}
			if descriptor["id"] != "component.repair" || !bounded || object(object(limit)["onExhausted"])["terminal"] != "needs_changes" {
				add("REPAIR_CYCLE_INVALID", path+"/visitLimit", "Only repair may consume a bounded visit allowance and exhaust to needs_changes", id)
			}
			checkTarget(object(limit)["onExhausted"], "", path+"/visitLimit/onExhausted")
		} else if descriptor["id"] == "component.repair" {
			add("REPAIR_CYCLE_INVALID", path, "Repair requires a retained-policy visit limit", id)
		}
	}

	reachable := make([]bool, len(steps))
	reachable[entry] = true
	for changed := true; changed; {
		changed = false
		for _, edge := range edges {
			if reachable[edge.from] && !reachable[edge.to] {
				reachable[edge.to], changed = true, true
			}
		}
	}
	for index, known := range reachable {
		if !known {
			add("STEP_UNREACHABLE", fmt.Sprintf("/steps/%d", index), "Every step must be reachable from entry", stringValue(object(steps[index])["id"]))
		}
	}

	// Removing the sole permitted repair back-edge must leave an acyclic graph.
	// Its matching forward edge and exact action identities are checked first.
	indegree := make([]int, len(steps))
	var acyclicEdges []graphEdge
	for _, edge := range edges {
		if resolved[edge.from]["id"] == "component.repair" && resolved[edge.to]["id"] == "component.validate" && edge.outcome == "ready" {
			paired := false
			for _, forward := range edges {
				paired = paired || (forward.from == edge.to && forward.to == edge.from && forward.outcome == "repairable")
				if forward.from == edge.to && forward.to == edge.from && forward.outcome != "repairable" {
					add("REPAIR_CYCLE_INVALID", "/steps", "Only the repairable validation outcome may enter the repair cycle", "")
				}
			}
			if paired && len(actions["component.repair"]) == 1 && len(actions["component.validate"]) == 1 {
				// A second, longer route between the same two actions would also
				// be cyclic after restoring this edge, and is not the allowed pair.
				forward, backward := make([]bool, len(steps)), make([]bool, len(steps))
				forward[edge.to], backward[edge.from] = true, true
				for changed := true; changed; {
					changed = false
					for _, candidate := range edges {
						if candidate == edge {
							continue
						}
						if forward[candidate.from] && !forward[candidate.to] {
							forward[candidate.to], changed = true, true
						}
						if backward[candidate.to] && !backward[candidate.from] {
							backward[candidate.from], changed = true, true
						}
					}
				}
				for index := range steps {
					if index != edge.from && index != edge.to && forward[index] && backward[index] {
						add("REPAIR_CYCLE_INVALID", "/steps", "The repair cycle cannot include another action", "")
						break
					}
				}
				continue
			}
		}
		acyclicEdges = append(acyclicEdges, edge)
		indegree[edge.to]++
	}
	var queue []int
	for index, degree := range indegree {
		if degree == 0 {
			queue = append(queue, index)
		}
	}
	for cursor := 0; cursor < len(queue); cursor++ {
		for _, edge := range acyclicEdges {
			if edge.from == queue[cursor] {
				indegree[edge.to]--
				if indegree[edge.to] == 0 {
					queue = append(queue, edge.to)
				}
			}
		}
	}
	if len(queue) != len(steps) {
		add("REPAIR_CYCLE_INVALID", "/steps", "Only the bounded validate-repair cycle is supported", "")
	}

	// Definite availability is the intersection of every predecessor outcome.
	// Starting other nodes at the slot universe computes a descending fixed point;
	// the entry's initial slots prevent a cycle from inventing its own inputs.
	available := make([]map[string]bool, len(steps))
	for index := range steps {
		available[index] = make(map[string]bool)
		for slot := range slotKinds {
			available[index][slot] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for index := range steps {
			for _, slot := range sortedKeys(slotKinds) {
				present := index != entry || initialSlots[slot]
				for _, edge := range edges {
					if edge.to != index || !reachable[edge.from] {
						continue
					}
					produced := available[edge.from][slot]
					written, outcomeAvailable := false, false
					for port, bound := range object(object(steps[edge.from])["outputs"]) {
						output, known := object(resolved[edge.from]["outputs"])[port]
						if known && bound == slot {
							written = true
							outcomeAvailable = outcomeAvailable || contains(object(output)["availableOn"], edge.outcome)
						}
					}
					if written {
						produced = outcomeAvailable
					}
					present = present && produced
				}
				if available[index][slot] && !present {
					available[index][slot], changed = false, true
				}
			}
		}
	}
	for index, item := range steps {
		step := object(item)
		for _, port := range sortedKeys(object(step["inputs"])) {
			if !available[index][stringValue(object(step["inputs"])[port])] {
				add("INPUT_UNAVAILABLE", fmt.Sprintf("/steps/%d/inputs", index), "The input must be available on every predecessor outcome", stringValue(step["id"]))
			}
		}
	}
	if len(actions["component.validate"]) == 1 {
		validation := object(steps[actions["component.validate"][0]])
		for _, action := range []string{"component.register-candidate", "component.repair"} {
			for _, index := range actions[action] {
				consumer := object(steps[index])
				if object(consumer["inputs"])["source"] != object(validation["inputs"])["source"] || object(consumer["inputs"])["report"] != object(validation["outputs"])["report"] {
					add("PROTECTED_GATE_MISSING", fmt.Sprintf("/steps/%d/inputs", index), "The source and report must match the protected validation subject", stringValue(consumer["id"]))
				}
			}
		}
	}
	// Every path to a protected consumer must traverse its producer's successful
	// outcome. Merely visiting validation is insufficient: its invalid report has
	// the same reference kind as its valid report.
	for _, pair := range [][3]string{{"component.validate", "component.register-candidate", "valid"}, {"component.certify-release", "component.request-publication", "certified"}, {"component.review-status", "component.request-publication", "approved"}, {"component.publication-status", "component.request-activation", "published"}} {
		if len(actions[pair[0]]) != 1 {
			continue
		}
		without := make([]bool, len(steps))
		gate := actions[pair[0]][0]
		without[entry] = true
		for changed := true; changed; {
			changed = false
			for _, edge := range edges {
				if edge.from == gate && edge.outcome == pair[2] {
					continue
				}
				invalidatesSource := false
				if pair[0] == "component.validate" {
					for port, slot := range object(object(steps[edge.from])["outputs"]) {
						output := object(object(resolved[edge.from]["outputs"])[port])
						for _, consumer := range actions[pair[1]] {
							if output["kind"] == "source" && contains(output["availableOn"], edge.outcome) && slot == object(object(steps[consumer])["inputs"])["source"] {
								invalidatesSource = reachable[edge.from]
							}
						}
					}
				}
				if (without[edge.from] || invalidatesSource) && !without[edge.to] {
					without[edge.to], changed = true, true
				}
			}
		}
		for _, consumer := range actions[pair[1]] {
			if without[consumer] {
				add("PROTECTED_GATE_MISSING", fmt.Sprintf("/steps/%d", consumer), "All paths to this action require its protected producer's successful outcome", stringValue(object(steps[consumer])["id"]))
			}
		}
	}
	return issues
}

func object(value any) map[string]any { result, _ := value.(map[string]any); return result }
func array(value any) []any           { result, _ := value.([]any); return result }
func stringValue(value any) string    { result, _ := value.(string); return result }
func contains(value any, item string) bool {
	return slices.Contains(array(value), any(item))
}
func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, strings.Compare)
	return keys
}
