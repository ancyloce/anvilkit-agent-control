package definition

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	pb "github.com/ancyloce/anvilkit-agent-control/internal/contracts/definitionvalidationv1"
	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/proto"
)

const RequestMaxBytes = 65536

// BoundaryError carries a safe contract code, never submitted content.
type BoundaryError struct{ Code string }

func (e *BoundaryError) Error() string { return e.Code }

type Validator struct {
	requestSchema                *jsonschema.Schema
	registry, profile, effective map[string]any
	polls                        map[string]bool
	waitPolicies                 map[string]string
}

type retainedOnly struct{}

func (retainedOnly) Load(string) (any, error) { return nil, errors.New("schema is not retained") }

// New verifies retained provenance before serving any valid digest. No schema
// loader or validation request can read a URL or mutable parent checkout.
func New() (*Validator, error) {
	inputs, err := contracts.VerifiedInputs()
	if err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(retainedOnly{})
	compiler.AssertFormat()
	documents := make(map[string]any)
	for _, name := range sortedKeys(inputs) {
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		raw := inputs[name]
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		documents[name] = value
		if strings.HasSuffix(name, ".schema.json") {
			if err := compiler.AddResource(stringValue(object(value)["$id"]), value); err != nil {
				return nil, err
			}
		}
	}
	schema, err := compiler.Compile("urn:anvilkit:definition-validation-interface:v1#/$defs/Request")
	if err != nil {
		return nil, err
	}
	if err := verifyCanonicalizer(object(documents["canonicalization-v1.fixtures.json"])); err != nil {
		return nil, err
	}
	v := &Validator{requestSchema: schema, registry: object(documents["registry.json"]), profile: object(documents["validation-local-profile-v1.json"]), effective: object(documents["component-effective-policy.example.json"]), polls: map[string]bool{}, waitPolicies: map[string]string{}}
	encoded, err := json.Marshal(v.registry["descriptors"])
	if err != nil {
		return nil, err
	}
	canonical, err := jsoncanonicalizer.Transform(encoded)
	if err != nil {
		return nil, err
	}
	if digest(canonical) != v.profile["descriptorDigest"] || v.profile["descriptorDigest"] != v.registry["descriptorSetDigest"] || v.profile["effectivePolicyRef"] != v.effective["id"] || len(array(v.profile["enabledEffectPermissions"])) != 0 {
		return nil, errors.New("retained validation profile does not match its inputs")
	}
	if v.profile["runtimeBindingsRef"] != object(documents["runtime-bindings.example.json"])["id"] || v.profile["safetyBindingsRef"] != object(documents["runner-safety-bindings.example.json"])["id"] {
		return nil, errors.New("retained bindings do not match the profile")
	}
	for _, name := range []string{"review-poll-v1.json", "publication-poll-v1.json", "activation-poll-v1.json"} {
		v.polls[stringValue(object(documents[name])["id"])] = true
	}
	if err := v.checkRetainedReferences(documents); err != nil {
		return nil, err
	}
	policySchema, err := compiler.Compile("urn:anvilkit:policy-record:v1")
	if err != nil {
		return nil, err
	}
	policies := append([]any{v.effective}, array(documents["retry-profiles.example.json"])...)
	policies = append(policies, array(object(documents["meter-policies.example.json"])["policies"])...)
	for _, policy := range policies {
		if err := policySchema.Validate(policy); err != nil {
			return nil, errors.New("retained policy schema validation failed")
		}
	}
	return v, nil
}

// Validate applies the same size, presence and canonical JSON rules to direct
// private callers as to HTTP callers. Structural errors are boundary failures;
// semantic errors are bounded reports without a digest.
func (v *Validator) Validate(request *pb.ValidateDefinitionRequest) (*pb.ValidateDefinitionResponse, error) {
	invalid := &BoundaryError{Code: "INVALID_ARGUMENT"}
	if request == nil || request.DefinitionJson == nil || request.DescriptorDigest == nil || request.RuntimeProfileRef == nil || len(request.ProtoReflect().GetUnknown()) != 0 || proto.Size(request) > RequestMaxBytes {
		return nil, invalid
	}
	// Preserve the submitted definition whitespace when measuring the complete
	// JSON envelope; marshaling the decoded object would undercount this boundary.
	metadata, err := json.Marshal(map[string]any{"descriptorDigest": request.GetDescriptorDigest(), "runtimeProfileRef": request.GetRuntimeProfileRef(), "policyRefs": request.PolicyRefs})
	if err != nil || len(`{"definition":`)+len(request.DefinitionJson)+len(metadata) > RequestMaxBytes {
		return nil, invalid
	}
	document, err := contracts.DecodeObject(request.DefinitionJson, RequestMaxBytes)
	if err != nil {
		return nil, invalid
	}
	if version, ok := document["schemaVersion"].(json.Number); ok {
		number, err := version.Float64()
		if err != nil || number != 1 {
			return nil, &BoundaryError{Code: "UNSUPPORTED_SCHEMA"}
		}
	}
	policies := make([]any, len(request.PolicyRefs))
	for i, policy := range request.PolicyRefs {
		policies[i] = policy
	}
	public := map[string]any{"definition": document, "descriptorDigest": request.GetDescriptorDigest(), "runtimeProfileRef": request.GetRuntimeProfileRef(), "policyRefs": policies}
	encoded, err := json.Marshal(public)
	if err != nil || len(encoded) > RequestMaxBytes || v.requestSchema.Validate(public) != nil {
		return nil, invalid
	}
	response := &pb.ValidateDefinitionResponse{Valid: proto.Bool(false), DescriptorDigest: proto.String(request.GetDescriptorDigest()), RuntimeProfileRef: proto.String(request.GetRuntimeProfileRef()), Issues: []*pb.ValidationIssue{}}
	add := func(code, path, message string) {
		if len(response.Issues) < maxIssues {
			response.Issues = append(response.Issues, &pb.ValidationIssue{Code: proto.String(code), Path: proto.String(path), Message: proto.String(message)})
		}
	}
	if request.GetRuntimeProfileRef() != v.profile["id"] {
		message := "Runtime profile is not retained for local validation"
		if request.GetRuntimeProfileRef() == "fixture-runtime" {
			message = "Fixture runtime is not qualified"
		}
		add("PROFILE_QUALIFICATION_FAILED", "", message)
		return response, nil
	}
	if request.GetDescriptorDigest() != v.registry["descriptorSetDigest"] {
		add("DESCRIPTOR_MISMATCH", "/descriptorDigest", "The exact descriptor set must be retained")
	}
	for _, ref := range request.PolicyRefs {
		if _, ok := object(v.profile["policyMappings"])[ref]; !ok {
			add("POLICY_MISSING", "/policyRefs", "Every policy reference must be retained")
		}
	}
	policyRef := stringValue(document["policyRef"])
	mapping := object(object(v.profile["policyMappings"])[policyRef])
	if !slices.Contains(request.PolicyRefs, policyRef) || mapping["definitionId"] != document["id"] || document["family"] != v.effective["family"] {
		add("REFERENCE_UNQUALIFIED", "/policyRef", "The definition must match its retained policy and family")
	}
	maxRepair := 0
	for _, policy := range array(v.effective["actionPolicies"]) {
		if object(policy)["actionId"] == "component.repair" {
			maxRepair = int(object(policy)["maximumRepairVisits"].(float64))
		}
	}
	for i, item := range array(document["steps"]) {
		step := object(item)
		if !contains(mapping["actionIds"], stringValue(object(step["action"])["id"])) {
			add("REFERENCE_UNQUALIFIED", fmt.Sprintf("/steps/%d/action", i), "The action must belong to the retained definition policy")
		}
		pollRef := stringValue(object(step["wait"])["pollPolicyRef"])
		if _, requiresWait := v.waitPolicies[stringValue(object(step["action"])["id"])]; requiresWait && step["kind"] != "wait" {
			add("REFERENCE_UNQUALIFIED", fmt.Sprintf("/steps/%d/kind", i), "Retained status actions require their wait contract")
		}
		if step["kind"] == "wait" && (!v.polls[pollRef] || v.waitPolicies[stringValue(object(step["action"])["id"])] != pollRef) {
			add("REFERENCE_UNQUALIFIED", fmt.Sprintf("/steps/%d/wait", i), "The poll policy must be retained")
		}
	}
	// The schema has bounded the only graph numeric field to integer 0..2.
	// Convert just that field for the existing graph checker; all other values
	// remain lossless until the verified canonicalizer consumes original bytes.
	for _, item := range array(document["steps"]) {
		if limit := object(object(item)["visitLimit"]); limit != nil {
			if n, ok := limit["max"].(json.Number); ok {
				x, _ := n.Float64()
				limit["max"] = x
			}
		}
	}
	for _, issue := range validateGraph(document, v.registry, maxRepair) {
		if len(response.Issues) >= maxIssues {
			break
		}
		p := &pb.ValidationIssue{Code: proto.String(issue.code), Path: proto.String(issue.path), Message: proto.String(issue.message)}
		if issue.stepID != "" {
			p.StepId = proto.String(issue.stepID)
		}
		response.Issues = append(response.Issues, p)
	}
	if len(response.Issues) != 0 {
		return response, nil
	}
	canonical, err := jsoncanonicalizer.Transform(request.DefinitionJson)
	if err != nil {
		return nil, invalid
	}
	response.Valid = proto.Bool(true)
	response.DefinitionDigest = proto.String(digest(canonical))
	return response, nil
}

func digest(raw []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(raw)) }

func (v *Validator) checkRetainedReferences(documents map[string]any) error {
	invalid := errors.New("retained action, policy, profile or safety references disagree")
	bindings := make(map[string]map[string]any)
	for _, item := range array(object(documents["runtime-bindings.example.json"])["bindings"]) {
		binding := object(item)
		key := stringValue(binding["actionId"]) + "@" + stringValue(binding["actionVersion"])
		if bindings[key] != nil {
			return invalid
		}
		bindings[key] = binding
	}
	actionPolicies := make(map[string]map[string]any)
	for _, item := range array(v.effective["actionPolicies"]) {
		policy := object(item)
		key := stringValue(policy["actionId"]) + "@" + stringValue(policy["actionVersion"])
		if actionPolicies[key] != nil {
			return invalid
		}
		actionPolicies[key] = policy
	}
	retries, meters := make(map[string]bool), make(map[string]bool)
	for _, item := range array(documents["retry-profiles.example.json"]) {
		retries[stringValue(object(item)["id"])] = true
	}
	for _, item := range array(object(documents["meter-policies.example.json"])["policies"]) {
		meters[stringValue(object(item)["id"])] = true
	}
	for _, item := range array(v.registry["descriptors"]) {
		descriptor := object(item)
		key := stringValue(descriptor["id"]) + "@" + stringValue(descriptor["version"])
		binding, policy := bindings[key], actionPolicies[key]
		if binding == nil || policy == nil || binding["executionProfileRef"] != policy["executionProfileRef"] || !contains(v.profile["executionProfileRefs"], stringValue(binding["executionProfileRef"])) || binding["resourceClass"] != descriptor["resourceClass"] || binding["retryProfileRef"] != policy["retryProfileRef"] || binding["retryProfileRef"] != descriptor["retryProfileRef"] || !retries[stringValue(binding["retryProfileRef"])] || binding["meterPolicyRef"] != descriptor["meterPolicyRef"] || !meters[stringValue(binding["meterPolicyRef"])] || !contains(v.effective["meterPolicyRefs"], stringValue(binding["meterPolicyRef"])) {
			return invalid
		}
		for _, outcome := range array(descriptor["controlOutcomes"]) {
			if !contains(descriptor["outcomes"], stringValue(outcome)) {
				return invalid
			}
			count := 0
			for _, item := range array(object(documents["runner-safety-bindings.example.json"])["actionControlBindings"]) {
				safety := object(item)
				if safety["actionId"] == descriptor["id"] && safety["actionVersion"] == descriptor["version"] && safety["outcome"] == outcome && stringValue(safety["handler"]) != "" {
					count++
				}
			}
			if count != 1 {
				return invalid
			}
		}
	}
	for _, name := range []string{"component-generation.example.json", "component-release.example.json"} {
		for _, item := range array(object(documents[name])["steps"]) {
			step := object(item)
			if step["kind"] == "wait" {
				reference := stringValue(object(step["wait"])["pollPolicyRef"])
				v.waitPolicies[stringValue(object(step["action"])["id"])] = reference
				if !v.polls[reference] {
					return invalid
				}
			}
		}
	}
	return nil
}

func verifyCanonicalizer(vectors map[string]any) error {
	if len(array(vectors["cases"])) == 0 {
		return errors.New("canonicalization vectors are missing")
	}
	for _, item := range array(vectors["cases"]) {
		vector := object(item)
		canonical, err := jsoncanonicalizer.Transform([]byte(stringValue(vector["input"])))
		if err != nil || string(canonical) != vector["canonical"] || digest(canonical) != vector["sha256"] {
			return errors.New("pinned canonicalizer failed its retained vectors")
		}
	}
	return nil
}
