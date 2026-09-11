package definition

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	pb "github.com/ancyloce/anvilkit-agent-control/internal/contracts/definitionvalidationv1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func retained(t *testing.T, name string) map[string]any {
	t.Helper()
	data, err := contracts.Retained.ReadFile("retained/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func requestFromJSON(t *testing.T, value map[string]any) *pb.ValidateDefinitionRequest {
	t.Helper()
	definition, err := json.Marshal(value["definition"])
	if err != nil {
		t.Fatal(err)
	}
	message := &pb.ValidateDefinitionRequest{DefinitionJson: definition, DescriptorDigest: proto.String(stringValue(value["descriptorDigest"])), RuntimeProfileRef: proto.String(stringValue(value["runtimeProfileRef"]))}
	for _, policy := range array(value["policyRefs"]) {
		message.PolicyRefs = append(message.PolicyRefs, stringValue(policy))
	}
	return message
}

func newValidator(t *testing.T) *Validator {
	t.Helper()
	v, err := New()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func positiveRequest(t *testing.T) *pb.ValidateDefinitionRequest {
	t.Helper()
	return requestFromJSON(t, object(object(array(retained(t, "validation-positive-v1.fixtures.json")["cases"])[0])["request"]))
}

func TestExactRetainedReports(t *testing.T) {
	v := newValidator(t)
	cases := array(retained(t, "validation-positive-v1.fixtures.json")["cases"])
	legacy := retained(t, "p0-contract-v1.fixtures.json")
	negative := map[string]any{"id": "unqualified legacy profile"}
	for _, item := range array(legacy["examples"]) {
		if object(item)["id"] == "validation-request" {
			negative["request"] = object(item)["value"]
		}
		if object(item)["id"] == "validation-response" {
			negative["response"] = object(item)["value"]
		}
	}
	cases = append(cases, negative)
	for _, item := range cases {
		value := object(item)
		t.Run(stringValue(value["id"]), func(t *testing.T) {
			request := requestFromJSON(t, object(value["request"]))
			response, err := v.Validate(request)
			if err != nil {
				t.Fatal(err)
			}
			// This is an explicit compatibility assertion over generated output;
			// requests are validated by the JSON Schema, never through ProtoJSON.
			wireJSON, err := (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			var actual any
			if err := json.Unmarshal(wireJSON, &actual); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, value["response"]) {
				t.Fatalf("report mismatch: %s", wireJSON)
			}
			encoded, err := proto.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			var decoded pb.ValidateDefinitionResponse
			if err := proto.Unmarshal(encoded, &decoded); err != nil || !proto.Equal(&decoded, response) {
				t.Fatal("generated response lost presence on the wire")
			}
		})
	}
}

func TestPrivateRequestBoundary(t *testing.T) {
	v := newValidator(t)
	cases := []struct {
		name   string
		change func(*pb.ValidateDefinitionRequest)
	}{
		{"absent definition", func(r *pb.ValidateDefinitionRequest) { r.DefinitionJson = nil }},
		{"absent digest", func(r *pb.ValidateDefinitionRequest) { r.DescriptorDigest = nil }},
		{"empty digest", func(r *pb.ValidateDefinitionRequest) { r.DescriptorDigest = proto.String("") }},
		{"malformed digest", func(r *pb.ValidateDefinitionRequest) { r.DescriptorDigest = proto.String("sha256:ABC") }},
		{"absent runtime", func(r *pb.ValidateDefinitionRequest) { r.RuntimeProfileRef = nil }},
		{"empty runtime", func(r *pb.ValidateDefinitionRequest) { r.RuntimeProfileRef = proto.String("") }},
		{"absent policies", func(r *pb.ValidateDefinitionRequest) { r.PolicyRefs = nil }},
		{"duplicate policies", func(r *pb.ValidateDefinitionRequest) { r.PolicyRefs = append(r.PolicyRefs, r.PolicyRefs[0]) }},
		{"too many policies", func(r *pb.ValidateDefinitionRequest) {
			for range 16 {
				r.PolicyRefs = append(r.PolicyRefs, "extra")
			}
		}},
		{"unknown wire authority", func(r *pb.ValidateDefinitionRequest) {
			r.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 50, protowire.VarintType), 1))
		}},
		{"definition null", func(r *pb.ValidateDefinitionRequest) { r.DefinitionJson = []byte(`null`) }},
		{"duplicate key", func(r *pb.ValidateDefinitionRequest) {
			r.DefinitionJson = bytes.Replace(r.DefinitionJson, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":1,"schemaVersion":1`), 1)
		}},
		{"unknown authority", func(r *pb.ValidateDefinitionRequest) {
			r.DefinitionJson = append([]byte(`{"dispatchAllowed":true,`), r.DefinitionJson[1:]...)
		}},
		{"null property", func(r *pb.ValidateDefinitionRequest) {
			r.DefinitionJson = bytes.Replace(r.DefinitionJson, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":null`), 1)
		}},
		{"unsupported schema", func(r *pb.ValidateDefinitionRequest) {
			r.DefinitionJson = bytes.Replace(r.DefinitionJson, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":2`), 1)
		}},
		{"ambiguous integer", func(r *pb.ValidateDefinitionRequest) {
			r.DefinitionJson = bytes.Replace(r.DefinitionJson, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":1.00000000000000000001`), 1)
		}},
		{"out of bounds visits", func(r *pb.ValidateDefinitionRequest) {
			r.DefinitionJson = bytes.Replace(r.DefinitionJson, []byte(`"max":1`), []byte(`"max":18446744073709551616`), 1)
		}},
		{"overflow exponent", func(r *pb.ValidateDefinitionRequest) {
			r.DefinitionJson = bytes.Replace(r.DefinitionJson, []byte(`"max":1`), []byte(`"max":1e1000001`), 1)
		}},
		{"underflow exponent", func(r *pb.ValidateDefinitionRequest) {
			r.DefinitionJson = bytes.Replace(r.DefinitionJson, []byte(`"max":1`), []byte(`"max":1e-1000001`), 1)
		}},
		{"unrepresentable exponent", func(r *pb.ValidateDefinitionRequest) {
			r.DefinitionJson = bytes.Replace(r.DefinitionJson, []byte(`"max":1`), []byte(`"max":0e999999999999999999999999`), 1)
		}},
		{"unpaired surrogate", func(r *pb.ValidateDefinitionRequest) { r.DefinitionJson = []byte(`{"id":"\ud800\u0000"}`) }},
		{"non UTF-8", func(r *pb.ValidateDefinitionRequest) { r.DefinitionJson = []byte{'{', '"', 0xff, '"', ':', '1', '}'} }},
		{"multiple JSON documents", func(r *pb.ValidateDefinitionRequest) { r.DefinitionJson = append(r.DefinitionJson, []byte(` {}`)...) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := positiveRequest(t)
			tc.change(r)
			response, err := v.Validate(r)
			expected := "INVALID_ARGUMENT"
			if tc.name == "unsupported schema" {
				expected = "UNSUPPORTED_SCHEMA"
			}
			if response != nil || boundaryCode(err) != expected {
				t.Fatalf("expected boundary rejection, got %v, %v", response, err)
			}
		})
	}
	r := positiveRequest(t)
	metadata, _ := json.Marshal(map[string]any{"descriptorDigest": r.GetDescriptorDigest(), "runtimeProfileRef": r.GetRuntimeProfileRef(), "policyRefs": r.PolicyRefs})
	overhead := len(`{"definition":`) + len(metadata)
	r.DefinitionJson = append(r.DefinitionJson, bytes.Repeat([]byte(" "), RequestMaxBytes-overhead-len(r.DefinitionJson))...)
	if response, err := v.Validate(r); err != nil || !response.GetValid() {
		t.Fatalf("exact 64-KiB request rejected: %v", err)
	}
	r.DefinitionJson = append(r.DefinitionJson, ' ')
	if _, err := v.Validate(r); boundaryCode(err) != "INVALID_ARGUMENT" {
		t.Fatalf("64-KiB plus one byte accepted: %v", err)
	}
}

func TestStrictJSONUnicodeAndDuplicates(t *testing.T) {
	for _, input := range []string{`{"a":{"b":1,"\u0062":2}}`, `{"x":"\udc00"}`, `{"x":"\udc00\ud800"}`, `{"x":"\ud800\ud800"}`, `{"x":"\ud800"}`, strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65)} {
		if _, err := contracts.DecodeObject([]byte(input), RequestMaxBytes); err == nil {
			t.Fatalf("invalid JSON accepted: %s", input)
		}
	}
	for _, input := range []string{`{"x":"\ud834\udd1e"}`, `{"x":"\\ud800"}`, `{"x":"�"}`} {
		if _, err := contracts.DecodeObject([]byte(input), RequestMaxBytes); err != nil {
			t.Fatalf("valid Unicode rejected: %s", input)
		}
	}
}

func TestCanonicalDigestIgnoresKeyOrder(t *testing.T) {
	v := newValidator(t)
	r := positiveRequest(t)
	first, err := v.Validate(r)
	if err != nil {
		t.Fatal(err)
	}
	var d map[string]json.RawMessage
	if err := json.Unmarshal(r.DefinitionJson, &d); err != nil {
		t.Fatal(err)
	}
	keys := sortedKeys(d)
	var b bytes.Buffer
	b.WriteByte('{')
	for index := len(keys) - 1; index >= 0; index-- {
		if index != len(keys)-1 {
			b.WriteByte(',')
		}
		key, _ := json.Marshal(keys[index])
		b.Write(key)
		b.WriteByte(':')
		b.Write(d[keys[index]])
	}
	b.WriteByte('}')
	r.DefinitionJson = b.Bytes()
	second, err := v.Validate(r)
	if err != nil || !proto.Equal(first, second) {
		t.Fatalf("key order changed report: %v", err)
	}
	for _, change := range []func(*pb.ValidateDefinitionRequest){func(r *pb.ValidateDefinitionRequest) {
		r.DescriptorDigest = proto.String("sha256:" + strings.Repeat("b", 64))
	}, func(r *pb.ValidateDefinitionRequest) { r.RuntimeProfileRef = proto.String("absent") }, func(r *pb.ValidateDefinitionRequest) { r.PolicyRefs = []string{"absent"} }} {
		r := positiveRequest(t)
		change(r)
		response, err := v.Validate(r)
		if err != nil || response.GetValid() || response.DefinitionDigest != nil || len(response.Issues) == 0 {
			t.Fatalf("unsupported retained reference minted a digest: %v", err)
		}
	}
}

func TestZeroExponentsPreserveCanonicalValue(t *testing.T) {
	v := newValidator(t)
	var expected *pb.ValidateDefinitionResponse
	for _, zero := range []string{"0", "-0", "0e1000001", "0e-1000001"} {
		r := positiveRequest(t)
		r.DefinitionJson = bytes.Replace(r.DefinitionJson, []byte(`"max":1`), []byte(`"max":`+zero), 1)
		response, err := v.Validate(r)
		if err != nil || !response.GetValid() {
			t.Fatalf("zero visit limit rejected: %v", err)
		}
		if expected == nil {
			expected = response
		} else if !proto.Equal(response, expected) {
			t.Fatal("equivalent zero changed canonical report")
		}
	}
}

func boundaryCode(err error) string {
	var boundary *BoundaryError
	if errors.As(err, &boundary) {
		return boundary.Code
	}
	return ""
}

func TestCanonicalizerQualificationFailsClosed(t *testing.T) {
	vectors := retained(t, "canonicalization-v1.fixtures.json")
	if err := verifyCanonicalizer(vectors); err != nil {
		t.Fatal(err)
	}
	object(array(vectors["cases"])[0])["canonical"] = "incorrect"
	if err := verifyCanonicalizer(vectors); err == nil {
		t.Fatal("incorrect canonicalizer output was qualified")
	}
	if err := verifyCanonicalizer(map[string]any{}); err == nil {
		t.Fatal("missing vectors were qualified")
	}
}

func TestRetainedRuntimeReferencesMustAgree(t *testing.T) {
	for _, filename := range []string{"runtime-bindings.example.json", "component-effective-policy.example.json"} {
		t.Run(filename, func(t *testing.T) {
			v := newValidator(t)
			documents := make(map[string]any)
			entries, err := contracts.Retained.ReadDir("retained")
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if !strings.HasSuffix(entry.Name(), ".json") {
					continue
				}
				raw, err := contracts.Retained.ReadFile("retained/" + entry.Name())
				if err != nil {
					t.Fatal(err)
				}
				var document any
				if err := json.Unmarshal(raw, &document); err != nil {
					t.Fatal(err)
				}
				documents[entry.Name()] = document
			}
			if filename == "runtime-bindings.example.json" {
				object(array(object(documents[filename])["bindings"])[0])["executionProfileRef"] = "unretained"
			} else {
				object(array(v.effective["actionPolicies"])[0])["retryProfileRef"] = "unretained"
			}
			if err := v.checkRetainedReferences(documents); err == nil {
				t.Fatal("mismatched runtime references accepted")
			}
		})
	}
}

func FuzzDefinitionBoundary(f *testing.F) {
	v, err := New()
	if err != nil {
		f.Fatal(err)
	}
	seed, err := contracts.Retained.ReadFile("retained/component-generation.example.json")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	for _, seed := range []string{`{}`, `{"a":1,"a":2}`, `{"a":"\ud800\u0000"}`, `{"schemaVersion":1e1000001}`, `{"schemaVersion":0e999999999999999999999999}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		request := &pb.ValidateDefinitionRequest{DefinitionJson: raw, DescriptorDigest: proto.String(stringValue(v.profile["descriptorDigest"])), RuntimeProfileRef: proto.String(stringValue(v.profile["id"])), PolicyRefs: []string{"component-generation-pilot-v1"}}
		before := bytes.Clone(raw)
		response, err := v.Validate(request)
		repeated, repeatErr := v.Validate(request)
		if !bytes.Equal(before, raw) || !proto.Equal(response, repeated) || boundaryCode(err) != boundaryCode(repeatErr) {
			t.Fatal("validation is not deterministic and read-only")
		}
		if err != nil {
			if response != nil {
				t.Fatal("boundary failure returned a report")
			}
			return
		}
		if response.GetValid() {
			if response.DefinitionDigest == nil || len(response.Issues) != 0 {
				t.Fatal("invalid success report")
			}
		} else if response.DefinitionDigest != nil || len(response.Issues) < 1 || len(response.Issues) > maxIssues {
			t.Fatal("invalid rejection report")
		}
	})
}
