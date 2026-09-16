package application

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// The five durable external-obligation classes (DD-02 §5). Every obligation
// is published under "<class>/<id>" before the effect it guards (the
// accepted operation, the Kubernetes create, the first send permission, the
// Pagix mutation). The body holds the minimum binding of the class and the
// evidence metadata needed to read the obligation back by its original
// identity after a database rollback: no secrets, prompts, source, native
// bodies, presigned URLs or object keys of artifacts ever enter it.
const (
	ClassIntake        = "intake"
	ClassJobLaunch     = "job-launch"
	ClassModelDispatch = "model-dispatch"
	ClassToolDispatch  = "tool-dispatch"
	ClassBusinessWrite = "business-write"
)

// ObligationClasses lists the classes in enumeration order.
var ObligationClasses = []string{ClassIntake, ClassJobLaunch, ClassModelDispatch, ClassToolDispatch, ClassBusinessWrite}

// ObligationKey is the inventory key of an obligation.
func ObligationKey(class, id string) string { return class + "/" + id }

// DescribeKey names an inventory object in errors and logs by its
// controlled identity (the class and the obligation or attestation id),
// never by a storage locator: a full object key, a prefix, a bucket or a
// URL never leaves the adapter (security.md data classification).
func DescribeKey(key string) string {
	first, rest, ok := strings.Cut(key, "/")
	if !ok || rest == "" {
		return "object of class " + key
	}
	id := rest
	if i := strings.LastIndex(rest, "/"); i >= 0 {
		id = rest[i+1:]
	}
	return first + " object " + id
}

// ParseObligationKey splits an inventory key into its class and identity;
// ok is false for keys outside the five classes (attestations, probes).
func ParseObligationKey(key string) (class, id string, ok bool) {
	class, id, found := strings.Cut(key, "/")
	if !found || id == "" || strings.Contains(id, "/") {
		return "", "", false
	}
	for _, c := range ObligationClasses {
		if c == class {
			return class, id, true
		}
	}
	return "", "", false
}

// IntakeRecord is the intake obligation: command/operation/kind/tenant/
// subject/profile/requestDigest (DD-02 §5).
type IntakeRecord struct {
	Class          string `json:"class"`
	CommandID      string `json:"commandId"`
	OperationID    string `json:"operationId"`
	Kind           string `json:"kind"`
	TenantID       string `json:"tenantId"`
	ProfileID      string `json:"profileId"`
	SubjectDigest  string `json:"subjectDigest"`
	RequestDigest  string `json:"requestDigest"`
	CreatedAt      string `json:"createdAt"`
	ExecutionEpoch uint64 `json:"executionEpoch"`
}

func intakeObligation(op *domain.Operation) (key string, body []byte) {
	body, _ = json.Marshal(IntakeRecord{
		Class: ClassIntake, CommandID: op.CommandID, OperationID: op.ID, Kind: string(op.Kind), TenantID: op.TenantID,
		ProfileID: op.Subject.ProfileID, SubjectDigest: string(op.Subject.SubjectDigest), RequestDigest: string(op.SemanticDigest),
		CreatedAt: op.CreatedAt.UTC().Format(time.RFC3339Nano), ExecutionEpoch: op.ExecutionEpoch,
	})
	return ObligationKey(ClassIntake, op.ID), body
}

// LaunchRecord is the job-launch obligation: attempt/launchKey/profile/
// image/epochs/deadline, persisted before any Kubernetes create.
type LaunchRecord struct {
	Class          string `json:"class"`
	LaunchID       string `json:"launchId"`
	AttemptID      string `json:"attemptId"`
	OperationID    string `json:"operationId"`
	TenantID       string `json:"tenantId"`
	LaunchKey      string `json:"launchKey"`
	Backend        string `json:"backend"`
	ProfileID      string `json:"profileId"`
	ImageDigest    string `json:"imageDigest"`
	ExecutionEpoch uint64 `json:"executionEpoch"`
	LaunchEpoch    uint64 `json:"launchEpoch"`
	Deadline       string `json:"deadline"`
	CreatedAt      string `json:"createdAt"`
}

func launchObligation(l *domain.Launch, tenantID string) (key string, body []byte) {
	body, _ = json.Marshal(LaunchRecord{
		Class: ClassJobLaunch, LaunchID: l.ID, AttemptID: l.AttemptID, OperationID: l.OperationID, TenantID: tenantID, LaunchKey: l.LaunchKey,
		Backend: l.Backend, ProfileID: l.ProfileID, ImageDigest: string(l.ImageDigest), ExecutionEpoch: l.ExecutionEpoch,
		LaunchEpoch: l.LaunchEpoch, Deadline: l.Deadline.UTC().Format(time.RFC3339Nano), CreatedAt: l.CreatedAt.UTC().Format(time.RFC3339Nano),
	})
	return ObligationKey(ClassJobLaunch, l.ID), body
}

// DispatchRecord is the model-dispatch or tool-dispatch obligation: call/
// attempt/instance/route or grant/exposure/pricing/requestDigest.
type DispatchRecord struct {
	Class          string `json:"class"`
	DispatchID     string `json:"dispatchId"`
	CallID         string `json:"callId"`
	Owner          string `json:"owner"`
	TenantID       string `json:"tenantId"`
	OperationID    string `json:"operationId"`
	AttemptID      string `json:"attemptId"`
	InstanceID     string `json:"instanceId,omitempty"`
	RouteID        string `json:"routeId"`
	GrantID        string `json:"grantId,omitempty"`
	GrantRevision  uint64 `json:"grantRevision,omitempty"`
	RequestDigest  string `json:"requestDigest"`
	Currency       string `json:"currency"`
	Exposure       string `json:"exposure"`
	MeterRevision  string `json:"meterRevision"`
	ExecutionEpoch uint64 `json:"executionEpoch"`
	Deadline       string `json:"deadline"`
	AdmittedAt     string `json:"admittedAt"`
}

func dispatchObligation(d *domain.Dispatch) (key string, body []byte) {
	class := ClassModelDispatch
	if d.Kind == domain.DispatchTool {
		class = ClassToolDispatch
	}
	body, _ = json.Marshal(DispatchRecord{
		Class: class, DispatchID: d.ID, CallID: d.CallID, Owner: d.Owner, TenantID: d.TenantID, OperationID: d.OperationID, AttemptID: d.AttemptID,
		InstanceID: d.InstanceID, RouteID: d.RouteID, GrantID: d.GrantID, GrantRevision: d.GrantRevision, RequestDigest: string(d.RequestDigest),
		Currency: d.Reserved.Currency, Exposure: d.Reserved.AmountString(), MeterRevision: d.MeterRevision, ExecutionEpoch: d.ExecutionEpoch,
		Deadline: d.Deadline.UTC().Format(time.RFC3339Nano), AdmittedAt: d.AdmittedAt.UTC().Format(time.RFC3339Nano),
	})
	return ObligationKey(class, d.ID), body
}

// EffectRecord is the business-write obligation: effect/command/kind/
// subject/expected revision/requestDigest, persisted before the guarded
// upstream mutation is permitted.
type EffectRecord struct {
	Class            string `json:"class"`
	EffectID         string `json:"effectId"`
	CommandID        string `json:"commandId"`
	OperationID      string `json:"operationId"`
	AttemptID        string `json:"attemptId,omitempty"`
	TenantID         string `json:"tenantId"`
	Kind             string `json:"kind"`
	Occurrence       uint64 `json:"occurrence"`
	Owner            string `json:"owner"`
	CanonicalSubject string `json:"canonicalSubject"`
	ExpectedRevision string `json:"expectedRevision,omitempty"`
	RequestDigest    string `json:"requestDigest"`
	ExecutionEpoch   uint64 `json:"executionEpoch"`
	LeaseID          string `json:"leaseId,omitempty"`
	LeaseFence       uint64 `json:"leaseFence,omitempty"`
	Deadline         string `json:"deadline"`
	CreatedAt        string `json:"createdAt"`
}

func effectObligation(e *domain.Effect) (key string, body []byte) {
	body, _ = json.Marshal(EffectRecord{
		Class: ClassBusinessWrite, EffectID: e.ID, CommandID: e.CommandID, OperationID: e.OperationID, AttemptID: e.AttemptID, TenantID: e.TenantID, Kind: string(e.Kind),
		Occurrence: e.Occurrence, Owner: e.Owner, CanonicalSubject: e.CanonicalSubject, ExpectedRevision: e.ExpectedRevision, RequestDigest: string(e.RequestDigest),
		ExecutionEpoch: e.ExecutionEpoch, LeaseID: e.LeaseID, LeaseFence: e.LeaseFence, Deadline: e.Deadline.UTC().Format(time.RFC3339Nano),
		CreatedAt: e.CreatedAt.UTC().Format(time.RFC3339Nano),
	})
	return ObligationKey(ClassBusinessWrite, e.ID), body
}

// Obligation is one decoded inventory object: its class, identity, the
// tenant it binds and the moment it was recorded, plus the class record.
type Obligation struct {
	Class      string
	ID         string
	TenantID   string
	RecordedAt time.Time
	Intake     *IntakeRecord
	Launch     *LaunchRecord
	Dispatch   *DispatchRecord
	Effect     *EffectRecord
}

// DecodeObligation reads a published obligation body of the class named
// by its key. A body that is not the class it was published under, whose
// identity differs from the key, or that lacks one of the minimum bindings
// of its class (DD-02 §5: the tenant above all, since without it no scope
// can be decided) is domain.ErrIntegrity: it is never reconciled as the
// obligation and never disappears as an out-of-scope record. A key outside
// the five classes is domain.ErrInvalid (attestations, probes).
func DecodeObligation(key string, body []byte) (Obligation, error) {
	class, id, ok := ParseObligationKey(key)
	if !ok {
		return Obligation{}, fmt.Errorf("%w: %s is not an obligation key", domain.ErrInvalid, DescribeKey(key))
	}
	o := Obligation{Class: class, ID: id}
	name := DescribeKey(key)
	var recorded, recordClass, recordID string
	var missing []string
	switch class {
	case ClassIntake:
		var r IntakeRecord
		if err := json.Unmarshal(body, &r); err != nil {
			return Obligation{}, fmt.Errorf("%w: %s does not decode: %v", domain.ErrIntegrity, name, err)
		}
		o.Intake, o.TenantID, recorded, recordClass, recordID = &r, r.TenantID, r.CreatedAt, r.Class, r.OperationID
		missing = required("commandId", r.CommandID, "operationId", r.OperationID, "kind", r.Kind, "tenantId", r.TenantID, "profileId", r.ProfileID, "subjectDigest", r.SubjectDigest, "requestDigest", r.RequestDigest, "createdAt", r.CreatedAt)
	case ClassJobLaunch:
		var r LaunchRecord
		if err := json.Unmarshal(body, &r); err != nil {
			return Obligation{}, fmt.Errorf("%w: %s does not decode: %v", domain.ErrIntegrity, name, err)
		}
		o.Launch, o.TenantID, recorded, recordClass, recordID = &r, r.TenantID, r.CreatedAt, r.Class, r.LaunchID
		missing = required("launchId", r.LaunchID, "attemptId", r.AttemptID, "operationId", r.OperationID, "tenantId", r.TenantID, "launchKey", r.LaunchKey, "backend", r.Backend, "profileId", r.ProfileID, "imageDigest", r.ImageDigest, "deadline", r.Deadline, "createdAt", r.CreatedAt)
		if r.ExecutionEpoch == 0 {
			missing = append(missing, "executionEpoch")
		}
		if r.LaunchEpoch == 0 {
			missing = append(missing, "launchEpoch")
		}
	case ClassModelDispatch, ClassToolDispatch:
		var r DispatchRecord
		if err := json.Unmarshal(body, &r); err != nil {
			return Obligation{}, fmt.Errorf("%w: %s does not decode: %v", domain.ErrIntegrity, name, err)
		}
		o.Dispatch, o.TenantID, recorded, recordClass, recordID = &r, r.TenantID, r.AdmittedAt, r.Class, r.DispatchID
		missing = required("dispatchId", r.DispatchID, "callId", r.CallID, "owner", r.Owner, "tenantId", r.TenantID, "operationId", r.OperationID, "attemptId", r.AttemptID, "requestDigest", r.RequestDigest, "currency", r.Currency, "exposure", r.Exposure, "meterRevision", r.MeterRevision, "deadline", r.Deadline, "admittedAt", r.AdmittedAt)
		if class == ClassModelDispatch && r.RouteID == "" {
			missing = append(missing, "routeId")
		}
		if class == ClassToolDispatch && r.GrantID == "" {
			missing = append(missing, "grantId")
		}
		if r.ExecutionEpoch == 0 {
			missing = append(missing, "executionEpoch")
		}
	case ClassBusinessWrite:
		var r EffectRecord
		if err := json.Unmarshal(body, &r); err != nil {
			return Obligation{}, fmt.Errorf("%w: %s does not decode: %v", domain.ErrIntegrity, name, err)
		}
		o.Effect, o.TenantID, recorded, recordClass, recordID = &r, r.TenantID, r.CreatedAt, r.Class, r.EffectID
		missing = required("effectId", r.EffectID, "commandId", r.CommandID, "operationId", r.OperationID, "tenantId", r.TenantID, "kind", r.Kind, "owner", r.Owner, "canonicalSubject", r.CanonicalSubject, "requestDigest", r.RequestDigest, "deadline", r.Deadline, "createdAt", r.CreatedAt)
		if r.Occurrence == 0 {
			missing = append(missing, "occurrence")
		}
		if r.ExecutionEpoch == 0 {
			missing = append(missing, "executionEpoch")
		}
	}
	if recordClass != class || recordID != id {
		return Obligation{}, fmt.Errorf("%w: %s holds a record of %s/%s", domain.ErrIntegrity, name, recordClass, recordID)
	}
	if len(missing) > 0 {
		return Obligation{}, fmt.Errorf("%w: %s lacks the required bindings %s", domain.ErrIntegrity, name, strings.Join(missing, ", "))
	}
	t, err := time.Parse(time.RFC3339Nano, recorded)
	if err != nil {
		return Obligation{}, fmt.Errorf("%w: %s has no valid recording time", domain.ErrIntegrity, name)
	}
	o.RecordedAt = t.UTC()
	return o, nil
}

// required returns the names of the fields, given as name/value pairs,
// whose value is empty.
func required(pairs ...string) []string {
	var missing []string
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] == "" {
			missing = append(missing, pairs[i])
		}
	}
	return missing
}
