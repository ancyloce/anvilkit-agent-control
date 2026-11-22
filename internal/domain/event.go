package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Control's domain events (P2.8, contracts/events/events.schema.json and
// the event catalog of contracts.md): operation.changed is the only type
// Control publishes. The catalog has no attempt.result-accepted type: no
// Knowledge projection follows accepted results.
const (
	EventProducer           = "anvilkit-agent-control"
	SubjectOperationChanged = "anvilkit.control.operation.changed"
	AggregateOperation      = "operation"
	// conversation.changed (D4a, DEC-070) wakes conversation event streams
	// on every replica; it carries no event content.
	EventConversationChangedType = "conversation.changed"
	SubjectConversationChanged   = "anvilkit.control.conversation.changed"
	AggregateConversation        = "conversation"
)

// Envelope is the EventEnvelope of the events schema with a reviewed small
// payload: identities, states and sequence numbers, never a prompt, body,
// token or grant. The eventId is a standard UUID (version 7).
type Envelope struct {
	EventID           string          `json:"eventId"`
	EventType         string          `json:"eventType"`
	SchemaVersion     int             `json:"schemaVersion"`
	Producer          string          `json:"producer"`
	Subject           string          `json:"subject"`
	TenantID          string          `json:"tenantId"`
	AggregateType     string          `json:"aggregateType"`
	AggregateID       string          `json:"aggregateId"`
	AggregateRevision string          `json:"aggregateRevision"`
	OccurredAt        string          `json:"occurredAt"`
	CorrelationID     string          `json:"correlationId"`
	Payload           json.RawMessage `json:"payload"`
}

type operationChanged struct {
	Kind        string       `json:"kind"`
	Lifecycle   Lifecycle    `json:"lifecycle"`
	Phase       string       `json:"phase"`
	Control     ControlState `json:"control"`
	Cleanup     CleanupState `json:"cleanup"`
	Finance     FinanceState `json:"finance"`
	Revision    string       `json:"revision"`
	FailureCode string       `json:"failureCode,omitempty"`
}

// OperationChangedEnvelope is the operation.changed event of a committed
// operation event of the tenant's operation. aggregateRevision is the
// event's sequence, which strictly increases over every event of the
// operation (MCP-14); the payload carries the projection and the
// operation's own revision. The operation is its events' correlation.
func OperationChangedEnvelope(ev Event, tenantID string) (Envelope, error) {
	if ev.EventType != EventOperationChanged {
		return Envelope{}, fmt.Errorf("operation event type %q is not %s", ev.EventType, EventOperationChanged)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return Envelope{}, err
	}
	p := ev.Payload
	raw, err := json.Marshal(operationChanged{
		Kind: EventOperationChanged, Lifecycle: p.Lifecycle, Phase: p.Phase, Control: p.Control, Cleanup: p.Cleanup, Finance: p.Finance,
		Revision: fmt.Sprintf("%d", ev.Revision), FailureCode: p.FailureCode,
	})
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		EventID: id.String(), EventType: EventOperationChanged, SchemaVersion: 1, Producer: EventProducer, Subject: SubjectOperationChanged,
		TenantID: tenantID, AggregateType: AggregateOperation, AggregateID: ev.OperationID, AggregateRevision: fmt.Sprintf("%d", ev.EventSeq),
		OccurredAt: ev.OccurredAt.UTC().Format("2006-01-02T15:04:05.000000Z"), CorrelationID: ev.OperationID, Payload: raw,
	}, nil
}

type conversationChanged struct {
	Kind            string `json:"kind"`
	ThroughEventSeq string `json:"throughEventSeq"`
}

// ConversationChangedEnvelope is the wake-up of a commit of conversation
// events through throughSeq (design 0001 §8.5): aggregateRevision is that
// sequence, the payload names it, and no body or event content travels.
func ConversationChangedEnvelope(conversationID, tenantID string, throughSeq uint64, occurredAt time.Time) (Envelope, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Envelope{}, err
	}
	seq := fmt.Sprintf("%d", throughSeq)
	raw, err := json.Marshal(conversationChanged{Kind: EventConversationChangedType, ThroughEventSeq: seq})
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		EventID: id.String(), EventType: EventConversationChangedType, SchemaVersion: 1, Producer: EventProducer, Subject: SubjectConversationChanged,
		TenantID: tenantID, AggregateType: AggregateConversation, AggregateID: conversationID, AggregateRevision: seq,
		OccurredAt: occurredAt.UTC().Format("2006-01-02T15:04:05.000000Z"), CorrelationID: conversationID, Payload: raw,
	}, nil
}
