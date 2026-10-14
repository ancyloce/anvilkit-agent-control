// Package outbox binds the pinned watermill-sql PostgreSQL adapter to
// Control's transactions (P2.8, DD-09 §2): the physical schema is
// migration 00017's outbox/outbox_offsets (identical to Knowledge's and
// MCP's); nothing here initializes schema. Every event is published
// through the forwarder envelope so the forwarder module unwraps it and
// publishes to the destination subject on JetStream; the waker consumes
// operation.changed and wakes the event streams (the API SSE path).
package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jackc/pgx/v5"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// The forwarder topic and the physical table names of the adapter.
const (
	ForwarderTopic = "outbox"
	MessagesTable  = `"outbox"`
	OffsetsTable   = `"outbox_offsets"`
)

// Metadata keys carried beside the envelope (they become NATS headers).
const (
	MetadataEventType = "anvilkit_event_type"
	MetadataTenantID  = "anvilkit_tenant_id"
	MetadataSchema    = "anvilkit_schema_version"
)

// Schema is the adapter's PostgreSQL schema with the owner's table names.
func Schema(batchSize int) sql.DefaultPostgreSQLSchema {
	return sql.DefaultPostgreSQLSchema{
		GenerateMessagesTableName: func(string) string { return MessagesTable },
		SubscribeBatchSize:        batchSize,
	}
}

// Offsets is the adapter's offsets schema with the owner's table name.
func Offsets() sql.DefaultPostgreSQLOffsetsAdapter {
	return sql.DefaultPostgreSQLOffsetsAdapter{GenerateMessagesOffsetsTableName: func(string) string { return OffsetsTable }}
}

// Publish inserts the envelope into the outbox of the given transaction:
// the same pgx.Tx the operation event and its projection use, so both
// commit or roll back together. The watermill message UUID is the eventId
// (a standard UUID); the forwarder wrapper allocates its own row UUID.
func Publish(ctx context.Context, tx pgx.Tx, env domain.Envelope) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	pub, err := sql.NewPublisher(sql.TxFromPgx(tx), sql.PublisherConfig{SchemaAdapter: Schema(0), AutoInitializeSchema: false}, watermill.NopLogger{})
	if err != nil {
		return fmt.Errorf("outbox publisher: %w", err)
	}
	msg := message.NewMessage(env.EventID, raw)
	msg.Metadata.Set(MetadataEventType, env.EventType)
	msg.Metadata.Set(MetadataTenantID, env.TenantID)
	msg.Metadata.Set(MetadataSchema, "1")
	msg.SetContext(ctx)
	fp := forwarder.NewPublisher(pub, forwarder.PublisherConfig{ForwarderTopic: ForwarderTopic})
	if err := fp.Publish(env.Subject, msg); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	return nil
}

// row is the forwarder envelope a stored outbox row holds (watermill's
// components/forwarder JSON: the destination subject, the message UUID,
// the payload bytes and the metadata).
type row struct {
	DestinationTopic string            `json:"destination_topic"`
	UUID             string            `json:"uuid"`
	Payload          []byte            `json:"payload"`
	Metadata         map[string]string `json:"metadata"`
}

// Unwrap reads the event envelope and its destination subject out of a
// stored outbox row's payload (verification and the operator's tools).
func Unwrap(stored []byte) (domain.Envelope, string, error) {
	var r row
	if err := json.Unmarshal(stored, &r); err != nil {
		return domain.Envelope{}, "", fmt.Errorf("outbox row: %w", err)
	}
	var env domain.Envelope
	if err := json.Unmarshal(r.Payload, &env); err != nil {
		return domain.Envelope{}, "", fmt.Errorf("outbox envelope: %w", err)
	}
	if r.UUID != env.EventID {
		return domain.Envelope{}, "", fmt.Errorf("outbox row message %s carries event %s", r.UUID, env.EventID)
	}
	return env, r.DestinationTopic, nil
}
