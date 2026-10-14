package outbox

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmjs "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
)

// NATSConfig is the JetStream connection of the replica, shared by the
// forwarder and the waker.
type NATSConfig struct {
	URL            string
	Name           string
	PublishTimeout time.Duration
	// TLS is the verified transport (nil under the DEVELOPMENT_ONLY
	// plaintext foundation, admitted by the configuration guard only).
	TLS *tls.Config
	// CredsFile is the mounted client credential, a user .creds file or,
	// with CredsNKey, a bare NKey user seed; empty only under the
	// DEVELOPMENT_ONLY foundation.
	CredsFile string
	CredsNKey bool
}

// Connect opens the replica's NATS connection and probes JetStream. The
// connection reconnects without bound; the credential options re-read the
// mounted file when the connection authenticates, so a rotated file is
// presented on the next reconnect.
func Connect(cfg NATSConfig) (*nats.Conn, error) {
	opts := []nats.Option{nats.Name(cfg.Name), nats.Timeout(cfg.PublishTimeout), nats.MaxReconnects(-1), nats.RetryOnFailedConnect(false)}
	if cfg.TLS != nil {
		opts = append(opts, nats.Secure(cfg.TLS))
	}
	switch {
	case cfg.CredsFile == "":
	case cfg.CredsNKey:
		opt, err := nats.NkeyOptionFromSeed(cfg.CredsFile)
		if err != nil {
			return nil, fmt.Errorf("nats credentials: %w", err)
		}
		opts = append(opts, opt)
	default:
		opts = append(opts, nats.UserCredentials(cfg.CredsFile))
	}
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	if _, err := jetstream.New(nc); err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	return nc, nil
}

// ForwarderConfig are the reviewed bounds of the forwarder module.
type ForwarderConfig struct {
	ConsumerGroup  string
	PollInterval   time.Duration
	AckDeadline    time.Duration
	ResendInterval time.Duration
	BatchSize      int
	CloseTimeout   time.Duration
}

// Forwarder is the watermill forwarder: the watermill-sql subscriber over
// outbox/outbox_offsets (its consumer group owns one offsets row; the FOR
// UPDATE of the adapter coordinates the replicas) publishing each
// unwrapped envelope to its destination subject on JetStream with
// Nats-Msg-Id = eventId (the stream's duplicate window absorbs a
// redelivery after a crash between publish and offset ack). Nothing here
// initializes schema or streams: both are deployment inputs.
type Forwarder struct {
	sub *sql.Subscriber
	fwd *forwarder.Forwarder
}

// NewForwarder builds the pieces over the replica's connection; nothing
// runs until Run.
func NewForwarder(pool *pgxpool.Pool, nc *nats.Conn, cfg ForwarderConfig, forwarded, failures prometheus.Counter) (*Forwarder, error) {
	wmLogger := watermill.NopLogger{}
	ack := cfg.AckDeadline
	sub, err := sql.NewSubscriber(sql.BeginnerFromPgx(pool), sql.SubscriberConfig{
		ConsumerGroup: cfg.ConsumerGroup, AckDeadline: &ack, PollInterval: cfg.PollInterval, ResendInterval: cfg.ResendInterval, RetryInterval: cfg.PollInterval,
		SchemaAdapter: Schema(cfg.BatchSize), OffsetsAdapter: Offsets(), InitializeSchema: false,
	}, wmLogger)
	if err != nil {
		return nil, fmt.Errorf("outbox subscriber: %w", err)
	}
	pub, err := wmjs.NewPublisher(wmjs.PublisherConfig{
		Conn: nc, Logger: wmLogger, TrackMessageID: true,
		// The publisher only reads the stream name of this configurator as the
		// publish subject (watermill-nats v2.2.0); the destination topic of
		// the envelope is the subject, and the stream that captures it is a
		// deployment input (ANVILKIT_CONTROL: anvilkit.control.>).
		ConfigureStream: func(topic string) jetstream.StreamConfig { return jetstream.StreamConfig{Name: topic} },
	})
	if err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("jetstream publisher: %w", err)
	}
	counted := &countingPublisher{Publisher: pub, forwarded: forwarded, failures: failures}
	fwd, err := forwarder.NewForwarder(sub, counted, wmLogger, forwarder.Config{ForwarderTopic: ForwarderTopic, HandlerName: "anvilkit-control-outbox-forwarder", CloseTimeout: cfg.CloseTimeout, AckWhenCannotUnwrap: false})
	if err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("forwarder: %w", err)
	}
	return &Forwarder{sub: sub, fwd: fwd}, nil
}

// Run forwards until ctx ends or Close is called.
func (f *Forwarder) Run(ctx context.Context) error { return f.fwd.Run(ctx) }

// Running is closed once the router consumes.
func (f *Forwarder) Running() chan struct{} { return f.fwd.Running() }

// Close stops the router (in-flight message handling ends within the close
// timeout) and the subscriber; the connection belongs to the caller.
func (f *Forwarder) Close() error {
	err := f.fwd.Close()
	_ = f.sub.Close()
	return err
}

type countingPublisher struct {
	message.Publisher
	forwarded prometheus.Counter
	failures  prometheus.Counter
}

func (c *countingPublisher) Publish(topic string, msgs ...*message.Message) error {
	if err := c.Publisher.Publish(topic, msgs...); err != nil {
		c.failures.Inc()
		return err
	}
	c.forwarded.Add(float64(len(msgs)))
	return nil
}

// Prune deletes up to batch outbox rows the consumer group acknowledged
// (at or before its position) that are older than before, and answers the
// count. created_at is watermill's timestamp without time zone written in
// the session's zone, so the bound is compared in that zone.
func Prune(ctx context.Context, pool *pgxpool.Pool, consumerGroup string, before time.Time, batch int) (int64, error) {
	tag, err := pool.Exec(ctx, `DELETE FROM outbox WHERE ctid IN (
		SELECT o.ctid FROM outbox o JOIN outbox_offsets f ON f.consumer_group = $1
		WHERE (o.transaction_id < f.last_processed_transaction_id
		       OR (o.transaction_id = f.last_processed_transaction_id AND o."offset" <= COALESCE(f.offset_acked, 0)))
		  AND o.created_at < ($2::timestamptz AT TIME ZONE current_setting('TimeZone')) LIMIT $3)`, consumerGroup, before, batch)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Oldest is the age of the oldest row the consumer group has not
// acknowledged (0 when none): the forwarding lag.
func Oldest(ctx context.Context, pool *pgxpool.Pool, consumerGroup string) (time.Duration, error) {
	var seconds float64
	err := pool.QueryRow(ctx, `SELECT COALESCE(EXTRACT(EPOCH FROM (LOCALTIMESTAMP - min(o.created_at))), 0)::float8 FROM outbox o
		LEFT JOIN outbox_offsets f ON f.consumer_group = $1
		WHERE f.consumer_group IS NULL OR o.transaction_id > f.last_processed_transaction_id
		   OR (o.transaction_id = f.last_processed_transaction_id AND o."offset" > COALESCE(f.offset_acked, 0))`, consumerGroup).Scan(&seconds)
	return time.Duration(seconds * float64(time.Second)), err
}
