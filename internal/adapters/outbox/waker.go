package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Stream is the JetStream stream that captures Control's subjects
// (anvilkit.control.>), a deployment input.
const Stream = "ANVILKIT_CONTROL"

// maxEnvelopeBytes bounds a message the waker decodes (the stream's
// reviewed message bound is 1 MiB; a wake envelope is a few hundred bytes).
const maxEnvelopeBytes = 64 << 10

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// Waker is the consumer of operation.changed (P2.8; event catalog
// "authorized API SSE projection"): every replica reads the subject
// through its own ordered JetStream consumer from the newest message and
// wakes the event streams of the event's operation through wake (the
// notifier of P1.4). A wake carries no state: the stream re-reads the
// committed events after its cursor under the caller's authorization, so a
// duplicate, a reordered or a stale message causes one more read and never
// a wrong one; a message that is not a well-formed operation.changed of
// this producer is counted and dropped.
type Waker struct {
	js       jetstream.JetStream
	wake     func(operationID string)
	log      *slog.Logger
	received prometheus.Counter
	rejected prometheus.Counter
	retry    time.Duration
}

func NewWaker(nc *nats.Conn, wake func(operationID string), log *slog.Logger, received, rejected prometheus.Counter) (*Waker, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return &Waker{js: js, wake: wake, log: log, received: received, rejected: rejected, retry: time.Second}, nil
}

// Run consumes until ctx ends. The ordered consumer recreates itself after
// a gap or a lost connection; a consumer that cannot be created (the
// stream is absent, the connection is down) is retried with backoff while
// the streams fall back to the LISTEN wake and their poll.
func (w *Waker) Run(ctx context.Context) {
	backoff := w.retry
	for ctx.Err() == nil {
		cons, err := w.js.OrderedConsumer(ctx, Stream, jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{domain.SubjectOperationChanged}, DeliverPolicy: jetstream.DeliverNewPolicy,
		})
		if err == nil {
			var cc jetstream.ConsumeContext
			if cc, err = cons.Consume(func(m jetstream.Msg) { w.handle(m.Data()) }); err == nil {
				backoff = w.retry
				select {
				case <-ctx.Done():
				case <-cc.Closed():
				}
				cc.Stop()
				continue
			}
		}
		if ctx.Err() != nil {
			return
		}
		w.log.Warn("operation event consumer unavailable; streams wake by LISTEN and poll until it returns", "error", err, "retryIn", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (w *Waker) handle(data []byte) {
	opID, err := WakeTarget(data)
	if err != nil {
		w.rejected.Inc()
		w.log.Warn("operation event consumer dropped a message", "reason", err.Error())
		return
	}
	w.received.Inc()
	w.wake(opID)
}

// WakeTarget validates a message as an operation.changed envelope of this
// producer and answers the operation it names.
func WakeTarget(data []byte) (string, error) {
	if len(data) > maxEnvelopeBytes {
		return "", errors.New("envelope over the bound")
	}
	var env domain.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return "", errors.New("not an envelope")
	}
	switch {
	case env.SchemaVersion != 1:
		return "", errors.New("schemaVersion is not 1")
	case env.EventType != domain.EventOperationChanged || env.Subject != domain.SubjectOperationChanged:
		return "", errors.New("not operation.changed")
	case env.Producer != domain.EventProducer || env.AggregateType != domain.AggregateOperation:
		return "", errors.New("producer or aggregate type mismatch")
	case !idPattern.MatchString(env.AggregateID) || !idPattern.MatchString(env.TenantID):
		return "", errors.New("malformed identity")
	}
	return env.AggregateID, nil
}
