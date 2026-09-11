// Package logging emits bounded, content-free Control execution records.
package logging

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Logger struct {
	output  *log.Logger
	version string
}

func New(output io.Writer, version string) *Logger { return &Logger{log.New(output, "", 0), version} }
func InstanceID() string                           { return "control:" + strconv.Itoa(os.Getpid()) }

func (l *Logger) Record(event, id string, fields map[string]any) {
	record := map[string]any{"schemaVersion": 1, "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "severity": "INFO", "eventName": event, "origin": "service", "service.name": "anvilkit-agent-control", "service.version": l.version, "service.instance.id": InstanceID(), "environment": "local"}
	if id != "" {
		record["operationId"] = id
	}
	for key, value := range fields {
		record[key] = value
	}
	if record["outcome"] != "ok" && record["outcome"] != nil {
		record["severity"] = "WARN"
	}
	raw, _ := json.Marshal(record)
	l.output.Print(string(raw))
}

// SDK strings, error chains and payloads never enter structured log fields.
func (l *Logger) Debug(string, ...any) {}
func (l *Logger) Info(string, ...any)  {}
func (l *Logger) Warn(string, ...any)  { l.Error("") }
func (l *Logger) Error(string, ...any) {
	l.Record("health.transition", "", map[string]any{"outcome": "unavailable", "error.type": "unavailable", "error.code": "DEPENDENCY_UNAVAILABLE"})
}

type callKey struct{}
type callIdentity struct {
	operationID string
	attempt     atomic.Int32
}

func OperationCall(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, callKey{}, &callIdentity{operationID: id})
}

func (l *Logger) UnaryClient(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	start := time.Now()
	id := ""
	attempt := int32(1)
	if call, ok := ctx.Value(callKey{}).(*callIdentity); ok {
		id = call.operationID
		attempt = call.attempt.Add(1)
	}
	err := invoke(ctx, method, req, reply, cc, opts...)
	fields := map[string]any{"caller": "anvilkit-agent-control", "callee": "temporal", "protocol": "grpc", "routeTemplate": method, "transportAttempt": attempt, "durationMs": time.Since(start).Milliseconds(), "outcome": "ok", "rpc.code": status.Code(err).String()}
	if err != nil {
		fields["outcome"] = "unavailable"
		fields["error.type"] = "unavailable"
		fields["error.code"] = "DEPENDENCY_UNAVAILABLE"
	}
	l.Record("rpc.client.completed", id, fields)
	return err
}

func (l *Logger) EventAppended(id string, sequence int64) {
	l.Record("event.appended", id, map[string]any{"eventSeq": strconv.FormatInt(sequence, 10)})
}

// Write implements the HTTP server's diagnostic sink without retaining remote
// addresses, TLS errors or other unstructured network-supplied text.
func (l *Logger) Write(data []byte) (int, error) { l.Error(""); return len(data), nil }
