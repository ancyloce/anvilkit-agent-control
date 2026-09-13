// Package storage implements the selected CD-02 persistence transactions.
// Callers establish authorization and resolve external evidence before invoking
// these internal methods. This package performs database I/O only; it provides
// no admission, activation or synthetic-seeding RPC.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

var (
	ErrInvalid     = errors.New("INVALID_ARGUMENT")
	ErrConflict    = errors.New("IDEMPOTENCY_CONFLICT")
	ErrRevision    = errors.New("REVISION_CONFLICT")
	ErrNotFound    = errors.New("NOT_FOUND")
	ErrLeaseLost   = errors.New("DEPENDENCY_UNAVAILABLE: authorization lease lost")
	errRuntimeRole = errors.New("storage: login must have only the Control runtime role")
)

const transactionRetryMax = 3
const refreshLeaseSeconds = 12
const authorizationReadsPerSecond = 20
const authorizationReadBurst = 40

type Store struct {
	pool                  *pgxpool.Pool
	idSchema, eventSchema *jsonschema.Schema
	localEventLogger      func(string, int64)
}

type retainedOnly struct{}

func (retainedOnly) Load(string) (any, error) { return nil, errors.New("schema is not retained") }

// Open verifies contracts and each physical connection's restricted login.
// Pool sizing may be set with pgxpool DSN parameters; credentials never enter errors.
func Open(ctx context.Context, dsn string) (*Store, error) {
	return openPool(ctx, dsn, 0)
}

// OpenReserved keeps two connections unavailable to new-intake requests.
// Cancellation and its authorization reads use this independent local lane.
func OpenReserved(ctx context.Context, dsn string) (*Store, error) { return openPool(ctx, dsn, 2) }

func openPool(ctx context.Context, dsn string, maximum int32) (*Store, error) {
	id, event, err := schemas()
	if err != nil {
		return nil, err
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("storage: invalid database configuration")
	}
	config.ConnConfig.RuntimeParams["anvilkit.service_context"] = "control"
	if maximum > 0 {
		config.MaxConns = maximum
	}
	config.ConnConfig.RuntimeParams["anvilkit.tenant_id"] = ""
	delete(config.ConnConfig.RuntimeParams, "role")
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		var allowed bool
		err := conn.QueryRow(ctx, `SELECT NOT rolsuper AND NOT rolbypassrls AND NOT rolcreaterole
			AND NOT rolcreatedb AND NOT rolreplication
			AND pg_has_role(session_user, 'anvilkit_control_rw', 'USAGE')
			AND NOT pg_has_role(session_user, 'anvilkit_control_migrator', 'MEMBER')
			AND NOT pg_has_role(session_user, 'anvilkit_api_ro', 'MEMBER')
			FROM pg_roles WHERE rolname=session_user`).Scan(&allowed)
		if err != nil {
			return err
		}
		if !allowed {
			return errRuntimeRole
		}
		_, err = conn.Exec(ctx, "SET ROLE anvilkit_control_rw")
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.New("storage: database pool unavailable")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		if errors.Is(err, errRuntimeRole) {
			return nil, errRuntimeRole
		}
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) {
			return nil, fmt.Errorf("storage: restricted database connection unavailable (SQLSTATE %s)", pgerr.Code)
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("storage: restricted database connection unavailable: %w", ctx.Err())
		}
		return nil, errors.New("storage: restricted database connection unavailable")
	}
	return &Store{pool: pool, idSchema: id, eventSchema: event}, nil
}

func (s *Store) Close() { s.pool.Close() }

func schemas() (*jsonschema.Schema, *jsonschema.Schema, error) {
	inputs, err := contracts.VerifiedInputs()
	if err != nil {
		return nil, nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(retainedOnly{})
	compiler.AssertFormat()
	// operation-view-v1 is compiled with the payload schema because the operation.lifecycle payload
	// references its LocalCheckResult definition, and preparation-v1 because both reference its
	// PreparationProjection (S0-T04, 2026-09-12); the retained loader resolves no other reference.
	names := []string{"common-v1.schema.json", "agent-enums-v1.schema.json", "preparation-v1.schema.json", "operation-view-v1.schema.json", "operation-event-v1.schema.json", "operation-event-payloads-v1.schema.json"}
	for _, name := range names {
		var document map[string]any
		if err := json.Unmarshal(inputs[name], &document); err != nil {
			return nil, nil, err
		}
		if err := compiler.AddResource(document["$id"].(string), document); err != nil {
			return nil, nil, err
		}
	}
	var limits struct {
		Entries []struct {
			Key   string
			Value json.RawMessage
		}
	}
	// Verify consumed constants; other limits have other value types.
	if err := json.Unmarshal(inputs["pilot-limits-v1.json"], &limits); err != nil {
		return nil, nil, err
	}
	matched := 0
	for _, limit := range limits.Entries {
		if limit.Key == "control.transactionRetryMax" && string(limit.Value) == strconv.Itoa(transactionRetryMax) {
			matched++
		}
		if limit.Key == "authorization.singleFlightLeaseSeconds" && string(limit.Value) == strconv.Itoa(refreshLeaseSeconds) {
			matched++
		}
		if limit.Key == "authorization.readsPerSecond" && string(limit.Value) == strconv.Itoa(authorizationReadsPerSecond) {
			matched++
		}
		if limit.Key == "authorization.readBurst" && string(limit.Value) == strconv.Itoa(authorizationReadBurst) {
			matched++
		}
	}
	if matched != 4 {
		return nil, nil, errors.New("storage: retained transaction limits disagree")
	}
	id, err := compiler.Compile("urn:anvilkit:values:v1#/$defs/id")
	if err != nil {
		return nil, nil, err
	}
	event, err := compiler.Compile("urn:anvilkit:operation-event-payloads:v1")
	return id, event, err
}

func (s *Store) validIDs(ids ...string) bool {
	for _, id := range ids {
		if s.idSchema.Validate(id) != nil {
			return false
		}
	}
	return true
}

func hash(raw []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(raw)) }

// Canonical returns the RFC 8785 form of already validated JSON bytes.
func Canonical(raw []byte) ([]byte, error) {
	result, err := jsoncanonicalizer.Transform(raw)
	if err != nil {
		return nil, ErrInvalid
	}
	return result, nil
}
func canonical(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, ErrInvalid
	}
	result, err := jsoncanonicalizer.Transform(raw)
	if err != nil {
		return nil, ErrInvalid
	}
	return result, nil
}

// transact retries only PostgreSQL's explicit transaction-abort SQLSTATEs.
// It never retries an unknown commit result or a transport failure. Its private
// callbacks contain SQL only and return no success to callers before COMMIT.
func (s *Store) transact(ctx context.Context, work func(pgx.Tx) error) error {
	for attempt := 0; ; attempt++ {
		err := func() error {
			tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				return err
			}
			defer func() {
				cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
				defer cancel()
				_ = tx.Rollback(cleanup)
			}()
			if err := work(tx); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}()
		if !retryable(err) || attempt >= transactionRetryMax || ctx.Err() != nil {
			return err
		}
	}
}

func retryable(err error) bool {
	var pgerr *pgconn.PgError
	return errors.As(err, &pgerr) && slices.Contains([]string{"40001", "40P01"}, pgerr.Code)
}

func boundedObject(raw []byte, maximum int) (map[string]any, []byte, error) {
	doc, err := contracts.DecodeObject(raw, maximum)
	if err != nil {
		return nil, nil, ErrInvalid
	}
	encoded, err := canonical(doc)
	if err != nil || len(encoded) > maximum {
		return nil, nil, ErrInvalid
	}
	return doc, encoded, nil
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func digestShape(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	return strings.Trim(value[7:], "0123456789abcdef") == ""
}
