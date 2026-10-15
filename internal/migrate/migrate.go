// Package migrate applies the anvilkit_control schema with goose/v3 (A07):
// the service-owned migration source of Control (architecture.md
// "Repositories and ownership": migrations belong to the service that owns
// the schema and run through a separate migration Job). It is the single
// migration-version authority of anvilkit_control; the runtime service
// never runs DDL and never imports this package.
package migrate

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed all:sql
var sqlFS embed.FS

// Domain is the owned database this source migrates.
const Domain = "control"

// Latest is the migration version an empty database reaches from
// 00001_init.sql plus the forward migrations of this build.
const Latest int64 = 18

// Safety bounds of a migration run (P3.7, plan 0004).
type Options struct {
	// LockTimeout bounds every statement's wait for a table lock: a DDL
	// behind a long transaction fails (SQLSTATE 55P03) instead of queueing
	// every later reader of the table behind its pending exclusive lock.
	LockTimeout time.Duration
	// StatementTimeout bounds one statement (a long backfill or index build).
	StatementTimeout time.Duration
	// Retries is how often a run that hit LockTimeout is retried, RetryDelay
	// apart (growing linearly); goose rolled the failed migration back.
	Retries    int
	RetryDelay time.Duration
	// LockWait bounds the wait for the session's advisory lock: a second
	// runner waits for the first (one applies, both end at one version)
	// and gives up after LockWait.
	LockWait time.Duration
	// OnRetry observes each retry (logs and tests).
	OnRetry func(attempt int, err error)
}

// DefaultOptions are the Job's bounds.
func DefaultOptions() Options {
	return Options{LockTimeout: 5 * time.Second, StatementTimeout: 15 * time.Minute, Retries: 5, RetryDelay: 2 * time.Second, LockWait: 5 * time.Minute}
}

// advisoryLockID is the session lock of Control's schema (a database-scoped
// PostgreSQL advisory lock; another domain's runner uses its own id).
const advisoryLockID int64 = 0x616e76696c6b6301 // "anvilkc" + 1

// Source is the embedded migration directory (00001_init.sql and the
// forward migrations), the same files the Job applies; tests install a
// disposable database from it exactly as the Job does.
func Source() fs.FS {
	sub, err := fs.Sub(sqlFS, "sql")
	if err != nil {
		panic(err)
	}
	return sub
}

// Open connects with the migrator role DSN through the pgx stdlib adapter,
// which is what goose drives; the service keeps using pgx/v5 natively.
// Every session carries the default bounds' lock_timeout and
// statement_timeout.
func Open(dsn string) (*sql.DB, error) { return OpenWith(dsn, DefaultOptions()) }

// OpenWith is Open with explicit bounds.
func OpenWith(dsn string, o Options) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("migrate: parse dsn: %w", err)
	}
	if o.LockTimeout > 0 {
		cfg.RuntimeParams["lock_timeout"] = fmt.Sprintf("%dms", o.LockTimeout.Milliseconds())
	}
	if o.StatementTimeout > 0 {
		cfg.RuntimeParams["statement_timeout"] = fmt.Sprintf("%dms", o.StatementTimeout.Milliseconds())
	}
	return stdlib.OpenDB(*cfg), nil
}

func provider(db *sql.DB, o Options) (*goose.Provider, error) {
	return providerFor(db, Source(), o)
}

func providerFor(db *sql.DB, src fs.FS, o Options) (*goose.Provider, error) {
	if _, err := fs.Stat(src, "00001_init.sql"); err != nil {
		return nil, fmt.Errorf("migrate: %s has no 00001_init.sql", Domain)
	}
	wait := o.LockWait
	if wait <= 0 {
		wait = DefaultOptions().LockWait
	}
	// goose polls pg_try_advisory_lock every period seconds, failureThreshold times.
	const period = 1
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(advisoryLockID), lock.WithLockTimeout(period, uint64(max(1, wait/time.Second))))
	if err != nil {
		return nil, err
	}
	return goose.NewProvider(goose.DialectPostgres, db, src, goose.WithVerbose(false), goose.WithSessionLocker(locker))
}

// LockTimedOut reports whether err is a statement's lock_timeout (SQLSTATE 55P03).
func LockTimedOut(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "55P03"
}

// Up applies every pending migration and returns the resulting version.
func Up(ctx context.Context, db *sql.DB) (int64, error) { return UpWith(ctx, db, DefaultOptions()) }

// UpWith is Up with explicit bounds: a run whose statement hit lock_timeout
// is retried up to o.Retries times (goose rolled that migration back); any
// other error ends the run.
func UpWith(ctx context.Context, db *sql.DB, o Options) (int64, error) {
	return upFS(ctx, db, Source(), o)
}

func upFS(ctx context.Context, db *sql.DB, src fs.FS, o Options) (int64, error) {
	p, err := providerFor(db, src, o)
	if err != nil {
		return 0, err
	}
	for attempt := 1; ; attempt++ {
		_, err := p.Up(ctx)
		if err == nil {
			break
		}
		if !LockTimedOut(err) || attempt > o.Retries {
			return 0, fmt.Errorf("migrate: up %s: %w", Domain, err)
		}
		if o.OnRetry != nil {
			o.OnRetry(attempt, err)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(o.RetryDelay * time.Duration(attempt)):
		}
	}
	return p.GetDBVersion(ctx)
}

// DownTo rolls the schema back to the given version; version 0 is an empty
// schema. It destroys data: the Job calls it only with -destructive.
func DownTo(ctx context.Context, db *sql.DB, version int64) (int64, error) {
	p, err := provider(db, DefaultOptions())
	if err != nil {
		return 0, err
	}
	if _, err := p.DownTo(ctx, version); err != nil {
		return 0, fmt.Errorf("migrate: down %s: %w", Domain, err)
	}
	return p.GetDBVersion(ctx)
}

// ExpectedVersion is the highest version of the embedded migrations: what
// a binary's schema check compares the database against (P1.4).
func ExpectedVersion() (int64, error) {
	entries, err := fs.ReadDir(Source(), ".")
	if err != nil {
		return 0, err
	}
	var max int64
	for _, e := range entries {
		name := e.Name()
		var v int64
		for _, ch := range name {
			if ch < '0' || ch > '9' {
				break
			}
			v = v*10 + int64(ch-'0')
		}
		if v > max {
			max = v
		}
	}
	if max == 0 {
		return 0, fmt.Errorf("no embedded migration")
	}
	return max, nil
}

// Version reports the applied version without changing anything.
func Version(ctx context.Context, db *sql.DB) (int64, error) {
	p, err := provider(db, DefaultOptions())
	if err != nil {
		return 0, err
	}
	return p.GetDBVersion(ctx)
}
