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
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed all:sql
var sqlFS embed.FS

// Domain is the owned database this source migrates.
const Domain = "control"

// Latest is the migration version an empty database reaches from
// 00001_init.sql plus the forward migrations of this build.
const Latest int64 = 8

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
func Open(dsn string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("migrate: parse dsn: %w", err)
	}
	return stdlib.OpenDB(*cfg), nil
}

func provider(db *sql.DB) (*goose.Provider, error) {
	src := Source()
	if _, err := fs.Stat(src, "00001_init.sql"); err != nil {
		return nil, fmt.Errorf("migrate: %s has no 00001_init.sql", Domain)
	}
	return goose.NewProvider(goose.DialectPostgres, db, src, goose.WithVerbose(false))
}

// Up applies every pending migration and returns the resulting version.
func Up(ctx context.Context, db *sql.DB) (int64, error) {
	p, err := provider(db)
	if err != nil {
		return 0, err
	}
	if _, err := p.Up(ctx); err != nil {
		return 0, fmt.Errorf("migrate: up %s: %w", Domain, err)
	}
	return p.GetDBVersion(ctx)
}

// DownTo rolls the schema back to the given version; version 0 is an empty schema.
func DownTo(ctx context.Context, db *sql.DB, version int64) (int64, error) {
	p, err := provider(db)
	if err != nil {
		return 0, err
	}
	if _, err := p.DownTo(ctx, version); err != nil {
		return 0, fmt.Errorf("migrate: down %s: %w", Domain, err)
	}
	return p.GetDBVersion(ctx)
}

// Version reports the applied version without changing anything.
func Version(ctx context.Context, db *sql.DB) (int64, error) {
	p, err := provider(db)
	if err != nil {
		return 0, err
	}
	return p.GetDBVersion(ctx)
}
