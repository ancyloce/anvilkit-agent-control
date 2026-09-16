// anvilkit-migration is Control's migration Job (architecture.md naming: job
// kind "migration"): it applies the anvilkit_control schema with the
// migrator role from the service-owned source (internal/migrate/sql) and
// exits. The runtime service never executes DDL; this binary is the only
// writer of the schema.
//
//	anvilkit-migration -dsn "$ANVILKIT_MIGRATION_DSN" [-to N] [-status]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/migrate"
)

func main() {
	var (
		dsn    = flag.String("dsn", os.Getenv("ANVILKIT_MIGRATION_DSN"), "migrator-role DSN of anvilkit_control (default $ANVILKIT_MIGRATION_DSN)")
		to     = flag.Int64("to", -1, "roll back to this version instead of applying pending migrations")
		status = flag.Bool("status", false, "print the applied version and exit")
	)
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "anvilkit-migration: -dsn (or ANVILKIT_MIGRATION_DSN) is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := migrate.Open(*dsn)
	if err != nil {
		fail(err)
	}
	defer db.Close()

	var version int64
	switch {
	case *status:
		version, err = migrate.Version(ctx, db)
	case *to >= 0:
		version, err = migrate.DownTo(ctx, db, *to)
	default:
		version, err = migrate.Up(ctx, db)
	}
	if err != nil {
		fail(err)
	}
	fmt.Printf("domain=%s version=%d\n", migrate.Domain, version)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "anvilkit-migration:", err)
	os.Exit(1)
}
