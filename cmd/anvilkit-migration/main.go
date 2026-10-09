// anvilkit-migration is Control's migration Job (architecture.md naming: job
// kind "migration"): it applies the anvilkit_control schema with the
// migrator role from the service-owned source (internal/migrate/sql) and
// exits. The runtime service never executes DDL; this binary is the only
// writer of the schema.
//
//	anvilkit-migration (-dsn "$ANVILKIT_MIGRATION_DSN" | -dsn-file "$ANVILKIT_MIGRATION_DSN_FILE") [-development] [-to N] [-status]
//
// The DSN comes from exactly one source: the value, or a mounted secret
// file (the OpenBao CSI volume, P0.6) read once. Outside development it
// must name sslmode=verify-full; -development (or
// ANVILKIT_MIGRATION_DEVELOPMENT=true) admits the plaintext DSN of the
// development foundation (DEVELOPMENT_ONLY).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/config"
	"github.com/ancyloce/anvilkit-agent-control/internal/migrate"
)

func main() {
	var (
		dsn         = flag.String("dsn", os.Getenv("ANVILKIT_MIGRATION_DSN"), "migrator-role DSN of anvilkit_control (default $ANVILKIT_MIGRATION_DSN)")
		dsnFile     = flag.String("dsn-file", os.Getenv("ANVILKIT_MIGRATION_DSN_FILE"), "mounted secret file holding the migrator-role DSN (default $ANVILKIT_MIGRATION_DSN_FILE)")
		development = flag.Bool("development", os.Getenv("ANVILKIT_MIGRATION_DEVELOPMENT") == "true", "DEVELOPMENT_ONLY: admit a DSN without sslmode=verify-full (default $ANVILKIT_MIGRATION_DEVELOPMENT == true)")
		to          = flag.Int64("to", -1, "roll back to this version instead of applying pending migrations")
		status      = flag.Bool("status", false, "print the applied version and exit")
	)
	flag.Parse()
	resolved, err := migrationDSN(*dsn, *dsnFile, *development)
	if err != nil {
		fmt.Fprintln(os.Stderr, "anvilkit-migration:", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := migrate.Open(resolved)
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

// migrationDSN resolves the migrator DSN from exactly one source (the value
// or the secret file) and applies the data-plane TLS rule (P0.6): outside
// development it must name sslmode=verify-full. No error echoes the DSN.
func migrationDSN(dsn, dsnFile string, development bool) (string, error) {
	switch {
	case dsn != "" && dsnFile != "":
		return "", errors.New("-dsn (ANVILKIT_MIGRATION_DSN) and -dsn-file (ANVILKIT_MIGRATION_DSN_FILE) are mutually exclusive")
	case dsnFile != "":
		v, err := config.ReadSecretFile("-dsn-file", dsnFile)
		if err != nil {
			return "", err
		}
		dsn = v
	case dsn == "":
		return "", errors.New("-dsn (ANVILKIT_MIGRATION_DSN) or -dsn-file (ANVILKIT_MIGRATION_DSN_FILE) is required")
	}
	if err := config.CheckPostgresTLS("dsn", dsn, development); err != nil {
		return "", fmt.Errorf("%w; the development foundation's DSN needs -development or ANVILKIT_MIGRATION_DEVELOPMENT=true (DEVELOPMENT_ONLY)", err)
	}
	return dsn, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "anvilkit-migration:", err)
	os.Exit(1)
}
