package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMigrationDSN (P0.6): exactly one DSN source; outside development the
// DSN must name sslmode=verify-full; no error echoes the DSN.
func TestMigrationDSN(t *testing.T) {
	const verified = "postgres://anvilkit_control_migrator:pw-in-dsn@db.internal:5432/anvilkit_control?sslmode=verify-full"
	const plain = "postgres://anvilkit_control_migrator:pw-in-dsn@127.0.0.1:25432/anvilkit_control?sslmode=disable"
	file := func(body string) string {
		path := filepath.Join(t.TempDir(), "dsn")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return path
	}
	for _, tc := range []struct {
		name, dsn, dsnFile string
		development        bool
		want, err          string
	}{
		{name: "value verify-full", dsn: verified, want: verified},
		{name: "file verify-full, trimmed", dsnFile: file(verified + "\n"), want: verified},
		{name: "value disable outside development", dsn: plain, err: `dsn: sslmode must be verify-full outside development (got "disable")`},
		{name: "file require outside development", dsnFile: file("host=db password=pw-in-dsn sslmode=require\n"), err: `(got "require")`},
		{name: "missing sslmode outside development", dsn: "postgres://anvilkit_control_migrator:pw-in-dsn@db.internal/anvilkit_control", err: `(got "")`},
		{name: "value disable in development", dsn: plain, development: true, want: plain},
		{name: "file disable in development", dsnFile: file(plain), development: true, want: plain},
		{name: "both sources", dsn: verified, dsnFile: file(verified), err: "mutually exclusive"},
		{name: "no source", err: "is required"},
		{name: "empty file", dsnFile: file("\n"), err: "-dsn-file:"},
		{name: "missing file", dsnFile: filepath.Join(t.TempDir(), "absent"), err: "-dsn-file: open "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := migrationDSN(tc.dsn, tc.dsnFile, tc.development)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				require.NotContains(t, err.Error(), "pw-in-dsn", "the DSN carries the password and is never echoed")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
