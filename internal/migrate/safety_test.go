package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// safetyDB starts a disposable PostgreSQL with Control's roles and returns
// the migrator's DSN.
func safetyDB(t *testing.T) (context.Context, string) {
	t.Helper()
	if os.Getenv("ANVILKIT_SKIP_DOCKER_TESTS") != "" {
		t.Skip("ANVILKIT_SKIP_DOCKER_TESTS set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	pg, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithUsername("postgres"), postgres.WithPassword("postgres"), postgres.WithDatabase("postgres"),
		postgres.BasicWaitStrategies())
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, pg)
	adminDSN, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	admin, err := sql.Open("pgx", adminDSN)
	require.NoError(t, err)
	defer admin.Close()
	for _, stmt := range []string{
		"CREATE ROLE anvilkit_control_app LOGIN PASSWORD 'app'",
		"CREATE ROLE anvilkit_control_migrator LOGIN PASSWORD 'migrator'",
		"CREATE DATABASE anvilkit_control OWNER anvilkit_control_migrator",
	} {
		_, err := admin.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	host, err := pg.Host(ctx)
	require.NoError(t, err)
	port, err := pg.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	return ctx, fmt.Sprintf("postgres://anvilkit_control_migrator:migrator@%s:%s/anvilkit_control?sslmode=disable", host, port.Port())
}

// P3.7 AC4: two runners started at once on an empty database serialize on
// the session's advisory lock - each ends at the latest version and every
// migration is recorded once - and a runner whose lock is held elsewhere
// gives up after its bounded wait instead of waiting forever.
func TestConcurrentRunnersSerializeOnTheAdvisoryLock(t *testing.T) {
	ctx, dsn := safetyDB(t)
	var wg sync.WaitGroup
	versions := make([]int64, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db, err := Open(dsn)
			if err != nil {
				errs[i] = err
				return
			}
			defer db.Close()
			versions[i], errs[i] = Up(ctx, db)
		}()
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, []int64{Latest, Latest}, versions, "both runners end at one version")
	db, err := Open(dsn)
	require.NoError(t, err)
	defer db.Close()
	var doubled int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT version_id FROM goose_db_version WHERE is_applied GROUP BY version_id HAVING count(*) > 1) d").Scan(&doubled))
	require.Zero(t, doubled, "no migration applied twice")

	// The lock held by another session: the runner's wait is bounded.
	holder, err := db.Conn(ctx)
	require.NoError(t, err)
	defer holder.Close()
	_, err = holder.ExecContext(ctx, "SELECT pg_advisory_lock($1)", advisoryLockID)
	require.NoError(t, err)
	o := DefaultOptions()
	o.LockWait = 2 * time.Second
	started := time.Now()
	_, err = UpWith(ctx, db, o)
	require.Error(t, err, "a held schema lock is not waited on past LockWait")
	require.Less(t, time.Since(started), 30*time.Second)
	_, err = holder.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", advisoryLockID)
	require.NoError(t, err)
	version, err := UpWith(ctx, db, o)
	require.NoError(t, err)
	require.Equal(t, Latest, version)
}

// P3.7 AC4: a migration blocked by a long transaction fails at lock_timeout
// and is retried; while it waits for the long transaction, readers of the
// table are never queued behind its pending exclusive lock for longer than
// the timeout (no stacking), and once the transaction ends the migration
// applies.
func TestALockTimeoutIsRetriedNotStacked(t *testing.T) {
	ctx, dsn := safetyDB(t)
	first := fstest.MapFS{"00001_init.sql": {Data: []byte("-- +goose Up\nCREATE TABLE t (x int);\nINSERT INTO t VALUES (1);\n-- +goose Down\nDROP TABLE t;\n")}}
	second := fstest.MapFS{"00001_init.sql": first["00001_init.sql"],
		"00002_add.sql": {Data: []byte("-- +goose Up\nALTER TABLE t ADD COLUMN y int;\n-- +goose Down\nALTER TABLE t DROP COLUMN y;\n")}}
	o := Options{LockTimeout: 300 * time.Millisecond, StatementTimeout: time.Minute, Retries: 20, RetryDelay: 100 * time.Millisecond, LockWait: 30 * time.Second}
	db, err := OpenWith(dsn, o)
	require.NoError(t, err)
	defer db.Close()
	v, err := upFS(ctx, db, first, o)
	require.NoError(t, err)
	require.Equal(t, int64(1), v)

	// The long transaction: it read t and keeps its ACCESS SHARE lock.
	plain, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer plain.Close()
	long, err := plain.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = long.ExecContext(ctx, "SELECT * FROM t")
	require.NoError(t, err)

	var retries atomic.Int32
	o.OnRetry = func(int, error) { retries.Add(1) }
	done := make(chan error, 1)
	go func() {
		_, err := upFS(ctx, db, second, o)
		done <- err
	}()
	require.Eventually(t, func() bool { return retries.Load() >= 1 }, 20*time.Second, 50*time.Millisecond, "the blocked ALTER failed at lock_timeout and was retried")
	// Readers keep reading while the migration retries: none waits behind
	// it for longer than one lock_timeout.
	for range 5 {
		readStart := time.Now()
		var n int
		require.NoError(t, plain.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n))
		require.Less(t, time.Since(readStart), 2*time.Second, "a reader stacked behind the pending migration")
	}
	require.NoError(t, long.Commit())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the migration did not apply after the long transaction ended")
	}
	var cols int
	require.NoError(t, plain.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_name = 't' AND column_name = 'y'").Scan(&cols))
	require.Equal(t, 1, cols)
}
