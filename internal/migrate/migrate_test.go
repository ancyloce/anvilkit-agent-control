package migrate_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ancyloce/anvilkit-agent-control/internal/migrate"
)

// pgImage is the business PostgreSQL line of the baseline (technology.md:
// PostgreSQL 17.11). The exact patch is whatever the pinned image resolves to.
const pgImage = "postgres:17-alpine"

// TestControlSchema proves, on a real PostgreSQL, that anvilkit_control
// installs from an empty database with 00001_init.sql plus its forward
// migrations, that the runtime role receives DML but no DDL, that the
// migrations are reversible, and that 00002 migrates existing accepted
// stages forward without inventing original bytes.
func TestControlSchema(t *testing.T) {
	if os.Getenv("ANVILKIT_SKIP_DOCKER_TESTS") != "" {
		t.Skip("ANVILKIT_SKIP_DOCKER_TESTS set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	pg, err := postgres.Run(ctx, pgImage,
		postgres.WithUsername("postgres"), postgres.WithPassword("postgres"), postgres.WithDatabase("postgres"),
		postgres.BasicWaitStrategies())
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, pg)

	adminDSN, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	admin, err := sql.Open("pgx", adminDSN)
	require.NoError(t, err)
	defer admin.Close()

	const app, migrator, dbName = "anvilkit_control_app", "anvilkit_control_migrator", "anvilkit_control"
	for _, stmt := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'app'", app),
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'migrator'", migrator),
		fmt.Sprintf("CREATE DATABASE %s OWNER %s", dbName, migrator),
	} {
		_, err := admin.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	host, err := pg.Host(ctx)
	require.NoError(t, err)
	port, err := pg.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	dsn := func(role, pw string) string {
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", role, pw, host, port.Port(), dbName)
	}

	db, err := migrate.Open(dsn(migrator, "migrator"))
	require.NoError(t, err)
	defer db.Close()

	version, err := migrate.Up(ctx, db)
	require.NoError(t, err)
	require.Equal(t, migrate.Latest, version, "00001_init.sql plus the forward migrations")
	version, err = migrate.Version(ctx, db)
	require.NoError(t, err)
	require.Equal(t, migrate.Latest, version, "Version reports the applied version")

	var tables int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'goose_db_version'").Scan(&tables))
	require.Greater(t, tables, 5, "owned tables exist")

	// The runtime role can write rows but cannot change the schema.
	appDB, err := migrate.Open(dsn(app, "app"))
	require.NoError(t, err)
	defer appDB.Close()
	_, err = appDB.ExecContext(ctx, "CREATE TABLE forbidden (x int)")
	require.Error(t, err, "app role must not hold DDL")
	require.NoError(t, appDB.QueryRowContext(ctx, "SELECT count(*) FROM goose_db_version").Scan(new(int)),
		"app role may read (grants applied)")
	_, err = appDB.ExecContext(ctx, "DELETE FROM goose_db_version")
	require.Error(t, err, "app role has no DELETE on migration bookkeeping")

	// Reversible: down to empty, up again lands on the same version.
	version, err = migrate.DownTo(ctx, db, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), version)
	version, err = migrate.Up(ctx, db)
	require.NoError(t, err)
	require.Equal(t, migrate.Latest, version)

	forwardMigrateExistingStage(ctx, t, db)
	grantPolicyBarrier(ctx, t, db)
	// 00011: tool dispatches bind their argument digest and effect class.
	var n int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'dispatches' AND column_name IN ('argument_digest', 'side_effecting')`).Scan(&n))
	require.Equal(t, 2, n)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pg_constraint WHERE conname = 'dispatches_argument_digest_check'`).Scan(&n))
	require.Equal(t, 1, n)
	// 00012: preview builds keep their committed projection with its checks.
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pg_constraint WHERE conname IN ('previews_saved_check', 'previews_conflict_check', 'previews_built_check', 'previews_failed_check')`).Scan(&n))
	require.Equal(t, 4, n)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'operations' AND column_name = 'source_handle'`).Scan(&n))
	require.Equal(t, 1, n)
	// 00013: releases keep their committed projection with its checks and
	// a release operation names its source operation and package version.
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM pg_constraint WHERE conname IN ('releases_subject_check', 'releases_review_check', 'releases_activated_check', 'releases_failed_check')`).Scan(&n))
	require.Equal(t, 4, n)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'operations' AND column_name IN ('source_operation_id', 'package_version')`).Scan(&n))
	require.Equal(t, 2, n)
}

// grantPolicyBarrier: 00010 keeps one registration per (tenant, command),
// a fence names its revocation command and time, and a converged barrier
// has no counted sender left.
func grantPolicyBarrier(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	const digest = "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"
	insert := func(grant, command string) error {
		_, err := db.ExecContext(ctx, `INSERT INTO grant_policies (grant_id, grant_revision, tenant_id, policy_digest, server_id, methods, policy_epoch, receipt_id, revocation_state, register_command_id, register_request_digest)
			VALUES ($1, 1, 'tenant_a', $2, 'srv', '{m}', nextval('grant_policy_epoch_seq'), 'rcpt-' || $1, 'none', $3, $2)`, grant, digest, command)
		return err
	}
	require.NoError(t, insert("g1", "reg-1"))
	require.Error(t, insert("g2", "reg-1"), "one registration per (tenant, command)")
	_, err := db.ExecContext(ctx, "UPDATE grant_policies SET revocation_state = 'fenced' WHERE grant_id = 'g1'")
	require.Error(t, err, "a fence names its revocation command and time")
	_, err = db.ExecContext(ctx, "UPDATE grant_policies SET revocation_state = 'fenced', fenced_at = now(), revocation_command_id = 'rev-1', revocation_request_digest = $1, in_flight_calls = 1 WHERE grant_id = 'g1'", digest)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "UPDATE grant_policies SET revocation_state = 'converged', converged_at = now() WHERE grant_id = 'g1'")
	require.Error(t, err, "converged only without counted senders")
	_, err = db.ExecContext(ctx, "DELETE FROM grant_policies")
	require.NoError(t, err)
}

// forwardMigrateExistingStage installs version 1, accepts a stage the way
// the first implementation stored it (normalized jsonb only), then applies
// 00002 and the rest: the row survives, its jsonb copy is intact and its
// original bytes are NULL, never backfilled from the normalized copy.
func forwardMigrateExistingStage(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	version, err := migrate.DownTo(ctx, db, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), version)
	const digest = "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"
	for _, stmt := range []string{
		`INSERT INTO operations (operation_id, tenant_id, actor_id, command_id, kind, profile_id, subject_digest, semantic_digest, lifecycle, phase, control_state, cleanup_state, finance_state, deadline, intake_state, relay_state)
		 VALUES ('op_legacy', 'tenant_a', 'user_a', 'cmd_legacy', 'local_check', 'local-check-v1', '` + digest + `', '` + digest + `', 'succeeded', 'closed', 'none', 'complete', 'not_funded', now(), 'confirmed', 'settled')`,
		`INSERT INTO attempts (attempt_id, operation_id, tenant_id, step_id, visit_ordinal, attempt_ordinal, profile_id, execution_epoch, command_id, request_digest, state, deadline)
		 VALUES ('att_legacy', 'op_legacy', 'tenant_a', 'local-check', 0, 1, 'local-check-v1', 1, 'cmd_open', '` + digest + `', 'closed', now())`,
		`INSERT INTO launches (launch_id, attempt_id, operation_id, launch_key, backend, profile_id, image_digest, execution_epoch, launch_epoch, deadline, command_id, request_digest, inventory_state)
		 VALUES ('lch_legacy', 'att_legacy', 'op_legacy', 'lc-legacy', 'kind', 'local-check-v1', '` + digest + `', 1, 1, now(), 'cmd_launch', '` + digest + `', 'confirmed')`,
		`INSERT INTO physical_instances (instance_id, attempt_id, launch_id, launch_key, backend, job_uid, pod_uid, image_digest, launch_epoch, phase, is_current)
		 VALUES ('inst_legacy', 'att_legacy', 'lch_legacy', 'lc-legacy', 'kind', 'job', 'pod', '` + digest + `', 1, 'succeeded', true)`,
		`INSERT INTO stage_manifests (stage_id, attempt_id, instance_id, operation_id, phase_ordinal, profile_id, verdict, result_digest, result_manifest, observer_identity, command_id, request_digest)
		 VALUES ('stg_legacy', 'att_legacy', 'inst_legacy', 'op_legacy', 1, 'local-check-v1', 'certified', '` + digest + `', '{"verdict": "certified", "schemaVersion": 1}', 'observer', 'cmd_accept', '` + digest + `')`,
	} {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	version, err = migrate.Up(ctx, db)
	require.NoError(t, err)
	require.Equal(t, migrate.Latest, version)
	var jsonb string
	var bytes []byte
	require.NoError(t, db.QueryRowContext(ctx, "SELECT result_manifest::text, result_manifest_bytes FROM stage_manifests WHERE stage_id = 'stg_legacy'").Scan(&jsonb, &bytes))
	require.Nil(t, bytes, "existing accepted stages have no original bytes after the forward migration")
	require.JSONEq(t, `{"verdict": "certified", "schemaVersion": 1}`, jsonb, "the normalized copy is intact")
}
