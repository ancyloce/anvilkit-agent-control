// Package testdb starts a disposable PostgreSQL 17 for Control tests and
// installs anvilkit_control from the service-owned migration source
// (internal/migrate) with goose, exactly as the migration Job does. Tests
// that need it skip when Docker is unavailable and never touch a shared
// database.
package testdb

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ancyloce/anvilkit-agent-control/internal/migrate"
)

const image = "postgres:17-alpine"

// Instance is one running database with the Control schema installed.
type Instance struct {
	Container *postgres.PostgresContainer
	AppDSN    string
	AdminDSN  string
}

// Start provisions the container, roles, database and schema.
func Start(t *testing.T) *Instance {
	t.Helper()
	if os.Getenv("ANVILKIT_SKIP_DOCKER_TESTS") != "" {
		t.Skip("ANVILKIT_SKIP_DOCKER_TESTS set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// A fixed host port keeps the DSN stable across a container restart.
	hostPort := freePort(t)
	pg, err := postgres.Run(ctx, image,
		postgres.WithUsername("postgres"), postgres.WithPassword("postgres"), postgres.WithDatabase("postgres"),
		postgres.BasicWaitStrategies(),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{network.MustParsePort("5432/tcp"): []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: hostPort}}}
		}))
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	testcontainers.CleanupContainer(t, pg)
	adminDSN, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"CREATE ROLE anvilkit_control_app LOGIN PASSWORD 'app'",
		"CREATE ROLE anvilkit_control_migrator LOGIN PASSWORD 'migrator'",
		"CREATE DATABASE anvilkit_control OWNER anvilkit_control_migrator",
	} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	admin.Close(ctx)
	host, _ := pg.Host(ctx)
	port, _ := pg.MappedPort(ctx, "5432/tcp")
	inst := &Instance{
		Container: pg,
		AdminDSN:  adminDSN,
		AppDSN:    fmt.Sprintf("postgres://anvilkit_control_app:app@%s:%s/anvilkit_control?sslmode=disable", host, port.Port()),
	}
	migratorDSN := fmt.Sprintf("postgres://anvilkit_control_migrator:migrator@%s:%s/anvilkit_control?sslmode=disable", host, port.Port())
	db, err := migrate.Open(migratorDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := migrate.Up(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return inst
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return port
}

// Pool opens a fresh application-role pool; each pool models one Control
// process.
func (i *Instance) Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), i.AppDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// Restart stops and starts the database container so tests can prove that
// identities survive a database restart.
func (i *Instance) Restart(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stop := 10 * time.Second
	if err := i.Container.Stop(ctx, &stop); err != nil {
		t.Fatalf("stop postgres: %v", err)
	}
	if err := i.Container.Start(ctx); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		conn, err := pgx.Connect(ctx, i.AppDSN)
		if err == nil {
			conn.Close(ctx)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("postgres did not come back: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
