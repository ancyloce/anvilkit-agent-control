package storage

import (
	"context"
	"errors"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/jackc/pgx/v5"
)

// Migrate applies one retained step atomically, in the documented order: roles,
// activation, control. Role bootstrap needs cluster role-creation authority;
// schema steps use a dedicated login mapped to anvilkit_control_migrator. Steps
// are one-time migrations: an already applied step fails without changing it.
func Migrate(ctx context.Context, dsn, step string) error {
	file, ok := map[string]string{"roles": "roles-v1.sql", "activation": "activation-v1.sql", "control": "control-v1.sql"}[step]
	if !ok {
		return errors.New("migration: choose roles, activation or control")
	}
	inputs, err := contracts.VerifiedInputs()
	if err != nil {
		return err
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return errors.New("migration: invalid database configuration")
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return errors.New("migration: database connection unavailable")
	}
	defer conn.Close(context.WithoutCancel(ctx))
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err = tx.Exec(ctx, string(inputs[file])); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
