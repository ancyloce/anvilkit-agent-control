package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
)

func main() {
	step := flag.String("step", "", "one migration step: roles, activation, control (in that order)")
	flag.Parse()
	dsn := os.Getenv("ANVILKIT_CONTROL_MIGRATION_DATABASE_URL")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "Migration requires ANVILKIT_CONTROL_MIGRATION_DATABASE_URL")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := storage.Migrate(ctx, dsn, *step); err != nil {
		// SQL/connection errors may contain credentials or data. The controlled
		// test driver supplies detailed assertions; the command emits no raw error.
		fmt.Fprintln(os.Stderr, "Control migration failed; the selected step was not acknowledged")
		os.Exit(1)
	}
	fmt.Println("Control migration step completed:", *step)
}
