package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-control/internal/disclosure"
	"github.com/ancyloce/anvilkit-agent-control/internal/intake"
	"github.com/ancyloce/anvilkit-agent-control/internal/localcheck"
	"github.com/ancyloce/anvilkit-agent-control/internal/logging"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Control startup or shutdown failed:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var store *storage.Store
	if dsn := os.Getenv("ANVILKIT_CONTROL_DATABASE_URL"); dsn != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var err error
		store, err = storage.Open(ctx, dsn)
		cancel()
		if err != nil {
			return err
		}
		defer store.Close()
	}
	var disclosureService *disclosure.Service
	profilePath, evidencePath := os.Getenv("ANVILKIT_CONTROL_DISCLOSURE_PROFILE"), os.Getenv("ANVILKIT_CONTROL_DISCLOSURE_EVIDENCE")
	if profilePath != "" || evidencePath != "" {
		var err error
		disclosureService, err = disclosure.NewLocal(store, profilePath, evidencePath)
		if err != nil {
			return err
		}
	}
	address := os.Getenv("ANVILKIT_CONTROL_LISTEN_ADDR")
	if address == "" {
		address = "127.0.0.1:8081"
	}
	var localChecks *localcheck.Service
	logger := logging.New(os.Stdout, "control-local-check")
	if profile := os.Getenv("ANVILKIT_CONTROL_LOCAL_CHECK_PROFILE"); profile != "" {
		if profile != contracts.LocalCheckProfileRef || disclosureService == nil || os.Getenv("ANVILKIT_CONTROL_TLS_CLIENT_CA") == "" {
			return errors.New("local checks require the fixed profile, fixture authorization and mTLS")
		}
		if err := contracts.Ready(); err != nil {
			return err
		}
		reserved, err := storage.OpenReserved(ctx, os.Getenv("ANVILKIT_CONTROL_DATABASE_URL"))
		if err != nil {
			return err
		}
		defer reserved.Close()
		cancelAuthorization, err := disclosure.NewLocal(reserved, profilePath, evidencePath)
		if err != nil {
			return err
		}
		objects, err := intake.Open(os.Getenv("ANVILKIT_CONTROL_INTAKE_DIRECTORY"))
		if err != nil {
			return err
		}
		defer objects.Close()
		options, err := localcheck.TemporalOptions(os.Getenv)
		if err != nil {
			return err
		}
		options.Logger = logger
		options.ConnectionOptions.DialOptions = []grpc.DialOption{grpc.WithChainUnaryInterceptor(logger.UnaryClient)}
		temporal, err := client.NewLazyClient(options)
		if err != nil {
			return errors.New("Temporal client configuration failed")
		}
		defer temporal.Close()
		store.SetLocalEventLogger(logger.EventAppended)
		reserved.SetLocalEventLogger(logger.EventAppended)
		localChecks, err = localcheck.New(store, reserved, disclosureService, cancelAuthorization, objects, temporal, options.Namespace, os.Getenv("ANVILKIT_CONTROL_ENVIRONMENT"), logger)
		if err != nil {
			return err
		}
	}
	server, err := transport.NewLocalServer(address, os.Getenv("ANVILKIT_CONTROL_DEVELOPMENT_TOKEN"), os.Stdout, disclosureService, localChecks)
	if err != nil {
		return err
	}
	if err := transport.ConfigureTLS(server, os.Getenv("ANVILKIT_CONTROL_TLS_CLIENT_CA"), os.Getenv("ANVILKIT_CONTROL_TLS_CERT"), os.Getenv("ANVILKIT_CONTROL_TLS_KEY")); err != nil {
		return err
	}
	if localChecks != nil {
		recovery, recoveryCancel := context.WithCancel(ctx)
		recoveryDone := make(chan struct{})
		go func() { defer close(recoveryDone); localChecks.Run(recovery) }()
		defer func() { recoveryCancel(); <-recoveryDone }()
	}
	done := make(chan error, 1)
	go func() {
		if server.TLSConfig != nil {
			done <- server.ListenAndServeTLS("", "")
		} else {
			done <- server.ListenAndServe()
		}
	}()
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	return nil
}
