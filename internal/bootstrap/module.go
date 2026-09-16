// Package bootstrap assembles Control with Fx (A08): configuration, logging,
// the pgx pool, adapters, use cases, the gRPC server and the relay. Domain
// and application code never see the container.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	artifactstore "github.com/ancyloce/anvilkit-agent-control/internal/adapters/artifacts"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/development"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/jobs"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	temporaladapter "github.com/ancyloce/anvilkit-agent-control/internal/adapters/temporal"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/config"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	grpctransport "github.com/ancyloce/anvilkit-agent-control/internal/transport/grpc"
)

// LocalCheckWorkflowName is the stable registered workflow type of the
// LocalCheck fixture (architecture.md "API, events and Temporal");
// RecoveryWorkflowName the reconciliation workflow of a recovery run (P07).
const (
	LocalCheckWorkflowName = "LocalCheckWorkflow"
	RecoveryWorkflowName   = "RecoveryWorkflow"
)

// Profiles returns the reviewed operation profiles of this build. The only
// initial profile is the LocalCheck fixture; other kinds are rejected with
// PROFILE_UNQUALIFIED until their units deliver reviewed profiles.
func Profiles(cfg config.Config) []domain.Profile {
	return []domain.Profile{{
		ID: "local-check-v1", Kind: domain.KindLocalCheck, OperationDeadline: cfg.Profiles.LocalCheckDeadline,
		StepID: "local-check", JobProfileID: "local-check-v1",
	}}
}

func Module() fx.Option {
	return fx.Options(
		// The stop hooks drain the gRPC server for up to grpc.shutdown_timeout
		// (at most 5m by validation); the container's own bound must not cut
		// that short.
		fx.StopTimeout(6*time.Minute),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: log} }),
		fx.Provide(
			config.Load,
			func() *slog.Logger {
				return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
			},
			func() domain.Clock { return domain.SystemClock{} },
			newPool,
			func(pool *pgxpool.Pool) *postgres.Store { return postgres.NewStore(pool) },
			func(s *postgres.Store) application.Store { return s },
			newInventory,
			newArtifactStore,
			func(cfg config.Config, store application.Store, objects application.ArtifactStore, clock domain.Clock, log *slog.Logger) *application.Artifacts {
				return application.NewArtifacts(store, objects, application.ArtifactLimits{MaxObjectBytes: cfg.Artifacts.MaxObjectBytes, MaxWindow: cfg.Artifacts.MaxWindow, CapabilityTTL: cfg.Artifacts.CapabilityTTL}, clock, log)
			},
			func() application.ManifestValidator { return jobs.Contract{} },
			func(cfg config.Config) (client.Client, error) {
				return temporaladapter.Dial(cfg.Temporal.Address, cfg.Temporal.Namespace)
			},
			func(cfg config.Config, c client.Client) application.WorkflowRelay {
				return temporaladapter.NewRelay(c, cfg.Temporal.TaskQueue, LocalCheckWorkflowName, RecoveryWorkflowName)
			},
			func(cfg config.Config, store application.Store, inv application.Inventory, clock domain.Clock, log *slog.Logger) *application.Operations {
				return application.NewOperations(store, inv, Profiles(cfg), clock, log)
			},
			func(store application.Store, inv application.Inventory, mv application.ManifestValidator, clock domain.Clock, log *slog.Logger) *application.Execution {
				return application.NewExecution(store, inv, mv, clock, log)
			},
			// DEVELOPMENT_ONLY sources of prices, authority and not-sent
			// evidence; disabled fixtures deny every paid route (P06, ENV-06/07).
			func(cfg config.Config, clock domain.Clock, log *slog.Logger) (application.PriceBook, error) {
				prices, err := cfg.Dispatch.Development.DomainPrices()
				if err != nil {
					return nil, err
				}
				if cfg.Dispatch.Development.Enabled {
					log.Warn("DEVELOPMENT_ONLY dispatch fixtures enabled", "prices", len(prices), "authorizedRoutes", len(cfg.Dispatch.Development.AuthorizedRoutes), "notSentIssuers", len(cfg.Dispatch.Development.NotSentIssuers))
				}
				return development.NewPriceBook(prices)
			},
			func(cfg config.Config, clock domain.Clock) application.Authority {
				routes := make([]development.RouteAuthorization, 0, len(cfg.Dispatch.Development.AuthorizedRoutes))
				for _, r := range cfg.Dispatch.Development.AuthorizedRoutes {
					routes = append(routes, development.RouteAuthorization{TenantID: r.TenantID, RouteID: r.RouteID})
				}
				operators := make([]development.OperatorAuthorization, 0, len(cfg.Dispatch.Development.Operators))
				for _, o := range cfg.Dispatch.Development.Operators {
					operators = append(operators, development.OperatorAuthorization{TenantID: o.TenantID, ActorID: o.ActorID})
				}
				return development.NewAuthority(routes, cfg.Dispatch.AuthorityFreshness, clock).WithOperators(operators)
			},
			// DEVELOPMENT_ONLY controlled doubles of the original-identity
			// query (Model Proxy P11, Pagix ENV-07) and disposition evidence.
			func(cfg config.Config, inv application.Inventory) application.OutcomeQuery {
				return development.NewOutcomeQuery(inv, cfg.Dispatch.Development.NotSentIssuers)
			},
			func(cfg config.Config, inv application.Inventory) application.DispositionEvidence {
				return development.NewDispositionEvidence(inv, cfg.Dispatch.Development.NotSentIssuers)
			},
			func(store application.Store, inv application.Inventory, queries application.OutcomeQuery, clock domain.Clock, log *slog.Logger) *application.Effects {
				return application.NewEffects(store, inv, queries, clock, log)
			},
			func(cfg config.Config, store application.Store, inv application.Inventory, queries application.OutcomeQuery, dispatch *application.Dispatch, effects *application.Effects, exec *application.Execution, auth application.Authority, ev application.NotSentEvidence, de application.DispositionEvidence, clock domain.Clock, log *slog.Logger) *application.Recovery {
				return application.NewRecovery(store, inv, queries, dispatch, effects, exec, Profiles(cfg), auth, ev, de, clock, log, cfg.Recovery.EnumerationPage)
			},
			func(cfg config.Config, inv application.Inventory) application.NotSentEvidence {
				return development.NewNotSentEvidence(inv, cfg.Dispatch.Development.NotSentIssuers)
			},
			func(store application.Store, inv application.Inventory, prices application.PriceBook, auth application.Authority, ev application.NotSentEvidence, clock domain.Clock, log *slog.Logger) *application.Dispatch {
				return application.NewDispatch(store, inv, prices, auth, ev, clock, log)
			},
			func(cfg config.Config, store application.Store, wf application.WorkflowRelay, ops *application.Operations, recovery *application.Recovery, clock domain.Clock, log *slog.Logger) *application.Relay {
				return application.NewRelay(store, wf, ops, recovery, clock, log, cfg.Relay.Interval)
			},
			func(cfg config.Config, ops *application.Operations, exec *application.Execution, dispatch *application.Dispatch, effects *application.Effects, recovery *application.Recovery, artifacts *application.Artifacts) (*grpctransport.Server, error) {
				return grpctransport.NewServer(cfg.GRPC.Listen, cfg.GRPC.ControlCapacity, cfg.GRPC.ExecutionCapacity, ops, exec, dispatch, effects, recovery, artifacts)
			},
		),
		fx.Invoke(run),
	)
}

// newInventory selects the obligation inventory backend. The S3 backend is
// qualified against the actual service before Control serves (conditional
// create, same-body idempotency, read-after-write, resumable listing);
// a backend that fails the probe never holds the inventory. The filesystem
// backend is DEVELOPMENT_ONLY and is reported as such.
func newInventory(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (application.Inventory, error) {
	switch cfg.Inventory.Backend {
	case config.InventoryS3Backend:
		s3cfg := cfg.Inventory.S3
		inv, err := inventory.NewS3(context.Background(), inventory.S3Config{
			Endpoint: s3cfg.Endpoint, Region: s3cfg.Region, Bucket: s3cfg.Bucket, Prefix: s3cfg.Prefix, PathStyle: s3cfg.PathStyle,
			AccessKeyID: s3cfg.AccessKeyID, SecretAccessKey: s3cfg.SecretAccessKey,
		})
		if err != nil {
			return nil, err
		}
		lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
			if !s3cfg.QualifyOnStart {
				log.Warn("inventory s3 backend not qualified on start (inventory.s3.qualify_on_start: false)", "bucket", s3cfg.Bucket)
				return nil
			}
			if err := inv.Qualify(ctx); err != nil {
				return fmt.Errorf("inventory s3 backend %s failed qualification: %w", s3cfg.Bucket, err)
			}
			log.Info("inventory s3 backend qualified: conditional create, same-body idempotency, read-after-write and resumable listing verified against the backend", "bucket", s3cfg.Bucket, "prefix", s3cfg.Prefix)
			return nil
		}})
		return inv, nil
	default:
		log.Warn("DEVELOPMENT_ONLY filesystem inventory backend", "dir", cfg.Inventory.Dir)
		return inventory.NewFilesystem(cfg.Inventory.Dir)
	}
}

// newArtifactStore selects the artifact store: the S3 backend, qualified
// against the actual service before Control serves (versioning, presigned
// upload, read by version), or nothing, which leaves ArtifactService
// answering DEPENDENCY_UNAVAILABLE until the deployment supplies one.
func newArtifactStore(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (application.ArtifactStore, error) {
	if cfg.Artifacts.Backend != config.ArtifactsS3Backend {
		log.Warn("artifact store disabled: ArtifactService answers DEPENDENCY_UNAVAILABLE")
		return nil, nil
	}
	s3cfg := cfg.Artifacts.S3
	store, err := artifactstore.NewS3(context.Background(), artifactstore.S3Config{
		Endpoint: s3cfg.Endpoint, Region: s3cfg.Region, Bucket: s3cfg.Bucket, Prefix: s3cfg.Prefix, PathStyle: s3cfg.PathStyle,
		AccessKeyID: s3cfg.AccessKeyID, SecretAccessKey: s3cfg.SecretAccessKey,
	})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		if !s3cfg.QualifyOnStart {
			log.Warn("artifacts s3 backend not qualified on start (artifacts.s3.qualify_on_start: false)", "bucket", s3cfg.Bucket)
			return nil
		}
		if err := store.Qualify(ctx); err != nil {
			return fmt.Errorf("artifacts s3 backend %s failed qualification: %w", s3cfg.Bucket, err)
		}
		log.Info("artifacts s3 backend qualified: versioned uploads through presigned PUT and reads by exact version verified against the backend", "bucket", s3cfg.Bucket, "prefix", s3cfg.Prefix)
		return nil
	}})
	return store, nil
}

func newPool(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), cfg.Database.URL)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return pool.Ping(ctx) },
		OnStop:  func(context.Context) error { pool.Close(); return nil },
	})
	return pool, nil
}

// run orders the lifecycle so the server and relay stop before the pool and
// the Temporal client close (DD-09 §3).
func run(lc fx.Lifecycle, cfg config.Config, srv *grpctransport.Server, relay *application.Relay, tc client.Client, log *slog.Logger) {
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			addr, err := srv.Start()
			if err != nil {
				return err
			}
			go func() { relay.Run(relayCtx); close(done) }()
			log.Info("control serving", "listen", addr.String(), "temporalNamespace", cfg.Temporal.Namespace, "taskQueue", cfg.Temporal.TaskQueue)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			srv.Stop(cfg.GRPC.ShutdownTimeout)
			cancelRelay()
			select {
			case <-done:
			case <-ctx.Done():
			}
			tc.Close()
			return nil
		},
	})
}
