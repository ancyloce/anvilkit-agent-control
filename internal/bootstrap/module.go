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
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/client"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	artifactstore "github.com/ancyloce/anvilkit-agent-control/internal/adapters/artifacts"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/jobs"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	temporaladapter "github.com/ancyloce/anvilkit-agent-control/internal/adapters/temporal"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/config"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	grpctransport "github.com/ancyloce/anvilkit-agent-control/internal/transport/grpc"
)

// The stable registered workflow types (architecture.md "API, events and
// Temporal"): the LocalCheck fixture, the Preparation and Generation
// business workflows (P13) and the reconciliation workflow of a recovery
// run (P07). Every other kind has no registration and is not started.
const (
	LocalCheckWorkflowName  = "LocalCheckWorkflow"
	PreparationWorkflowName = "PreparationWorkflow"
	GenerationWorkflowName  = "GenerationWorkflow"
	PreviewWorkflowName     = "PreviewBuildWorkflow"
	ReleaseWorkflowName     = "ReleaseWorkflow"
	RecoveryWorkflowName    = "RecoveryWorkflow"
)

// GenerationPoolID is the execution capacity pool of the Generation
// profile (contracts.md §2 resource_pools).
const GenerationPoolID = "generation"

// WorkflowNames binds each startable operation kind to its workflow type.
func WorkflowNames() map[domain.OperationKind]string {
	return map[domain.OperationKind]string{
		domain.KindLocalCheck: LocalCheckWorkflowName, domain.KindPreparation: PreparationWorkflowName, domain.KindGeneration: GenerationWorkflowName,
		domain.KindPreviewBuild: PreviewWorkflowName, domain.KindRelease: ReleaseWorkflowName,
	}
}

// Profiles returns the reviewed operation profiles of this build: the
// LocalCheck fixture, the Preparation profile and the Generation profile
// (P13), the preview build (P20) and the release (P21); other kinds are rejected with PROFILE_UNQUALIFIED until their
// units deliver reviewed profiles. The money values were validated by the
// configuration.
func Profiles(cfg config.Config) []domain.Profile {
	prep, gen := cfg.Profiles.Preparation, cfg.Profiles.Generation
	prepFunding, _ := domain.ParseMoney(prep.Funding.Currency, prep.Funding.Amount)
	genFunding, _ := domain.ParseMoney(gen.Funding.Currency, gen.Funding.Amount)
	return []domain.Profile{
		{
			ID: "local-check-v1", Kind: domain.KindLocalCheck, OperationDeadline: cfg.Profiles.LocalCheckDeadline,
			StepID: "local-check", JobProfileID: "local-check-v1",
		},
		{
			ID: "preparation-v1", Kind: domain.KindPreparation, OperationDeadline: prep.Deadline, StepID: "analysis", MultiStep: true,
			Clarification: domain.ClarificationBounds{MaxRounds: prep.MaxRounds, MaxQuestions: prep.MaxQuestions, Wait: prep.Wait},
			Funding:       &prepFunding,
		},
		{
			ID: "generation-v1", Kind: domain.KindGeneration, OperationDeadline: gen.QueueDeadline, StepID: "codegen", MultiStep: true,
			Funding: &genFunding, QueuePool: GenerationPoolID, ActiveWindow: gen.ActiveWindow, Definitions: gen.Definitions, SupportsControl: true,
			MaxRepairs: gen.MaxRepairs, CodegenProfileID: gen.CodegenProfile, ValidatorProfileID: gen.ValidatorProfile, JobProfileID: gen.CodegenProfile,
		},
		{
			// P20: save the edited source conditionally, then build exactly
			// the saved revision in the isolated build Job (compute only).
			ID: "preview-build-v1", Kind: domain.KindPreviewBuild, OperationDeadline: cfg.Profiles.PreviewBuild.Deadline, StepID: "preview-build",
			MultiStep: true,
		},
		{
			// P21: certify the exact saved source, register the exact
			// subject for review, wait for the maintainer's decision, publish
			// both targets and activate (no funding: compute and guarded
			// business writes only).
			ID: "release-v1", Kind: domain.KindRelease, OperationDeadline: cfg.Profiles.Release.Deadline, StepID: "certify",
			MultiStep: true, AttemptWindow: cfg.Profiles.Release.AttemptWindow,
		},
	}
}

// Pools returns the reviewed execution pools of this build.
func Pools(cfg config.Config) []domain.ResourcePool {
	return []domain.ResourcePool{{ID: GenerationPoolID, Class: "generation", Capacity: cfg.Profiles.Generation.Capacity}}
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
			func(pool *pgxpool.Pool, reg *prometheus.Registry) *postgres.Store {
				return postgres.NewStore(pool).WithLifecycleLatency(newLifecycleLatency(reg))
			},
			func(s *postgres.Store) application.Store { return s },
			newInventory,
			newArtifactStore,
			func(cfg config.Config, store application.Store, objects application.ArtifactStore, clock domain.Clock, log *slog.Logger) *application.Artifacts {
				return application.NewArtifacts(store, objects, application.ArtifactLimits{MaxObjectBytes: cfg.Artifacts.MaxObjectBytes, MaxWindow: cfg.Artifacts.MaxWindow, CapabilityTTL: cfg.Artifacts.CapabilityTTL, FinalizeConcurrency: cfg.Artifacts.FinalizeConcurrency}, clock, log)
			},
			func() application.ManifestValidator { return jobs.Contract{} },
			func(cfg config.Config, log *slog.Logger) (client.Client, error) {
				transport, err := clientTransport("temporal.tls", cfg.Temporal.TLS, cfg.Development.Enabled, log)
				if err != nil {
					return nil, err
				}
				return temporaladapter.Dial(cfg.Temporal.Address, cfg.Temporal.Namespace, transport)
			},
			func(cfg config.Config, c client.Client) application.WorkflowRelay {
				return temporaladapter.NewRelay(c, cfg.Temporal.TaskQueue, WorkflowNames(), RecoveryWorkflowName)
			},
			func(cfg config.Config, store application.Store, inv application.Inventory, finance *application.Finance, clock domain.Clock, log *slog.Logger) *application.Operations {
				return application.NewOperations(store, inv, Profiles(cfg), clock, log).WithFinance(finance)
			},
			func(cfg config.Config, store application.Store, inv application.Inventory, mv application.ManifestValidator, finance *application.Finance, clock domain.Clock, log *slog.Logger) *application.Execution {
				return application.NewExecution(store, inv, mv, Profiles(cfg), clock, log).WithFinance(finance)
			},
			func(cfg config.Config, store application.Store, clock domain.Clock, log *slog.Logger) *application.Preparations {
				return application.NewPreparations(store, Profiles(cfg), clock, log)
			},
			func(cfg config.Config, store application.Store, dispatch *application.Dispatch, clock domain.Clock, log *slog.Logger) *application.Generations {
				return application.NewGenerations(store, dispatch, Profiles(cfg), Pools(cfg), clock, log)
			},
			// The authority inputs of the dispatch chain (P2.1): the
			// DEVELOPMENT_ONLY fixtures only under dispatch.development (which
			// the loader admits only with development.enabled), the
			// production price book, authority source, signed evidence and
			// Model Proxy query otherwise.
			newPriceBook,
			newAuthority,
			newEvidence,
			newOutcomeQuery,
			func(store application.Store, prices *application.StoredPrices, clock domain.Clock, log *slog.Logger) *application.Finance {
				var refresh func(context.Context) error
				if prices != nil {
					refresh = prices.Refresh
				}
				return application.NewFinance(store, refresh, clock, log)
			},
			func(source application.AuthorizationSource, clock domain.Clock) *application.Authorization {
				return application.NewAuthorization(source, clock)
			},
			func(cfg config.Config, store application.Store, inv application.Inventory, queries application.OutcomeQuery, clock domain.Clock, log *slog.Logger) *application.Effects {
				return application.NewEffects(store, inv, queries, clock, log).WithProfiles(Profiles(cfg))
			},
			func(cfg config.Config, store application.Store, inv application.Inventory, queries application.OutcomeQuery, dispatch *application.Dispatch, effects *application.Effects, exec *application.Execution, ev application.NotSentEvidence, de application.DispositionEvidence, clock domain.Clock, log *slog.Logger) *application.Recovery {
				return application.NewRecovery(store, inv, queries, dispatch, effects, exec, Profiles(cfg), ev, de, clock, log, cfg.Recovery.EnumerationPage)
			},
			func(cfg config.Config, store application.Store, inv application.Inventory, prices application.PriceBook, auth application.Authority, ev application.NotSentEvidence, clock domain.Clock, log *slog.Logger) *application.Dispatch {
				d := application.NewDispatch(store, inv, prices, auth, ev, clock, log).WithProfiles(Profiles(cfg))
				if cfg.Dispatch.Development.Enabled {
					d = d.WithDevelopmentCaps()
				}
				return d
			},
			func(cfg config.Config, store application.Store, wf application.WorkflowRelay, ops *application.Operations, recovery *application.Recovery, preparations *application.Preparations, generations *application.Generations, dispatch *application.Dispatch, exec *application.Execution, queries application.OutcomeQuery, clock domain.Clock, log *slog.Logger) *application.Relay {
				// P1.6: stored settlement intents are re-applied at the
				// dispatch reconciliation interval once the last obligation
				// of a reconciling operation resolved.
				return application.NewRelay(store, wf, ops, recovery, preparations, generations, clock, log, cfg.Relay.Interval).WithUpdateTimeout(cfg.Relay.UpdateTimeout).
					WithDispatchReconciler(dispatch, queries, cfg.Dispatch.ReconcileInterval, cfg.Dispatch.ReconcileAge, 100).
					WithSettlementReconciler(exec, cfg.Dispatch.ReconcileInterval, 100)
			},
			func(store application.Store, clock domain.Clock, log *slog.Logger) *application.GrantPolicies {
				return application.NewGrantPolicies(store, clock, log)
			},
			func(store application.Store, clock domain.Clock) *application.Previews {
				return application.NewPreviews(store, clock)
			},
			func(store application.Store, clock domain.Clock) *application.Releases {
				return application.NewReleases(store, clock)
			},
			newTracer,
			newMetrics,
			func(lc fx.Lifecycle, cfg config.Config, log *slog.Logger, ops *application.Operations, exec *application.Execution, dispatch *application.Dispatch, effects *application.Effects, recovery *application.Recovery, artifacts *application.Artifacts, preparations *application.Preparations, generations *application.Generations, grants *application.GrantPolicies, previews *application.Previews, releases *application.Releases, finance *application.Finance, authorization *application.Authorization, _ trace.Tracer, reg *prometheus.Registry, notifier *postgres.Notifier) (*grpctransport.Server, error) {
				id, reloader, err := serverIdentity(cfg, log)
				if err != nil {
					return nil, err
				}
				if reloader != nil {
					lc.Append(fx.Hook{OnStart: func(context.Context) error { reloader.Start(); return nil }, OnStop: func(context.Context) error { reloader.Stop(); return nil }})
				}
				return grpctransport.NewServerWithIdentity(cfg.GRPC.Listen, id,
					grpctransport.Capacity{Control: cfg.GRPC.ControlCapacity, Execution: cfg.GRPC.ExecutionCapacity, Streams: cfg.GRPC.StreamCapacity, StreamPerTenant: cfg.GRPC.StreamPerTenant, CommandReserve: cfg.GRPC.CommandReserve},
					grpctransport.Dependencies{Ops: ops, Exec: exec, Dispatch: dispatch, Effects: effects, Recovery: recovery, Artifacts: artifacts, Preparations: preparations, Generations: generations, Grants: grants, Previews: previews, Releases: releases,
						Finance: finance, Authorization: authorization, Notifier: notifier, DefaultDeadline: cfg.GRPC.DefaultDeadline, MaxConnectionAge: cfg.GRPC.Identity.MaxConnectionAge,
						MaxConnectionAgeGrace: cfg.GRPC.Identity.MaxConnectionAgeGrace},
					grpctransport.Observability(reg)...)
			},
			// The event notifier (P1.4): one LISTEN connection per replica wakes
			// the event streams at commit; it runs for the process lifetime.
			func(lc fx.Lifecycle, pool *pgxpool.Pool, log *slog.Logger) *postgres.Notifier {
				n := postgres.NewNotifier(pool, log)
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				lc.Append(fx.Hook{
					OnStart: func(context.Context) error { go func() { n.Run(ctx); close(done) }(); return nil },
					OnStop: func(stop context.Context) error {
						cancel()
						select {
						case <-done:
						case <-stop.Done():
						}
						return nil
					},
				})
				return n
			},
			newHealth,
		),
		// The event publication (P2.8): forwarder, waker and outbox retention.
		fx.Invoke(startEvents),
		fx.Invoke(startOperationalMetrics),
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
	poolCfg, err := pgxpool.ParseConfig(cfg.Database.URL)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = cfg.Database.MaxConns
	// Bounds of every connection (P1.4): connect, statement and
	// idle-in-transaction; no transaction outlives them.
	if cfg.Database.ConnectTimeout > 0 {
		poolCfg.ConnConfig.ConnectTimeout = cfg.Database.ConnectTimeout
	}
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	if cfg.Database.StatementTimeout > 0 {
		poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprint(cfg.Database.StatementTimeout.Milliseconds())
	}
	if cfg.Database.IdleInTransactionTimeout > 0 {
		poolCfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = fmt.Sprint(cfg.Database.IdleInTransactionTimeout.Milliseconds())
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
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
func run(lc fx.Lifecycle, cfg config.Config, srv *grpctransport.Server, h *health, relay *application.Relay, generations *application.Generations, prices *application.StoredPrices, finance *application.Finance, tc client.Client, log *slog.Logger) {
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	done := make(chan struct{})
	// The probe listener is registered first: up before the business
	// listener, down after it.
	h.register(lc, log)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// The reviewed execution pools of this build are configuration:
			// written before serving, never guessed at runtime.
			if err := generations.InstallPools(ctx); err != nil {
				return fmt.Errorf("install execution pools: %w", err)
			}
			if err := startPriceBook(ctx, cfg, prices, finance, log); err != nil {
				return err
			}
			if prices != nil {
				go prices.Run(relayCtx, cfg.Dispatch.Prices.RefreshInterval)
			}
			addr, err := srv.Start()
			if err != nil {
				return err
			}
			go func() { relay.Run(relayCtx); close(done) }()
			log.Info("control serving", "listen", addr.String(), "identity", cfg.GRPC.Identity.Mode, "trustDomain", cfg.TrustDomain(), "temporalNamespace", cfg.Temporal.Namespace, "taskQueue", cfg.Temporal.TaskQueue)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			h.withdraw()
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
