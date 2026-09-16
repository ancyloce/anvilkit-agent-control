# anvilkit-agent-control

The Control service of the AnvilKit Agent platform: admission, budgets and single-use dispatch, execution and effect registries, the obligation inventory, scoped artifact transfer, accepted results, projections and the Temporal relay. It is the sole writer of the `anvilkit_control` database and the authority the API and the Workflow worker call over gRPC (`anvilkit.control.v1`). The architecture that owns this service is the parent repository `anvilkit-services` (`docs/architecture/`: the service catalog, `contracts.md` for the RPC groups, data ownership and lock ranks, `execution.md` DD-02 for admission, effects, inventory and artifacts, `delivery.md` P04–P08 for what is implemented and verified); the parent mounts this repository as the submodule `services/agent/control`.

This repository holds the replacement implementation (grpc-go, Fx, pgx/v5 with sqlc-generated data access, goose/v3 migrations, koanf configuration, the AWS SDK v2 for the S3-compatible inventory and artifact backends, the Temporal client for the relay). The previous implementation that lived here (Connect RPC, the retained contracts) is outside the build closure of the replacement and is not a starting point; its last worktree is preserved by the parent's cleanup record, not here.

## Layout

| Path | Content |
| --- | --- |
| `cmd/anvilkit-agent-control` | `main`: `fx.New(bootstrap.Module()).Run()` |
| `cmd/anvilkit-migration` | The migration Job (Job kind `migration`): applies `internal/migrate/sql` with the migrator role and exits; the service never runs DDL |
| `internal/bootstrap` | Fx assembly: configuration snapshot, logger, pgx pool, inventory and artifact backends (qualified on start), use cases, the gRPC server and the relay, ordered shutdown |
| `internal/config`, `config.yaml` | One typed, validated configuration snapshot (defaults < the reviewed file < the allowlisted `ANVILKIT_CONTROL_*` overrides; secrets only from the environment; unknown keys, ranges and cross-field rules reject the candidate) |
| `internal/transport/grpc` | The `anvilkit.control.v1` services (Operation, Execution, Dispatch, Effect, Recovery, Artifact), capacity interceptors, the error mapping and the `grpc.health.v1` health service |
| `internal/application` | Use cases and ports: operations and commands, attempts/launches/instances/stages, dispatch admission, effects and obligations, recovery, artifacts, the relay |
| `internal/domain` | Values, budgets and money, operation/attempt/dispatch/effect/recovery/artifact rules; no framework or driver import |
| `internal/adapters/postgres` | The store over pgx/v5: reviewed queries (`queries/*.sql`) and their sqlc output (`sqlc/`) |
| `internal/adapters/{inventory,artifacts}` | The obligation inventory (filesystem, DEVELOPMENT_ONLY; S3 through the AWS SDK with its qualification probe) and the versioned S3 artifact store |
| `internal/adapters/{temporal,jobs,development}` | The Temporal relay, the Job profile contract reader and the DEVELOPMENT_ONLY price/authority/evidence fixtures |
| `internal/migrate` | The service-owned migration source: `sql/00001_init.sql` and the forward migrations, the goose provider the Job and the tests use |
| `internal/testdb` | A disposable PostgreSQL 17 (Testcontainers) installed from `internal/migrate` for the tests |
| `sqlc.yaml`, `tools/sqlc.sh` | sqlc 1.27.0 generation of the data-access code from the queries and the migration source; `--check` fails on drift |
| `deploy/chart` | The service's Helm chart (Deployment, Service, ServiceAccount, ConfigMap, PodDisruptionBudget, the migration Job as a pre-install/pre-upgrade hook) |
| `Dockerfile`, `.dockerignore` | Image build from this repository root alone; the image carries the service and the migration binary |
| `.github/workflows/ci.yml` | Build, vet, test (Docker-backed), race check, sqlc drift check, image build, chart lint and render |

Dependency direction: `cmd -> bootstrap(Fx) -> transport -> application -> domain`; adapters implement the application's ports. The generated contract (`github.com/ancyloce/anvilkit-agent-contracts/go`: `anvilkit/control/v1`, `jobschema`) is an ordinary versioned module requirement, resolved through GOPROXY. There is no replace directive, no workspace file and nothing read from a sibling checkout.

## Configuration

The service reads one reviewed, secret-free file (`config.yaml`, path from `ANVILKIT_CONTROL_CONFIG`, default `./config.yaml`) with the sections `grpc` (listen, capacities, shutdown), `inventory` (backend `filesystem` or `s3`), `artifacts` (backend `disabled` or `s3`, bounds), `temporal`, `relay`, `profiles`, `recovery` and `dispatch` (with its file-only DEVELOPMENT_ONLY fixtures). The only environment overrides are `ANVILKIT_CONTROL_LISTEN`, `ANVILKIT_CONTROL_TEMPORAL_ADDRESS`, `ANVILKIT_CONTROL_INVENTORY_DIR`, `ANVILKIT_CONTROL_INVENTORY_S3_{ENDPOINT,BUCKET,ACCESS_KEY_ID,SECRET_ACCESS_KEY}`, `ANVILKIT_CONTROL_ARTIFACTS_S3_{ENDPOINT,BUCKET,ACCESS_KEY_ID,SECRET_ACCESS_KEY}` and the secret `ANVILKIT_CONTROL_DATABASE_URL`; any other `ANVILKIT_CONTROL_*` variable stops the process, and a secret found in the file refuses the candidate. The gRPC health service reports `SERVING` once the listener is up and the relay runs, `NOT_SERVING` while the server drains.

## Migrations

`internal/migrate/sql` is the migration source of `anvilkit_control` (`00001_init.sql` plus the forward migrations `00002` to `00007`; goose/v3, append-only). `anvilkit-migration -dsn "$ANVILKIT_MIGRATION_DSN" [-to N] [-status]` applies them with the migrator role (`anvilkit_control_migrator`); the runtime role (`anvilkit_control_app`) receives DML only. The chart runs the same binary as a Job before the release's Pods roll. `internal/migrate/migrate_test.go` installs an empty PostgreSQL 17, checks the role boundary, the version, Down/Up and the forward migration of a stage accepted before `00002`.

## Build and verify from this repository alone

```sh
export GOWORK=off GOFLAGS=-mod=readonly
go build ./... && go vet ./... && go test -count=1 ./...        # Testcontainers: Docker; ANVILKIT_SKIP_DOCKER_TESTS=1 skips those tests
go test -race -count=1 ./internal/application/... ./internal/transport/... ./internal/domain/...
sh tools/sqlc.sh --check                                        # sqlc 1.27.0 in GOPATH/bin or PATH
docker build -t anvilkit-agent-control:dev .                    # --build-arg GOPROXY=... GONOSUMDB=... only for a private module proxy
helm lint deploy/chart --set database.secret.name=control-database --set temporal.address=temporal:7233 \
  --set inventory.s3.endpoint=http://minio:9000 --set inventory.s3.bucket=anvilkit-inventory --set inventory.s3.secret.name=control-inventory \
  --set artifacts.s3.endpoint=http://minio:9000 --set artifacts.s3.bucket=anvilkit-artifacts --set artifacts.s3.secret.name=control-artifacts \
  --set migration.secret.name=control-migrator
```

The contract module is an ordinary published dependency. This build requires `github.com/ancyloce/anvilkit-agent-contracts/go v0.1.2-0.20260916181159-a397fee37c16`: the pseudo-version of commit `a397fee` on `main` of the `anvilkit-agent-contracts` repository (pushed 2026-09-16; it carries `ExecutionService.GetInstance` with the attempt's `operation` in its answer and `profile.expectedResult.resultSizeBytes`), served by `proxy.golang.org` and verified against the checksum database (`go.sum`: `h1:Z0/y7RKUi3G95MAHVAv4dMyf5hivJ47v1R4tzsFpNug=`; the zip includes that repository's root `LICENSE`, as Go requires for a module in a subdirectory). No tag names that commit yet; once the contracts repository tags it (`go/v0.1.2`), the `require` line changes to the tag and nothing else does. No replace directive, workspace or local proxy is involved.

## Deploy

`deploy/chart` carries only what this service needs. Required values: `database.secret.name` (an existing Secret with the application-role URL), `temporal.address`, the shared obligation inventory `inventory.s3.{endpoint,bucket,secret.name}` (every replica must use the same backend; the filesystem adapter is DEVELOPMENT_ONLY and is not rendered by the chart), the artifact store `artifacts.s3.{endpoint,bucket,secret.name}` unless `artifacts.enabled: false`, and `migration.secret.name` (the migrator-role URL) unless `migration.enabled: false`. The chart never creates Secrets. `image.digest` pins the exact image; `config` renders the reviewed file into a ConfigMap; `resources`, gRPC probes, the security context and a PodDisruptionBudget are declared — the budget selects the runtime Pods by their `app.kubernetes.io/component: control` label (a Pod-template label, not part of the Deployment's immutable selector), so the migration Job's Pods, which share the selector labels under component `migration`, are neither counted nor held back by it. Environment values (addresses, buckets, replica counts, digests, Secret names) and the pinned deployment combination belong to the deploying repository (`anvilkit-services`, `deploy/dev` for the development foundation), never to this chart. Plaintext gRPC and the DEVELOPMENT_ONLY fixtures are development inputs; runtime qualification (the parent's G gates) is not claimed by any check here.

## License

MIT, see `LICENSE`.
