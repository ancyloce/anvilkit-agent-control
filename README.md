# anvilkit-agent-control

CONTROL-01 implements read-only validation of retained component definitions
through the existing private `DefinitionValidation.ValidateDefinition` method.
Valid definitions receive a deterministic RFC 8785/SHA-256 digest; invalid
definitions receive 1-100 bounded issues with no digest. This stage creates no
operations, activates no definitions and grants no execution permission.

CONTROL-02 adds the internal PostgreSQL storage transactions and a separate
migration command. Its selected transaction, role and normal restart checks
pass against the pinned disposable PostgreSQL environment described below.

CONTROL-03 implements `ControlService.GetDisclosureAuthorization` for the
approved two-tenant local fixture. It returns bounded decisions for existing
operations and denies incomplete, expired or unauthorized scope.

CONTROL-04/05 add the fixed local-check admission/cancel methods and the in-process
Temporal start/terminal observer. The two retained fixtures use independent
filesystem intake, one unresolved SQL slot and the original Workflow/Run identity.
The service uses the same Temporal Go SDK 1.48.0 already selected for the Worker.

## Run locally

Use Go 1.27.0. From this standalone service repository:

```sh
GOWORK=off go build -mod=readonly -o anvilkit-agent-control ./cmd/anvilkit-agent-control
export ANVILKIT_CONTROL_LISTEN_ADDR=127.0.0.1:8081
export ANVILKIT_CONTROL_DEVELOPMENT_TOKEN="$(openssl rand -hex 32)"
./anvilkit-agent-control
```

| Variable | Meaning |
| --- | --- |
| `ANVILKIT_CONTROL_LISTEN_ADDR` | Numeric loopback address and port; defaults to `127.0.0.1:8081`. Choose a distinct port if another local service uses it |
| `ANVILKIT_CONTROL_DEVELOPMENT_TOKEN` | Required process-configured credential, 32-256 characters without whitespace |
| `ANVILKIT_CONTROL_TLS_CLIENT_CA` | API-to-Control client CA; configure together with certificate and key to enable mTLS |
| `ANVILKIT_CONTROL_TLS_CERT` | Control server certificate, with the endpoint's DNS name or IP SAN |
| `ANVILKIT_CONTROL_TLS_KEY` | Matching Control server private key |
| `ANVILKIT_CONTROL_DATABASE_URL` | Optional PostgreSQL URL for the restricted Control login. When configured, startup verifies the connection and role; it never migrates. When absent, the CONTROL-01 validation-only server needs no database |
| `ANVILKIT_CONTROL_DISCLOSURE_PROFILE` | Opt-in path to the fixed CONTROL-03 identity/permission profile. Requires PostgreSQL and the evidence path |
| `ANVILKIT_CONTROL_DISCLOSURE_EVIDENCE` | Path to controlled membership evidence. Test tooling may replace this file atomically; no public endpoint changes it |
| `ANVILKIT_CONTROL_LOCAL_CHECK_PROFILE` | Opt in with exactly `local-check-v1`; requires PostgreSQL, fixture authorization and API-to-Control mTLS |
| `ANVILKIT_CONTROL_INTAKE_DIRECTORY` | Provisioned persistent directory outside database storage; contents are retained throughout this stage |
| `ANVILKIT_CONTROL_ENVIRONMENT` | Lowercase letters, digits and hyphens identifying the intake object environment |
| `ANVILKIT_CONTROL_TEMPORAL_ENDPOINT` | Existing Temporal `host:port`; connection is lazy so outages do not prevent independent intake |
| `ANVILKIT_CONTROL_TEMPORAL_NAMESPACE` | Original controlled namespace, with at least 24-hour retention |
| `ANVILKIT_CONTROL_TEMPORAL_TLS_SERVER_NAME` | Verified server name (`temporal.local` in the retained handoff) |
| `ANVILKIT_CONTROL_TEMPORAL_TLS_CA` | Temporal CA certificate, separate from the API-to-Control CA |
| `ANVILKIT_CONTROL_TEMPORAL_TLS_CERT` | Dedicated Control starter/observer client certificate |
| `ANVILKIT_CONTROL_TEMPORAL_TLS_KEY` | Matching Temporal client private key |

The listener accepts Protobuf gRPC over HTTP/2 on loopback. With the three TLS
inputs configured, it requires TLS 1.3 and a verified client certificate whose
DNS SAN contains exactly `anvilkit-agent-api`; another same-CA service is denied.
The method credential is still required. Partial or invalid TLS configuration
refuses startup. With all TLS inputs absent, the existing controlled tests use
unencrypted HTTP/2. Use the
retained generated client with Connect-Go's `WithGRPC()` and an HTTP client
configured for unencrypted HTTP/2. Send exactly one
`Authorization: Bearer <development credential>` header. The actual network
setup and calls are demonstrated in [server_test.go](internal/transport/server_test.go).
There is no additional JSON validation endpoint or CLI validation interface.
SIGINT/SIGTERM stops acceptance and allows five seconds for shutdown.

The approved local credential mapping fixes the caller to `anvilkit-agent-api`,
the actor to `fixture-platform-developer`, and the permission to
`definition.validate`. Request bodies and trace headers cannot select identity
or permissions. An absent, repeated or incorrect credential is rejected. This
mapping is local test authority; production workload/developer authentication
remains a separate gate. API-01 now wires its separate private validation
credential and mTLS identity. The parent
`python3 tools/run-verification.py --only api-validation` proof builds both
standalone service trees and checks retained reports over real public HTTP and
private mTLS/gRPC against this executable. It activates no database or external
service. Disclosure and command integration retain their separate task gates.

## Retained validation profile

The parent-owned `w1-component-validation-v1` profile explicitly maps
`component-generation-pilot-v1` and `component-release-pilot-v1` to the retained
effective policy, action set, runtime bindings and safety bindings. Its effect
permission list is empty. Execution profile references identify retained test
inputs and do not qualify those profiles for execution.

Startup verifies every retained input's SHA-256, descriptor canonical digest,
policy and binding agreement, and six upstream canonicalization vectors. Schema
resolution uses embedded files only; requests cannot fetch external schemas or
read the parent checkout. See [generated.json](internal/contracts/generated.json)
for source paths, digests and generator versions.

The retained [positive cases](internal/contracts/retained/validation-positive-v1.fixtures.json)
provide complete requests and exact expected reports:

| Definition | Canonical digest |
| --- | --- |
| `component-generation` | `sha256:bf9b864258ae101dba6a291170f43f9764a2a17200b61d76591510e075cab916` |
| `component-release` | `sha256:f7d17c5d37b6c6b16b686505f09ff821cec1720ff8862d4e96f20156bed8544b` |

The original `fixture-runtime` negative case stays invalid with
`PROFILE_QUALIFICATION_FAILED`. Unknown descriptor, runtime or policy references
cannot produce a valid digest.

## Validation and request boundaries

The private method rejects missing Protobuf fields, unknown wire fields,
duplicate JSON keys, unauthorized nulls, unknown schema fields, unsupported
versions, malformed Unicode and lossy numeric bounds. It measures the complete
equivalent JSON request, including definition whitespace, against 65,536 bytes;
the Protobuf message and decoded transport payload are also bounded.

Semantic checks resolve exact action versions and families, ports/reference
kinds, reachability, outcome coverage and input availability on every path.
Protected validation, certification, approval, publication and activation gates
must remain present and correctly ordered. Registration/repair must consume the
matching validation subject; a later source write invalidates the prior report.
Only the paired validate/repair cycle is permitted, with the retained policy's
maximum of one repair visit. Definitions may narrow or remove repair.

Structural failures use gRPC `INVALID_ARGUMENT` with a safe contract code;
transport overflow uses `RESOURCE_EXHAUSTED`. Semantic rejection returns a normal
validation report. Messages contain fixed diagnostic text and bounded identifiers.

Each request emits one JSON completion summary on stdout with its generated
request ID, fixed caller, duration and actual gRPC result. Rejected credentials
use `caller: unauthenticated` with no actor or tenant. Logs contain no request
bodies, definitions, credentials or candidate output and confer no authority.

## Build, generation and verification

The module path follows this service's repository URL. It has no parent module
imports or local replacements and requires no PostgreSQL or Temporal:

```sh
GOWORK=off go build -mod=readonly ./...
GOWORK=off go test -mod=readonly ./...
GOWORK=off go test -race -mod=readonly ./...
GOWORK=off go mod verify
go test -mod=readonly -run='^$' -fuzz=FuzzDefinitionBoundary -fuzztime=10s -parallel=2 ./internal/definition
```

Tests cover exact positive/negative reports, canonicalization, graph mutations,
strict decoding, byte/issue boundaries, cancellation, concurrent generated-client
calls over a real loopback listener, and schema-checked log redaction/outcomes.

Exact dependency pins are in [go.mod](go.mod) and [go.sum](go.sum): Connect-Go
`v1.21.0`, Protobuf `v1.36.12`, jsonschema/v6 `v6.0.3`, and the canonicalizer
`v0.0.0-20241213102144-19d51d7fe467`. Upstream vector provenance/license is retained
in [canonicalization-v1.fixtures.json](internal/contracts/retained/canonicalization-v1.fixtures.json).

The parent owns `contracts/proto`, schema/fixture inputs and
`tools/proto-bindings.json`. Generation uses `protoc 3.21.12`,
`protoc-gen-go v1.36.12` and `protoc-gen-connect-go v1.21.0`; the generator checks
installed versions before using them. From the parent checkout:

```sh
python3 tools/generate-proto-bindings.py --consumer anvilkit-agent-control
python3 tools/generate-proto-bindings.py --check
python3 tools/run-verification.py --only docs --only proto --only bindings --only contracts --only ddd
```

Generation is unnecessary when building a standalone clone. Retained bindings
and copied inputs must be regenerated through the parent tooling, not edited by
hand. Local validation evidence does not qualify execution, paid providers,
Pagix/Studio integration, persistence or a production deployment.

## PostgreSQL persistence (CONTROL-02)

[internal/storage](internal/storage/storage.go) contains the selected CD-02
transactions: immutable canonical records, scoped activation CAS, operation
command identity, atomic step projections/events, and authorization refresh
leases. It uses `github.com/jackc/pgx/v5 v5.11.0`; exact transitive checksums remain
in go.sum. No ORM or migration framework is added.

These methods are internal persistence operations. Callers must establish
authorization and resolve external evidence before calling them. The public
validation method stays read-only. CONTROL-04 adds only fixed local-check admission;
there is no business admission, activation or seeding RPC. Test tooling creates explicitly synthetic, unfunded operations;
runtime qualification and business intake gates remain separate.

Each physical pooled connection must authenticate as a login inheriting
`anvilkit_control_rw`. Startup rejects superusers, bypass-RLS roles, role/database
creators, replication roles, and logins that can assume the migrator or API role.
The connection then selects the Control group role and service context. Pool
size may be supplied through pgxpool URL parameters such as `pool_max_conns`.

Transaction order follows the retained lock contract: immutable writes take no
rank; activation CAS holds rank 1; operation insertion must lock its referenced
activation before rank 2; event writes hold the operation at rank 2 before the
step/event rows at rank 7. Foreign-key and unique-index locks count. No remote
API, artifact, Temporal or provider I/O runs inside a transaction. PostgreSQL
serialization/deadlock aborts (`40001`/`40P01`) get at most three retries; transport
failures and unknown commit results do not authorize retries.

Identical command/transition replay returns the original identity or sequence.
Projection, revision, sequence allocation and event commit together. A failed
transaction acknowledges no result. Refresh completion and release match the
tenant, owner, acquisition ID and epoch; completion also requires an unexpired
lease and evidence deadline, capped at 30 seconds from the first request send.

### Separate migration command

Build from this standalone service clone:

```sh
GOWORK=off go build -mod=readonly -o anvilkit-agent-control-migrate ./cmd/anvilkit-agent-control-migrate
```

Apply each retained step once, in order, to a prepared disposable database:

```sh
ANVILKIT_CONTROL_MIGRATION_DATABASE_URL="$BOOTSTRAP_DATABASE_URL" ./anvilkit-agent-control-migrate -step roles
ANVILKIT_CONTROL_MIGRATION_DATABASE_URL="$MIGRATOR_DATABASE_URL" ./anvilkit-agent-control-migrate -step activation
ANVILKIT_CONTROL_MIGRATION_DATABASE_URL="$MIGRATOR_DATABASE_URL" ./anvilkit-agent-control-migrate -step control
```

The roles step needs cluster role-creation authority. Before the schema steps,
bootstrap must map a separate login to `anvilkit_control_migrator` and grant that
group `CREATE` on the target database. Map separate runtime logins to
`anvilkit_control_rw` and `anvilkit_api_ro`; neither may assume the migrator.
Do not pass bootstrap/migrator credentials to the server.

Each step verifies its embedded source digest and runs in one transaction.
Out-of-order or repeated steps fail; reapplying is not an upgrade mechanism.
Server startup executes no DDL. The parent test driver supplies and removes the
disposable database and its synthetic login mappings.

### Real database verification

From the parent checkout:

```sh
python3 tools/run-verification.py --only control-storage
```

The driver pins
`postgres:18.6-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2`,
checks `server_version_num=180006`, uses unique container names and fixed
loopback host ports, and removes only its own containers. It runs the existing
64-check SQL regression, builds both binaries, applies migrations with real
separate logins, and runs race-enabled service tests. It restarts the actual
Control binary and PostgreSQL, then reads the original durable records from a
new test process. Ordinary Go unit tests require no database; integration tests
are behind the `integration` build tag and require the driver's explicit URLs.

The CONTROL-02 standalone baseline with `GOWORK=off` passed `go build -mod=readonly ./...`,
`go test -race -mod=readonly -count=1 ./...` (101 tests and subtests), both
canonical binary builds and `go mod verify`.

The complete driver passed on 2026-09-10: 64 SQL checks, all eight service
transaction groups with the race detector, and Control/database restart checks.
The service groups cover immutable/CAS replay, command identity, atomic
projections/events, rollback/retry, roles/RLS/tenant connection reuse, lease
takeover, monotonicity and cancellation. Synthetic operation fixtures and the
restart proof use the real Control command transaction under its restricted
login. Administrator access is limited to test setup and fault injection.

The approved column-only `UPDATE (activation_id)` grant permits PostgreSQL's
rank-1 `SELECT ... FOR KEY SHARE` activation lock. Tests verify that this is the
only updatable activation column by privilege, the immutable trigger rejects
actual updates, and API logins cannot lock or mutate activations. Both retained
consumer SQL copies and their source digests agree with the parent contracts.

This completes CONTROL-02's selected local persistence acceptance. Paid
admission, Temporal compatibility, production database deployment, high
availability and disaster recovery retain their separate gates.

## Controlled disclosure (CONTROL-03)

The existing generated `ControlService.GetDisclosureAuthorization` method is
enabled only when the database and both fixture paths are configured. The
validation credential remains scoped to definition validation. Disclosure
credentials map to two distinct synthetic actors and tenants; actor/tenant IDs
must start with `fixture-`. Credential hashes, reviewed role permissions and
exact service/operation bindings are fixed at startup. Reloading that authority
requires restarting the process.

The parent-owned [configuration schema](internal/contracts/retained/controlled-disclosure-v1.schema.json)
defines both local JSON files. Both require `schemaVersion: 1` and reject
duplicate keys, unknown fields, nulls and files larger than 64 KiB.

| File | Required content |
| --- | --- |
| Profile | `profile: "control-03-fixture"`, `sourceCredentialExpiresAt`, two `identities` with `token`, `actorId`, `tenantId`, `expiresAt`; `rolePermissions` with `role`, `resourceKind`, `action`; two `operationBindings` with `operationId`, `actorId`, `tenantId`, `serviceIdentity`, `serviceAuthorized`, `validUntil` |
| Evidence | Two `tenants`, each with `tenantId`, `complete`, `available`, `validUntil` and `memberRoles` mapping actor IDs to arrays of role strings. An empty membership or role array grants no action |

Test tooling supplies random synthetic credentials and real synthetic operation
IDs, writes private files, and binds each identity to its own operation. These
files never create operations or supply real SSO/Pagix claims. Evidence can be
atomically replaced to revoke membership or simulate incomplete/unavailable
enumeration; no test-control RPC is registered. Role, service and binding tests
use explicit profile variants loaded into new server instances.

Use the retained generated client with `connect.WithGRPC()` over loopback
HTTP/2. Send one `Authorization: Bearer <mapped fixture credential>` header.
The request's `AuthenticatedContext` must match its mapped actor and tenant,
`serviceIdentity: "anvilkit-agent-api"` and the generated disclosure procedure
name as `destinationMethod`. Omit `actorRole`: request bodies cannot grant a
role. Unknown Protobuf fields are rejected. An allow response contains exactly
decision, the requested operation ID as `boundResourceScope`, `freshUntil` and
`evidenceRevision`; a denial exposes only its decision.

The evaluator separately checks current cached membership, reviewed
`operation.read` permission on resource kind `operation`, service authority,
and both configured and stored actor/tenant/operation ownership. Cache refresh
uses the CONTROL-02 lease ID/owner/epoch predicates and the shared SQL token
bucket (20 reads/second, burst 40). Waiters poll at 250 ms for at most the
10-second read deadline. Only a lease holder reads the source file, outside all
database locks. A source read is attempted once; database retries never repeat
it or reset its first-send timestamp.

The absolute grant deadline is the earliest of that first send plus 30 seconds,
source validity, source credential expiry, caller credential expiry and binding
validity. Revocation is observed at renewal within the existing bound; cached
evidence never extends its original deadline. Control refreshes cached evidence
inside API's two-second expiry margin so a renewal cannot return the same
unusable grant. Incomplete, malformed or failed
renewal returns `DEPENDENCY_UNAVAILABLE` without replacing evidence. API treats
grants inside its existing two-second clock margin as expired.

Run from the parent checkout:

```sh
python3 tools/run-verification.py --only control-disclosure
```

The driver reuses CONTROL-02's pinned PostgreSQL environment and regression,
then tests exact ownership and shared read-budget concurrency, five generated
gRPC scenario groups, and an actual Control binary configured with the fixture
files. The RPC groups cover credential/body binding, reviewed permissions,
revocation and unavailable renewal, two replicas sharing one refresh, and the
canonical process's absolute deadline. RPC summaries are checked against the
retained log schema and exclude credentials and membership bodies.

On 2026-09-10 the full driver passed. A standalone copy with `GOWORK=off` passed
both binary/package builds, 132 tests/subtests with `-race -count=1`, and
`go mod verify`. No dependency was added. API-02 owns subsequent HTTP/SSE
integration, including its private gRPC and credential wiring. Production
workload identity, real Pagix authorization and deployment remain unqualified.

## Fixed local intake and recovery (CONTROL-04/05)

The fixture profile's optional `localCheckBindings` maps each permitted synthetic
actor to its tenant, API service identity and deadline. The existing role map
must separately grant `local-check.create` on `local-check`, and `operation.read`
and `operation.cancel` on `operation`. Read/cancel additionally match persisted
ownership. Predeclared operation bindings keep their existing behavior. The
validation credential cannot invoke these methods.

Admission prepares the immutable input, original 15-minute deadline and intake
obligation in SQL, publishes a synced file with atomic no-replace creation,
rechecks authorization/fence/generations, and confirms the initial event before
acknowledgment. Retries recover the same prepared identity. A conflicting or
unavailable object is never overwritten or treated as persisted. API has no
write access to these records. Cancellation and its authorization reads use an
independent two-connection pool; occupied intake capacity does not gate them.

The one-second observer uses five-second Temporal calls outside SQL locks.
It persists the attempted-start marker before sending the fixed input, rejects
duplicate Workflow IDs and verifies retained history before establishing the
original Run. Namespace changes, missing history and uncertain reads cannot
create a replacement. Never-attempted work can expire locally; an attempted
start beyond its original window is query-only.

Terminal identity/content, result, projection, revision, event and slot release
commit together. Invalid results fail locally. Identical evidence is a no-op;
changed evidence conflicts. A cancel fence survives a late completion. Workflow
code makes no Control calls; this observer accepts evidence without choosing
business successors. Committed event logs are emitted after locks are released.

From the parent, the shared three-service proof is:

```sh
ANVILKIT_WORKFLOW_TEST_ENVIRONMENT=/path/to/environment.json \
  python3 tools/run-verification.py --only workflow-recovery
```

It creates a separate retained database on the handoff's persistent PostgreSQL,
uses the canonical API/Control/Worker binaries, and restarts only its service
processes and the handoff's PostgreSQL/Temporal containers. Faults exist only in
test tooling: filesystem/SQL boundaries, withheld start/completion transport,
a lost PostgreSQL COMMIT reply, and a `NotFound` history-read response for an
already bound execution. The last case tests uncertainty handling, not actual
retention expiry. Source/binary/contract hashes, commands, process exits, logs
and real histories are retained in the printed proof directory. No production,
paid-provider, business Workflow or disaster-recovery qualification is implied.
