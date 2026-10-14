// Package config builds Control's immutable configuration snapshot with
// koanf (A09, DD-09 §4). Precedence is defaults < the service's reviewed
// configuration file < the allowed ANVILKIT_CONTROL_* environment overrides.
// The candidate is validated (unknown keys, required values, ranges,
// cross-field rules) before anything starts; a rejected candidate never
// starts the process. The database URL and the S3 credentials are secrets:
// each is accepted only from the environment or from the mounted secret
// file its *_file key names (the OpenBao CSI injection path, P0.6), never
// from the reviewed file, and it is never logged.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

const (
	envPrefix         = "ANVILKIT_CONTROL_"
	EnvConfigFile     = "ANVILKIT_CONTROL_CONFIG"
	DefaultConfigFile = "config.yaml"
)

// GRPC is the internal listener and its reserved capacity pools (DD-02 §2).
type GRPC struct {
	Listen            string `koanf:"listen"`
	ControlCapacity   int    `koanf:"control_capacity"`
	ExecutionCapacity int    `koanf:"execution_capacity"`
	// StreamCapacity bounds the long-lived event streams of a replica and
	// StreamPerTenant the streams one tenant may hold (P1.4): streams never
	// take a slot of the control or execution pools.
	StreamCapacity  int `koanf:"stream_capacity"`
	StreamPerTenant int `koanf:"stream_per_tenant"`
	// CommandReserve is the number of control slots only SubmitCommand may take.
	CommandReserve int `koanf:"command_reserve"`
	// DefaultDeadline bounds a unary RPC whose caller set no deadline.
	DefaultDeadline time.Duration `koanf:"default_deadline"`
	ShutdownTimeout time.Duration `koanf:"shutdown_timeout"`
	// Identity is the listener's workload identity (P0.1): mtls serves TLS
	// 1.3 with required, verified client certificates and authorizes every
	// RPC by the peer's SPIFFE URI SAN; development is plaintext and
	// authorizes nothing, admitted only with development.enabled.
	Identity ServerIdentity `koanf:"identity"`
}

// ServerIdentity names the mounted identity material and the trust domain
// peers must belong to. The files are placements (ANVILKIT_CONTROL_IDENTITY_
// {CERT,KEY,CA}_FILE); they are watched and reloaded as a whole.
type ServerIdentity struct {
	Mode        string `koanf:"mode"`
	TrustDomain string `koanf:"trust_domain"`
	CertFile    string `koanf:"cert_file"`
	KeyFile     string `koanf:"key_file"`
	CAFile      string `koanf:"ca_file"`
	// MaxConnectionAge bounds every accepted connection so that no
	// connection outlives a trust change by more than this.
	MaxConnectionAge time.Duration `koanf:"max_connection_age"`
	// ReloadInterval is the polling interval of the identity files.
	ReloadInterval time.Duration `koanf:"reload_interval"`
}

// ClientTLS is the transport of an outbound connection whose server is not
// an AnvilKit workload (Temporal, the OTLP collector): development is
// plaintext (admitted only with development.enabled), tls verifies the
// server against ca_file and server_name, mtls additionally presents
// cert_file/key_file.
type ClientTLS struct {
	Mode       string `koanf:"mode"`
	CAFile     string `koanf:"ca_file"`
	CertFile   string `koanf:"cert_file"`
	KeyFile    string `koanf:"key_file"`
	ServerName string `koanf:"server_name"`
}

// Development is the top-level DEVELOPMENT_ONLY guard: every plaintext
// business connection of this process requires both its own development
// mode and Enabled; Enabled alone downgrades nothing. The database and the
// S3 endpoints name their mode themselves (the DSN's sslmode, the
// endpoint's scheme): outside development only sslmode=verify-full and
// https are accepted (P0.6). File-only.
type Development struct {
	Enabled bool `koanf:"enabled"`
}

// Health is the plaintext HTTP probe listener (/healthz, /readyz), never a
// business endpoint; inside a Pod it binds the Pod IP for the kubelet.
type Health struct {
	Listen string `koanf:"listen"`
	// CheckInterval and CheckTimeout drive the readiness ping of the
	// database (P1.4): readiness drops when a ping fails and returns with
	// the next successful one; liveness stays process-only.
	CheckInterval time.Duration `koanf:"check_interval"`
	CheckTimeout  time.Duration `koanf:"check_timeout"`
}

// Database holds the app-role connection: URL (env-only) or URLFile, a
// mounted secret read once at load (exactly one of the two). MaxConns is the
// exact per-replica pool bound that the deployment lock's connection budget
// counts (pool x replicas plus reserves within the cluster's limit).
type Database struct {
	URL      string `koanf:"url"`
	URLFile  string `koanf:"url_file"`
	MaxConns int32  `koanf:"max_conns"`
	// Connection, statement and idle-in-transaction bounds of the pool (P1.4).
	ConnectTimeout           time.Duration `koanf:"connect_timeout"`
	StatementTimeout         time.Duration `koanf:"statement_timeout"`
	IdleInTransactionTimeout time.Duration `koanf:"idle_in_transaction_timeout"`
}

// Inventory selects the obligation inventory backend (DD-02 §5): the
// DEVELOPMENT_ONLY filesystem store, or an S3-compatible backend through the
// AWS SDK (Ceph RGW is the C09 primary; the independent failure domain is
// an ENV-02 input this file cannot establish). The S3 credentials are
// secrets and arrive only from the environment or from mounted secret
// files.
type Inventory struct {
	Backend string      `koanf:"backend"`
	Dir     string      `koanf:"dir"`
	S3      InventoryS3 `koanf:"s3"`
}

// InventoryS3 is one S3-compatible placement. Each credential is either the
// value (env-only) or the path of a mounted secret file read once at load
// (*_file), never both.
type InventoryS3 struct {
	Endpoint            string `koanf:"endpoint"`
	Region              string `koanf:"region"`
	Bucket              string `koanf:"bucket"`
	Prefix              string `koanf:"prefix"`
	PathStyle           bool   `koanf:"path_style"`
	QualifyOnStart      bool   `koanf:"qualify_on_start"`
	AccessKeyID         string `koanf:"access_key_id"`
	AccessKeyIDFile     string `koanf:"access_key_id_file"`
	SecretAccessKey     string `koanf:"secret_access_key"`
	SecretAccessKeyFile string `koanf:"secret_access_key_file"`
}

const (
	InventoryFilesystem = "filesystem"
	InventoryS3Backend  = "s3"
)

// Artifacts is the scoped artifact store of the eight artifact classes
// (DD-02 §6, P08): an S3-compatible backend with its own bucket and
// credentials (a permission boundary separate from the inventory), or
// disabled, in which case every ArtifactService call answers
// DEPENDENCY_UNAVAILABLE. The bounds are the reviewed limits of one
// transfer: the largest object, the longest transfer window and the
// lifetime of one upload capability.
type Artifacts struct {
	Backend        string        `koanf:"backend"`
	MaxObjectBytes int64         `koanf:"max_object_bytes"`
	MaxWindow      time.Duration `koanf:"max_window"`
	CapabilityTTL  time.Duration `koanf:"capability_ttl"`
	// FinalizeConcurrency bounds the finalizations that hold object bytes
	// (archive checks) at once (P1.5): sized to the memory limit.
	FinalizeConcurrency int         `koanf:"finalize_concurrency"`
	S3                  InventoryS3 `koanf:"s3"`
}

const (
	ArtifactsDisabled  = "disabled"
	ArtifactsS3Backend = "s3"
)

type Temporal struct {
	Address   string    `koanf:"address"`
	Namespace string    `koanf:"namespace"`
	TaskQueue string    `koanf:"task_queue"`
	TLS       ClientTLS `koanf:"tls"`
}

type Relay struct {
	Interval time.Duration `koanf:"interval"`
	// UpdateTimeout bounds one tracked Update call of the answer and command
	// relays (P1.4): an accepted Update still running at the bound stays sent
	// and is settled by a later tick under the same update id.
	UpdateTimeout time.Duration `koanf:"update_timeout"`
}

// ModelProxy is the Model Proxy placement of the original-identity query of
// model dispatches (DD-02 §4 "query original ID", P11): with an address,
// recovery asks the Proxy's GET /api/v1/model-calls/{callId} about an
// unresolved model dispatch; without one the DEVELOPMENT_ONLY attestation
// double keeps answering. The identity Control presents: development is a
// DEVELOPMENT_ONLY bearer token from ANVILKIT_CONTROL_MODEL_PROXY_TOKEN,
// mtls the workload certificate files (ENV-03). Owner is the sender
// identity the Proxy records on its dispatches; a dispatch of another owner
// is not the Proxy's to answer.
type ModelProxy struct {
	Address  string        `koanf:"address"`
	Timeout  time.Duration `koanf:"timeout"`
	Owner    string        `koanf:"owner"`
	Token    string        `koanf:"token"`
	Identity struct {
		Mode string `koanf:"mode"`
		MTLS struct {
			CertFile   string `koanf:"cert_file"`
			KeyFile    string `koanf:"key_file"`
			CAFile     string `koanf:"ca_file"`
			ServerName string `koanf:"server_name"`
		} `koanf:"mtls"`
	} `koanf:"identity"`
}

// Profiles carries the reviewed profile parameters of this build: the
// LocalCheck fixture, the Preparation profile (its clarification trial
// defaults and its analysis funding) and the Generation profile (its queue,
// active window, funding, definitions, repair bound and job profiles).
type Profiles struct {
	LocalCheckDeadline time.Duration      `koanf:"local_check_deadline"`
	Preparation        PreparationProfile `koanf:"preparation"`
	Generation         GenerationProfile  `koanf:"generation"`
	PreviewBuild       PreviewProfile     `koanf:"preview_build"`
	Release            ReleaseProfile     `koanf:"release"`
}

// ReleaseProfile: the release operation deadline (P21): certification of
// the exact source, the maintainer's approval wait, both publications and
// the activation. It bounds the long approval wait absolutely; nothing
// extends it. Destinations and job profiles are the Workflow's.
type ReleaseProfile struct {
	Deadline time.Duration `koanf:"deadline"`
	// AttemptWindow bounds each attempt (the certification Job, each
	// guarded mutation) below the operation deadline.
	AttemptWindow time.Duration `koanf:"attempt_window"`
}

// PreviewProfile: the preview_build operation deadline (conditional save
// and the isolated build of the saved revision; P20, compute only, no
// funding and no model call). The build's job profile is the Workflow's.
type PreviewProfile struct {
	Deadline time.Duration `koanf:"deadline"`
}

// PreparationProfile: the operation deadline bounds the whole preparation
// (analysis and every wait); the clarification bounds are the trial
// defaults of requirements.md §2 (two rounds, three questions, seven
// days); funding is the analysis allowance the operation allocates from
// the shared pools (currency and scale-6 amount as decimal strings).
type PreparationProfile struct {
	Deadline     time.Duration `koanf:"deadline"`
	MaxRounds    uint64        `koanf:"max_rounds"`
	MaxQuestions uint64        `koanf:"max_questions"`
	Wait         time.Duration `koanf:"wait"`
	Funding      MoneyValue    `koanf:"funding"`
}

// GenerationProfile: queue_deadline bounds admission waiting from intake;
// active_window is the execution window the first permit opens once;
// capacity is the size of the execution pool; funding is the reviewed
// allocation; definitions are the reviewed definition activations (the
// first is the default); max_repairs bounds the independently classified
// repair rounds; codegen_profile and validator_profile are the reviewed
// job profiles the steps launch.
type GenerationProfile struct {
	QueueDeadline    time.Duration `koanf:"queue_deadline"`
	ActiveWindow     time.Duration `koanf:"active_window"`
	Capacity         int32         `koanf:"capacity"`
	Funding          MoneyValue    `koanf:"funding"`
	Definitions      []string      `koanf:"definitions"`
	MaxRepairs       uint64        `koanf:"max_repairs"`
	CodegenProfile   string        `koanf:"codegen_profile"`
	ValidatorProfile string        `koanf:"validator_profile"`
}

// MoneyValue is a reviewed amount: currency and scale-6 decimal string.
type MoneyValue struct {
	Currency string `koanf:"currency"`
	Amount   string `koanf:"amount"`
}

// Dispatch bounds the single-use admission chain (DD-02 §3–§4).
// AuthorityFreshness is the absolute freshness of a commercial authority
// decision (at most 30 s in the baseline, never refreshed by a hit).
type Dispatch struct {
	AuthorityFreshness time.Duration       `koanf:"authority_freshness"`
	Development        DispatchDevelopment `koanf:"development"`
	// ReconcileInterval and ReconcileAge drive the original-identity
	// reconciliation of AUTHORIZED and UNKNOWN dispatches outside recovery
	// runs (P1.5, CTL-APP-05): a dispatch admitted longer ago than the age
	// is asked about every interval until its outcome is known.
	ReconcileInterval time.Duration `koanf:"reconcile_interval"`
	ReconcileAge      time.Duration `koanf:"reconcile_age"`
	// The production authority inputs (P2.1): the commercial authority
	// source, the trusted evidence issuers and the price book. Outside
	// dispatch.development every one is required; Control refuses to start
	// without them rather than deny every paid route silently.
	Authority AuthoritySource `koanf:"authority"`
	Evidence  Evidence        `koanf:"evidence"`
	Prices    Prices          `koanf:"prices"`
}

// AuthoritySource is the guarded client of the commercial authority decision
// endpoint of the ENV-07 declaration (DD-06 AuthorizationPort): an absolute
// https URL, the CA that verifies it and the workload certificate (mTLS) or
// a bearer token file (an OAuth client-credentials token the platform keeps
// current; the file is CSI-mounted and never the value).
type AuthoritySource struct {
	URL        string        `koanf:"url"`
	Timeout    time.Duration `koanf:"timeout"`
	CAFile     string        `koanf:"ca_file"`
	CertFile   string        `koanf:"cert_file"`
	KeyFile    string        `koanf:"key_file"`
	ServerName string        `koanf:"server_name"`
	TokenFile  string        `koanf:"token_file"`
}

// Evidence names the trusted issuers of signed not-sent and disposition
// evidence: one Ed25519 public key file (PEM, PKIX) per key id; rotation
// adds a key id.
type Evidence struct {
	Issuers []EvidenceIssuer `koanf:"issuers"`
}

type EvidenceIssuer struct {
	Name          string `koanf:"name"`
	KeyID         string `koanf:"key_id"`
	PublicKeyFile string `koanf:"public_key_file"`
}

// Prices place the price book: the refresh interval of every replica's
// snapshot and the reviewed ENV-06 price book file published (idempotently)
// at start; a database without any price revision and no initial file
// refuses to start outside dispatch.development.
type Prices struct {
	RefreshInterval time.Duration `koanf:"refresh_interval"`
	InitialFile     string        `koanf:"initial_file"`
}

// DispatchDevelopment is the DEVELOPMENT_ONLY source of prices, route
// authorizations and trusted not-sent issuers (delivery.md P06). Real
// prices and caps (ENV-06) and real authorization (ENV-07) are deployment
// inputs; with Enabled false every paid route is denied. It is file-only:
// no environment variable can enable it.
type DispatchDevelopment struct {
	Enabled          bool                 `koanf:"enabled"`
	Prices           []PriceFixture       `koanf:"prices"`
	AuthorizedRoutes []RouteAuthorization `koanf:"authorized_routes"`
	NotSentIssuers   []string             `koanf:"not_sent_issuers"`
}

// PriceFixture is one immutable pricing observation of the fixture: money
// values are canonical scale-6 decimal strings, unit prices are per one
// million units, the interval is RFC 3339. A model price binds the trusted
// provider and model of its route; a tool price binds neither.
type PriceFixture struct {
	Revision                string            `koanf:"revision"`
	Kind                    string            `koanf:"kind"`
	Route                   string            `koanf:"route"`
	Provider                string            `koanf:"provider"`
	Model                   string            `koanf:"model"`
	Currency                string            `koanf:"currency"`
	EffectiveFrom           time.Time         `koanf:"effective_from"`
	EffectiveUntil          time.Time         `koanf:"effective_until"`
	PerMillion              map[string]string `koanf:"per_million"`
	MaxExposure             string            `koanf:"max_exposure"`
	InputIncludesCached     bool              `koanf:"input_includes_cached"`
	OutputIncludesReasoning bool              `koanf:"output_includes_reasoning"`
}

type RouteAuthorization struct {
	TenantID string `koanf:"tenant_id"`
	RouteID  string `koanf:"route_id"`
}

// Recovery bounds the reconciliation of a rollback window (DD-02 §5):
// EnumerationPage is the inventory listing page size.
type Recovery struct {
	EnumerationPage int `koanf:"enumeration_page"`
}

// Telemetry places the redacted signals (security.md "data classification,
// logging and deletion"): spans over OTLP to the collector when an endpoint
// is placed (no exporter otherwise), sampled at SampleRatio, and the
// Prometheus metrics on their own listener.
type Telemetry struct {
	OTLPEndpoint  string  `koanf:"otlp_endpoint"`
	SampleRatio   float64 `koanf:"sample_ratio"`
	MetricsListen string  `koanf:"metrics_listen"`
	// OTLPTLS is validated only while an endpoint is placed.
	OTLPTLS ClientTLS `koanf:"otlp_tls"`
}

// Events is the publication of Control's domain events (P2.8): every
// operation event inserts its operation.changed envelope into the outbox
// on the same transaction (always); with Enabled, the in-process forwarder
// module publishes the outbox to JetStream (the ANVILKIT_CONTROL stream,
// a deployment input) and the waker consumes operation.changed to wake the
// event streams on every replica. Outside development it must be enabled.
// Retention deletes rows the consumer group acknowledged after Retention,
// a bounded batch every RetentionInterval.
type Events struct {
	Enabled           bool          `koanf:"enabled"`
	Wake              bool          `koanf:"wake"`
	ConsumerGroup     string        `koanf:"consumer_group"`
	PollInterval      time.Duration `koanf:"poll_interval"`
	AckDeadline       time.Duration `koanf:"ack_deadline"`
	ResendInterval    time.Duration `koanf:"resend_interval"`
	BatchSize         int           `koanf:"batch_size"`
	CloseTimeout      time.Duration `koanf:"close_timeout"`
	Retention         time.Duration `koanf:"retention"`
	RetentionInterval time.Duration `koanf:"retention_interval"`
	RetentionBatch    int           `koanf:"retention_batch"`
	NATS              NATS          `koanf:"nats"`
}

// NATS is the replica's JetStream connection. TLS is accepted from the
// file or the environment (ANVILKIT_CONTROL_NATS_TLS_{MODE,CA_FILE,
// CERT_FILE,KEY_FILE,SERVER_NAME}); CredsFile is a placement
// (ANVILKIT_CONTROL_NATS_CREDS_FILE): a mounted NATS user .creds file or a
// bare NKey user seed, read and classified at load (CredsKind) and required
// outside development (P0.6).
type NATS struct {
	URL            string        `koanf:"url"`
	Name           string        `koanf:"name"`
	PublishTimeout time.Duration `koanf:"publish_timeout"`
	TLS            ClientTLS     `koanf:"tls"`
	CredsFile      string        `koanf:"creds_file"`
	// CredsKind is NATSCredsUser or NATSCredsNKey, set by the loader from
	// the content of CredsFile; empty without a credential.
	CredsKind string `koanf:"-"`
}

// The kinds of a NATS client credential file.
const (
	// NATSCredsUser is a NATS user .creds file (the decorated user JWT and
	// its NKey seed).
	NATSCredsUser = "user"
	// NATSCredsNKey is a bare NKey user seed (the first non-empty line
	// starts with SU).
	NATSCredsNKey = "nkey"
)

type Config struct {
	Development Development `koanf:"development"`
	Health      Health      `koanf:"health"`
	Telemetry   Telemetry   `koanf:"telemetry"`
	GRPC        GRPC        `koanf:"grpc"`
	Database    Database    `koanf:"database"`
	Inventory   Inventory   `koanf:"inventory"`
	Artifacts   Artifacts   `koanf:"artifacts"`
	Temporal    Temporal    `koanf:"temporal"`
	Relay       Relay       `koanf:"relay"`
	Profiles    Profiles    `koanf:"profiles"`
	Dispatch    Dispatch    `koanf:"dispatch"`
	Recovery    Recovery    `koanf:"recovery"`
	ModelProxy  ModelProxy  `koanf:"model_proxy"`
	Events      Events      `koanf:"events"`
}

var defaults = map[string]any{
	"grpc.listen":                           "127.0.0.1:9101",
	"grpc.control_capacity":                 32,
	"grpc.execution_capacity":               64,
	"grpc.stream_capacity":                  256,
	"grpc.stream_per_tenant":                32,
	"grpc.command_reserve":                  4,
	"grpc.default_deadline":                 "30s",
	"grpc.shutdown_timeout":                 "20s",
	"grpc.identity.mode":                    "mtls",
	"grpc.identity.max_connection_age":      "1h",
	"grpc.identity.reload_interval":         "5s",
	"development.enabled":                   false,
	"health.listen":                         "127.0.0.1:9113",
	"health.check_interval":                 "5s",
	"health.check_timeout":                  "2s",
	"database.connect_timeout":              "5s",
	"database.statement_timeout":            "30s",
	"database.idle_in_transaction_timeout":  "30s",
	"temporal.tls.mode":                     "tls",
	"telemetry.otlp_tls.mode":               "tls",
	"database.max_conns":                    8,
	"telemetry.sample_ratio":                1.0,
	"temporal.namespace":                    "anvilkit",
	"temporal.task_queue":                   "anvilkit-workflow",
	"relay.interval":                        "500ms",
	"relay.update_timeout":                  "5s",
	"profiles.local_check_deadline":         "15m",
	"profiles.preparation.deadline":         "360h",
	"profiles.preparation.max_rounds":       2,
	"profiles.preparation.max_questions":    3,
	"profiles.preparation.wait":             "168h",
	"profiles.preparation.funding":          map[string]any{"currency": "USD", "amount": "500000"},
	"profiles.generation.queue_deadline":    "24h",
	"profiles.generation.active_window":     "2h",
	"profiles.generation.capacity":          2,
	"profiles.generation.funding":           map[string]any{"currency": "USD", "amount": "1000000"},
	"profiles.generation.definitions":       []string{"generation-v1:def-1", "generation-v1:def-2"},
	"profiles.generation.max_repairs":       1,
	"profiles.generation.codegen_profile":   "codegen-team-dev-v1",
	"profiles.generation.validator_profile": "validator-source-v1",
	"profiles.preview_build.deadline":       "30m",
	"profiles.release.deadline":             "720h",
	"profiles.release.attempt_window":       "1h",
	"dispatch.authority_freshness":          "30s",
	"dispatch.reconcile_interval":           "30s",
	"dispatch.reconcile_age":                "10m",
	"dispatch.authority.timeout":            "5s",
	"dispatch.prices.refresh_interval":      "2s",
	"artifacts.finalize_concurrency":        2,
	"dispatch.development.enabled":          false,
	"inventory.backend":                     InventoryFilesystem,
	"inventory.s3.path_style":               true,
	"inventory.s3.qualify_on_start":         true,
	"recovery.enumeration_page":             500,
	"model_proxy.timeout":                   "15s",
	"model_proxy.owner":                     "anvilkit-agent-model-proxy",
	"model_proxy.identity.mode":             "development",
	"artifacts.backend":                     ArtifactsDisabled,
	"artifacts.max_object_bytes":            64 << 20,
	"artifacts.max_window":                  "24h",
	"artifacts.capability_ttl":              "15m",
	"artifacts.s3.path_style":               true,
	"artifacts.s3.qualify_on_start":         true,
	"events.enabled":                        false,
	"events.wake":                           true,
	"events.consumer_group":                 "anvilkit-agent-control-forwarder",
	"events.poll_interval":                  "200ms",
	"events.ack_deadline":                   "30s",
	"events.resend_interval":                "1s",
	"events.batch_size":                     100,
	"events.close_timeout":                  "10s",
	"events.retention":                      "168h",
	"events.retention_interval":             "10m",
	"events.retention_batch":                1000,
	"events.nats.name":                      "anvilkit-agent-control",
	"events.nats.publish_timeout":           "5s",
	"events.nats.tls.mode":                  "tls",
}

// envOverrides is the complete set of accepted environment variables:
// deployment placement, the secrets and the mounted secret files. Any other
// ANVILKIT_CONTROL_* variable rejects the candidate.
var envOverrides = map[string]string{
	"ANVILKIT_CONTROL_LISTEN":                              "grpc.listen",
	"ANVILKIT_CONTROL_HEALTH_LISTEN":                       "health.listen",
	"ANVILKIT_CONTROL_IDENTITY_CERT_FILE":                  "grpc.identity.cert_file",
	"ANVILKIT_CONTROL_IDENTITY_KEY_FILE":                   "grpc.identity.key_file",
	"ANVILKIT_CONTROL_IDENTITY_CA_FILE":                    "grpc.identity.ca_file",
	"ANVILKIT_CONTROL_IDENTITY_TRUST_DOMAIN":               "grpc.identity.trust_domain",
	"ANVILKIT_CONTROL_DATABASE_URL":                        "database.url",
	"ANVILKIT_CONTROL_DATABASE_URL_FILE":                   "database.url_file",
	"ANVILKIT_CONTROL_TELEMETRY_OTLP_ENDPOINT":             "telemetry.otlp_endpoint",
	"ANVILKIT_CONTROL_TELEMETRY_METRICS_LISTEN":            "telemetry.metrics_listen",
	"ANVILKIT_CONTROL_INVENTORY_DIR":                       "inventory.dir",
	"ANVILKIT_CONTROL_INVENTORY_S3_ENDPOINT":               "inventory.s3.endpoint",
	"ANVILKIT_CONTROL_INVENTORY_S3_BUCKET":                 "inventory.s3.bucket",
	"ANVILKIT_CONTROL_INVENTORY_S3_ACCESS_KEY_ID":          "inventory.s3.access_key_id",
	"ANVILKIT_CONTROL_INVENTORY_S3_ACCESS_KEY_ID_FILE":     "inventory.s3.access_key_id_file",
	"ANVILKIT_CONTROL_INVENTORY_S3_SECRET_ACCESS_KEY":      "inventory.s3.secret_access_key",
	"ANVILKIT_CONTROL_INVENTORY_S3_SECRET_ACCESS_KEY_FILE": "inventory.s3.secret_access_key_file",
	"ANVILKIT_CONTROL_TEMPORAL_ADDRESS":                    "temporal.address",
	"ANVILKIT_CONTROL_MODEL_PROXY_ADDRESS":                 "model_proxy.address",
	"ANVILKIT_CONTROL_MODEL_PROXY_TOKEN":                   "model_proxy.token",
	"ANVILKIT_CONTROL_AUTHORITY_URL":                       "dispatch.authority.url",
	"ANVILKIT_CONTROL_AUTHORITY_TOKEN_FILE":                "dispatch.authority.token_file",
	"ANVILKIT_CONTROL_PRICES_INITIAL_FILE":                 "dispatch.prices.initial_file",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ENDPOINT":               "artifacts.s3.endpoint",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_BUCKET":                 "artifacts.s3.bucket",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ACCESS_KEY_ID":          "artifacts.s3.access_key_id",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ACCESS_KEY_ID_FILE":     "artifacts.s3.access_key_id_file",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_SECRET_ACCESS_KEY":      "artifacts.s3.secret_access_key",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_SECRET_ACCESS_KEY_FILE": "artifacts.s3.secret_access_key_file",
	"ANVILKIT_CONTROL_NATS_URL":                            "events.nats.url",
	"ANVILKIT_CONTROL_NATS_CREDS_FILE":                     "events.nats.creds_file",
	"ANVILKIT_CONTROL_NATS_TLS_MODE":                       "events.nats.tls.mode",
	"ANVILKIT_CONTROL_NATS_TLS_CA_FILE":                    "events.nats.tls.ca_file",
	"ANVILKIT_CONTROL_NATS_TLS_CERT_FILE":                  "events.nats.tls.cert_file",
	"ANVILKIT_CONTROL_NATS_TLS_KEY_FILE":                   "events.nats.tls.key_file",
	"ANVILKIT_CONTROL_NATS_TLS_SERVER_NAME":                "events.nats.tls.server_name",
}

// secretKeys may only arrive through the environment.
var secretKeys = []string{"database.url", "inventory.s3.access_key_id", "inventory.s3.secret_access_key", "artifacts.s3.access_key_id", "artifacts.s3.secret_access_key", "model_proxy.token"}

func Load() (Config, error) {
	path := os.Getenv(EnvConfigFile)
	if path == "" {
		path = DefaultConfigFile
	}
	return LoadFrom(path, os.Environ())
}

// LoadFrom is Load with explicit inputs (tests).
func LoadFrom(path string, environ []string) (Config, error) {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(defaults, "."), nil); err != nil {
		return Config{}, err
	}
	reviewed := koanf.New(".")
	if err := reviewed.Load(file.Provider(path), yaml.Parser()); err != nil {
		return Config{}, fmt.Errorf("config file %s: %w", path, err)
	}
	for _, key := range secretKeys {
		if reviewed.Exists(key) {
			return Config{}, fmt.Errorf("config file %s: %s is a secret and is accepted only from the environment", path, key)
		}
	}
	if err := k.Merge(reviewed); err != nil {
		return Config{}, err
	}
	if err := applyEnv(k, environ); err != nil {
		return Config{}, err
	}
	var c Config
	if err := k.UnmarshalWithConf("", &c, koanf.UnmarshalConf{DecoderConfig: &mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.ComposeDecodeHookFunc(mapstructure.StringToTimeDurationHookFunc(), mapstructure.StringToTimeHookFunc(time.RFC3339)),
		ErrorUnused:      true,
		WeaklyTypedInput: true,
		Result:           &c,
	}}); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if err := c.readSecretFiles(); err != nil {
		return Config{}, err
	}
	if c.Events.Enabled && c.Events.NATS.CredsFile != "" {
		kind, err := readNATSCredentials(c.Events.NATS.CredsFile)
		if err != nil {
			return Config{}, fmt.Errorf("config: events.nats.creds_file: %w", err)
		}
		c.Events.NATS.CredsKind = kind
	}
	return c, c.validate()
}

// readSecretFiles resolves each secret that names a mounted file instead of
// a value (P0.6, the OpenBao CSI volume): the value and its *_file key are
// mutually exclusive, and the file is read once, here; its content replaces
// the value for the life of the process.
func (c *Config) readSecretFiles() error {
	var errs []error
	for _, s := range []struct {
		key, env string
		value    *string
		file     string
	}{
		{"database.url", "ANVILKIT_CONTROL_DATABASE_URL", &c.Database.URL, c.Database.URLFile},
		{"inventory.s3.access_key_id", "ANVILKIT_CONTROL_INVENTORY_S3_ACCESS_KEY_ID", &c.Inventory.S3.AccessKeyID, c.Inventory.S3.AccessKeyIDFile},
		{"inventory.s3.secret_access_key", "ANVILKIT_CONTROL_INVENTORY_S3_SECRET_ACCESS_KEY", &c.Inventory.S3.SecretAccessKey, c.Inventory.S3.SecretAccessKeyFile},
		{"artifacts.s3.access_key_id", "ANVILKIT_CONTROL_ARTIFACTS_S3_ACCESS_KEY_ID", &c.Artifacts.S3.AccessKeyID, c.Artifacts.S3.AccessKeyIDFile},
		{"artifacts.s3.secret_access_key", "ANVILKIT_CONTROL_ARTIFACTS_S3_SECRET_ACCESS_KEY", &c.Artifacts.S3.SecretAccessKey, c.Artifacts.S3.SecretAccessKeyFile},
	} {
		switch {
		case s.file == "":
		case *s.value != "":
			errs = append(errs, fmt.Errorf("config: %s and %s_file are mutually exclusive (%s, %s_FILE)", s.key, s.key, s.env, s.env))
		default:
			v, err := ReadSecretFile(s.key+"_file", s.file)
			if err != nil {
				errs = append(errs, fmt.Errorf("config: %w", err))
				continue
			}
			*s.value = v
		}
	}
	return errors.Join(errs...)
}

// ReadSecretFile reads a mounted secret file once: surrounding whitespace
// (the trailing newline) is trimmed, and an unreadable or empty file is an
// error naming key and the path, never the content.
func ReadSecretFile(key, path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%s: %w", key, err)
	}
	v := strings.TrimSpace(string(raw))
	if v == "" {
		return "", fmt.Errorf("%s: %s is empty", key, path)
	}
	return v, nil
}

func applyEnv(k *koanf.Koanf, environ []string) error {
	var unknown []string
	for _, kv := range environ {
		name, value, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, envPrefix) || name == EnvConfigFile {
			continue
		}
		key, ok := envOverrides[name]
		if !ok {
			unknown = append(unknown, name)
			continue
		}
		if err := k.Set(key, value); err != nil {
			return err
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("config: environment variables are not allowed overrides: %s", strings.Join(unknown, ", "))
	}
	return nil
}

func (c Config) validate() error {
	var errs []error
	req := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	req("grpc.listen", c.GRPC.Listen)
	req("health.listen", c.Health.Listen)
	if c.Health.Listen != "" && c.Health.Listen == c.GRPC.Listen {
		errs = append(errs, errors.New("health.listen must not be grpc.listen: the probe listener never carries business traffic"))
	}
	errs = append(errs, c.validateIdentity()...)
	errs = append(errs, c.validateDataPlane()...)
	errs = append(errs, c.validateEvents()...)
	req("database.url (ANVILKIT_CONTROL_DATABASE_URL or ANVILKIT_CONTROL_DATABASE_URL_FILE)", c.Database.URL)
	if c.Telemetry.SampleRatio < 0 || c.Telemetry.SampleRatio > 1 {
		errs = append(errs, fmt.Errorf("telemetry.sample_ratio %v outside [0, 1]", c.Telemetry.SampleRatio))
	}
	if c.Telemetry.MetricsListen != "" && c.Telemetry.MetricsListen == c.GRPC.Listen {
		errs = append(errs, fmt.Errorf("telemetry.metrics_listen must not be grpc.listen"))
	}
	if c.Database.MaxConns < 1 || c.Database.MaxConns > 100 {
		errs = append(errs, fmt.Errorf("database.max_conns %d outside [1, 100]", c.Database.MaxConns))
	}
	switch c.Inventory.Backend {
	case InventoryFilesystem:
		req("inventory.dir", c.Inventory.Dir)
	case InventoryS3Backend:
		req("inventory.s3.endpoint (ANVILKIT_CONTROL_INVENTORY_S3_ENDPOINT)", c.Inventory.S3.Endpoint)
		req("inventory.s3.region", c.Inventory.S3.Region)
		req("inventory.s3.bucket (ANVILKIT_CONTROL_INVENTORY_S3_BUCKET)", c.Inventory.S3.Bucket)
		req("inventory.s3.access_key_id (ANVILKIT_CONTROL_INVENTORY_S3_ACCESS_KEY_ID or _FILE)", c.Inventory.S3.AccessKeyID)
		req("inventory.s3.secret_access_key (ANVILKIT_CONTROL_INVENTORY_S3_SECRET_ACCESS_KEY or _FILE)", c.Inventory.S3.SecretAccessKey)
	default:
		errs = append(errs, fmt.Errorf("inventory.backend %q is not filesystem or s3", c.Inventory.Backend))
	}
	if c.Recovery.EnumerationPage < 1 || c.Recovery.EnumerationPage > 1000 {
		errs = append(errs, fmt.Errorf("recovery.enumeration_page %d outside [1, 1000]", c.Recovery.EnumerationPage))
	}
	switch c.Artifacts.Backend {
	case ArtifactsDisabled:
	case ArtifactsS3Backend:
		req("artifacts.s3.endpoint (ANVILKIT_CONTROL_ARTIFACTS_S3_ENDPOINT)", c.Artifacts.S3.Endpoint)
		req("artifacts.s3.region", c.Artifacts.S3.Region)
		req("artifacts.s3.bucket (ANVILKIT_CONTROL_ARTIFACTS_S3_BUCKET)", c.Artifacts.S3.Bucket)
		req("artifacts.s3.access_key_id (ANVILKIT_CONTROL_ARTIFACTS_S3_ACCESS_KEY_ID or _FILE)", c.Artifacts.S3.AccessKeyID)
		req("artifacts.s3.secret_access_key (ANVILKIT_CONTROL_ARTIFACTS_S3_SECRET_ACCESS_KEY or _FILE)", c.Artifacts.S3.SecretAccessKey)
		if c.Inventory.Backend == InventoryS3Backend && c.Artifacts.S3.Bucket == c.Inventory.S3.Bucket && c.Artifacts.S3.Endpoint == c.Inventory.S3.Endpoint {
			errs = append(errs, errors.New("artifacts.s3 and inventory.s3 name the same bucket; the artifact store and the obligation inventory are separate permission boundaries"))
		}
		if c.Inventory.Backend == InventoryS3Backend && c.Artifacts.S3.AccessKeyID == c.Inventory.S3.AccessKeyID {
			errs = append(errs, errors.New("artifacts.s3 and inventory.s3 share an access key; the artifact store and the obligation inventory are separate permission boundaries"))
		}
	default:
		errs = append(errs, fmt.Errorf("artifacts.backend %q is not disabled or s3", c.Artifacts.Backend))
	}
	if c.Artifacts.MaxObjectBytes < 1<<10 || c.Artifacts.MaxObjectBytes > 1<<30 {
		errs = append(errs, fmt.Errorf("artifacts.max_object_bytes %d outside [1 KiB, 1 GiB]", c.Artifacts.MaxObjectBytes))
	}
	if c.Artifacts.MaxWindow < time.Minute || c.Artifacts.MaxWindow > 7*24*time.Hour {
		errs = append(errs, fmt.Errorf("artifacts.max_window %s outside [1m, 168h]", c.Artifacts.MaxWindow))
	}
	if c.Artifacts.CapabilityTTL < time.Minute || c.Artifacts.CapabilityTTL > c.Artifacts.MaxWindow {
		errs = append(errs, fmt.Errorf("artifacts.capability_ttl %s outside [1m, artifacts.max_window %s]", c.Artifacts.CapabilityTTL, c.Artifacts.MaxWindow))
	}
	req("temporal.address", c.Temporal.Address)
	req("temporal.namespace", c.Temporal.Namespace)
	req("temporal.task_queue", c.Temporal.TaskQueue)
	if c.GRPC.ControlCapacity < 1 || c.GRPC.ControlCapacity > 4096 {
		errs = append(errs, fmt.Errorf("grpc.control_capacity %d outside [1, 4096]", c.GRPC.ControlCapacity))
	}
	if c.GRPC.ExecutionCapacity < 1 || c.GRPC.ExecutionCapacity > 4096 {
		errs = append(errs, fmt.Errorf("grpc.execution_capacity %d outside [1, 4096]", c.GRPC.ExecutionCapacity))
	}
	if c.GRPC.ShutdownTimeout < time.Second || c.GRPC.ShutdownTimeout > 5*time.Minute {
		errs = append(errs, fmt.Errorf("grpc.shutdown_timeout %s outside [1s, 5m]", c.GRPC.ShutdownTimeout))
	}
	if c.GRPC.StreamCapacity < 1 || c.GRPC.StreamCapacity > 65536 {
		errs = append(errs, fmt.Errorf("grpc.stream_capacity %d outside [1, 65536]", c.GRPC.StreamCapacity))
	}
	if c.GRPC.StreamPerTenant < 1 || c.GRPC.StreamPerTenant > c.GRPC.StreamCapacity {
		errs = append(errs, fmt.Errorf("grpc.stream_per_tenant %d outside [1, grpc.stream_capacity]", c.GRPC.StreamPerTenant))
	}
	if c.GRPC.CommandReserve < 0 || c.GRPC.CommandReserve >= c.GRPC.ControlCapacity {
		errs = append(errs, fmt.Errorf("grpc.command_reserve %d outside [0, grpc.control_capacity)", c.GRPC.CommandReserve))
	}
	if c.GRPC.DefaultDeadline < time.Second || c.GRPC.DefaultDeadline > 10*time.Minute {
		errs = append(errs, fmt.Errorf("grpc.default_deadline %s outside [1s, 10m]", c.GRPC.DefaultDeadline))
	}
	if c.Health.CheckInterval < time.Second || c.Health.CheckInterval > time.Minute {
		errs = append(errs, fmt.Errorf("health.check_interval %s outside [1s, 1m]", c.Health.CheckInterval))
	}
	if c.Health.CheckTimeout < 100*time.Millisecond || c.Health.CheckTimeout > c.Health.CheckInterval {
		errs = append(errs, fmt.Errorf("health.check_timeout %s outside [100ms, health.check_interval]", c.Health.CheckTimeout))
	}
	for name, d := range map[string]time.Duration{"database.connect_timeout": c.Database.ConnectTimeout, "database.statement_timeout": c.Database.StatementTimeout, "database.idle_in_transaction_timeout": c.Database.IdleInTransactionTimeout} {
		if d < time.Second || d > 10*time.Minute {
			errs = append(errs, fmt.Errorf("%s %s outside [1s, 10m]", name, d))
		}
	}
	if c.Dispatch.ReconcileInterval < time.Second || c.Dispatch.ReconcileInterval > time.Hour {
		errs = append(errs, fmt.Errorf("dispatch.reconcile_interval %s outside [1s, 1h]", c.Dispatch.ReconcileInterval))
	}
	if c.Dispatch.ReconcileAge < time.Minute || c.Dispatch.ReconcileAge > 24*time.Hour {
		errs = append(errs, fmt.Errorf("dispatch.reconcile_age %s outside [1m, 24h]", c.Dispatch.ReconcileAge))
	}
	if c.Artifacts.FinalizeConcurrency < 1 || c.Artifacts.FinalizeConcurrency > 64 {
		errs = append(errs, fmt.Errorf("artifacts.finalize_concurrency %d outside [1, 64]", c.Artifacts.FinalizeConcurrency))
	}
	if c.Relay.UpdateTimeout < time.Second || c.Relay.UpdateTimeout > time.Minute {
		errs = append(errs, fmt.Errorf("relay.update_timeout %s outside [1s, 1m]", c.Relay.UpdateTimeout))
	}
	if c.Relay.Interval < 100*time.Millisecond || c.Relay.Interval > time.Minute {
		errs = append(errs, fmt.Errorf("relay.interval %s outside [100ms, 1m]", c.Relay.Interval))
	}
	if pb := c.Profiles.PreviewBuild; pb.Deadline < time.Minute || pb.Deadline > 24*time.Hour {
		errs = append(errs, fmt.Errorf("profiles.preview_build.deadline %s outside [1m, 24h]", pb.Deadline))
	}
	if rp := c.Profiles.Release; rp.Deadline < time.Hour || rp.Deadline > 2160*time.Hour {
		errs = append(errs, fmt.Errorf("profiles.release.deadline %s outside [1h, 2160h]", rp.Deadline))
	}
	if rp := c.Profiles.Release; rp.AttemptWindow < time.Minute || rp.AttemptWindow > 24*time.Hour || rp.AttemptWindow > rp.Deadline {
		errs = append(errs, fmt.Errorf("profiles.release.attempt_window %s outside [1m, min(24h, deadline)]", rp.AttemptWindow))
	}
	if c.Profiles.LocalCheckDeadline < time.Minute || c.Profiles.LocalCheckDeadline > 24*time.Hour {
		errs = append(errs, fmt.Errorf("profiles.local_check_deadline %s outside [1m, 24h]", c.Profiles.LocalCheckDeadline))
	}
	// The relay must poll more often than the shortest operation deadline
	// can elapse, otherwise a confirmed intake could wait past its clock.
	p := c.Profiles.Preparation
	if p.MaxRounds < 1 || p.MaxRounds > 8 || p.MaxQuestions < 1 || p.MaxQuestions > 16 {
		errs = append(errs, fmt.Errorf("profiles.preparation max_rounds %d / max_questions %d outside [1, 8] / [1, 16]", p.MaxRounds, p.MaxQuestions))
	}
	if p.Wait < time.Minute || p.Wait > 30*24*time.Hour {
		errs = append(errs, fmt.Errorf("profiles.preparation.wait %s outside [1m, 720h]", p.Wait))
	}
	if p.Deadline <= time.Duration(p.MaxRounds)*p.Wait {
		errs = append(errs, fmt.Errorf("profiles.preparation.deadline %s must exceed max_rounds x wait (%s)", p.Deadline, time.Duration(p.MaxRounds)*p.Wait))
	}
	if _, err := domain.ParseMoney(p.Funding.Currency, p.Funding.Amount); err != nil {
		errs = append(errs, fmt.Errorf("profiles.preparation.funding: %w", err))
	}
	g := c.Profiles.Generation
	if g.QueueDeadline < time.Minute || g.QueueDeadline > 7*24*time.Hour {
		errs = append(errs, fmt.Errorf("profiles.generation.queue_deadline %s outside [1m, 168h]", g.QueueDeadline))
	}
	if g.ActiveWindow < time.Minute || g.ActiveWindow > 24*time.Hour {
		errs = append(errs, fmt.Errorf("profiles.generation.active_window %s outside [1m, 24h]", g.ActiveWindow))
	}
	if g.Capacity < 1 || g.Capacity > 1024 {
		errs = append(errs, fmt.Errorf("profiles.generation.capacity %d outside [1, 1024]", g.Capacity))
	}
	if _, err := domain.ParseMoney(g.Funding.Currency, g.Funding.Amount); err != nil {
		errs = append(errs, fmt.Errorf("profiles.generation.funding: %w", err))
	}
	if len(g.Definitions) == 0 || g.CodegenProfile == "" || g.ValidatorProfile == "" {
		errs = append(errs, errors.New("profiles.generation needs definitions, codegen_profile and validator_profile"))
	}
	if c.Relay.Interval*10 > c.Profiles.LocalCheckDeadline {
		errs = append(errs, fmt.Errorf("relay.interval %s is too coarse for profiles.local_check_deadline %s", c.Relay.Interval, c.Profiles.LocalCheckDeadline))
	}
	mp := c.ModelProxy
	if mp.Timeout < time.Second || mp.Timeout > 5*time.Minute {
		errs = append(errs, fmt.Errorf("model_proxy.timeout %s outside [1s, 5m]", mp.Timeout))
	}
	if mp.Owner == "" {
		errs = append(errs, errors.New("model_proxy.owner is required"))
	}
	switch mp.Identity.Mode {
	case "development":
		if mp.Address != "" && mp.Token == "" {
			errs = append(errs, errors.New("ANVILKIT_CONTROL_MODEL_PROXY_TOKEN (model_proxy.token) is required with a model_proxy.address under identity mode development"))
		}
		if mp.Address != "" && !c.Development.Enabled {
			errs = append(errs, errors.New("model_proxy.identity.mode development (plaintext bearer) requires development.enabled: true (DEVELOPMENT_ONLY)"))
		}
	case "mtls":
		m := mp.Identity.MTLS
		if mp.Address != "" && (m.CertFile == "" || m.KeyFile == "" || m.CAFile == "") {
			errs = append(errs, errors.New("model_proxy.identity.mtls.cert_file, key_file and ca_file are required with a model_proxy.address under identity mode mtls"))
		}
	default:
		errs = append(errs, fmt.Errorf("model_proxy.identity.mode %q is not one of development, mtls", mp.Identity.Mode))
	}
	if mp.Address != "" {
		if u, err := url.Parse(mp.Address); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
			errs = append(errs, errors.New("model_proxy.address must be an absolute http(s) URL without credentials"))
		}
	}
	if c.Dispatch.AuthorityFreshness < time.Second || c.Dispatch.AuthorityFreshness > 30*time.Second {
		errs = append(errs, fmt.Errorf("dispatch.authority_freshness %s outside [1s, 30s]", c.Dispatch.AuthorityFreshness))
	}
	dev := c.Dispatch.Development
	if !dev.Enabled && (len(dev.Prices) > 0 || len(dev.AuthorizedRoutes) > 0 || len(dev.NotSentIssuers) > 0) {
		errs = append(errs, errors.New("dispatch.development fixtures require dispatch.development.enabled: true (DEVELOPMENT_ONLY)"))
	}
	if dev.Enabled && !c.Development.Enabled {
		errs = append(errs, errors.New("dispatch.development.enabled requires development.enabled: true (the dispatch fixtures are DEVELOPMENT_ONLY)"))
	}
	errs = append(errs, c.validateProductionAuthority()...)
	if _, err := dev.DomainPrices(); err != nil {
		errs = append(errs, err)
	}
	for i, r := range dev.AuthorizedRoutes {
		if r.TenantID == "" || r.RouteID == "" {
			errs = append(errs, fmt.Errorf("dispatch.development.authorized_routes[%d] needs tenant_id and route_id", i))
		}
	}
	return errors.Join(errs...)
}

// validateProductionAuthority refuses a non-development dispatch chain that
// would run on fixtures or on nothing (P2.1, B-14): the authority source,
// at least one evidence issuer and the Model Proxy's original-identity
// query are required, each named in the refusal.
func (c Config) validateProductionAuthority() []error {
	var errs []error
	if c.Dispatch.Prices.RefreshInterval < 500*time.Millisecond || c.Dispatch.Prices.RefreshInterval > time.Minute {
		errs = append(errs, fmt.Errorf("dispatch.prices.refresh_interval %s outside [500ms, 1m]", c.Dispatch.Prices.RefreshInterval))
	}
	a := c.Dispatch.Authority
	if a.Timeout < 100*time.Millisecond || a.Timeout > 30*time.Second {
		errs = append(errs, fmt.Errorf("dispatch.authority.timeout %s outside [100ms, 30s]", a.Timeout))
	}
	if (a.CertFile == "") != (a.KeyFile == "") {
		errs = append(errs, errors.New("dispatch.authority.cert_file and key_file are both set or both empty"))
	}
	if a.URL != "" {
		u, err := url.Parse(a.URL)
		switch {
		case err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http"):
			errs = append(errs, errors.New("dispatch.authority.url must be an absolute http(s) URL without credentials"))
		case u.Scheme == "http" && !c.Development.Enabled:
			errs = append(errs, errors.New("dispatch.authority.url must be https outside development"))
		}
	}
	for i, iss := range c.Dispatch.Evidence.Issuers {
		if iss.Name == "" || iss.KeyID == "" || iss.PublicKeyFile == "" {
			errs = append(errs, fmt.Errorf("dispatch.evidence.issuers[%d] needs name, key_id and public_key_file", i))
		}
	}
	if c.Dispatch.Development.Enabled {
		return errs
	}
	if a.URL == "" {
		errs = append(errs, errors.New("production authority: dispatch.authority.url is required outside dispatch.development (no commercial authority source; the DEVELOPMENT_ONLY route fixture is not admitted)"))
	} else if a.CertFile == "" && a.TokenFile == "" {
		errs = append(errs, errors.New("production authority: dispatch.authority needs a workload certificate (cert_file/key_file) or a token_file"))
	}
	if len(c.Dispatch.Evidence.Issuers) == 0 {
		errs = append(errs, errors.New("production evidence: dispatch.evidence.issuers is required outside dispatch.development (no trusted signer of not-sent or disposition evidence)"))
	}
	if c.ModelProxy.Address == "" {
		errs = append(errs, errors.New("production outcome query: model_proxy.address is required outside dispatch.development (UNKNOWN model dispatches would otherwise resolve through the DEVELOPMENT_ONLY attestation double)"))
	}
	return errs
}

// DomainPrices converts the fixture prices into validated domain
// observations; every string money value is parsed, never converted
// through a float.
func (d DispatchDevelopment) DomainPrices() ([]domain.Price, error) {
	return domainPrices("dispatch.development.prices", d.Prices)
}

func domainPrices(name string, fixtures []PriceFixture) ([]domain.Price, error) {
	out := make([]domain.Price, 0, len(fixtures))
	for i, f := range fixtures {
		bound, err := domain.ParseMoney(f.Currency, f.MaxExposure)
		if err != nil {
			return nil, fmt.Errorf("%s[%d].max_exposure: %w", name, i, err)
		}
		p := domain.Price{Revision: f.Revision, Kind: domain.DispatchKind(f.Kind), Route: f.Route, Provider: f.Provider, Model: f.Model, Currency: f.Currency, EffectiveFrom: f.EffectiveFrom.UTC(), EffectiveUntil: f.EffectiveUntil.UTC(), PerMillion: map[domain.UsageCategory]int64{}, MaxExposure: bound.Amount}
		p.InputIncludesCached, p.OutputIncludesReasoning = f.InputIncludesCached, f.OutputIncludesReasoning
		for category, value := range f.PerMillion {
			m, err := domain.ParseMoney(f.Currency, value)
			if err != nil {
				return nil, fmt.Errorf("%s[%d].per_million.%s: %w", name, i, category, err)
			}
			p.PerMillion[domain.UsageCategory(category)] = m.Amount
		}
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", name, i, err)
		}
		out = append(out, p)
	}
	return out, nil
}

// LoadPriceBook reads a reviewed ENV-06 price book file (P2.1): schemaVersion
// 1 and a prices list in the fixture format (scale-6 decimal strings per one
// million units, RFC 3339 interval). Unknown keys reject the file.
func LoadPriceBook(path string) ([]domain.Price, error) {
	k := koanf.New(".")
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("dispatch.prices.initial_file %s: %w", path, err)
	}
	var book struct {
		SchemaVersion int            `koanf:"schemaVersion"`
		Prices        []PriceFixture `koanf:"prices"`
	}
	if err := k.UnmarshalWithConf("", &book, koanf.UnmarshalConf{DecoderConfig: &mapstructure.DecoderConfig{
		DecodeHook:  mapstructure.ComposeDecodeHookFunc(mapstructure.StringToTimeHookFunc(time.RFC3339)),
		ErrorUnused: true, WeaklyTypedInput: true, Result: &book,
	}}); err != nil {
		return nil, fmt.Errorf("dispatch.prices.initial_file %s: %w", path, err)
	}
	if book.SchemaVersion != 1 || len(book.Prices) == 0 {
		return nil, fmt.Errorf("dispatch.prices.initial_file %s: needs schemaVersion 1 and at least one price", path)
	}
	return domainPrices("dispatch.prices.initial_file prices", book.Prices)
}

// validateIdentity checks the listener identity, the top-level guard and
// the outbound TLS modes (P0.1).
func (c Config) validateIdentity() []error {
	var errs []error
	id := c.GRPC.Identity
	switch id.Mode {
	case "mtls":
		if id.CertFile == "" || id.KeyFile == "" || id.CAFile == "" {
			errs = append(errs, errors.New("grpc.identity.cert_file, key_file and ca_file are required under grpc.identity.mode mtls (ANVILKIT_CONTROL_IDENTITY_{CERT,KEY,CA}_FILE)"))
		}
	case "development":
		if !c.Development.Enabled {
			errs = append(errs, errors.New("grpc.identity.mode development (plaintext, no caller identity) requires development.enabled: true (DEVELOPMENT_ONLY)"))
		}
	default:
		errs = append(errs, fmt.Errorf("grpc.identity.mode %q is not one of mtls, development", id.Mode))
	}
	switch {
	case id.TrustDomain == "" && c.Development.Enabled:
		// The development default is applied by the loader's caller
		// (TrustDomain()); outside development it is an explicit input.
	case id.TrustDomain == "":
		errs = append(errs, errors.New("grpc.identity.trust_domain is required outside development (the development default anvilkit.local applies only with development.enabled: true)"))
	case !trustDomainPattern.MatchString(id.TrustDomain):
		errs = append(errs, fmt.Errorf("grpc.identity.trust_domain %q is not a lowercase DNS name", id.TrustDomain))
	}
	if id.MaxConnectionAge < time.Minute || id.MaxConnectionAge > 24*time.Hour {
		errs = append(errs, fmt.Errorf("grpc.identity.max_connection_age %s outside [1m, 24h]", id.MaxConnectionAge))
	}
	if id.ReloadInterval < 100*time.Millisecond || id.ReloadInterval > time.Hour {
		errs = append(errs, fmt.Errorf("grpc.identity.reload_interval %s outside [100ms, 1h]", id.ReloadInterval))
	}
	errs = append(errs, c.Temporal.TLS.validate("temporal.tls", true, c.Development.Enabled)...)
	if c.Telemetry.OTLPEndpoint != "" {
		errs = append(errs, c.Telemetry.OTLPTLS.validate("telemetry.otlp_tls", true, c.Development.Enabled)...)
	}
	return errs
}

// validateDataPlane is the data-plane TLS rule (P0.6): outside development
// the database DSN verifies the server's certificate and name
// (sslmode=verify-full) and the endpoint of each selected S3 backend is
// https. The Temporal and OTLP transports are checked with the identity.
// Neither the DSN nor an endpoint is echoed.
func (c Config) validateDataPlane() []error {
	if c.Development.Enabled {
		return nil
	}
	var errs []error
	if c.Database.URL != "" {
		if err := CheckPostgresTLS("database.url", c.Database.URL, false); err != nil {
			errs = append(errs, err)
		}
	}
	if c.Inventory.Backend == InventoryS3Backend && c.Inventory.S3.Endpoint != "" {
		if err := requireHTTPS("inventory.s3.endpoint", c.Inventory.S3.Endpoint); err != nil {
			errs = append(errs, err)
		}
	}
	if c.Artifacts.Backend == ArtifactsS3Backend && c.Artifacts.S3.Endpoint != "" {
		if err := requireHTTPS("artifacts.s3.endpoint", c.Artifacts.S3.Endpoint); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// requireHTTPS refuses an endpoint that is not an absolute https URL; the
// error names only the scheme.
func requireHTTPS(name, endpoint string) error {
	u, err := url.Parse(endpoint)
	switch {
	case err != nil || (u.Scheme == "https" && u.Host == ""):
		return fmt.Errorf("%s must be an absolute https URL outside development", name)
	case u.Scheme != "https":
		return fmt.Errorf("%s: scheme must be https outside development (got %q)", name, u.Scheme)
	}
	return nil
}

var trustDomainPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`)

// DevelopmentTrustDomain is the trust domain of the development foundation.
const DevelopmentTrustDomain = "anvilkit.local"

// TrustDomain is the configured trust domain, or the development default
// when development is enabled and none was named.
func (c Config) TrustDomain() string {
	if c.GRPC.Identity.TrustDomain == "" {
		return DevelopmentTrustDomain
	}
	return c.GRPC.Identity.TrustDomain
}

func (t ClientTLS) validate(name string, required bool, development bool) []error {
	var errs []error
	switch t.Mode {
	case "development":
		if !development {
			errs = append(errs, fmt.Errorf("%s.mode development (plaintext) requires development.enabled: true (DEVELOPMENT_ONLY)", name))
		}
	case "tls":
		if required && t.CAFile == "" {
			errs = append(errs, fmt.Errorf("%s.ca_file is required under %s.mode tls", name, name))
		}
	case "mtls":
		if required && (t.CAFile == "" || t.CertFile == "" || t.KeyFile == "") {
			errs = append(errs, fmt.Errorf("%s.ca_file, cert_file and key_file are required under %s.mode mtls", name, name))
		}
	default:
		errs = append(errs, fmt.Errorf("%s.mode %q is not one of development, tls, mtls", name, t.Mode))
	}
	return errs
}

// validateEvents bounds the event publication (P2.8): outside development
// the forwarder runs, over a verified, authenticated NATS connection.
func (c Config) validateEvents() []error {
	e := c.Events
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}
	check(e.Enabled || c.Development.Enabled, "events.enabled must be true outside development: operation events reach JetStream through the forwarder (P2.8)")
	check(e.ConsumerGroup != "", "events.consumer_group is required")
	check(e.PollInterval >= 10*time.Millisecond && e.PollInterval <= time.Minute, "events.poll_interval %s outside [10ms, 1m]", e.PollInterval)
	check(e.AckDeadline > 0 && e.AckDeadline <= 5*time.Minute, "events.ack_deadline %s outside (0, 5m]", e.AckDeadline)
	check(e.ResendInterval > 0 && e.ResendInterval <= time.Minute, "events.resend_interval %s outside (0, 1m]", e.ResendInterval)
	check(e.BatchSize >= 1 && e.BatchSize <= 1000, "events.batch_size %d outside [1, 1000]", e.BatchSize)
	check(e.CloseTimeout >= time.Second && e.CloseTimeout <= 5*time.Minute, "events.close_timeout %s outside [1s, 5m]", e.CloseTimeout)
	check(e.Retention >= time.Hour && e.Retention <= 2160*time.Hour, "events.retention %s outside [1h, 2160h]", e.Retention)
	check(e.RetentionInterval >= time.Minute && e.RetentionInterval <= 24*time.Hour, "events.retention_interval %s outside [1m, 24h]", e.RetentionInterval)
	check(e.RetentionBatch >= 1 && e.RetentionBatch <= 100000, "events.retention_batch %d outside [1, 100000]", e.RetentionBatch)
	check(e.NATS.PublishTimeout > 0 && e.NATS.PublishTimeout <= time.Minute, "events.nats.publish_timeout %s outside (0, 1m]", e.NATS.PublishTimeout)
	if !e.Enabled {
		return errs
	}
	check(e.NATS.URL != "", "events.nats.url is required while events are enabled (ANVILKIT_CONTROL_NATS_URL)")
	if e.NATS.URL != "" {
		u, err := url.Parse(e.NATS.URL)
		check(err == nil && (u.Scheme == "nats" || u.Scheme == "tls"), "events.nats.url must be a nats:// or tls:// URL")
	}
	check(e.NATS.Name != "", "events.nats.name is required")
	errs = append(errs, e.NATS.TLS.validate("events.nats.tls", true, c.Development.Enabled)...)
	check(e.NATS.CredsFile != "" || c.Development.Enabled, "events.nats.creds_file is required outside development (ANVILKIT_CONTROL_NATS_CREDS_FILE: a NATS user .creds file or an NKey user seed)")
	return errs
}

// natsUserJWTMarker opens the user JWT block of a NATS .creds file.
var natsUserJWTMarker = []byte("-----BEGIN NATS USER JWT-----")

// readNATSCredentials reads and classifies a mounted NATS client
// credential: a user .creds file (it holds the decorated user JWT) or a
// bare NKey user seed (the first non-empty line starts with SU). The
// content never leaves this function, not even in an error, and the
// buffer is wiped; the connection's options read the file themselves
// when they authenticate.
func readNATSCredentials(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	defer clear(raw)
	if bytes.Contains(raw, natsUserJWTMarker) {
		return NATSCredsUser, nil
	}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if line = bytes.TrimSpace(line); len(line) > 0 {
			if bytes.HasPrefix(line, []byte("SU")) {
				return NATSCredsNKey, nil
			}
			break
		}
	}
	return "", errors.New("neither a NATS user credentials file (.creds) nor an NKey user seed")
}
