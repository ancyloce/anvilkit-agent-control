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
	Listen            string        `koanf:"listen"`
	ControlCapacity   int           `koanf:"control_capacity"`
	ExecutionCapacity int           `koanf:"execution_capacity"`
	ShutdownTimeout   time.Duration `koanf:"shutdown_timeout"`
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
}

// Database holds the app-role connection: URL (env-only) or URLFile, a
// mounted secret read once at load (exactly one of the two). MaxConns is the
// exact per-replica pool bound that the deployment lock's connection budget
// counts (pool x replicas plus reserves within the cluster's limit).
type Database struct {
	URL      string `koanf:"url"`
	URLFile  string `koanf:"url_file"`
	MaxConns int32  `koanf:"max_conns"`
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
	S3             InventoryS3   `koanf:"s3"`
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
}

var defaults = map[string]any{
	"grpc.listen":                           "127.0.0.1:9101",
	"grpc.control_capacity":                 32,
	"grpc.execution_capacity":               64,
	"grpc.shutdown_timeout":                 "20s",
	"grpc.identity.mode":                    "mtls",
	"grpc.identity.max_connection_age":      "1h",
	"grpc.identity.reload_interval":         "5s",
	"development.enabled":                   false,
	"health.listen":                         "127.0.0.1:9113",
	"temporal.tls.mode":                     "tls",
	"telemetry.otlp_tls.mode":               "tls",
	"database.max_conns":                    8,
	"telemetry.sample_ratio":                1.0,
	"temporal.namespace":                    "anvilkit",
	"temporal.task_queue":                   "anvilkit-workflow",
	"relay.interval":                        "500ms",
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
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ENDPOINT":               "artifacts.s3.endpoint",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_BUCKET":                 "artifacts.s3.bucket",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ACCESS_KEY_ID":          "artifacts.s3.access_key_id",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ACCESS_KEY_ID_FILE":     "artifacts.s3.access_key_id_file",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_SECRET_ACCESS_KEY":      "artifacts.s3.secret_access_key",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_SECRET_ACCESS_KEY_FILE": "artifacts.s3.secret_access_key_file",
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

// DomainPrices converts the fixture prices into validated domain
// observations; every string money value is parsed, never converted
// through a float.
func (d DispatchDevelopment) DomainPrices() ([]domain.Price, error) {
	out := make([]domain.Price, 0, len(d.Prices))
	for i, f := range d.Prices {
		bound, err := domain.ParseMoney(f.Currency, f.MaxExposure)
		if err != nil {
			return nil, fmt.Errorf("dispatch.development.prices[%d].max_exposure: %w", i, err)
		}
		p := domain.Price{Revision: f.Revision, Kind: domain.DispatchKind(f.Kind), Route: f.Route, Provider: f.Provider, Model: f.Model, Currency: f.Currency, EffectiveFrom: f.EffectiveFrom.UTC(), EffectiveUntil: f.EffectiveUntil.UTC(), PerMillion: map[domain.UsageCategory]int64{}, MaxExposure: bound.Amount}
		p.InputIncludesCached, p.OutputIncludesReasoning = f.InputIncludesCached, f.OutputIncludesReasoning
		for category, value := range f.PerMillion {
			m, err := domain.ParseMoney(f.Currency, value)
			if err != nil {
				return nil, fmt.Errorf("dispatch.development.prices[%d].per_million.%s: %w", i, category, err)
			}
			p.PerMillion[domain.UsageCategory(category)] = m.Amount
		}
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("dispatch.development.prices[%d]: %w", i, err)
		}
		out = append(out, p)
	}
	return out, nil
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
