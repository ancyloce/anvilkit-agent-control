// Package config builds Control's immutable configuration snapshot with
// koanf (A09, DD-09 §4). Precedence is defaults < the service's reviewed
// configuration file < the allowed ANVILKIT_CONTROL_* environment overrides.
// The candidate is validated (unknown keys, required values, ranges,
// cross-field rules) before anything starts; a rejected candidate never
// starts the process. The database URL is a secret: it is accepted only
// from the environment (the existing secure injection path) and never from
// the file, and it is never logged.
package config

import (
	"errors"
	"fmt"
	"os"
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
}

// Database holds the app-role connection; URL is env-only.
type Database struct {
	URL string `koanf:"url"`
}

// Inventory selects the obligation inventory backend (DD-02 §5): the
// DEVELOPMENT_ONLY filesystem store, or an S3-compatible backend through the
// AWS SDK (Ceph RGW is the C09 primary; the independent failure domain is
// an ENV-02 input this file cannot establish). The S3 credentials are
// secrets and arrive only from the environment.
type Inventory struct {
	Backend string      `koanf:"backend"`
	Dir     string      `koanf:"dir"`
	S3      InventoryS3 `koanf:"s3"`
}

type InventoryS3 struct {
	Endpoint        string `koanf:"endpoint"`
	Region          string `koanf:"region"`
	Bucket          string `koanf:"bucket"`
	Prefix          string `koanf:"prefix"`
	PathStyle       bool   `koanf:"path_style"`
	QualifyOnStart  bool   `koanf:"qualify_on_start"`
	AccessKeyID     string `koanf:"access_key_id"`
	SecretAccessKey string `koanf:"secret_access_key"`
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
	Address   string `koanf:"address"`
	Namespace string `koanf:"namespace"`
	TaskQueue string `koanf:"task_queue"`
}

type Relay struct {
	Interval time.Duration `koanf:"interval"`
}

// Profiles carries the reviewed profile parameters of this build.
type Profiles struct {
	LocalCheckDeadline time.Duration `koanf:"local_check_deadline"`
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
	Enabled          bool                    `koanf:"enabled"`
	Prices           []PriceFixture          `koanf:"prices"`
	AuthorizedRoutes []RouteAuthorization    `koanf:"authorized_routes"`
	NotSentIssuers   []string                `koanf:"not_sent_issuers"`
	Operators        []OperatorAuthorization `koanf:"operators"`
}

// OperatorAuthorization is one fixture actor allowed to begin recovery
// runs and record dispositions for a tenant ("*" for every scope).
type OperatorAuthorization struct {
	TenantID string `koanf:"tenant_id"`
	ActorID  string `koanf:"actor_id"`
}

// PriceFixture is one immutable pricing observation of the fixture: money
// values are canonical scale-6 decimal strings, unit prices are per one
// million units, the interval is RFC 3339. A model price binds the trusted
// provider and model of its route; a tool price binds neither.
type PriceFixture struct {
	Revision       string            `koanf:"revision"`
	Kind           string            `koanf:"kind"`
	Route          string            `koanf:"route"`
	Provider       string            `koanf:"provider"`
	Model          string            `koanf:"model"`
	Currency       string            `koanf:"currency"`
	EffectiveFrom  time.Time         `koanf:"effective_from"`
	EffectiveUntil time.Time         `koanf:"effective_until"`
	PerMillion     map[string]string `koanf:"per_million"`
	MaxExposure    string            `koanf:"max_exposure"`
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

type Config struct {
	GRPC      GRPC      `koanf:"grpc"`
	Database  Database  `koanf:"database"`
	Inventory Inventory `koanf:"inventory"`
	Artifacts Artifacts `koanf:"artifacts"`
	Temporal  Temporal  `koanf:"temporal"`
	Relay     Relay     `koanf:"relay"`
	Profiles  Profiles  `koanf:"profiles"`
	Dispatch  Dispatch  `koanf:"dispatch"`
	Recovery  Recovery  `koanf:"recovery"`
}

var defaults = map[string]any{
	"grpc.listen":                   "127.0.0.1:9101",
	"grpc.control_capacity":         32,
	"grpc.execution_capacity":       64,
	"grpc.shutdown_timeout":         "20s",
	"temporal.namespace":            "anvilkit",
	"temporal.task_queue":           "anvilkit-workflow",
	"relay.interval":                "500ms",
	"profiles.local_check_deadline": "15m",
	"dispatch.authority_freshness":  "30s",
	"dispatch.development.enabled":  false,
	"inventory.backend":             InventoryFilesystem,
	"inventory.s3.path_style":       true,
	"inventory.s3.qualify_on_start": true,
	"recovery.enumeration_page":     500,
	"artifacts.backend":             ArtifactsDisabled,
	"artifacts.max_object_bytes":    64 << 20,
	"artifacts.max_window":          "24h",
	"artifacts.capability_ttl":      "15m",
	"artifacts.s3.path_style":       true,
	"artifacts.s3.qualify_on_start": true,
}

// envOverrides is the complete set of accepted environment variables:
// deployment placement and the secret. Any other ANVILKIT_CONTROL_*
// variable rejects the candidate.
var envOverrides = map[string]string{
	"ANVILKIT_CONTROL_LISTEN":                         "grpc.listen",
	"ANVILKIT_CONTROL_DATABASE_URL":                   "database.url",
	"ANVILKIT_CONTROL_INVENTORY_DIR":                  "inventory.dir",
	"ANVILKIT_CONTROL_INVENTORY_S3_ENDPOINT":          "inventory.s3.endpoint",
	"ANVILKIT_CONTROL_INVENTORY_S3_BUCKET":            "inventory.s3.bucket",
	"ANVILKIT_CONTROL_INVENTORY_S3_ACCESS_KEY_ID":     "inventory.s3.access_key_id",
	"ANVILKIT_CONTROL_INVENTORY_S3_SECRET_ACCESS_KEY": "inventory.s3.secret_access_key",
	"ANVILKIT_CONTROL_TEMPORAL_ADDRESS":               "temporal.address",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ENDPOINT":          "artifacts.s3.endpoint",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_BUCKET":            "artifacts.s3.bucket",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ACCESS_KEY_ID":     "artifacts.s3.access_key_id",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_SECRET_ACCESS_KEY": "artifacts.s3.secret_access_key",
}

// secretKeys may only arrive through the environment.
var secretKeys = []string{"database.url", "inventory.s3.access_key_id", "inventory.s3.secret_access_key", "artifacts.s3.access_key_id", "artifacts.s3.secret_access_key"}

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
	return c, c.validate()
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
	req("database.url (ANVILKIT_CONTROL_DATABASE_URL)", c.Database.URL)
	switch c.Inventory.Backend {
	case InventoryFilesystem:
		req("inventory.dir", c.Inventory.Dir)
	case InventoryS3Backend:
		req("inventory.s3.endpoint (ANVILKIT_CONTROL_INVENTORY_S3_ENDPOINT)", c.Inventory.S3.Endpoint)
		req("inventory.s3.region", c.Inventory.S3.Region)
		req("inventory.s3.bucket (ANVILKIT_CONTROL_INVENTORY_S3_BUCKET)", c.Inventory.S3.Bucket)
		req("inventory.s3.access_key_id (ANVILKIT_CONTROL_INVENTORY_S3_ACCESS_KEY_ID)", c.Inventory.S3.AccessKeyID)
		req("inventory.s3.secret_access_key (ANVILKIT_CONTROL_INVENTORY_S3_SECRET_ACCESS_KEY)", c.Inventory.S3.SecretAccessKey)
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
		req("artifacts.s3.access_key_id (ANVILKIT_CONTROL_ARTIFACTS_S3_ACCESS_KEY_ID)", c.Artifacts.S3.AccessKeyID)
		req("artifacts.s3.secret_access_key (ANVILKIT_CONTROL_ARTIFACTS_S3_SECRET_ACCESS_KEY)", c.Artifacts.S3.SecretAccessKey)
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
	if c.Profiles.LocalCheckDeadline < time.Minute || c.Profiles.LocalCheckDeadline > 24*time.Hour {
		errs = append(errs, fmt.Errorf("profiles.local_check_deadline %s outside [1m, 24h]", c.Profiles.LocalCheckDeadline))
	}
	// The relay must poll more often than the shortest operation deadline
	// can elapse, otherwise a confirmed intake could wait past its clock.
	if c.Relay.Interval*10 > c.Profiles.LocalCheckDeadline {
		errs = append(errs, fmt.Errorf("relay.interval %s is too coarse for profiles.local_check_deadline %s", c.Relay.Interval, c.Profiles.LocalCheckDeadline))
	}
	if c.Dispatch.AuthorityFreshness < time.Second || c.Dispatch.AuthorityFreshness > 30*time.Second {
		errs = append(errs, fmt.Errorf("dispatch.authority_freshness %s outside [1s, 30s]", c.Dispatch.AuthorityFreshness))
	}
	dev := c.Dispatch.Development
	if !dev.Enabled && (len(dev.Prices) > 0 || len(dev.AuthorizedRoutes) > 0 || len(dev.NotSentIssuers) > 0 || len(dev.Operators) > 0) {
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
	for i, o := range dev.Operators {
		if o.TenantID == "" || o.ActorID == "" {
			errs = append(errs, fmt.Errorf("dispatch.development.operators[%d] needs tenant_id and actor_id", i))
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
