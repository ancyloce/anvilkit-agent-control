package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/config"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

const minimal = "temporal:\n  address: 127.0.0.1:27233\n  tls:\n    ca_file: /etc/anvilkit/identity/ca.crt\n"

// identityEnv places the mounted identity files and the trust domain
// (P0.1); the loader does not read the files.
var identityEnv = []string{
	"ANVILKIT_CONTROL_IDENTITY_CERT_FILE=/etc/anvilkit/identity/tls.crt",
	"ANVILKIT_CONTROL_IDENTITY_KEY_FILE=/etc/anvilkit/identity/tls.key",
	"ANVILKIT_CONTROL_IDENTITY_CA_FILE=/etc/anvilkit/identity/ca.crt",
	"ANVILKIT_CONTROL_IDENTITY_TRUST_DOMAIN=anvilkit.local",
}

var env = []string{
	"ANVILKIT_CONTROL_DATABASE_URL=postgres://anvilkit_control_app:secret@127.0.0.1:25432/anvilkit_control",
	"ANVILKIT_CONTROL_INVENTORY_DIR=/var/lib/anvilkit/inventory",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ENDPOINT=http://127.0.0.1:29000",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_BUCKET=anvilkit-artifacts",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ACCESS_KEY_ID=artifacts-key",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_SECRET_ACCESS_KEY=artifacts-secret",
	"ANVILKIT_API_LISTEN=other-service",
	"ANVILKIT_CONTROL_IDENTITY_CERT_FILE=/etc/anvilkit/identity/tls.crt",
	"ANVILKIT_CONTROL_IDENTITY_KEY_FILE=/etc/anvilkit/identity/tls.key",
	"ANVILKIT_CONTROL_IDENTITY_CA_FILE=/etc/anvilkit/identity/ca.crt",
	"ANVILKIT_CONTROL_IDENTITY_TRUST_DOMAIN=anvilkit.local",
}

func TestPrecedenceDefaultsFileEnvironment(t *testing.T) {
	c, err := config.LoadFrom(write(t, minimal+"grpc:\n  listen: 0.0.0.0:9101\n  control_capacity: 8\n"), append(env, "ANVILKIT_CONTROL_LISTEN=127.0.0.1:9111"))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9111", c.GRPC.Listen, "environment overrides the file")
	require.Equal(t, 8, c.GRPC.ControlCapacity, "file overrides the default")
	require.Equal(t, 64, c.GRPC.ExecutionCapacity, "default kept")
	require.Equal(t, 500*time.Millisecond, c.Relay.Interval)
	require.Equal(t, "/var/lib/anvilkit/inventory", c.Inventory.Dir)
	require.Contains(t, c.Database.URL, "secret")
}

func TestCheckedInFileLoads(t *testing.T) {
	c, err := config.LoadFrom(filepath.Join("..", "..", "config.yaml"), env)
	require.NoError(t, err)
	require.Equal(t, "anvilkit", c.Temporal.Namespace)
	require.Equal(t, 15*time.Minute, c.Profiles.LocalCheckDeadline)
	require.Equal(t, config.ArtifactsS3Backend, c.Artifacts.Backend)
	require.Equal(t, "anvilkit-artifacts", c.Artifacts.S3.Bucket)
	require.Equal(t, int64(64<<20), c.Artifacts.MaxObjectBytes)
	_, err = config.LoadFrom(filepath.Join("..", "..", "config.yaml"), env[:2])
	require.ErrorContains(t, err, "artifacts.s3.endpoint", "the checked-in file selects the S3 artifact store, whose placement and credentials come from the environment")
}

func TestArtifactStoreIsASeparatePermissionBoundary(t *testing.T) {
	_, err := config.LoadFrom(write(t, minimal+"artifacts:\n  s3:\n    access_key_id: leaked\n"), env)
	require.ErrorContains(t, err, "artifacts.s3.access_key_id is a secret")
	shared := append(env[:0:0], env...)
	shared = append(shared, "ANVILKIT_CONTROL_INVENTORY_S3_ENDPOINT=http://127.0.0.1:29000", "ANVILKIT_CONTROL_INVENTORY_S3_BUCKET=anvilkit-artifacts",
		"ANVILKIT_CONTROL_INVENTORY_S3_ACCESS_KEY_ID=artifacts-key", "ANVILKIT_CONTROL_INVENTORY_S3_SECRET_ACCESS_KEY=artifacts-secret")
	_, err = config.LoadFrom(write(t, minimal+"inventory:\n  backend: s3\n  s3:\n    region: default\nartifacts:\n  backend: s3\n  s3:\n    region: default\n"), shared)
	require.ErrorContains(t, err, "separate permission boundaries")
	c, err := config.LoadFrom(write(t, minimal+"artifacts:\n  backend: disabled\n"), env)
	require.NoError(t, err)
	require.Equal(t, config.ArtifactsDisabled, c.Artifacts.Backend)
}

func TestSecretsAreRefusedFromTheFile(t *testing.T) {
	_, err := config.LoadFrom(write(t, minimal+"database:\n  url: postgres://app:pw@db/anvilkit_control\n"), env)
	require.ErrorContains(t, err, "database.url is a secret")
}

func TestUnknownKeysAreRejected(t *testing.T) {
	_, err := config.LoadFrom(write(t, minimal+"relay:\n  intervall: 1s\n"), env)
	require.ErrorContains(t, err, "intervall")
	_, err = config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_CONTROL_RELAY_INTERVAL=1s"))
	require.ErrorContains(t, err, "ANVILKIT_CONTROL_RELAY_INTERVAL")
}

func TestRequiredValuesAndRanges(t *testing.T) {
	_, err := config.LoadFrom(write(t, minimal), []string{"ANVILKIT_CONTROL_INVENTORY_DIR=/tmp/inv"})
	require.ErrorContains(t, err, "database.url")
	_, err = config.LoadFrom(write(t, minimal), env[:1])
	require.ErrorContains(t, err, "inventory.dir is required")
	_, err = config.LoadFrom(write(t, "grpc:\n  listen: 127.0.0.1:9101\n"), env)
	require.ErrorContains(t, err, "temporal.address is required")
	_, err = config.LoadFrom(write(t, minimal+"grpc:\n  control_capacity: 0\n"), env)
	require.ErrorContains(t, err, "grpc.control_capacity")
	_, err = config.LoadFrom(write(t, minimal+"database:\n  max_conns: 0\n"), env)
	require.ErrorContains(t, err, "database.max_conns")
	_, err = config.LoadFrom(write(t, minimal+"relay:\n  interval: 10ms\n"), env)
	require.ErrorContains(t, err, "relay.interval")
	_, err = config.LoadFrom(write(t, minimal+"profiles:\n  local_check_deadline: 48h\n"), env)
	require.ErrorContains(t, err, "profiles.local_check_deadline")
	_, err = config.LoadFrom(write(t, minimal+"relay:\n  interval: 30s\nprofiles:\n  local_check_deadline: 1m\n"), env)
	require.ErrorContains(t, err, "too coarse", "cross-field rule")
}

func TestDispatchFixturesAreFileOnlyValidatedAndOffByDefault(t *testing.T) {
	c, err := config.LoadFrom(filepath.Join("..", "..", "config.yaml"), env)
	require.NoError(t, err)
	require.False(t, c.Dispatch.Development.Enabled, "the checked-in file denies every paid route")
	require.Equal(t, 30*time.Second, c.Dispatch.AuthorityFreshness)
	_, err = config.LoadFrom(write(t, minimal+"dispatch:\n  authority_freshness: 2m\n"), env)
	require.ErrorContains(t, err, "authority_freshness", "never fresher than the 30 s baseline bound")
	_, err = config.LoadFrom(write(t, minimal+"dispatch:\n  development:\n    authorized_routes:\n      - tenant_id: tenant_a\n        route_id: r\n"), env)
	require.ErrorContains(t, err, "require dispatch.development.enabled")
	_, err = config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_CONTROL_DISPATCH_DEVELOPMENT_ENABLED=true"))
	require.ErrorContains(t, err, "ANVILKIT_CONTROL_DISPATCH_DEVELOPMENT_ENABLED", "fixtures are never enabled from the environment")
	const price = "dispatch:\n  development:\n    enabled: true\n    prices:\n      - revision: fx-1\n        kind: model\n        route: fixture-route\n        provider: fixture\n        model: fixture-model\n        currency: USD\n        effective_from: 2026-01-01T00:00:00Z\n        effective_until: 2027-01-01T00:00:00Z\n        per_million: {input_units: \"3000000\", output_units: \"15000000\", reasoning_units: \"15000000\", cached_input_units: \"300000\"}\n        max_exposure: \"5000000\"\n"
	c, err = config.LoadFrom(write(t, minimal+price), env)
	require.NoError(t, err)
	prices, err := c.Dispatch.Development.DomainPrices()
	require.NoError(t, err)
	require.Len(t, prices, 1)
	require.Equal(t, int64(5_000_000), prices[0].MaxExposure)
	require.False(t, prices[0].InputIncludesCached)
	require.False(t, prices[0].OutputIncludesReasoning)
	inclusive, err := config.LoadFrom(write(t, minimal+price+"        input_includes_cached: true\n        output_includes_reasoning: true\n"), env)
	require.NoError(t, err)
	inclusivePrices, err := inclusive.Dispatch.Development.DomainPrices()
	require.NoError(t, err)
	require.True(t, inclusivePrices[0].InputIncludesCached)
	require.True(t, inclusivePrices[0].OutputIncludesReasoning)
	require.Equal(t, [2]string{"fixture", "fixture-model"}, [2]string{prices[0].Provider, prices[0].Model}, "a model price binds the trusted provider and model of its route")
	_, err = config.LoadFrom(write(t, minimal+strings.Replace(price, "        model: fixture-model\n", "", 1)), env)
	require.ErrorContains(t, err, "must bind a provider and a model", "a model price without its binding is rejected at startup")
	_, err = config.LoadFrom(write(t, minimal+strings.Replace(price, "kind: model", "kind: tool", 1)), env)
	require.ErrorContains(t, err, "binds a provider or model", "a tool price binds neither")
	_, err = config.LoadFrom(write(t, minimal+strings.Replace(price, "\"3000000\"", "\"3.5\"", 1)), env)
	require.ErrorContains(t, err, "per_million.input_units", "money is a canonical integer string, never a float")
	_, err = config.LoadFrom(write(t, minimal+strings.Replace(price, ", cached_input_units: \"300000\"", "", 1)), env)
	require.ErrorContains(t, err, "does not meter cached_input_units")
}

func TestInventoryS3BackendRequiresPlacementAndEnvOnlyCredentials(t *testing.T) {
	s3 := minimal + "inventory:\n  backend: s3\n  s3:\n    region: default\n    prefix: control\n"
	_, err := config.LoadFrom(write(t, s3), env[:1])
	require.ErrorContains(t, err, "inventory.s3.endpoint")
	require.ErrorContains(t, err, "inventory.s3.bucket")
	require.ErrorContains(t, err, "inventory.s3.access_key_id")
	_, err = config.LoadFrom(write(t, s3+"    access_key_id: AKIA\n"), env[:1])
	require.ErrorContains(t, err, "inventory.s3.access_key_id is a secret")
	c, err := config.LoadFrom(write(t, s3), append(append(env[:1:1], identityEnv...),
		"ANVILKIT_CONTROL_INVENTORY_S3_ENDPOINT=http://rgw.internal:7480", "ANVILKIT_CONTROL_INVENTORY_S3_BUCKET=anvilkit-inventory",
		"ANVILKIT_CONTROL_INVENTORY_S3_ACCESS_KEY_ID=AKIA", "ANVILKIT_CONTROL_INVENTORY_S3_SECRET_ACCESS_KEY=secret"))
	require.NoError(t, err)
	require.Equal(t, config.InventoryS3Backend, c.Inventory.Backend)
	require.True(t, c.Inventory.S3.PathStyle, "default kept")
	require.True(t, c.Inventory.S3.QualifyOnStart, "the backend is probed before serving by default")
	require.Equal(t, 500, c.Recovery.EnumerationPage)
	_, err = config.LoadFrom(write(t, minimal+"inventory:\n  backend: nfs\n"), env)
	require.ErrorContains(t, err, "inventory.backend")
	_, err = config.LoadFrom(write(t, minimal+"recovery:\n  enumeration_page: 0\n"), env)
	require.ErrorContains(t, err, "recovery.enumeration_page")
}

func TestModelProxyPlacementAndSecret(t *testing.T) {
	c, err := config.LoadFrom(write(t, minimal), env)
	require.NoError(t, err)
	require.Empty(t, c.ModelProxy.Address, "no placement: the attestation double answers model dispatches")
	require.Equal(t, "anvilkit-agent-model-proxy", c.ModelProxy.Owner)
	_, err = config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_CONTROL_MODEL_PROXY_ADDRESS=http://127.0.0.1:9103"))
	require.ErrorContains(t, err, "ANVILKIT_CONTROL_MODEL_PROXY_TOKEN")
	_, err = config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_CONTROL_MODEL_PROXY_ADDRESS=http://127.0.0.1:9103", "ANVILKIT_CONTROL_MODEL_PROXY_TOKEN=secret"))
	require.ErrorContains(t, err, "model_proxy.identity.mode development (plaintext bearer) requires development.enabled", "the bearer path is DEVELOPMENT_ONLY")
	c, err = config.LoadFrom(write(t, minimal+"development:\n  enabled: true\n"), append(env, "ANVILKIT_CONTROL_MODEL_PROXY_ADDRESS=http://127.0.0.1:9103", "ANVILKIT_CONTROL_MODEL_PROXY_TOKEN=secret"))
	require.NoError(t, err)
	require.Equal(t, "secret", c.ModelProxy.Token)
	_, err = config.LoadFrom(write(t, minimal+"model_proxy:\n  token: in-file\n"), env)
	require.ErrorContains(t, err, "model_proxy.token is a secret")
	_, err = config.LoadFrom(write(t, minimal+"model_proxy:\n  identity:\n    mode: mtls\n"), append(env, "ANVILKIT_CONTROL_MODEL_PROXY_ADDRESS=https://proxy"))
	require.ErrorContains(t, err, "model_proxy.identity.mtls.cert_file, key_file and ca_file are required")
}

func TestTelemetryPlacement(t *testing.T) {
	_, err := config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_CONTROL_TELEMETRY_OTLP_ENDPOINT=collector:4317"))
	require.ErrorContains(t, err, "telemetry.otlp_tls.ca_file is required", "a placed collector needs a verified transport")
	c, err := config.LoadFrom(write(t, minimal+"telemetry:\n  otlp_tls:\n    ca_file: /etc/anvilkit/identity/ca.crt\n"), append(env, "ANVILKIT_CONTROL_TELEMETRY_OTLP_ENDPOINT=collector:4317", "ANVILKIT_CONTROL_TELEMETRY_METRICS_LISTEN=0.0.0.0:9111"))
	require.NoError(t, err)
	require.Equal(t, "collector:4317", c.Telemetry.OTLPEndpoint)
	require.Equal(t, "0.0.0.0:9111", c.Telemetry.MetricsListen)
	_, err = config.LoadFrom(write(t, minimal+"telemetry:\n  sample_ratio: -0.5\n"), env)
	require.ErrorContains(t, err, "telemetry.sample_ratio")
	_, err = config.LoadFrom(write(t, minimal+"telemetry:\n  metrics_listen: 127.0.0.1:9101\n"), env)
	require.ErrorContains(t, err, "must not be grpc.listen")
}

// TestIdentityAndDevelopmentGuard (P0.1): the listener is mTLS by default
// and needs its files; plaintext anywhere needs that connection's
// development mode and the top-level guard; the guard downgrades nothing
// by itself; the trust domain is explicit outside development.
func TestIdentityAndDevelopmentGuard(t *testing.T) {
	base := append(env[:0:0], env...)
	c, err := config.LoadFrom(write(t, minimal), base)
	require.NoError(t, err)
	require.Equal(t, "mtls", c.GRPC.Identity.Mode)
	require.Equal(t, "anvilkit.local", c.TrustDomain())
	require.Equal(t, "127.0.0.1:9113", c.Health.Listen)
	require.Equal(t, "tls", c.Temporal.TLS.Mode)
	require.False(t, c.Development.Enabled)

	noFiles := []string{}
	for _, kv := range base {
		if !strings.HasPrefix(kv, "ANVILKIT_CONTROL_IDENTITY_CERT_FILE") && !strings.HasPrefix(kv, "ANVILKIT_CONTROL_IDENTITY_KEY_FILE") && !strings.HasPrefix(kv, "ANVILKIT_CONTROL_IDENTITY_CA_FILE") {
			noFiles = append(noFiles, kv)
		}
	}
	_, err = config.LoadFrom(write(t, minimal), noFiles)
	require.ErrorContains(t, err, "grpc.identity.cert_file, key_file and ca_file are required")

	noDomain := []string{}
	for _, kv := range base {
		if !strings.HasPrefix(kv, "ANVILKIT_CONTROL_IDENTITY_TRUST_DOMAIN") {
			noDomain = append(noDomain, kv)
		}
	}
	_, err = config.LoadFrom(write(t, minimal), noDomain)
	require.ErrorContains(t, err, "grpc.identity.trust_domain is required outside development")
	c, err = config.LoadFrom(write(t, minimal+"development:\n  enabled: true\n"), noDomain)
	require.NoError(t, err)
	require.Equal(t, config.DevelopmentTrustDomain, c.TrustDomain(), "the development default applies only under the guard")
	_, err = config.LoadFrom(write(t, minimal+"grpc:\n  identity:\n    trust_domain: Not_A_Domain\n"), noDomain)
	require.ErrorContains(t, err, "grpc.identity.trust_domain")

	_, err = config.LoadFrom(write(t, minimal+"grpc:\n  identity:\n    mode: development\n"), base)
	require.ErrorContains(t, err, "grpc.identity.mode development (plaintext, no caller identity) requires development.enabled")
	c, err = config.LoadFrom(write(t, minimal+"grpc:\n  identity:\n    mode: development\ndevelopment:\n  enabled: true\n"), base)
	require.NoError(t, err)
	require.Equal(t, "development", c.GRPC.Identity.Mode)
	require.Equal(t, "tls", c.Temporal.TLS.Mode, "the guard does not downgrade other connections")
	_, err = config.LoadFrom(write(t, minimal+"grpc:\n  identity:\n    mode: plaintext\n"), base)
	require.ErrorContains(t, err, "grpc.identity.mode")

	_, err = config.LoadFrom(write(t, "temporal:\n  address: 127.0.0.1:27233\n  tls:\n    mode: development\n"), base)
	require.ErrorContains(t, err, "temporal.tls.mode development (plaintext) requires development.enabled")
	_, err = config.LoadFrom(write(t, "temporal:\n  address: 127.0.0.1:27233\n"), base)
	require.ErrorContains(t, err, "temporal.tls.ca_file is required")
	_, err = config.LoadFrom(write(t, "temporal:\n  address: 127.0.0.1:27233\n  tls:\n    mode: mtls\n    ca_file: /c\n"), base)
	require.ErrorContains(t, err, "temporal.tls.ca_file, cert_file and key_file are required")
	_, err = config.LoadFrom(write(t, minimal+"health:\n  listen: 127.0.0.1:9101\n"), base)
	require.ErrorContains(t, err, "health.listen must not be grpc.listen")
	_, err = config.LoadFrom(write(t, minimal+"grpc:\n  identity:\n    max_connection_age: 10s\n"), base)
	require.ErrorContains(t, err, "grpc.identity.max_connection_age")
	_, err = config.LoadFrom(write(t, minimal+"development:\n  enabled: true\n"), append(base, "ANVILKIT_CONTROL_DEVELOPMENT_ENABLED=true"))
	require.ErrorContains(t, err, "not allowed overrides", "the guard is file-only")
}
