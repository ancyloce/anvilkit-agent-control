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

const minimal = "temporal:\n  address: 127.0.0.1:27233\n"

var env = []string{
	"ANVILKIT_CONTROL_DATABASE_URL=postgres://anvilkit_control_app:secret@127.0.0.1:25432/anvilkit_control",
	"ANVILKIT_CONTROL_INVENTORY_DIR=/var/lib/anvilkit/inventory",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ENDPOINT=http://127.0.0.1:29000",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_BUCKET=anvilkit-artifacts",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_ACCESS_KEY_ID=artifacts-key",
	"ANVILKIT_CONTROL_ARTIFACTS_S3_SECRET_ACCESS_KEY=artifacts-secret",
	"ANVILKIT_API_LISTEN=other-service",
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
	c, err := config.LoadFrom(write(t, s3), append(env[:1:1],
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
	c, err = config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_CONTROL_MODEL_PROXY_ADDRESS=http://127.0.0.1:9103", "ANVILKIT_CONTROL_MODEL_PROXY_TOKEN=secret"))
	require.NoError(t, err)
	require.Equal(t, "secret", c.ModelProxy.Token)
	_, err = config.LoadFrom(write(t, minimal+"model_proxy:\n  token: in-file\n"), env)
	require.ErrorContains(t, err, "model_proxy.token is a secret")
	_, err = config.LoadFrom(write(t, minimal+"model_proxy:\n  identity:\n    mode: mtls\n"), append(env, "ANVILKIT_CONTROL_MODEL_PROXY_ADDRESS=https://proxy"))
	require.ErrorContains(t, err, "model_proxy.identity.mtls.cert_file, key_file and ca_file are required")
}
