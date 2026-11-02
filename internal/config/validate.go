package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

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
	// A zero grace would never close an aged connection (gRPC's default).
	if id.MaxConnectionAgeGrace < time.Second || id.MaxConnectionAgeGrace > 24*time.Hour {
		errs = append(errs, fmt.Errorf("grpc.identity.max_connection_age_grace %s outside [1s, 24h]", id.MaxConnectionAgeGrace))
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
