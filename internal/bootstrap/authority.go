package bootstrap

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/authority"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/development"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/evidence"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/modelproxy"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/config"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// The authority inputs of the dispatch chain (P2.1, B-14). Every
// internal/adapters/development source is reachable only under
// dispatch.development.enabled, which the configuration loader admits only
// with development.enabled; outside it Control runs on the database price
// book, the declared authority source, signed evidence and the Model Proxy
// query, or does not start.

// newPriceBook is the fixture book in development, otherwise the database
// price book (StoredPrices, also returned for its start check and refresh).
func newPriceBook(cfg config.Config, store application.Store, log *slog.Logger) (application.PriceBook, *application.StoredPrices, error) {
	if cfg.Dispatch.Development.Enabled {
		prices, err := cfg.Dispatch.Development.DomainPrices()
		if err != nil {
			return nil, nil, err
		}
		log.Warn("DEVELOPMENT_ONLY dispatch fixtures enabled", "prices", len(prices), "authorizedRoutes", len(cfg.Dispatch.Development.AuthorizedRoutes), "notSentIssuers", len(cfg.Dispatch.Development.NotSentIssuers))
		book, err := development.NewPriceBook(prices)
		return book, nil, err
	}
	stored := application.NewStoredPrices(store, log)
	return stored, stored, nil
}

// startPriceBook publishes the reviewed initial price book and loads the
// snapshot before Control serves; a production deployment without any
// price revision refuses to start (P2.1 AC7).
func startPriceBook(ctx context.Context, cfg config.Config, prices *application.StoredPrices, finance *application.Finance, log *slog.Logger) error {
	if prices == nil {
		return nil
	}
	if path := cfg.Dispatch.Prices.InitialFile; path != "" {
		book, err := config.LoadPriceBook(path)
		if err != nil {
			return fmt.Errorf("production price book: %w", err)
		}
		n, err := finance.InstallPrices(ctx, book)
		if err != nil {
			return fmt.Errorf("production price book: %w", err)
		}
		log.Info("initial price book installed", "file", path, "revisions", len(book), "published", n)
	}
	if err := prices.Refresh(ctx); err != nil {
		return fmt.Errorf("production price book: %w", err)
	}
	if prices.Count() == 0 {
		return fmt.Errorf("production price book: the database holds no price revision and dispatch.prices.initial_file names none (ENV-06); every paid route would be denied")
	}
	log.Info("price book loaded", "revisions", prices.Count())
	return nil
}

// newAuthority is the commercial authority of admission and of
// AuthorizationService: the route fixture in development, otherwise the
// declared decision endpoint (ENV-07).
func newAuthority(cfg config.Config, clock domain.Clock, reg *prometheus.Registry, log *slog.Logger) (application.Authority, application.AuthorizationSource, error) {
	if cfg.Dispatch.Development.Enabled {
		routes := make([]development.RouteAuthorization, 0, len(cfg.Dispatch.Development.AuthorizedRoutes))
		for _, r := range cfg.Dispatch.Development.AuthorizedRoutes {
			routes = append(routes, development.RouteAuthorization{TenantID: r.TenantID, RouteID: r.RouteID})
		}
		a := development.NewAuthority(routes, cfg.Dispatch.AuthorityFreshness, clock)
		return a, a, nil
	}
	a := cfg.Dispatch.Authority
	// P3.6 (platform.md SLO row 3): each decision's age at its use; the
	// configured freshness (at most 30 s) bounds it.
	age := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "anvilkit_control_authority_decision_age_seconds",
		Help:    "Age of a commercial authorization decision when admission or AuthorizationService relies on it (0 when just fetched), by kind and decision.",
		Buckets: []float64{0.001, 0.5, 1, 2.5, 5, 10, 15, 20, 25, 30}}, []string{"kind", "decision"})
	reg.MustRegister(age)
	src, err := authority.New(authority.Options{
		URL: a.URL, Timeout: a.Timeout, Freshness: cfg.Dispatch.AuthorityFreshness,
		CAFile: a.CAFile, CertFile: a.CertFile, KeyFile: a.KeyFile, ServerName: a.ServerName, TokenFile: a.TokenFile,
		DecisionAge: age,
	}, clock)
	if err != nil {
		return nil, nil, fmt.Errorf("production authority: %w", err)
	}
	log.Info("commercial authority from the declared decision endpoint", "url", a.URL, "freshness", cfg.Dispatch.AuthorityFreshness.String())
	return src, src, nil
}

// newEvidence verifies not-sent and disposition evidence: the unsigned
// fixture attestations in development, otherwise signed envelopes of the
// configured issuer keys.
func newEvidence(cfg config.Config, inv application.Inventory, log *slog.Logger) (application.NotSentEvidence, application.DispositionEvidence, error) {
	if cfg.Dispatch.Development.Enabled {
		issuers := cfg.Dispatch.Development.NotSentIssuers
		return development.NewNotSentEvidence(inv, issuers), development.NewDispositionEvidence(inv, issuers), nil
	}
	specs := make([]evidence.IssuerSpec, 0, len(cfg.Dispatch.Evidence.Issuers))
	for _, i := range cfg.Dispatch.Evidence.Issuers {
		specs = append(specs, evidence.IssuerSpec{Name: i.Name, KeyID: i.KeyID, PublicKeyFile: i.PublicKeyFile})
	}
	issuers, err := evidence.LoadIssuers(specs)
	if err != nil {
		return nil, nil, fmt.Errorf("production evidence: %w", err)
	}
	v := evidence.NewVerifier(inv, issuers)
	log.Info("signed evidence issuers loaded", "issuers", len(issuers))
	return v, v, nil
}

// newOutcomeQuery is the original-identity query: the Model Proxy for model
// dispatches when placed; the DEVELOPMENT_ONLY attestation double answers
// everything else in development, while outside it the other classes are
// reported by their senders (MCP's reconciler, the Workflow's Pagix ports).
func newOutcomeQuery(cfg config.Config, inv application.Inventory, log *slog.Logger) (application.OutcomeQuery, error) {
	var fallback application.OutcomeQuery = application.SenderReported{}
	if cfg.Dispatch.Development.Enabled {
		fallback = development.NewOutcomeQuery(inv, cfg.Dispatch.Development.NotSentIssuers)
	}
	mp := cfg.ModelProxy
	if mp.Address == "" {
		return fallback, nil
	}
	o := modelproxy.Options{BaseURL: mp.Address, Timeout: mp.Timeout, Owner: mp.Owner}
	if mp.Identity.Mode == "mtls" {
		m := mp.Identity.MTLS
		o.TLS = &modelproxy.TLSFiles{CertFile: m.CertFile, KeyFile: m.KeyFile, CAFile: m.CAFile, ServerName: m.ServerName}
	} else {
		log.Warn("DEVELOPMENT_ONLY model proxy identity: bearer token; qualifies no production identity")
		o.Token = mp.Token
	}
	log.Info("model dispatch outcomes are queried from the Model Proxy", "address", mp.Address, "owner", mp.Owner)
	return modelproxy.New(o, fallback)
}
