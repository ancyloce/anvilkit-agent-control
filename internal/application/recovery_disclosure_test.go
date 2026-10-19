package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// API-17 (F-P3.7-1): a run is disclosed only to an operator of its scope —
// the operator or platform_operator role in its own tenant, the
// platform_operator role for a run of every tenant — and to nobody else,
// not even as an existence answer.
func TestRecoveryRunsDiscloseOnlyToTheirOperators(t *testing.T) {
	tenantRun := &domain.RecoveryRun{ID: "rec_a", ScopeKey: "tenant_a"}
	everyRun := &domain.RecoveryRun{ID: "rec_all", ScopeKey: domain.ScopeAll}
	for _, c := range []struct {
		name   string
		run    *domain.RecoveryRun
		viewer domain.Scope
		ok     bool
	}{
		{"the tenant's operator", tenantRun, domain.Scope{TenantID: "tenant_a", ActorID: "op", Roles: []string{domain.RoleOperator}}, true},
		{"the tenant's platform operator", tenantRun, domain.Scope{TenantID: "tenant_a", ActorID: "op", Roles: []string{domain.RolePlatformOperator}}, true},
		{"a member of the tenant without the role", tenantRun, domain.Scope{TenantID: "tenant_a", ActorID: "user"}, false},
		{"another tenant's operator", tenantRun, domain.Scope{TenantID: "tenant_b", ActorID: "op", Roles: []string{domain.RoleOperator}}, false},
		{"another tenant's platform operator", tenantRun, domain.Scope{TenantID: "tenant_b", ActorID: "op", Roles: []string{domain.RolePlatformOperator}}, false},
		{"a tenant operator reading every tenant's run", everyRun, domain.Scope{TenantID: "tenant_a", ActorID: "op", Roles: []string{domain.RoleOperator}}, false},
		{"a platform operator reading every tenant's run", everyRun, domain.Scope{TenantID: "tenant_b", ActorID: "op", Roles: []string{domain.RolePlatformOperator}}, true},
		{"a tenant literally named *", everyRun, domain.Scope{TenantID: domain.ScopeAll, ActorID: "op", Roles: []string{domain.RoleOperator}}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := application.Discloses(c.run, c.viewer)
			if c.ok {
				require.NoError(t, err)
				return
			}
			require.True(t, errors.Is(err, domain.ErrNotFound), "refused as NOT_FOUND, got %v", err)
		})
	}
}

func TestRecoveryViewableReadsTheStoredScope(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(t.TempDir())
	require.NoError(t, err)
	p := newP07Process(t, inst, &hookedInventory{inner: fsInv}, &authorityDouble{allowed: map[string]bool{}}, newPriceSource(fixturePrices(t, "fx-model-v", 1)))
	ctx := context.Background()
	operator := domain.CommandIdentity{TenantID: "tenant_a", CommandID: "view_begin", ActorID: operatorActor, RequestDigest: application.DigestOf([]byte("view"))}
	run, _, err := p.recovery.Begin(ctx, operator, opScope(operator), "tenant_a", time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 0, "disclosure")
	require.NoError(t, err)

	require.NoError(t, p.recovery.Viewable(ctx, run.ID, opScope(operator)))
	other := domain.Scope{TenantID: "tenant_b", ActorID: operatorActor, Roles: []string{domain.RoleOperator}}
	require.ErrorIs(t, p.recovery.Viewable(ctx, run.ID, other), domain.ErrNotFound, "another tenant's operator")
	require.ErrorIs(t, p.recovery.Viewable(ctx, run.ID, domain.Scope{TenantID: "tenant_a", ActorID: "user_a"}), domain.ErrNotFound, "no operator role")
	require.ErrorIs(t, p.recovery.Viewable(ctx, "rec_does_not_exist", opScope(operator)), domain.ErrNotFound, "an unknown run")
}
