package grpc_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/development"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/jobs"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
	grpctransport "github.com/ancyloce/anvilkit-agent-control/internal/transport/grpc"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity/identitytest"
)

// TestAuthorityFromWorkloadIdentity (P0.2 AC1, AC2, AC4) runs the real
// application stack behind the mTLS listener: the dispatch owner is the
// verified caller, never the request's claim; an owner reads and reports
// only its own dispatches; reads and observations are bound to the tenant
// they act for; a result's observer must be the caller.
func TestAuthorityFromWorkloadIdentity(t *testing.T) {
	inst := testdb.Start(t)
	store := postgres.NewStore(inst.Pool(t))
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inv"))
	require.NoError(t, err)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	profiles := []domain.Profile{{ID: "local-check-v1", Kind: domain.KindLocalCheck, OperationDeadline: 15 * time.Minute, StepID: "local-check", JobProfileID: "local-check-v1"}}
	ops := application.NewOperations(store, inv, profiles, domain.SystemClock{}, log)
	exec := application.NewExecution(store, inv, jobs.Contract{}, profiles, domain.SystemClock{}, log)
	authority := development.NewAuthority([]development.RouteAuthorization{{TenantID: "tenant_a", RouteID: "authorized-route"}}, 30*time.Second, domain.SystemClock{})
	dispatch := application.NewDispatch(store, inv, priceBook(t), authority, development.NewNotSentEvidence(inv, nil), domain.SystemClock{}, log)
	effects := application.NewEffects(store, inv, development.NewOutcomeQuery(inv, nil), domain.SystemClock{}, log)
	recovery := application.NewRecovery(store, inv, development.NewOutcomeQuery(inv, nil), dispatch, effects, exec, profiles, development.NewNotSentEvidence(inv, nil), development.NewDispositionEvidence(inv, nil), domain.SystemClock{}, log, 100)

	ca := identitytest.NewCA(t, "ca")
	td := "anvilkit.local"
	mount := func(ns, sa string) string {
		dir := filepath.Join(t.TempDir(), ns+"-"+sa)
		identitytest.Mount(t, dir, ca.Issue(t, sa, []string{identitytest.SPIFFE(td, ns, sa)}, sa), ca.PEM)
		return dir
	}
	cert, key, caFile := identitytest.Files(mount("anvilkit-apps", "anvilkit-agent-control"))
	r, err := identity.New(identity.Files{CertFile: cert, KeyFile: key, CAFile: caFile}, 0, nil)
	require.NoError(t, err)
	srv, err := grpctransport.NewServerWithIdentity("127.0.0.1:0", &grpctransport.Identity{Reloader: r, TrustDomain: td, Policy: grpctransport.Policy()}, 4, 4,
		ops, exec, dispatch, effects, recovery, application.NewArtifacts(store, nil, application.ArtifactLimits{}, domain.SystemClock{}, log),
		application.NewPreparations(store, profiles, domain.SystemClock{}, log), application.NewGenerations(store, dispatch, profiles, nil, domain.SystemClock{}, log),
		application.NewGrantPolicies(store, domain.SystemClock{}, log), application.NewPreviews(store, domain.SystemClock{}), application.NewReleases(store, domain.SystemClock{}))
	require.NoError(t, err)
	addr, err := srv.Start()
	require.NoError(t, err)
	t.Cleanup(func() { srv.Stop(2 * time.Second) })
	dial := func(ns, sa string) *grpc.ClientConn {
		cert, key, caFile := identitytest.Files(mount(ns, sa))
		cr, err := identity.New(identity.Files{CertFile: cert, KeyFile: key, CAFile: caFile}, 0, nil)
		require.NoError(t, err)
		creds, err := identity.NewClientCredentials(cr, "anvilkit-agent-control")
		require.NoError(t, err)
		conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(creds))
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	apiConn, workflowConn := dial("anvilkit-apps", "anvilkit-agent-api"), dial("anvilkit-apps", "anvilkit-agent-workflow")
	proxy := controlv1.NewDispatchServiceClient(dial("anvilkit-apps", "anvilkit-agent-model-proxy"))
	mcp := controlv1.NewDispatchServiceClient(dial("anvilkit-apps", "anvilkit-agent-mcp"))
	knowledge := controlv1.NewDispatchServiceClient(dial("anvilkit-apps", "anvilkit-agent-knowledge"))
	workflowExec := controlv1.NewExecutionServiceClient(workflowConn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	digest := string(application.DigestOf([]byte("subject")))
	command := func(id string) *controlv1.CommandIdentity {
		return &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: id, ActorId: "u", RequestDigest: digest}
	}

	op, err := controlv1.NewOperationServiceClient(apiConn).CreateOperation(ctx, &controlv1.CreateOperationRequest{
		Command: command("create"), Scope: &controlv1.Scope{TenantId: "tenant_a", ActorId: "u"}, Kind: controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK,
		Subject: &controlv1.OperationSubject{ProfileId: "local-check-v1", SubjectDigest: digest},
	})
	require.NoError(t, err)
	opID := op.GetOperation().GetOperationId()
	opened, err := workflowExec.OpenAttempt(ctx, &controlv1.OpenAttemptRequest{Command: command("open"), OperationId: opID, StepId: "local-check", VisitOrdinal: "0", ProfileId: "local-check-v1"})
	require.NoError(t, err)
	attemptID := opened.GetAttempt().GetAttemptId()
	_, err = workflowExec.PrepareLaunch(ctx, &controlv1.PrepareLaunchRequest{Command: command("launch"), AttemptId: attemptID, LaunchKey: "authority-launch", Backend: "kind-anvilkit-dev",
		ImageDigest: digest, Deadline: timestamppb.New(time.Now().Add(5 * time.Minute))})
	require.NoError(t, err)
	registered, err := workflowExec.RegisterInstance(ctx, &controlv1.RegisterInstanceRequest{Command: command("register"), AttemptId: attemptID, LaunchKey: "authority-launch",
		Backend: "kind-anvilkit-dev", JobUid: "job-authority", PodUid: "pod-authority", ImageDigest: digest})
	require.NoError(t, err)
	instanceID := registered.GetInstance().GetInstanceId()
	pool := inst.Pool(t)
	for _, row := range []struct {
		level         string
		tenant, actor *string
	}{{"platform", nil, nil}, {"tenant", strPtr("tenant_a"), nil}, {"actor", strPtr("tenant_a"), strPtr("u")}} {
		_, err := pool.Exec(ctx, "INSERT INTO budget_pools (pool_id, level, tenant_id, actor_id, period_start, period_end, currency, cap_amount) VALUES ($1, $2, $3, $4, now() - interval '1 hour', now() + interval '1 hour', 'USD', 1000000)",
			"pool-authority-"+row.level, row.level, row.tenant, row.actor)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "INSERT INTO allocations (allocation_id, pool_id, operation_id, currency, amount) VALUES ($1, $2, $3, 'USD', 100000)",
			"alloc-authority-"+row.level, "pool-authority-"+row.level, opID)
		require.NoError(t, err)
	}
	admit := func(c controlv1.DispatchServiceClient, callID, owner string) (*controlv1.AdmitModelResponse, error) {
		return c.AdmitModel(ctx, &controlv1.AdmitModelRequest{
			Command: command("adm-" + callID), Binding: &controlv1.ExecutionBinding{OperationId: opID, AttemptId: attemptID, ExecutionEpoch: "1"},
			CallId: callID, Owner: owner, RouteId: "authorized-route", Provider: "fixture", Model: "fixture-model",
			MaxExposure: &controlv1.Money{Currency: "USD", Amount: "1000"}, Deadline: timestamppb.New(time.Now().Add(time.Minute)),
		})
	}

	// AC1: the owner is the verified caller.
	_, err = admit(proxy, "call-forged", "anvilkit-agent-mcp")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the proxy may not admit under another owner: %v", err)
	_, err = admit(controlv1.NewDispatchServiceClient(workflowConn), "call-workflow", "anvilkit-agent-model-proxy")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the Workflow may not admit as the proxy: %v", err)
	admitted, err := admit(proxy, "call-own", "anvilkit-agent-model-proxy")
	require.NoError(t, err)
	require.True(t, admitted.GetAdmission().GetDispatchAllowed())
	require.Equal(t, "anvilkit-agent-model-proxy", admitted.GetAdmission().GetDispatch().GetOwner())
	byID, err := admit(proxy, "call-by-id", "spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-model-proxy")
	require.NoError(t, err)
	require.Equal(t, "anvilkit-agent-model-proxy", byID.GetAdmission().GetDispatch().GetOwner(), "the complete ID names the caller; the owner is recorded by ServiceAccount")
	dispatchID := admitted.GetAdmission().GetDispatch().GetDispatchId()

	// AC4: reads and reports are bound to the owner and the tenant.
	get := func(c controlv1.DispatchServiceClient, tenant string) error {
		_, err := c.GetDispatch(ctx, &controlv1.GetDispatchRequest{DispatchId: dispatchID, TenantId: tenant})
		return err
	}
	require.NoError(t, get(proxy, "tenant_a"))
	require.Equal(t, codes.NotFound, status.Code(get(proxy, "tenant_b")), "a dispatch of another tenant is not found")
	require.Equal(t, codes.NotFound, status.Code(get(mcp, "tenant_a")), "another owner's dispatch is not found")
	require.NoError(t, get(knowledge, "tenant_a"), "a non-owner reader is bound to its tenant")
	require.Equal(t, codes.NotFound, status.Code(get(knowledge, "tenant_b")))
	byCall, err := proxy.GetDispatch(ctx, &controlv1.GetDispatchRequest{CallId: "call-own", TenantId: "tenant_a"})
	require.NoError(t, err, "an owner looks its call ids up under itself")
	require.Equal(t, dispatchID, byCall.GetDispatch().GetDispatchId())
	_, err = mcp.ObserveDispatch(ctx, &controlv1.ObserveDispatchRequest{DispatchId: dispatchID, Source: "mcp", Sequence: "1", Outcome: controlv1.DispatchOutcome_DISPATCH_OUTCOME_UNKNOWN, ObservedAt: timestamppb.Now()})
	require.Equal(t, codes.NotFound, status.Code(err), "only the owner reports on a dispatch: %v", err)
	_, err = proxy.ObserveDispatch(ctx, &controlv1.ObserveDispatchRequest{DispatchId: dispatchID, Source: "proxy", Sequence: "1", Outcome: controlv1.DispatchOutcome_DISPATCH_OUTCOME_UNKNOWN, ObservedAt: timestamppb.Now()})
	require.NoError(t, err)

	observe := func(tenant string) error {
		_, err := workflowExec.ObserveInstance(ctx, &controlv1.ObserveInstanceRequest{TenantId: tenant, AttemptId: attemptID, InstanceId: instanceID,
			Phase: controlv1.InstancePhase_INSTANCE_PHASE_RUNNING, ObservedAt: timestamppb.Now()})
		return err
	}
	require.Equal(t, codes.NotFound, status.Code(observe("tenant_b")), "an instance of another tenant is not found")
	require.NoError(t, observe("tenant_a"))

	// AC2: the observer is the verified caller; a result naming anyone else
	// is refused before the application runs.
	_, err = workflowExec.AcceptResult(ctx, &controlv1.AcceptResultRequest{Command: command("accept"), AttemptId: attemptID, InstanceId: instanceID, ProfileId: "local-check-v1",
		Verdict: controlv1.Verdict_VERDICT_CERTIFIED, ResultDigest: digest, ResultManifest: []byte("{}"), ObserverIdentity: "anvilkit-codegen-supervisor"})
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	var stages int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM stage_manifests WHERE attempt_id = $1", attemptID).Scan(&stages))
	require.Zero(t, stages)
}
