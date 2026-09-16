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
	"google.golang.org/grpc/credentials/insecure"
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
)

// priceBook holds the DEVELOPMENT_ONLY fixture prices of the transport
// test: one route the fixture authority never authorizes and one it does,
// each bound to its trusted provider/model.
func priceBook(t *testing.T) application.PriceBook {
	t.Helper()
	perMillion := map[domain.UsageCategory]int64{domain.UsageInput: 3_000_000, domain.UsageOutput: 15_000_000, domain.UsageReasoning: 15_000_000, domain.UsageCachedInput: 300_000}
	from, until := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	book, err := development.NewPriceBook([]domain.Price{
		{Revision: "fx-transport", Kind: domain.DispatchModel, Route: "fixture-route", Provider: "fixture", Model: "fixture-model", Currency: "USD", EffectiveFrom: from, EffectiveUntil: until, PerMillion: perMillion, MaxExposure: 5_000_000},
		{Revision: "fx-transport-authorized", Kind: domain.DispatchModel, Route: "authorized-route", Provider: "fixture", Model: "fixture-model", Currency: "USD", EffectiveFrom: from, EffectiveUntil: until, PerMillion: perMillion, MaxExposure: 5_000_000},
	})
	require.NoError(t, err)
	return book
}

// TestTransport proves the generated client, protovalidate interceptor,
// capacity pools and status mapping over a real listener and database.
func TestTransport(t *testing.T) {
	inst := testdb.Start(t)
	store := postgres.NewStore(inst.Pool(t))
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inv"))
	require.NoError(t, err)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	profiles := []domain.Profile{{ID: "local-check-v1", Kind: domain.KindLocalCheck, OperationDeadline: 15 * time.Minute, StepID: "local-check", JobProfileID: "local-check-v1"}}
	ops := application.NewOperations(store, inv, profiles, domain.SystemClock{}, log)
	exec := application.NewExecution(store, inv, jobs.Contract{}, domain.SystemClock{}, log)
	authority := development.NewAuthority([]development.RouteAuthorization{{TenantID: "tenant_a", RouteID: "authorized-route"}}, 30*time.Second, domain.SystemClock{})
	dispatch := application.NewDispatch(store, inv, priceBook(t), authority, development.NewNotSentEvidence(inv, nil), domain.SystemClock{}, log)
	effects := application.NewEffects(store, inv, development.NewOutcomeQuery(inv, nil), domain.SystemClock{}, log)
	recovery := application.NewRecovery(store, inv, development.NewOutcomeQuery(inv, nil), dispatch, effects, exec, profiles, authority, development.NewNotSentEvidence(inv, nil), development.NewDispositionEvidence(inv, nil), domain.SystemClock{}, log, 100)
	srv, err := grpctransport.NewServer("127.0.0.1:0", 4, 4, ops, exec, dispatch, effects, recovery, application.NewArtifacts(store, nil, application.ArtifactLimits{}, domain.SystemClock{}, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))))
	require.NoError(t, err)
	addr, err := srv.Start()
	require.NoError(t, err)
	t.Cleanup(func() { srv.Stop(2 * time.Second) })

	conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	client := controlv1.NewOperationServiceClient(conn)
	ctx := context.Background()
	digest := string(application.DigestOf([]byte("subject")))

	_, err = client.CreateOperation(ctx, &controlv1.CreateOperationRequest{
		Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "c1", ActorId: "u", RequestDigest: "not-a-digest"},
		Scope:   &controlv1.Scope{TenantId: "tenant_a", ActorId: "u"}, Kind: controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK,
		Subject: &controlv1.OperationSubject{ProfileId: "local-check-v1", SubjectDigest: digest},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "protovalidate rejects a malformed digest: %v", err)

	_, err = client.CreateOperation(ctx, &controlv1.CreateOperationRequest{
		Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "c1", ActorId: "u", RequestDigest: digest},
		Scope:   &controlv1.Scope{TenantId: "tenant_a", ActorId: "u"}, Kind: controlv1.OperationKind_OPERATION_KIND_UNSPECIFIED,
		Subject: &controlv1.OperationSubject{ProfileId: "local-check-v1", SubjectDigest: digest},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "an unspecified enum authorizes nothing")

	created, err := client.CreateOperation(ctx, &controlv1.CreateOperationRequest{
		Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "c1", ActorId: "u", RequestDigest: digest},
		Scope:   &controlv1.Scope{TenantId: "tenant_a", ActorId: "u"}, Kind: controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK,
		Subject: &controlv1.OperationSubject{ProfileId: "local-check-v1", SubjectDigest: digest},
	})
	require.NoError(t, err)
	require.False(t, created.Existing)
	require.Equal(t, controlv1.Lifecycle_LIFECYCLE_ACCEPTED, created.Operation.Lifecycle)
	require.Equal(t, "2", created.Operation.Revision)
	require.Equal(t, "2", created.Operation.CoveredEventSeq)

	_, err = client.GetOperation(ctx, &controlv1.GetOperationRequest{Scope: &controlv1.Scope{TenantId: "tenant_b", ActorId: "v"}, OperationId: created.Operation.OperationId})
	require.Equal(t, codes.NotFound, status.Code(err), "cross-tenant read is not found")

	_, err = client.SubmitCommand(ctx, &controlv1.SubmitCommandRequest{
		Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "c2", ActorId: "u", RequestDigest: digest},
		Scope:   &controlv1.Scope{TenantId: "tenant_a", ActorId: "u"}, OperationId: created.Operation.OperationId,
		Kind: controlv1.CommandKind_COMMAND_KIND_CANCEL, ExpectedRevision: "1",
	})
	require.Equal(t, codes.Aborted, status.Code(err), "stale expected revision aborts with REVISION_CONFLICT")

	stream, err := client.StreamOperationEvents(ctx, &controlv1.StreamOperationEventsRequest{Scope: &controlv1.Scope{TenantId: "tenant_a", ActorId: "u"}, OperationId: created.Operation.OperationId})
	require.NoError(t, err)
	first, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, "1", first.Event.EventSeq)
	require.Equal(t, "intake", first.Event.GetOperationChanged().Phase)
	second, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, "accepted", second.Event.GetOperationChanged().Phase)

	receipt, err := client.SubmitCommand(ctx, &controlv1.SubmitCommandRequest{
		Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "c3", ActorId: "u", RequestDigest: digest},
		Scope:   &controlv1.Scope{TenantId: "tenant_a", ActorId: "u"}, OperationId: created.Operation.OperationId,
		Kind: controlv1.CommandKind_COMMAND_KIND_CANCEL, ExpectedRevision: "2",
	})
	require.NoError(t, err)
	require.Equal(t, controlv1.CommandOutcome_COMMAND_OUTCOME_APPLIED, receipt.Receipt.Outcome)
	third, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, controlv1.Lifecycle_LIFECYCLE_CANCELED, third.Event.GetOperationChanged().Lifecycle)
	_, err = stream.Recv()
	require.Error(t, err, "the stream ends after the terminal event")

	// DispatchService over the wire: an unspecified enum and a malformed
	// money amount are rejected before any handler; a route without an
	// authorized tenant is a recorded denial, not an error; a call is
	// queried without permission; insufficient evidence is an accepted
	// business outcome (FAILED_PRECONDITION EFFECT_UNCERTAIN), not a 503.
	dc := controlv1.NewDispatchServiceClient(conn)
	secondOp, err := client.CreateOperation(ctx, &controlv1.CreateOperationRequest{
		Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "c4", ActorId: "u", RequestDigest: digest},
		Scope:   &controlv1.Scope{TenantId: "tenant_a", ActorId: "u"}, Kind: controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK,
		Subject: &controlv1.OperationSubject{ProfileId: "local-check-v1", SubjectDigest: digest},
	})
	require.NoError(t, err)
	ec := controlv1.NewExecutionServiceClient(conn)
	opened, err := ec.OpenAttempt(ctx, &controlv1.OpenAttemptRequest{Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "c5", ActorId: "u", RequestDigest: digest},
		OperationId: secondOp.Operation.OperationId, StepId: "local-check", VisitOrdinal: "0", ProfileId: "local-check-v1"})
	require.NoError(t, err)

	// GetInstance (P09): the registration the launcher recorded, with the
	// attempt and the operation as of the same read; the sidecar decides
	// authority from those facts. An unregistered Pod is not found.
	_, err = ec.GetInstance(ctx, &controlv1.GetInstanceRequest{Backend: "kind-anvilkit-dev", LaunchKey: "transport-launch", PodUid: "pod-transport"})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = ec.PrepareLaunch(ctx, &controlv1.PrepareLaunchRequest{Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "c5-launch", ActorId: "u", RequestDigest: digest},
		AttemptId: opened.Attempt.AttemptId, LaunchKey: "transport-launch", Backend: "kind-anvilkit-dev", ImageDigest: digest, Deadline: timestamppb.New(time.Now().Add(5 * time.Minute))})
	require.NoError(t, err)
	registered, err := ec.RegisterInstance(ctx, &controlv1.RegisterInstanceRequest{Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "c5-register", ActorId: "u", RequestDigest: digest},
		AttemptId: opened.Attempt.AttemptId, LaunchKey: "transport-launch", Backend: "kind-anvilkit-dev", JobUid: "job-transport", PodUid: "pod-transport", ImageDigest: digest})
	require.NoError(t, err)
	scope, err := ec.GetInstance(ctx, &controlv1.GetInstanceRequest{Backend: "kind-anvilkit-dev", LaunchKey: "transport-launch", PodUid: "pod-transport"})
	require.NoError(t, err)
	require.Equal(t, registered.Instance.InstanceId, scope.Instance.InstanceId)
	require.True(t, scope.Instance.Current)
	require.Equal(t, controlv1.AttemptState_ATTEMPT_STATE_RUNNING, scope.Attempt.State)
	require.Equal(t, "tenant_a", scope.TenantId)
	require.NotNil(t, scope.Operation, "the operation travels with the registration")
	require.Equal(t, secondOp.Operation.OperationId, scope.Operation.OperationId)
	require.Equal(t, controlv1.Lifecycle_LIFECYCLE_RUNNING, scope.Operation.Lifecycle)
	require.Equal(t, controlv1.ControlState_CONTROL_STATE_NONE, scope.Operation.Control)
	require.Equal(t, scope.Attempt.ExecutionEpoch, scope.Operation.ExecutionEpoch)
	require.Equal(t, scope.RecoveryEpoch, "1")
	bind := &controlv1.ExecutionBinding{OperationId: secondOp.Operation.OperationId, AttemptId: opened.Attempt.AttemptId, ExecutionEpoch: "1"}
	admit := func(callID, amount string) (*controlv1.AdmitModelResponse, error) {
		return dc.AdmitModel(ctx, &controlv1.AdmitModelRequest{
			Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "adm-" + callID, ActorId: "proxy", RequestDigest: digest},
			Binding: bind, CallId: callID, Owner: "proxy-a", RouteId: "fixture-route", Provider: "fixture", Model: "fixture-model",
			MaxExposure: &controlv1.Money{Currency: "USD", Amount: amount}, Deadline: timestamppb.New(time.Now().Add(time.Minute)),
		})
	}
	_, err = admit("call-bad", "1.5")
	require.Equal(t, codes.InvalidArgument, status.Code(err), "money is a canonical integer string: %v", err)
	denied, err := admit("call-1", "1000")
	require.NoError(t, err)
	require.False(t, denied.Admission.DispatchAllowed)
	require.Equal(t, controlv1.DispatchState_DISPATCH_STATE_DENIED, denied.Admission.Dispatch.State)
	require.Equal(t, "FORBIDDEN", denied.Admission.GetDenialCode(), "no authorized route for the tenant (the fixture authority has none): the route is denied before any budget is consulted")
	got, err := dc.GetDispatch(ctx, &controlv1.GetDispatchRequest{Owner: "proxy-a", CallId: "call-1"})
	require.NoError(t, err)
	require.Equal(t, denied.Admission.Dispatch.DispatchId, got.Dispatch.DispatchId)
	_, err = dc.ObserveDispatch(ctx, &controlv1.ObserveDispatchRequest{DispatchId: got.Dispatch.DispatchId, Source: "proxy", Sequence: "1",
		Outcome: controlv1.DispatchOutcome_DISPATCH_OUTCOME_UNSPECIFIED, ObservedAt: timestamppb.Now()})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "an unspecified outcome is rejected by protovalidate")
	_, err = dc.ObserveDispatch(ctx, &controlv1.ObserveDispatchRequest{DispatchId: got.Dispatch.DispatchId, Source: "proxy", Sequence: "1",
		Outcome: controlv1.DispatchOutcome_DISPATCH_OUTCOME_SUCCEEDED, ObservedAt: timestamppb.Now()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "usage for a call that was never authorized: %v", err)
	_, err = dc.ConfirmNotSent(ctx, &controlv1.ConfirmNotSentRequest{Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "c6", ActorId: "u", RequestDigest: digest},
		DispatchId: got.Dispatch.DispatchId, EvidenceRef: "not-sent/" + got.Dispatch.DispatchId, EvidenceDigest: digest})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "EFFECT_UNCERTAIN", "missing evidence leaves the call as it is")

	// On the authorized, priced route with a funded operation: the wire
	// provider/model must be the route's trusted binding; a reentry that
	// keeps the call identity but changes its binding is ABORTED, not a
	// second answer; a definite outcome without cumulative_usage is refused
	// as insufficient settlement evidence (FAILED_PRECONDITION
	// EFFECT_UNCERTAIN) and an explicit zero usage settles the call.
	pool := inst.Pool(t)
	for _, row := range []struct {
		level         string
		tenant, actor *string
	}{{"platform", nil, nil}, {"tenant", strPtr("tenant_a"), nil}, {"actor", strPtr("tenant_a"), strPtr("u")}} {
		_, err := pool.Exec(ctx, "INSERT INTO budget_pools (pool_id, level, tenant_id, actor_id, period_start, period_end, currency, cap_amount) VALUES ($1, $2, $3, $4, now() - interval '1 hour', now() + interval '1 hour', 'USD', 1000000)",
			"pool-transport-"+row.level, row.level, row.tenant, row.actor)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "INSERT INTO allocations (allocation_id, pool_id, operation_id, currency, amount) VALUES ($1, $2, $3, 'USD', 100000)",
			"alloc-transport-"+row.level, "pool-transport-"+row.level, secondOp.Operation.OperationId)
		require.NoError(t, err)
	}
	deadline := timestamppb.New(time.Now().Add(time.Minute)) // one deadline: a reentry carries the recorded one
	admitOn := func(callID, provider, model string, binding *controlv1.ExecutionBinding) (*controlv1.Admission, error) {
		resp, err := dc.AdmitModel(ctx, &controlv1.AdmitModelRequest{
			Command: &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "adm-" + callID, ActorId: "proxy", RequestDigest: digest},
			Binding: binding, CallId: callID, Owner: "proxy-a", RouteId: "authorized-route", Provider: provider, Model: model,
			MaxExposure: &controlv1.Money{Currency: "USD", Amount: "1000"}, Deadline: deadline,
		})
		if err != nil {
			return nil, err
		}
		return resp.GetAdmission(), nil
	}
	_, err = admitOn("call-empty", "", "fixture-model", bind)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "protovalidate rejects an incomplete provider/model binding before any handler: %v", err)
	mismatch, err := admitOn("call-mismatch", "fixture", "other-model", bind)
	require.NoError(t, err)
	require.False(t, mismatch.GetDispatchAllowed())
	require.Equal(t, "PROFILE_UNQUALIFIED", mismatch.GetDenialCode(), "a model the route's observation does not bind has no reviewed price")
	ok, err := admitOn("call-ok", "fixture", "fixture-model", bind)
	require.NoError(t, err)
	require.True(t, ok.GetDispatchAllowed())
	require.Equal(t, "fx-transport-authorized", ok.GetDispatch().GetMeterRevision())
	_, err = admitOn("call-ok", "fixture", "other-model", bind)
	require.Equal(t, codes.Aborted, status.Code(err), "the same call with another model is a conflicting request, not a reentry: %v", err)
	require.Contains(t, status.Convert(err).Message(), "IDEMPOTENCY_CONFLICT")
	_, err = admitOn("call-ok", "fixture", "fixture-model", &controlv1.ExecutionBinding{OperationId: created.Operation.OperationId, AttemptId: opened.Attempt.AttemptId, ExecutionEpoch: "1"})
	require.Equal(t, codes.Aborted, status.Code(err), "the same call bound to another operation is a conflicting request: %v", err)
	same, err := admitOn("call-ok", "fixture", "fixture-model", bind)
	require.NoError(t, err)
	require.False(t, same.GetDispatchAllowed(), "the recorded request is a reentry and never a second permission")
	require.Equal(t, ok.GetDispatch().GetDispatchId(), same.GetDispatch().GetDispatchId())

	id := ok.GetDispatch().GetDispatchId()
	_, err = dc.ObserveDispatch(ctx, &controlv1.ObserveDispatchRequest{DispatchId: id, Source: "proxy", Sequence: "1", Outcome: controlv1.DispatchOutcome_DISPATCH_OUTCOME_SUCCEEDED, ObservedAt: timestamppb.Now()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a definite outcome without cumulative_usage: %v", err)
	require.Contains(t, status.Convert(err).Message(), "EFFECT_UNCERTAIN")
	after, err := dc.GetDispatch(ctx, &controlv1.GetDispatchRequest{DispatchId: id})
	require.NoError(t, err)
	require.Equal(t, controlv1.DispatchState_DISPATCH_STATE_AUTHORIZED, after.GetDispatch().GetState(), "the refused report changed nothing")
	var reserved int64
	require.NoError(t, pool.QueryRow(ctx, "SELECT max(reserved) FROM allocations WHERE operation_id = $1", secondOp.Operation.OperationId).Scan(&reserved))
	require.Equal(t, int64(1000), reserved, "the exposure was not released as zero cost")
	unknown, err := dc.ObserveDispatch(ctx, &controlv1.ObserveDispatchRequest{DispatchId: id, Source: "proxy", Sequence: "1", Outcome: controlv1.DispatchOutcome_DISPATCH_OUTCOME_UNKNOWN, ObservedAt: timestamppb.Now()})
	require.NoError(t, err, "unknown needs no usage")
	require.Equal(t, controlv1.DispatchState_DISPATCH_STATE_UNKNOWN, unknown.GetDispatch().GetState())
	_, err = dc.ObserveDispatch(ctx, &controlv1.ObserveDispatchRequest{DispatchId: id, Source: "proxy", Sequence: "2", Outcome: controlv1.DispatchOutcome_DISPATCH_OUTCOME_SUCCEEDED,
		CumulativeUsage: &controlv1.Usage{InputUnits: "0", OutputUnits: "", ReasoningUnits: "0", CachedInputUnits: "0"}, ObservedAt: timestamppb.Now()})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a present usage message must carry every canonical counter: %v", err)
	settled, err := dc.ObserveDispatch(ctx, &controlv1.ObserveDispatchRequest{DispatchId: id, Source: "proxy", Sequence: "2", Outcome: controlv1.DispatchOutcome_DISPATCH_OUTCOME_SUCCEEDED,
		CumulativeUsage: &controlv1.Usage{InputUnits: "0", OutputUnits: "0", ReasoningUnits: "0", CachedInputUnits: "0"}, ObservedAt: timestamppb.Now()})
	require.NoError(t, err)
	require.False(t, settled.GetExisting(), "the refused report was never recorded")
	require.Equal(t, controlv1.DispatchState_DISPATCH_STATE_OBSERVED, settled.GetDispatch().GetState(), "explicitly reported zero usage settles")
	require.NoError(t, pool.QueryRow(ctx, "SELECT max(reserved) FROM allocations WHERE operation_id = $1", secondOp.Operation.OperationId).Scan(&reserved))
	require.Zero(t, reserved)
}

func strPtr(s string) *string { return &s }
