package grpc_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/development"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/jobs"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
	transport "github.com/ancyloce/anvilkit-agent-control/internal/transport/grpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The closed candidate binding stays rejected. A current trusted registration
// attempt crosses the real RPC/transaction boundary and consumes its permit once.
func TestCandidateRegistrationExecutionBinding(t *testing.T) {
	inst := testdb.Start(t)
	store := postgres.NewStore(inst.Pool(t))
	inv, err := inventory.NewFilesystem(t.TempDir())
	require.NoError(t, err)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	profiles := []domain.Profile{{ID: "generation-binding-test", Kind: domain.KindLocalCheck, OperationDeadline: time.Hour, MultiStep: true}}
	ops := application.NewOperations(store, inv, profiles, domain.SystemClock{}, log)
	execution := application.NewExecution(store, inv, jobs.Contract{}, profiles, domain.SystemClock{}, log)
	effects := application.NewEffects(store, inv, development.NewOutcomeQuery(inv, nil), domain.SystemClock{}, log)
	srv, err := transport.NewServer("127.0.0.1:0", 4, 4, ops, execution, nil, effects, nil, nil, nil, nil)
	require.NoError(t, err)
	addr, err := srv.Start()
	require.NoError(t, err)
	t.Cleanup(func() { srv.Stop(time.Second) })
	conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	ec := controlv1.NewExecutionServiceClient(conn)
	fx := controlv1.NewEffectServiceClient(conn)
	ctx := context.Background()
	for _, current := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed_validator_denied", true: "current_trusted_registration_permitted"}[current], func(t *testing.T) {
			command := func(id string) *controlv1.CommandIdentity {
				return &controlv1.CommandIdentity{TenantId: "tenant_a", ActorId: "workflow", CommandId: id, RequestDigest: string(application.DigestOf([]byte("accepted-source-and-certification")))}
			}
			id := domain.NewID("regression")
			op, _, err := ops.Create(ctx, domain.CommandIdentity{TenantID: "tenant_a", ActorID: "workflow", CommandID: id, RequestDigest: application.DigestOf([]byte(id))}, domain.Scope{TenantID: "tenant_a", ActorID: "workflow"}, domain.KindLocalCheck, domain.Subject{ProfileID: profiles[0].ID, SubjectDigest: application.DigestOf([]byte(id))}, nil)
			require.NoError(t, err)
			opened, err := ec.OpenAttempt(ctx, &controlv1.OpenAttemptRequest{Command: command(id + ":validator"), OperationId: op.ID, StepId: "validate", VisitOrdinal: "0", ProfileId: profiles[0].ID})
			require.NoError(t, err)
			_, err = ec.CloseAttempt(ctx, &controlv1.CloseAttemptRequest{Command: command(id + ":close"), AttemptId: opened.Attempt.AttemptId, Outcome: controlv1.AttemptOutcome_ATTEMPT_OUTCOME_COMPLETED, Cleanup: controlv1.CleanupState_CLEANUP_STATE_COMPLETE})
			require.NoError(t, err)
			if current {
				opened, err = ec.OpenAttempt(ctx, &controlv1.OpenAttemptRequest{Command: command(id + ":registration"), OperationId: op.ID, StepId: "register_candidate", VisitOrdinal: "0", ProfileId: profiles[0].ID})
				require.NoError(t, err)
			}
			req := &controlv1.PrepareEffectRequest{Command: command(id + ":candidate:1"), Binding: &controlv1.ExecutionBinding{OperationId: op.ID, AttemptId: opened.Attempt.AttemptId, ExecutionEpoch: "1"}, Owner: "workflow", Kind: controlv1.EffectKind_EFFECT_KIND_BUSINESS_WRITE, Occurrence: "1", CanonicalSubject: "component/c1/page/p1", Deadline: timestamppb.New(time.Now().Add(time.Minute))}
			first, err := fx.PrepareEffect(ctx, req)
			require.NoError(t, err)
			require.Equal(t, current, first.Permitted)
			again, err := fx.PrepareEffect(ctx, req)
			require.NoError(t, err)
			require.False(t, again.Permitted, "a replay cannot obtain another permit")
			if !current {
				require.Equal(t, "STALE_EXECUTION", first.Effect.GetDenialCode())
			}
		})
	}
}
