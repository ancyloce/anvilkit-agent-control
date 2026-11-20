package grpc

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// TestTurnBusyCrossesAsCapacity (DEC-068): the API maps RESOURCE_EXHAUSTED
// to the public CAPACITY_EXHAUSTED, retryable, and keeps the message, so the
// refusal reaches a client as CAPACITY_EXHAUSTED whose message begins
// TURN_BUSY: (the public ErrorCode enum cannot grow).
func TestTurnBusyCrossesAsCapacity(t *testing.T) {
	st, ok := status.FromError(toStatus(fmt.Errorf("%w: conversation cnv_1 is answering turn trn_1", domain.ErrTurnBusy)))
	if !ok || st.Code() != codes.ResourceExhausted || !strings.HasPrefix(st.Message(), "TURN_BUSY: ") {
		t.Fatalf("turn busy: %v %q", st.Code(), st.Message())
	}
}

func TestReplyBindingAndPositive(t *testing.T) {
	b, err := replyBinding(&controlv1.ExecutionBinding{OperationId: "op_1", AttemptId: "att_1", ExecutionEpoch: "3"})
	if err != nil || b.OperationID != "op_1" || b.AttemptID != "att_1" || b.ExecutionEpoch != 3 {
		t.Fatalf("binding: %+v %v", b, err)
	}
	if _, err := replyBinding(&controlv1.ExecutionBinding{OperationId: "op_1", AttemptId: "att_1", InstanceId: "inst_1", ExecutionEpoch: "1"}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("a chat attempt names no instance: %v", err)
	}
	if _, err := positive("0", "writer epoch"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("epoch 0: %v", err)
	}
	if v, err := positive("12", "writer epoch"); err != nil || v != 12 {
		t.Fatalf("epoch 12: %d %v", v, err)
	}
	if len(replyOutcomeFromProto) != 4 || len(senderStatusFromProto) != 4 || len(turnStateToProto) != 7 || len(contentStatusToProto) != 3 {
		t.Fatal("every turn enum value maps")
	}
	if kindToProto[domain.KindChatTurn] != controlv1.OperationKind_OPERATION_KIND_CHAT_TURN || kindFromProto[controlv1.OperationKind_OPERATION_KIND_CHAT_TURN] != domain.KindChatTurn {
		t.Fatal("the chat_turn kind maps both ways")
	}
}
