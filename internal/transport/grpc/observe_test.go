package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// P1.5 (CTL-INF-13): the observability interceptors are outermost, so a
// refusal by the capacity pool is counted under its code.
func TestCapacityRefusalsAreCounted(t *testing.T) {
	reg := prometheus.NewRegistry()
	cap := newCapacity(Capacity{Control: 1, Execution: 1, Streams: 1, StreamPerTenant: 1})
	opts := append(append([]grpc.ServerOption(nil), Observability(reg)...), grpc.ChainUnaryInterceptor(cap.unary()))
	s := grpc.NewServer(opts...)
	controlv1.RegisterOperationServiceServer(s, controlv1.UnimplementedOperationServiceServer{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := controlv1.NewOperationServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The one control slot is held: the query is refused by the pool.
	release, err := cap.acquire("/anvilkit.control.v1.OperationService/GetOperation")
	require.NoError(t, err)
	_, err = client.GetOperation(ctx, &controlv1.GetOperationRequest{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	release()
	_, err = client.GetOperation(ctx, &controlv1.GetOperationRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err), "with the slot free the handler answers")
	families, err := reg.Gather()
	require.NoError(t, err)
	counts := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "anvilkit_control_rpcs_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			code := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "code" {
					code = l.GetValue()
				}
			}
			counts[code] = m.GetCounter().GetValue()
		}
	}
	require.Equal(t, float64(1), counts[codes.ResourceExhausted.String()], "the pool's refusal is counted: %v", counts)
	require.Equal(t, float64(1), counts[codes.Unimplemented.String()])
	_ = dto.MetricFamily{}
}
