package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// P1.4 (B-25): three independent pools, the health service exempt, control
// slots reserved for SubmitCommand, streams bounded per tenant.
func TestCapacityPoolsAreIndependentAndReserved(t *testing.T) {
	c := newCapacity(Capacity{Control: 4, Execution: 2, Streams: 3, StreamPerTenant: 2, CommandReserve: 1})
	hold := func(method string, n int) []func() {
		var releases []func()
		for i := 0; i < n; i++ {
			release, err := c.acquire(method)
			require.NoError(t, err, "%s slot %d", method, i)
			releases = append(releases, release)
		}
		return releases
	}
	// Streams fill their own pool and leave control and execution untouched.
	streams := hold(streamEventsRPC, 3)
	_, err := c.acquire(streamEventsRPC)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	queries := hold("/anvilkit.control.v1.OperationService/GetOperation", 3)
	_, err = c.acquire("/anvilkit.control.v1.OperationService/GetOperation")
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "queries may take control minus the reserve")
	release, err := c.acquire(submitCommand)
	require.NoError(t, err, "SubmitCommand takes the reserved slot")
	release()
	exec := hold("/anvilkit.control.v1.DispatchService/AdmitModel", 2)
	_, err = c.acquire("/anvilkit.control.v1.DispatchService/AdmitModel")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	// Health never competes.
	for i := 0; i < 10; i++ {
		release, err := c.acquire("/grpc.health.v1.Health/Check")
		require.NoError(t, err)
		release()
	}
	for _, r := range append(append(streams, queries...), exec...) {
		r()
	}
	// Per tenant: the third stream of one tenant is refused while another tenant's first is admitted.
	r1, err := c.tenants.acquire("tenant_a")
	require.NoError(t, err)
	r2, err := c.tenants.acquire("tenant_a")
	require.NoError(t, err)
	_, err = c.tenants.acquire("tenant_a")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	rb, err := c.tenants.acquire("tenant_b")
	require.NoError(t, err)
	r1()
	r3, err := c.tenants.acquire("tenant_a")
	require.NoError(t, err, "a released slot is reusable")
	r2()
	r3()
	rb()
}

func TestDefaultDeadlineBoundsOnlyUnboundedCalls(t *testing.T) {
	i := defaultDeadline(50 * time.Millisecond)
	_, err := i(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x"}, func(ctx context.Context, _ any) (any, error) {
		d, ok := ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(50*time.Millisecond), d, 20*time.Millisecond)
		return nil, nil
	})
	require.NoError(t, err)
	own, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	_, err = i(own, nil, &grpc.UnaryServerInfo{FullMethod: "/x"}, func(ctx context.Context, _ any) (any, error) {
		d, _ := ctx.Deadline()
		require.WithinDuration(t, time.Now().Add(time.Hour), d, time.Second, "the caller's deadline stays")
		return nil, nil
	})
	require.NoError(t, err)
}
