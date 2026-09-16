// Package temporal is Control's start/cancel relay adapter (DD-01 §1). It
// starts registered workflows by stable name with the operation id as the
// Workflow ID, so a lost start receipt is recovered by the same call.
package temporal

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type Relay struct {
	client               client.Client
	taskQueue            string
	workflowName         string
	recoveryWorkflowName string
}

// Dial connects to the namespace; the caller closes the returned client.
func Dial(address, namespace string) (client.Client, error) {
	return client.Dial(client.Options{HostPort: address, Namespace: namespace})
}

func NewRelay(c client.Client, taskQueue, workflowName, recoveryWorkflowName string) *Relay {
	return &Relay{client: c, taskQueue: taskQueue, workflowName: workflowName, recoveryWorkflowName: recoveryWorkflowName}
}

// RecoveryInput is the recovery Workflow argument: the run identity only.
type RecoveryInput struct {
	RunID string `json:"runId"`
}

// StartRecovery starts the reconciliation workflow of a recovery run with
// the run id as the Workflow ID (idempotent like Start).
func (r *Relay) StartRecovery(ctx context.Context, runID string) (string, error) {
	run, err := r.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       runID,
		TaskQueue:                r.taskQueue,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, r.recoveryWorkflowName, RecoveryInput{RunID: runID})
	if err != nil {
		return "", fmt.Errorf("start recovery %s: %w", runID, err)
	}
	return run.GetRunID(), nil
}

// StartInput is the Workflow argument: identities only. The Workflow reads
// every other fact through Control.
type StartInput struct {
	OperationID string `json:"operationId"`
	TenantID    string `json:"tenantId"`
}

// Start is idempotent by Workflow ID: a running workflow with that id is
// reused (USE_EXISTING) and a lost first response is repaired by reissuing
// the same start.
func (r *Relay) Start(ctx context.Context, operationID, tenantID string) (string, error) {
	run, err := r.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       operationID,
		TaskQueue:                r.taskQueue,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, r.workflowName, StartInput{OperationID: operationID, TenantID: tenantID})
	if err != nil {
		return "", fmt.Errorf("start %s: %w", operationID, err)
	}
	return run.GetRunID(), nil
}

// Cancel requests workflow cancellation; a workflow that already closed or
// never started is treated as settled.
func (r *Relay) Cancel(ctx context.Context, operationID string) error {
	err := r.client.CancelWorkflow(ctx, operationID, "")
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil
	}
	return err
}
