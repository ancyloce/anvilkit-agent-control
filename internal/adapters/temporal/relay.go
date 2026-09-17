// Package temporal is Control's start/cancel/update relay adapter (DD-01
// §1). It starts registered workflows by stable name with the operation id
// as the Workflow ID, so a lost start receipt is recovered by the same
// call, and it issues the tracked Updates of accepted answers and
// supported control commands under stable update identities, so a lost
// receipt is repaired by reissuing the same Update (Temporal deduplicates
// it per run) and never by a second identity.
package temporal

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Update names shared with the Workflow's registrations (workflows package
// of anvilkit-agent-workflow): the answer Update of a Preparation and the
// control-command Update of a Generation.
const (
	AnswerUpdateName  = "PreparationAnswer"
	CommandUpdateName = "ControlCommand"
)

type Relay struct {
	client               client.Client
	taskQueue            string
	workflowNames        map[domain.OperationKind]string
	recoveryWorkflowName string
}

// Dial connects to the namespace; the caller closes the returned client.
func Dial(address, namespace string) (client.Client, error) {
	return client.Dial(client.Options{HostPort: address, Namespace: namespace})
}

// NewRelay binds the registered workflow type of every operation kind this
// build starts; a kind without a registration is refused at start.
func NewRelay(c client.Client, taskQueue string, workflowNames map[domain.OperationKind]string, recoveryWorkflowName string) *Relay {
	return &Relay{client: c, taskQueue: taskQueue, workflowNames: workflowNames, recoveryWorkflowName: recoveryWorkflowName}
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
func (r *Relay) Start(ctx context.Context, operationID, tenantID string, kind domain.OperationKind) (string, error) {
	name, ok := r.workflowNames[kind]
	if !ok {
		return "", fmt.Errorf("start %s: no workflow is registered for kind %s", operationID, kind)
	}
	run, err := r.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       operationID,
		TaskQueue:                r.taskQueue,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, name, StartInput{OperationID: operationID, TenantID: tenantID})
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

// AnswerUpdate is the argument of the answer Update: the identities the
// deterministic validator checks against the open question set; the
// handler reads the accepted answer itself through an Activity.
type AnswerUpdate struct {
	AnswerID            string `json:"answerId"`
	QuestionSetID       string `json:"questionSetId"`
	QuestionSetRevision string `json:"questionSetRevision"`
	Handle              string `json:"handle"`
	Digest              string `json:"digest"`
}

// CommandUpdate is the argument of the control-command Update.
type CommandUpdate struct {
	CommandID                  string `json:"commandId"`
	Kind                       string `json:"kind"`
	ExpectedRevision           string `json:"expectedRevision"`
	TargetDefinitionActivation string `json:"targetDefinitionActivation,omitempty"`
}

// UpdateResult is what the Workflow's handlers answer.
type UpdateResult struct {
	Outcome    string `json:"outcome"`
	ReasonCode string `json:"reasonCode,omitempty"`
}

// UpdateAnswer issues the answer's Update under the answer id.
func (r *Relay) UpdateAnswer(ctx context.Context, operationID string, a *domain.Answer) (application.RelayOutcome, error) {
	return r.update(ctx, operationID, a.UpdateID, AnswerUpdateName, AnswerUpdate{
		AnswerID: a.ID, QuestionSetID: a.QuestionSetID, QuestionSetRevision: domain.Revision(a.QuestionSetRevision).String(), Handle: a.Handle, Digest: string(a.Answer.Digest),
	})
}

// UpdateCommand issues a control command's Update under the command id.
func (r *Relay) UpdateCommand(ctx context.Context, operationID string, c *domain.Command) (application.RelayOutcome, error) {
	return r.update(ctx, operationID, c.CommandID, CommandUpdateName, CommandUpdate{
		CommandID: c.CommandID, Kind: string(c.Kind), ExpectedRevision: c.ExpectedRevision.String(), TargetDefinitionActivation: c.TargetDefinitionActivation,
	})
}

// update issues one tracked Update and waits for its completion. The same
// update id reaches the same Update: a repeated call after a lost receipt
// receives the original outcome. A validator rejection is the handler's
// answer (rejected with its reason); a run that cannot take the Update
// (closed, never started) is ErrRelayClosed; anything else is a transport
// failure the relay retries.
func (r *Relay) update(ctx context.Context, workflowID, updateID, name string, arg any) (application.RelayOutcome, error) {
	handle, err := r.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: workflowID, UpdateID: updateID, UpdateName: name, Args: []any{arg}, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return application.RelayOutcome{}, classify(err)
	}
	var result UpdateResult
	if err := handle.Get(ctx, &result); err != nil {
		var app *temporal.ApplicationError
		if errors.As(err, &app) {
			// The handler or its validator refused the Update: a business
			// answer under the same identity, never something to reissue.
			return application.RelayOutcome{Outcome: domain.OutcomeRejected, ReasonCode: reason(app)}, nil
		}
		return application.RelayOutcome{}, classify(err)
	}
	outcome := domain.CommandOutcome(result.Outcome)
	switch outcome {
	case domain.OutcomeApplied, domain.OutcomeBlocked, domain.OutcomeRejected:
	default:
		return application.RelayOutcome{Outcome: domain.OutcomeRejected, ReasonCode: "INVALID_UPDATE_RESULT"}, nil
	}
	return application.RelayOutcome{Outcome: outcome, ReasonCode: result.ReasonCode}, nil
}

func reason(app *temporal.ApplicationError) string {
	if t := app.Type(); t != "" {
		return t
	}
	return "REJECTED"
}

func classify(err error) error {
	var notFound *serviceerror.NotFound
	var failedPrecondition *serviceerror.FailedPrecondition
	if errors.As(err, &notFound) || errors.As(err, &failedPrecondition) {
		return fmt.Errorf("%w: %v", application.ErrRelayClosed, err)
	}
	return err
}
