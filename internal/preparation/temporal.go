package preparation

import (
	"context"
	"errors"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-control/internal/logging"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

func isNotFound(err error) bool { var target *serviceerror.NotFound; return errors.As(err, &target) }
func isAlreadyStarted(err error) bool {
	var target *serviceerror.WorkflowExecutionAlreadyStarted
	return errors.As(err, &target)
}

// definitiveUpdateFailure reports an Update outcome the Workflow or the server
// decided (a rejected or failed update, or an execution that has ended), as
// opposed to a transport failure that leaves delivery unknown.
func definitiveUpdateFailure(err error) bool {
	var application *temporal.ApplicationError
	var notFound *serviceerror.NotFound
	var precondition *serviceerror.FailedPrecondition
	return errors.As(err, &application) || errors.As(err, &notFound) || errors.As(err, &precondition)
}

func describeNamespace(namespace string) *workflowservice.DescribeNamespaceRequest {
	return &workflowservice.DescribeNamespaceRequest{Namespace: namespace}
}

func (s *Service) start(ctx context.Context, p storage.Preparation) error {
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.temporal.ExecuteWorkflow(logging.OperationCall(call, p.ID), client.StartWorkflowOptions{ID: p.WorkflowID, TaskQueue: s.profile.TaskQueue,
		WorkflowExecutionTimeout: time.Duration(s.profile.WorkflowExecutionTimeoutSeconds) * time.Second,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL, WorkflowExecutionErrorWhenAlreadyStarted: true}, s.profile.WorkflowType, p.Input)
	return err
}

// observe establishes the original Run from its start event, then follows the
// execution: a running one keeps the projection healthy (and receives the
// cancel request when one is fenced), a closed one yields the terminal fact.
func (s *Service) observe(ctx context.Context, p storage.Preparation) error {
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	description, err := s.temporal.DescribeWorkflowExecution(logging.OperationCall(call, p.ID), p.WorkflowID, p.RunID)
	if err != nil {
		return err
	}
	info := description.GetWorkflowExecutionInfo()
	runID := info.GetExecution().GetRunId()
	if info.GetExecution().GetWorkflowId() != p.WorkflowID || runID == "" || (p.RunID != "" && runID != p.RunID) {
		return storage.ErrConflict
	}
	if p.RunID == "" {
		iterator := s.temporal.GetWorkflowHistory(logging.OperationCall(call, p.ID), p.WorkflowID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		if !iterator.HasNext() {
			return storage.ErrConflict
		}
		first, err := iterator.Next()
		if err != nil {
			return err
		}
		started := first.GetWorkflowExecutionStartedEventAttributes()
		if first.GetEventId() != 1 || started == nil || started.GetWorkflowType().GetName() != s.profile.WorkflowType || started.GetTaskQueue().GetName() != s.profile.TaskQueue ||
			started.GetWorkflowExecutionTimeout().AsDuration() != time.Duration(s.profile.WorkflowExecutionTimeoutSeconds)*time.Second || started.GetRetryPolicy() != nil || started.GetCronSchedule() != "" ||
			started.GetContinuedExecutionRunId() != "" || started.GetFirstExecutionRunId() != runID || started.GetParentWorkflowExecution() != nil || len(started.GetInput().GetPayloads()) != 1 {
			return storage.ErrConflict
		}
		var input contracts.PreparationWorkflowInputV1
		if converter.GetDefaultDataConverter().FromPayloads(started.GetInput(), &input) != nil || input != p.Input {
			return storage.ErrConflict
		}
		if err := s.store.EstablishPreparationRun(ctx, p, runID, input); err != nil {
			return err
		}
		p.RunID = runID
	}
	if info.GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		if p.CancelRequested {
			if err := s.temporal.CancelWorkflow(logging.OperationCall(call, p.ID), p.WorkflowID, runID); err != nil && !isNotFound(err) {
				return err
			}
		}
		return s.store.ObservePreparationRunning(ctx, p)
	}
	iterator := s.temporal.GetWorkflowHistory(logging.OperationCall(call, p.ID), p.WorkflowID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
	if !iterator.HasNext() {
		return storage.ErrConflict
	}
	last, err := iterator.Next()
	if err != nil {
		return err
	}
	evidence := storage.PreparationTerminal{WorkflowID: p.WorkflowID, RunID: runID, Input: p.Input, EventID: last.GetEventId(), At: last.GetEventTime().AsTime()}
	switch last.GetEventType() {
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		evidence.Type = "completed"
		payloads := last.GetWorkflowExecutionCompletedEventAttributes().GetResult().GetPayloads()
		if len(payloads) == 1 && string(payloads[0].GetMetadata()["encoding"]) == "json/plain" {
			evidence.Result = payloads[0].GetData()
		}
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		evidence.Type = "failed"
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		evidence.Type = "timed_out"
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		evidence.Type = "canceled"
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		evidence.Type = "terminated"
	default:
		return storage.ErrConflict
	}
	return s.store.AcceptPreparationTerminal(ctx, evidence)
}
