package localcheck

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-control/internal/logging"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// TemporalOptions requires verified mTLS and permits lazy connection so a
// Temporal outage does not prevent durable, independently confirmed intake.
func TemporalOptions(getenv func(string) string) (client.Options, error) {
	settings := map[string]string{}
	for _, key := range []string{"ENDPOINT", "NAMESPACE", "TLS_SERVER_NAME", "TLS_CA", "TLS_CERT", "TLS_KEY"} {
		settings[key] = getenv("ANVILKIT_CONTROL_TEMPORAL_" + key)
		if settings[key] == "" {
			return client.Options{}, errors.New("incomplete local Temporal configuration")
		}
	}
	ca, err := os.ReadFile(settings["TLS_CA"])
	if err != nil {
		return client.Options{}, errors.New("Temporal CA is unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return client.Options{}, errors.New("invalid Temporal CA")
	}
	certificate, err := tls.LoadX509KeyPair(settings["TLS_CERT"], settings["TLS_KEY"])
	if err != nil {
		return client.Options{}, errors.New("invalid Temporal credential")
	}
	return client.Options{HostPort: settings["ENDPOINT"], Namespace: settings["NAMESPACE"], ConnectionOptions: client.ConnectionOptions{TLS: &tls.Config{MinVersion: tls.VersionTLS13, ServerName: settings["TLS_SERVER_NAME"], RootCAs: roots, Certificates: []tls.Certificate{certificate}}}}, nil
}

func isNotFound(err error) bool { var target *serviceerror.NotFound; return errors.As(err, &target) }
func isAlreadyStarted(err error) bool {
	var target *serviceerror.WorkflowExecutionAlreadyStarted
	return errors.As(err, &target)
}

func (s *Service) checkRetention(ctx context.Context, id string) error {
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response, err := s.temporal.WorkflowService().DescribeNamespace(logging.OperationCall(call, id), &workflowservice.DescribeNamespaceRequest{Namespace: s.namespace})
	if err != nil {
		return err
	}
	if response.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration() < contracts.LocalCheckHistoryRetentionHours*time.Hour {
		return errors.New("insufficient local history retention")
	}
	return nil
}

func (s *Service) start(ctx context.Context, c storage.LocalCheck) error {
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.temporal.ExecuteWorkflow(logging.OperationCall(call, c.ID), client.StartWorkflowOptions{ID: c.WorkflowID, TaskQueue: contracts.LocalCheckTaskQueue,
		WorkflowExecutionTimeout: contracts.LocalCheckWorkflowExecutionTimeoutSeconds * time.Second,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL, WorkflowExecutionErrorWhenAlreadyStarted: true}, contracts.LocalCheckWorkflowType, c.Input)
	return err
}

func (s *Service) observe(ctx context.Context, c storage.LocalCheck) error {
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	runID := c.RunID
	if runID == "" {
		description, err := s.temporal.DescribeWorkflowExecution(logging.OperationCall(call, c.ID), c.WorkflowID, "")
		if err != nil {
			return err
		}
		runID = description.GetWorkflowExecutionInfo().GetExecution().GetRunId()
		if description.GetWorkflowExecutionInfo().GetExecution().GetWorkflowId() != c.WorkflowID || runID == "" {
			return storage.ErrConflict
		}
	}
	iterator := s.temporal.GetWorkflowHistory(logging.OperationCall(call, c.ID), c.WorkflowID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var first, last *historypb.HistoryEvent
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return err
		}
		if first == nil {
			first = event
		}
		last = event
	}
	if first == nil || first.GetEventId() != 1 {
		return storage.ErrConflict
	}
	started := first.GetWorkflowExecutionStartedEventAttributes()
	if started == nil || started.GetWorkflowType().GetName() != contracts.LocalCheckWorkflowType || started.GetTaskQueue().GetName() != contracts.LocalCheckTaskQueue || started.GetWorkflowExecutionTimeout().AsDuration() != contracts.LocalCheckWorkflowExecutionTimeoutSeconds*time.Second || started.GetRetryPolicy() != nil || started.GetCronSchedule() != "" || started.GetContinuedExecutionRunId() != "" || started.GetFirstExecutionRunId() != runID {
		return storage.ErrConflict
	}
	var input contracts.LocalCheckInputV1
	if len(started.GetInput().GetPayloads()) != 1 || started.GetParentWorkflowExecution() != nil {
		return storage.ErrConflict
	}
	if converter.GetDefaultDataConverter().FromPayloads(started.GetInput(), &input) != nil || input != c.Input {
		return storage.ErrConflict
	}
	if c.RunID == "" {
		if err := s.store.EstablishLocalRun(ctx, c, runID, input); err != nil {
			return err
		}
		c.RunID = runID
	}
	evidence := storage.LocalTerminal{WorkflowID: c.WorkflowID, RunID: runID, Input: input, EventID: last.GetEventId(), At: last.GetEventTime().AsTime()}
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
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		return storage.ErrConflict
	default:
		if c.CancelRequested {
			if err := s.temporal.CancelWorkflow(logging.OperationCall(call, c.ID), c.WorkflowID, runID); err != nil {
				return err
			}
		}
		return s.store.ObserveLocalRunning(ctx, c)
	}
	return s.store.AcceptLocalTerminal(ctx, evidence)
}
