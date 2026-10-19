package grpc

import "github.com/ancyloce/anvilkit-agent-control/internal/transport/identity"

// The workloads that may call Control (architecture.md communication
// matrix; docs/plans/0001 P0.1 allowlist). The namespace is part of the
// identity: a ServiceAccount of the same name elsewhere is a different
// principal.
var (
	api        = identity.Workload{Namespace: "anvilkit-apps", ServiceAccount: "anvilkit-agent-api"}
	workflow   = identity.Workload{Namespace: "anvilkit-apps", ServiceAccount: "anvilkit-agent-workflow"}
	modelProxy = identity.Workload{Namespace: "anvilkit-apps", ServiceAccount: "anvilkit-agent-model-proxy"}
	mcp        = identity.Workload{Namespace: "anvilkit-apps", ServiceAccount: "anvilkit-agent-mcp"}
	knowledge  = identity.Workload{Namespace: "anvilkit-apps", ServiceAccount: "anvilkit-agent-knowledge"}
	sidecar    = identity.Workload{Namespace: "anvilkit-components", ServiceAccount: "anvilkit-job-access-sidecar"}
)

// Policy is the method→principal allowlist of every RPC Control registers.
// A method without an entry is denied and fails the Authorizer's
// completeness check at startup. The entries of a service the server does
// not register (Finance or Authorization absent from a test build) are
// dropped by withoutService.
func Policy() identity.Policy {
	p := identity.Policy{}
	add := func(service string, methods []string, allowed ...identity.Workload) {
		for _, m := range methods {
			p["/anvilkit.control.v1."+service+"/"+m] = allowed
		}
	}
	add("OperationService", []string{"CreateOperation", "GetOperation", "ListOperationEvents", "StreamOperationEvents", "SubmitCommand", "GetCommand"}, api, workflow)
	add("PreparationService", []string{"GetPreparation", "GetAnswer", "GetBrief"}, api, workflow)
	add("PreparationService", []string{"SubmitAnswer"}, api)
	add("PreparationService", []string{"RecordQuestionSet", "RecordAnswerRelay", "RecordBrief"}, workflow)
	add("GenerationService", []string{"GetGeneration"}, api, workflow)
	add("GenerationService", []string{"RequestExecutionPermit", "RecordLease", "RecordFunding", "RecordCommandRelay"}, workflow)
	add("ExecutionService", []string{"OpenAttempt", "PrepareLaunch", "RegisterInstance", "ObserveInstance", "CloseAttempt", "SettleOperation"}, workflow)
	add("ExecutionService", []string{"AcceptResult", "GetAcceptedStage", "GetInstance"}, workflow, sidecar)
	add("DispatchService", []string{"AdmitModel"}, modelProxy)
	add("DispatchService", []string{"AdmitTool"}, mcp)
	add("DispatchService", []string{"ObserveDispatch", "ConfirmNotSent"}, modelProxy, mcp)
	add("DispatchService", []string{"GetDispatch"}, modelProxy, mcp, knowledge)
	add("GrantPolicyService", []string{"RegisterPolicy", "BeginRevocation", "GetRevocation"}, mcp)
	add("EffectService", []string{"PrepareEffect", "ObserveEffect", "GetEffect", "QueryEffectOutcome"}, workflow)
	// The API workload may carry operator commands; whether the user behind
	// it holds the operator role is the separate business authorization
	// (the DEVELOPMENT_ONLY operator fixture until P0.3).
	// API-17 (F-P3.7-1): the operator's recovery routes. The API also reads
	// a run's findings for the operator; Control discloses a run only to an
	// operator of its scope (application.Discloses).
	add("RecoveryService", []string{"BeginRecovery", "GetRecovery", "DisposeObligation"}, api)
	add("RecoveryService", []string{"ListFindings"}, api, workflow)
	add("RecoveryService", []string{"EnumerateInventory", "ReconcileFinding", "RecordLaunchOutcome", "EvaluateRecovery"}, workflow)
	add("ArtifactService", []string{"BeginTransfer", "FinalizeTransfer", "GetTransfer", "ResolveHandle", "ReadArtifact"}, api, workflow, sidecar)
	add("PreviewService", []string{"RecordPreview"}, workflow)
	add("PreviewService", []string{"GetPreview", "GetSource"}, api, workflow)
	add("ReleaseService", []string{"RecordRelease"}, workflow)
	add("ReleaseService", []string{"GetRelease"}, api, workflow)
	// P2.1: operator administration through the API workload (Finance
	// decides on the verified roles); commercial authorization for the
	// owners that disclose or execute on a user's behalf.
	add("PriceAdminService", []string{"PublishPriceRevision", "WithdrawPriceRevision", "GetPriceRevision"}, api)
	add("BudgetPoolService", []string{"CreatePool", "AdjustPool", "GetPool", "SetCostCap", "GetCostCap"}, api)
	add("AuthorizationService", []string{"CheckDisclosure"}, knowledge)
	add("AuthorizationService", []string{"CheckExecution"}, mcp)
	for _, m := range []string{"Check", "Watch", "List"} {
		p["/grpc.health.v1.Health/"+m] = []identity.Workload{identity.AnyAuthenticated}
	}
	return p
}
