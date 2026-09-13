package transport

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	controlrpc "github.com/ancyloce/anvilkit-agent-control/internal/contracts/controlv1/controlv1connect"
	pb "github.com/ancyloce/anvilkit-agent-control/internal/contracts/definitionvalidationv1"
	rpc "github.com/ancyloce/anvilkit-agent-control/internal/contracts/definitionvalidationv1/definitionvalidationv1connect"
	"github.com/ancyloce/anvilkit-agent-control/internal/definition"
	"github.com/ancyloce/anvilkit-agent-control/internal/disclosure"
	"github.com/ancyloce/anvilkit-agent-control/internal/localcheck"
	"github.com/ancyloce/anvilkit-agent-control/internal/logging"
	"github.com/ancyloce/anvilkit-agent-control/internal/preparation"
)

type definitionHandler struct{ validator *definition.Validator }

func (h definitionHandler) ValidateDefinition(ctx context.Context, request *connect.Request[pb.ValidateDefinitionRequest]) (*connect.Response[pb.ValidateDefinitionResponse], error) {
	if err := ctx.Err(); err != nil {
		code := connect.CodeCanceled
		if errors.Is(err, context.DeadlineExceeded) {
			code = connect.CodeDeadlineExceeded
		}
		return nil, connect.NewError(code, errors.New("Validation request ended"))
	}
	response, err := h.validator.Validate(request.Msg)
	if err != nil {
		var boundary *definition.BoundaryError
		if errors.As(err, &boundary) {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(boundary.Code))
		}
		return nil, connect.NewError(connect.CodeInternal, errors.New("Validation failed"))
	}
	return connect.NewResponse(response), nil
}

// NewLocalServer keeps the validation credential scoped to definition.validate
// and the Workflow service credential (S2) to the preparation round methods.
// Optional disclosure uses its own fixed fixture identities; no mapping
// supplies production workload or delegated actor authority.
func NewLocalServer(address, credential string, output io.Writer, disclosureService *disclosure.Service, localChecks *localcheck.Service, preparations *preparation.Service, workflowCredential string) (*http.Server, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.New("Control requires a numeric loopback listen address")
	}
	ip, err := netip.ParseAddr(host)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || !ip.IsLoopback() || portErr != nil || portNumber < 0 || portNumber > 65535 {
		return nil, errors.New("Control requires a numeric loopback listen address")
	}
	if len(credential) < 32 || len(credential) > 256 || strings.ContainsAny(credential, " \t\r\n") {
		return nil, errors.New("Control requires a 32-256 character development credential without whitespace")
	}
	if preparations != nil && (len(workflowCredential) < 32 || len(workflowCredential) > 256 || strings.ContainsAny(workflowCredential, " \t\r\n") || workflowCredential == credential) {
		return nil, errors.New("preparations require a distinct 32-256 character Workflow service credential without whitespace")
	}
	validator, err := definition.New()
	if err != nil {
		return nil, err
	}
	_, handler := rpc.NewDefinitionValidationHandler(definitionHandler{validator}, connect.WithReadMaxBytes(definition.RequestMaxBytes))
	var disclosureRPC http.Handler
	if disclosureService != nil {
		_, disclosureRPC = controlrpc.NewControlServiceHandler(controlHandler{service: disclosureService, localChecks: localChecks, preparations: preparations}, connect.WithReadMaxBytes(definition.RequestMaxBytes))
	}
	errorWriter := connect.NewErrorWriter()
	logger := log.New(output, "", 0)
	instanceID := logging.InstanceID()
	expected := []byte("Bearer " + credential)
	expectedWorkflow := []byte("Bearer " + workflowCredential)
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, requestID := time.Now(), "req-"+rand.Text()
		w.Header().Set("X-Request-Id", requestID)
		code, caller := connect.Code(0), "unauthenticated"
		actorID, tenantID := "", ""
		auth := &disclosureRequest{}
		route := rpc.DefinitionValidationValidateDefinitionProcedure
		isLocalCommand := localChecks != nil && (r.URL.Path == controlrpc.ControlServiceAdmitOperationProcedure || r.URL.Path == controlrpc.ControlServiceCancelProcedure)
		// The actor-facing preparation methods share the local command lane; the
		// round methods are the Workflow's, and the read is shared by both callers.
		isActorPreparation := preparations != nil && (r.URL.Path == controlrpc.ControlServiceRecordPreparationAnswersProcedure || r.URL.Path == controlrpc.ControlServiceReadPreparationProcedure)
		isWorkflowPreparation := preparations != nil && (r.URL.Path == controlrpc.ControlServiceRecordPreparationRoundProcedure || r.URL.Path == controlrpc.ControlServiceReadPreparationProcedure)
		if r.URL.Path == controlrpc.ControlServiceGetDisclosureAuthorizationProcedure || isLocalCommand || isActorPreparation || isWorkflowPreparation {
			route = r.URL.Path
		}
		defer func() {
			if code == 0 {
				status := w.Header().Get("Grpc-Status")
				if status == "" {
					status = w.Header().Get(http.TrailerPrefix + "Grpc-Status")
				}
				if wireCode, err := strconv.Atoi(status); err == nil {
					code = connect.Code(wireCode)
				}
			}
			record := map[string]any{"schemaVersion": 1, "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "severity": "INFO", "eventName": "rpc.server.completed", "origin": "service", "service.name": "anvilkit-agent-control", "service.version": "control-03-local", "service.instance.id": instanceID, "environment": "local", "requestId": requestID, "trace.source": "new", "callee": "anvilkit-agent-control", "protocol": "grpc", "routeTemplate": route, "outcome": "ok", "durationMs": time.Since(start).Milliseconds(), "rpc.code": "ok"}
			record["caller"] = caller
			if actorID != "" {
				record["actorId"] = actorID
			}
			if tenantID != "" {
				record["tenantId"] = tenantID
			}
			if auth.operationID != "" {
				record["operationId"] = auth.operationID
			}
			if auth.denied {
				record["outcome"] = "denied"
				record["severity"], record["error.type"], record["error.code"] = "WARN", "auth", "PERMISSION_DENIED"
			}
			if code != 0 {
				record["rpc.code"], record["severity"], record["outcome"] = code.String(), "WARN", "error"
				record["error.type"], record["error.code"] = "validation", "INVALID_ARGUMENT"
				if code == connect.CodeUnauthenticated || code == connect.CodePermissionDenied {
					record["outcome"], record["error.type"], record["error.code"] = "denied", "auth", strings.ToUpper(code.String())
				}
				if code == connect.CodeResourceExhausted {
					record["outcome"], record["error.type"], record["error.code"] = "overloaded", "overloaded", "OVERLOADED"
				}
				if code == connect.CodeUnavailable {
					record["outcome"], record["error.type"], record["error.code"] = "unavailable", "unavailable", "DEPENDENCY_UNAVAILABLE"
				}
				if code == connect.CodeCanceled || code == connect.CodeDeadlineExceeded {
					record["outcome"], record["error.type"], record["error.code"] = "canceled", "canceled", "CANCELED"
					if code == connect.CodeDeadlineExceeded {
						record["outcome"], record["error.type"], record["error.code"] = "timeout", "timeout", "TRANSPORT_TIMEOUT"
					}
				}
			}
			encoded, _ := json.Marshal(record)
			logger.Print(string(encoded))
		}()
		deny := func(reason connect.Code, message string) {
			code = reason
			_ = errorWriter.Write(w, r, connect.NewError(reason, errors.New(message)))
		}
		// Each credential class is bound to the mTLS workload that presents it:
		// the API's credentials over the API identity, the Workflow credential
		// over the Worker identity. The h2c test profile carries no workload.
		workload := ""
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			switch names := r.TLS.PeerCertificates[0].DNSNames; {
			case slices.Contains(names, WorkflowWorkload):
				workload = WorkflowWorkload
			case slices.Contains(names, APIWorkload):
				workload = APIWorkload
			}
		}
		canValidate, canDisclose, canRecordRounds := false, false, false
		if len(r.Header.Values("Authorization")) == 1 {
			if workload != WorkflowWorkload {
				canValidate = subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), expected) == 1
				if disclosureService != nil {
					auth.principal, canDisclose = disclosureService.Authenticate(r.Header.Get("Authorization"))
				}
			}
			if workload != APIWorkload {
				canRecordRounds = preparations != nil && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), expectedWorkflow) == 1
			}
		}
		if !canValidate && !canDisclose && !canRecordRounds {
			deny(connect.CodeUnauthenticated, "UNAUTHENTICATED: a mapped development credential is required")
			return
		}
		caller = "anvilkit-agent-api"
		if canDisclose {
			actorID, tenantID = auth.principal.ActorID, auth.principal.TenantID
		} else if canRecordRounds {
			caller = preparation.WorkflowService
			auth.workflow = true
		} else {
			actorID = "fixture-platform-developer"
		}
		if (r.URL.Path != rpc.DefinitionValidationValidateDefinitionProcedure || !canValidate) &&
			((r.URL.Path != controlrpc.ControlServiceGetDisclosureAuthorizationProcedure && !isLocalCommand && !isActorPreparation) || !canDisclose) &&
			(!isWorkflowPreparation || !canRecordRounds) {
			deny(connect.CodePermissionDenied, "PERMISSION_DENIED: credential is not mapped to this method")
			return
		}
		contentType := strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0])
		if r.ProtoMajor != 2 || (contentType != "application/grpc" && contentType != "application/grpc+proto") {
			deny(connect.CodeInvalidArgument, "INVALID_ARGUMENT: the private method requires Protobuf gRPC over HTTP/2")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, definition.RequestMaxBytes+5)
		if route == controlrpc.ControlServiceGetDisclosureAuthorizationProcedure || isLocalCommand || isActorPreparation || isWorkflowPreparation {
			disclosureRPC.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), disclosureContextKey{}, auth)))
		} else {
			handler.ServeHTTP(w, r)
		}
	})
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	return &http.Server{Addr: address, Handler: wrapped, Protocols: protocols, ErrorLog: log.New(logging.New(output, "control-local-check"), "", 0), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}, nil
}
