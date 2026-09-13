// Package preparation coordinates the owner-confirmed preparation kind
// (development plan S2, 2026-09-13): durable intake of a Prompt, the fixed
// PreparationWorkflow's start and observation, the rounds its Activities
// record, the answers an actor submits, and the one tracked Temporal Update
// that delivers an accepted answer set. Persistence methods contain SQL only;
// artifact, filesystem and Temporal calls run here, between transactions.
package preparation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/artifacts"
	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-control/internal/disclosure"
	"github.com/ancyloce/anvilkit-agent-control/internal/intake"
	"github.com/ancyloce/anvilkit-agent-control/internal/logging"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	"go.temporal.io/sdk/client"
)

var ErrDenied = errors.New("PERMISSION_DENIED")

// ErrStale reports an answer set for a question set that already has one.
var ErrStale = errors.New("STALE")

// WorkflowService is the service identity the Worker presents.
const WorkflowService = "anvilkit-agent-workflow"

// AnswersDeliveryV1 is the argument of the tracked Update: the accepted
// answer-set identity, never the answer bytes. The Workflow validates it
// against its current question-set revision and reads the answers by identity.
type AnswersDeliveryV1 struct {
	SchemaVersion       int         `json:"schemaVersion"`
	OperationID         string      `json:"operationId"`
	QuestionSetRevision string      `json:"questionSetRevision"`
	AnswerSetRef        storage.Ref `json:"answerSetRef"`
}

// AnswersDeliveryResultV1 is what the Update handler returns.
type AnswersDeliveryResultV1 struct {
	Outcome string `json:"outcome"` // applied | duplicate | stale
}

type Service struct {
	store, reserved                    *storage.Store
	authorization, cancelAuthorization *disclosure.Service
	intake                             *intake.Filesystem
	objects                            *artifacts.Store
	temporal                           client.Client
	namespace, environment             string
	logger                             *logging.Logger
	profile                            contracts.PreparationProfile
	limits                             contracts.PreparationLimits
	billing                            contracts.TestBillingPolicy
}

func New(store, reserved *storage.Store, authorization, cancelAuthorization *disclosure.Service, inventory *intake.Filesystem, objects *artifacts.Store, c client.Client, namespace, environment string, logger *logging.Logger) (*Service, error) {
	if store == nil || reserved == nil || authorization == nil || cancelAuthorization == nil || inventory == nil || objects == nil || c == nil || namespace == "" || environment == "" || logger == nil {
		return nil, errors.New("preparations require all configured dependencies")
	}
	profile, limits, billing, err := contracts.Preparation()
	if err != nil {
		return nil, err
	}
	return &Service{store: store, reserved: reserved, authorization: authorization, cancelAuthorization: cancelAuthorization, intake: inventory, objects: objects, temporal: c, namespace: namespace, environment: environment, logger: logger, profile: profile, limits: limits, billing: billing}, nil
}

func (s *Service) Limits() contracts.PreparationLimits   { return s.limits }
func (s *Service) Profile() contracts.PreparationProfile { return s.profile }

func subjectDigest(operationID, kind, refID string) string {
	raw, _ := json.Marshal(map[string]string{"kind": kind, "operationId": operationID, "refId": refID})
	return artifacts.Digest(raw)
}

// storeArtifact writes one preparation document under the operation's key and
// returns the reference the records carry. Identical bytes replay as the same
// object; different bytes under the same key are a conflict.
func (s *Service) storeArtifact(tenantID, operationID, kind, refID string, body []byte) (storage.Ref, error) {
	key, err := artifacts.Key(tenantID, operationID, kind, refID)
	if err != nil {
		return storage.Ref{}, storage.ErrInvalid
	}
	object, err := s.objects.Write(key, body, artifacts.ByteCeiling(kind))
	if err != nil {
		if errors.Is(err, artifacts.ErrConflict) {
			return storage.Ref{}, storage.ErrConflict
		}
		return storage.Ref{}, err
	}
	return storage.Ref{Kind: kind, RefID: refID, SubjectDigest: subjectDigest(operationID, kind, refID), ContentDigest: object.ContentDigest, SizeBytes: artifacts.SizeString(object.SizeBytes), ObjectVersion: object.ObjectVersion}, nil
}

func (s *Service) readArtifact(tenantID, operationID string, ref storage.Ref) ([]byte, error) {
	size, err := strconv.ParseUint(ref.SizeBytes, 10, 64)
	if err != nil {
		return nil, storage.ErrInvalid
	}
	body, _, err := s.objects.Read(tenantID, operationID, artifacts.Reference{Kind: ref.Kind, RefID: ref.RefID, SubjectDigest: ref.SubjectDigest, ContentDigest: ref.ContentDigest, SizeBytes: size, ObjectVersion: ref.ObjectVersion})
	return body, err
}

// Admit is the durable intake of a preparation command: the PreparationInputV1
// bytes become the operation's input artifact, the zero-credit test quote is
// bound, the intake obligation is persisted and acknowledged.
func (s *Service) Admit(ctx context.Context, actor disclosure.Principal, commandID, intakeSource string, input []byte) (p storage.Preparation, err error) {
	defer func() {
		fields := map[string]any{"outcome": "ok"}
		if err != nil {
			fields["outcome"], fields["error.type"], fields["error.code"] = "error", "internal", "PREPARATION_ADMISSION_FAILED"
		}
		s.logger.Record("admission.decided", p.ID, fields)
	}()
	document, err := contracts.ValidatePreparationDocument(input, "PreparationInputV1", int(s.limits.CommandBodyMaxBytes))
	if err != nil {
		return p, storage.ErrInvalid
	}
	if prompt, ok := document["prompt"].(map[string]any); !ok || len([]byte(prompt["text"].(string))) > int(s.limits.PromptMaxBytes) {
		return p, storage.ErrInvalid
	}
	decision, err := s.authorization.AuthorizeLocal(ctx, actor, "", disclosure.ComponentPrepare)
	if err != nil {
		return p, err
	}
	if !decision.Allowed {
		return p, ErrDenied
	}
	scope := storage.Scope{TenantID: actor.TenantID, ActorID: actor.ActorID}
	existing, err := s.store.FindPreparationCommand(ctx, scope, commandID)
	if err == nil {
		if existing.InputRef.ContentDigest != artifacts.Digest(canonicalBytes(input)) {
			return existing, storage.ErrConflict
		}
		p = existing
	} else if errors.Is(err, storage.ErrNotFound) {
		id := storage.NewPreparationID()
		ref, err := s.storeArtifact(actor.TenantID, id, "preparation-input", "input-1", canonicalBytes(input))
		if err != nil {
			return p, err
		}
		p, err = s.store.PreparePreparation(ctx, storage.PreparationIntake{Scope: scope, OperationID: id, CommandID: commandID, IntakeSource: intakeSource, Namespace: s.namespace, Environment: s.environment, InputRef: ref, Profile: s.profile, Limits: s.limits, Billing: s.billing}, decision.FreshUntil)
		if err != nil {
			return p, err
		}
	} else {
		return p, err
	}
	if p.Recorded {
		return p, nil
	}
	return s.confirm(ctx, actor, p)
}

func canonicalBytes(raw []byte) []byte {
	// The schema validation already proved strict JSON; canonical bytes make
	// the stored object and its digest independent of client whitespace.
	canonical, err := storage.Canonical(raw)
	if err != nil {
		return raw
	}
	return canonical
}

func (s *Service) confirm(ctx context.Context, actor disclosure.Principal, p storage.Preparation) (storage.Preparation, error) {
	body, err := p.IntakeBody()
	if err != nil {
		return p, err
	}
	version, err := s.intake.Persist(p.ObjectKey, body)
	if err != nil {
		return p, err
	}
	decision, err := s.authorization.AuthorizeLocal(ctx, actor, "", disclosure.ComponentPrepare)
	if err != nil {
		return p, err
	}
	if !decision.Allowed {
		return p, ErrDenied
	}
	return s.store.ConfirmPreparationIntake(ctx, p, version, decision.FreshUntil)
}

// Answers records one answer set for the current question set and its
// delivery intent. The answer bytes become an evidence artifact first; the
// acceptance then commits under the operation lock.
func (s *Service) Answers(ctx context.Context, actor disclosure.Principal, operationID, commandID string, expectedRevision uint64, questionSetRef storage.Ref, answers []byte) (storage.AnswerOutcome, error) {
	decision, err := s.authorization.AuthorizeLocal(ctx, actor, operationID, disclosure.ComponentPrepare)
	if err != nil {
		return storage.AnswerOutcome{}, err
	}
	if !decision.Allowed {
		return storage.AnswerOutcome{}, ErrDenied
	}
	document, err := contracts.ValidatePreparationDocument(answers, "AnswerSetV1", int(s.limits.CommandBodyMaxBytes))
	if err != nil || document["operationId"] != operationID {
		return storage.AnswerOutcome{}, storage.ErrInvalid
	}
	revision, err := strconv.ParseInt(document["questionSetRevision"].(string), 10, 64)
	if err != nil {
		return storage.AnswerOutcome{}, storage.ErrInvalid
	}
	p, err := s.store.ReadPreparation(ctx, operationID)
	if err != nil {
		return storage.AnswerOutcome{}, err
	}
	if p.TenantID != actor.TenantID || p.ActorID != actor.ActorID {
		return storage.AnswerOutcome{}, storage.ErrNotFound
	}
	scope := storage.Scope{TenantID: actor.TenantID, ActorID: actor.ActorID}
	current := p.Current()
	for _, r := range p.Rounds {
		if r.AnswerCommandID == commandID {
			// Same command again: storage answers duplicate or conflict.
			return s.store.RecordPreparationAnswers(ctx, storage.AnswerRecord{Scope: scope, OperationID: operationID, CommandID: commandID, ExpectedRevision: expectedRevision, QuestionSetRef: questionSetRef, AnswerRef: *r.AnswerSetRef, QuestionSetRevision: revision}, decision.FreshUntil)
		}
	}
	if p.Resolved || p.Stage != "awaiting_input" || current == nil || current.QuestionSetRef == nil {
		return storage.AnswerOutcome{}, storage.ErrPreparationTerminal
	}
	if *current.QuestionSetRef != questionSetRef || current.QuestionSetRevision != revision {
		return storage.AnswerOutcome{}, storage.ErrRevision
	}
	if current.AnswerSetRef != nil {
		return storage.AnswerOutcome{Decision: storage.AnswerStale, Preparation: p, Round: *current}, ErrStale
	}
	// Every posed question is answered exactly once, none invented.
	questionSet, err := s.readArtifact(actor.TenantID, operationID, *current.QuestionSetRef)
	if err != nil {
		return storage.AnswerOutcome{}, err
	}
	posed, err := contracts.ValidatePreparationDocument(questionSet, "QuestionSetV1", len(questionSet))
	if err != nil {
		return storage.AnswerOutcome{}, err
	}
	expected := map[string]bool{}
	for _, q := range posed["questions"].([]any) {
		expected[q.(map[string]any)["questionId"].(string)] = false
	}
	for _, a := range document["answers"].([]any) {
		id := a.(map[string]any)["questionId"].(string)
		seen, known := expected[id]
		if !known || seen {
			return storage.AnswerOutcome{}, storage.ErrInvalid
		}
		expected[id] = true
	}
	for _, seen := range expected {
		if !seen {
			return storage.AnswerOutcome{}, storage.ErrInvalid
		}
	}
	ref, err := s.storeArtifact(actor.TenantID, operationID, "evidence", "answer-set-"+strconv.FormatInt(revision, 10), canonicalBytes(answers))
	if err != nil {
		if errors.Is(err, storage.ErrConflict) {
			// Different answers for a revision already answered by another command.
			return storage.AnswerOutcome{Decision: storage.AnswerStale, Preparation: p, Round: *current}, ErrStale
		}
		return storage.AnswerOutcome{}, err
	}
	return s.store.RecordPreparationAnswers(ctx, storage.AnswerRecord{Scope: scope, OperationID: operationID, CommandID: commandID, ExpectedRevision: expectedRevision, QuestionSetRef: questionSetRef, AnswerRef: ref, QuestionSetRevision: revision}, decision.FreshUntil)
}

// Round records the Workflow's posed question set or frozen brief.
func (s *Service) Round(ctx context.Context, operationID string, executionGeneration, round uint64, questionSet, brief []byte) (storage.RoundOutcome, error) {
	if (questionSet == nil) == (brief == nil) || round < 1 || executionGeneration < 1 {
		return storage.RoundOutcome{}, storage.ErrInvalid
	}
	p, err := s.store.ReadPreparation(ctx, operationID)
	if err != nil {
		return storage.RoundOutcome{}, err
	}
	record := storage.RoundRecord{OperationID: operationID, ExecutionGeneration: int64(executionGeneration), Ordinal: int64(round)}
	if questionSet != nil {
		document, err := contracts.ValidatePreparationDocument(questionSet, "QuestionSetV1", int(s.limits.CommandBodyMaxBytes))
		if err != nil || document["operationId"] != operationID || document["round"] != strconv.FormatUint(round, 10) || len(document["questions"].([]any)) > int(s.limits.MaxQuestionsPerRound) {
			return storage.RoundOutcome{}, storage.ErrInvalid
		}
		revision, err := strconv.ParseInt(document["questionSetRevision"].(string), 10, 64)
		if err != nil || revision < 1 {
			return storage.RoundOutcome{}, storage.ErrInvalid
		}
		ref, err := s.storeArtifact(p.TenantID, operationID, "evidence", "question-set-"+strconv.FormatInt(revision, 10), canonicalBytes(questionSet))
		if err != nil {
			return storage.RoundOutcome{}, err
		}
		record.QuestionSetRef, record.QuestionSetRevision = &ref, revision
	} else {
		document, err := contracts.ValidatePreparationDocument(brief, "BriefV1", int(artifacts.ByteCeiling("evidence")))
		if err != nil || document["operationId"] != operationID {
			return storage.RoundOutcome{}, storage.ErrInvalid
		}
		revision, err := strconv.ParseInt(document["briefRevision"].(string), 10, 64)
		if err != nil || revision < 1 {
			return storage.RoundOutcome{}, storage.ErrInvalid
		}
		var carried storage.Ref
		raw, _ := json.Marshal(document["inputRef"])
		if json.Unmarshal(raw, &carried) != nil || carried != p.InputRef {
			return storage.RoundOutcome{}, storage.ErrInvalid
		}
		ref, err := s.storeArtifact(p.TenantID, operationID, "evidence", "brief-"+strconv.FormatInt(revision, 10), canonicalBytes(brief))
		if err != nil {
			return storage.RoundOutcome{}, err
		}
		record.BriefRef, record.BriefRevision = &ref, revision
	}
	return s.store.RecordPreparationRound(ctx, record, s.limits)
}

// Detail is the committed preparation with the bytes of its artifacts.
type Detail struct {
	Preparation                                  storage.Preparation
	Input, QuestionSet, AcceptedAnswerSet, Brief []byte
}

// Read resolves the projection's references to their stored bytes. The caller
// has already authorized the read (the API through disclosure, the Workflow
// through its service identity).
func (s *Service) Read(ctx context.Context, operationID string) (Detail, error) {
	p, err := s.store.ReadPreparation(ctx, operationID)
	if err != nil {
		return Detail{}, err
	}
	d := Detail{Preparation: p}
	if d.Input, err = s.readArtifact(p.TenantID, p.ID, p.InputRef); err != nil {
		return Detail{}, err
	}
	var answered *storage.PreparationRound
	for i := range p.Rounds {
		r := &p.Rounds[i]
		if r.AnswerSetRef != nil {
			answered = r
		}
		if r.BriefRef != nil {
			if d.Brief, err = s.readArtifact(p.TenantID, p.ID, *r.BriefRef); err != nil {
				return Detail{}, err
			}
		}
	}
	if current := p.Current(); current != nil && current.QuestionSetRef != nil {
		if d.QuestionSet, err = s.readArtifact(p.TenantID, p.ID, *current.QuestionSetRef); err != nil {
			return Detail{}, err
		}
	}
	if answered != nil {
		if d.AcceptedAnswerSet, err = s.readArtifact(p.TenantID, p.ID, *answered.AnswerSetRef); err != nil {
			return Detail{}, err
		}
	}
	return d, nil
}

// AuthorizeRead is the actor-scoped read authorization the API's ReadPreparation carries.
func (s *Service) AuthorizeRead(ctx context.Context, actor disclosure.Principal, operationID string) error {
	decision, err := s.authorization.AuthorizeLocal(ctx, actor, operationID, disclosure.OperationRead)
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return ErrDenied
	}
	return nil
}

func (s *Service) Cancel(ctx context.Context, actor disclosure.Principal, id, commandID, reason string, revision uint64) (storage.Preparation, bool, error) {
	decision, err := s.cancelAuthorization.AuthorizeLocal(ctx, actor, id, disclosure.OperationCancel)
	if err != nil {
		return storage.Preparation{}, false, err
	}
	if !decision.Allowed {
		return storage.Preparation{}, false, ErrDenied
	}
	return s.reserved.CancelPreparation(ctx, storage.Scope{TenantID: actor.TenantID, ActorID: actor.ActorID}, id, commandID, reason, revision, decision.FreshUntil)
}

// Run is Control's relay for preparations: it starts the fixed Workflow,
// observes the original execution, ends expired waits and delivers accepted
// answer sets as tracked Updates. Temporal alone advances the business steps.
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := s.reconcileAll(ctx); err != nil && ctx.Err() == nil {
			s.logger.Record("health.transition", "", map[string]any{"outcome": "unavailable", "error.type": "unavailable", "error.code": "DEPENDENCY_UNAVAILABLE"})
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) reconcileAll(ctx context.Context) error {
	db, cancel := context.WithTimeout(ctx, 5*time.Second)
	ids, err := s.store.UnresolvedPreparations(db)
	cancel()
	if err != nil {
		return err
	}
	var failed error
	for _, id := range ids {
		if err := s.reconcile(ctx, id); err != nil && failed == nil {
			failed = err
		}
	}
	db, cancel = context.WithTimeout(ctx, 5*time.Second)
	pending, err := s.store.PendingPreparationDeliveries(db)
	cancel()
	if err != nil {
		return err
	}
	for _, delivery := range pending {
		if err := s.deliver(ctx, delivery); err != nil && failed == nil {
			failed = err
		}
	}
	return failed
}

func (s *Service) reconcile(ctx context.Context, id string) error {
	db, cancel := context.WithTimeout(ctx, 5*time.Second)
	p, err := s.store.ReadPreparation(db, id)
	cancel()
	if err != nil {
		return err
	}
	if p.Namespace != s.namespace {
		return s.store.PreparationObservationUnavailable(ctx, p, "PREPARATION_START_UNRESOLVED")
	}
	if p.StartState == "start_unattempted" && !time.Now().Before(p.QueueExpiresAt) {
		return s.store.ExpirePreparedPreparation(ctx, p.ID)
	}
	actor, known := s.authorization.LocalPrincipal(p.Scope)
	if !p.Recorded {
		if !known {
			return ErrDenied
		}
		if p, err = s.confirm(ctx, actor, p); err != nil {
			return err
		}
	}
	if p.Resolved {
		return nil
	}
	if p.Stage == "awaiting_input" {
		if expired, err := s.store.ExpireAwaitingInput(ctx, p.ID); err != nil {
			return err
		} else if expired {
			s.logger.Record("preparation.expired", p.ID, map[string]any{"outcome": "ok"})
		}
	}
	if p.StartState != "start_unattempted" {
		err = s.observe(ctx, p)
		if err == nil {
			return nil
		}
		if p.RunID != "" || p.CancelRequested || !time.Now().Before(p.QueueExpiresAt) || !isNotFound(err) {
			return s.store.PreparationObservationUnavailable(ctx, p, "PREPARATION_RESULT_UNAVAILABLE")
		}
	}
	if !known {
		return ErrDenied
	}
	decision, err := s.authorization.AuthorizeLocal(ctx, actor, "", disclosure.ComponentPrepare)
	if err != nil || !decision.Allowed {
		return s.store.PreparationObservationUnavailable(ctx, p, "PREPARATION_START_UNRESOLVED")
	}
	if err := s.checkRetention(ctx, p.ID); err != nil {
		return s.store.PreparationObservationUnavailable(ctx, p, "PREPARATION_START_UNRESOLVED")
	}
	marked, send, err := s.store.MarkPreparationStart(ctx, p, decision.FreshUntil)
	if err != nil {
		return s.store.PreparationObservationUnavailable(ctx, p, "PREPARATION_START_UNRESOLVED")
	}
	if !send {
		return nil
	}
	p = marked
	if err := s.start(ctx, p); err != nil && !isAlreadyStarted(err) {
		return s.store.PreparationObservationUnavailable(ctx, p, "PREPARATION_START_UNRESOLVED")
	}
	if err := s.observe(ctx, p); err != nil {
		return s.store.PreparationObservationUnavailable(ctx, p, "PREPARATION_RESULT_UNAVAILABLE")
	}
	return nil
}

// deliver sends the one tracked Update for an accepted answer set. The Update
// ID is the delivery intent, so a repeated delivery returns the same result;
// a definitive answer from the Workflow (applied, duplicate, stale, rejected)
// or an ended execution marks the intent delivered, and a transport failure
// leaves it pending for the next tick.
func (s *Service) deliver(ctx context.Context, d storage.PendingDelivery) error {
	p := d.Preparation
	if p.RunID == "" {
		return nil
	}
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	fields := map[string]any{"outcome": "ok", "deliveryIntentId": d.Round.DeliveryIntentID}
	handle, err := s.temporal.UpdateWorkflow(logging.OperationCall(call, p.ID), client.UpdateWorkflowOptions{WorkflowID: p.WorkflowID, RunID: p.RunID, UpdateID: d.Round.DeliveryIntentID, UpdateName: s.profile.AnswersUpdateName,
		Args:         []any{AnswersDeliveryV1{SchemaVersion: 1, OperationID: p.ID, QuestionSetRevision: strconv.FormatInt(d.Round.QuestionSetRevision, 10), AnswerSetRef: *d.Round.AnswerSetRef}},
		WaitForStage: client.WorkflowUpdateStageCompleted})
	var result AnswersDeliveryResultV1
	if err == nil {
		err = handle.Get(call, &result)
	}
	if err != nil {
		if !definitiveUpdateFailure(err) {
			fields["outcome"], fields["error.type"], fields["error.code"] = "unavailable", "unavailable", "DEPENDENCY_UNAVAILABLE"
			s.logger.Record("preparation.delivery", p.ID, fields)
			return err
		}
		fields["outcome"], fields["error.type"], fields["error.code"] = "denied", "validation", "PREPARATION_DELIVERY_REJECTED"
	} else {
		fields["deliveryOutcome"] = result.Outcome
	}
	s.logger.Record("preparation.delivery", p.ID, fields)
	return s.store.MarkPreparationDelivered(ctx, p.ID, d.Round.Ordinal, d.Round.DeliveryIntentID)
}

func (s *Service) checkRetention(ctx context.Context, id string) error {
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response, err := s.temporal.WorkflowService().DescribeNamespace(logging.OperationCall(call, id), describeNamespace(s.namespace))
	if err != nil {
		return err
	}
	if response.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration() < time.Duration(s.profile.HistoryRetentionHours)*time.Hour {
		return fmt.Errorf("insufficient preparation history retention")
	}
	return nil
}
