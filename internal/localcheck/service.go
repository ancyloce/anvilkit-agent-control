// Package localcheck coordinates the approved local intake and Temporal relay.
// Persistence methods contain SQL only; filesystem and Temporal calls run here.
package localcheck

import (
	"context"
	"errors"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/disclosure"
	"github.com/ancyloce/anvilkit-agent-control/internal/intake"
	"github.com/ancyloce/anvilkit-agent-control/internal/logging"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	"go.temporal.io/sdk/client"
)

var ErrDenied = errors.New("PERMISSION_DENIED")

type Service struct {
	store, reserved                    *storage.Store
	authorization, cancelAuthorization *disclosure.Service
	intake                             *intake.Filesystem
	temporal                           client.Client
	namespace, environment             string
	logger                             *logging.Logger
}

func New(store, reserved *storage.Store, authorization, cancelAuthorization *disclosure.Service, objects *intake.Filesystem, c client.Client, namespace, environment string, logger *logging.Logger) (*Service, error) {
	if store == nil || reserved == nil || authorization == nil || cancelAuthorization == nil || objects == nil || c == nil || namespace == "" || environment == "" {
		return nil, errors.New("local checks require all configured dependencies")
	}
	return &Service{store: store, reserved: reserved, authorization: authorization, cancelAuthorization: cancelAuthorization, intake: objects, temporal: c, namespace: namespace, environment: environment, logger: logger}, nil
}

func (s *Service) Admit(ctx context.Context, actor disclosure.Principal, commandID, fixtureID string) (operation storage.LocalCheck, err error) {
	defer func() {
		outcome := "ok"
		fields := map[string]any{}
		if err != nil {
			outcome = "error"
			fields["error.type"] = "internal"
			fields["error.code"] = "LOCAL_ADMISSION_FAILED"
		}
		fields["outcome"] = outcome
		s.logger.Record("admission.decided", operation.ID, fields)
	}()
	decision, err := s.authorization.AuthorizeLocal(ctx, actor, "", disclosure.LocalCheckCreate)
	if err != nil {
		return operation, err
	}
	if !decision.Allowed {
		return operation, ErrDenied
	}
	operation, err = s.store.PrepareLocalCheck(ctx, storage.Scope{TenantID: actor.TenantID, ActorID: actor.ActorID}, commandID, fixtureID, s.namespace, s.environment, decision.FreshUntil)
	if err != nil {
		return operation, err
	}
	if operation.Recorded {
		return operation, nil
	}
	return s.confirm(ctx, actor, operation)
}

func (s *Service) confirm(ctx context.Context, actor disclosure.Principal, c storage.LocalCheck) (storage.LocalCheck, error) {
	body, err := c.IntakeBody()
	if err != nil {
		return c, err
	}
	version, err := s.intake.Persist(c.ObjectKey, body)
	if err != nil {
		return c, err
	}
	decision, err := s.authorization.AuthorizeLocal(ctx, actor, "", disclosure.LocalCheckCreate)
	if err != nil {
		return c, err
	}
	if !decision.Allowed {
		return c, ErrDenied
	}
	return s.store.ConfirmLocalIntake(ctx, c, version, decision.FreshUntil)
}

func (s *Service) Cancel(ctx context.Context, actor disclosure.Principal, id, commandID, reason string, revision uint64) (storage.LocalCheck, bool, error) {
	decision, err := s.cancelAuthorization.AuthorizeLocal(ctx, actor, id, disclosure.OperationCancel)
	if err != nil {
		return storage.LocalCheck{}, false, err
	}
	if !decision.Allowed {
		return storage.LocalCheck{}, false, ErrDenied
	}
	return s.reserved.CancelLocalCheck(ctx, storage.Scope{TenantID: actor.TenantID, ActorID: actor.ActorID}, id, commandID, reason, revision, decision.FreshUntil)
}

// Run is part of Control's process. Temporal alone executes Workflow steps;
// this loop recovers intake/start delivery and observes the original outcome.
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := s.reconcile(ctx); err != nil && ctx.Err() == nil {
			s.logger.Record("health.transition", "", map[string]any{"outcome": "unavailable", "error.type": "unavailable", "error.code": "DEPENDENCY_UNAVAILABLE"})
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) reconcile(ctx context.Context) error {
	db, cancel := context.WithTimeout(ctx, 5*time.Second)
	c, err := s.store.PendingLocalCheck(db)
	cancel()
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if c.Namespace != s.namespace {
		return s.store.LocalObservationUnavailable(ctx, c, "LOCAL_START_UNRESOLVED")
	}
	if c.StartState == "start_unattempted" && !time.Now().Before(c.QueueExpiresAt) {
		return s.store.ExpirePreparedLocalCheck(ctx, c.ID)
	}
	actor, known := s.authorization.LocalPrincipal(c.Scope)
	if !c.Recorded {
		if !known {
			return ErrDenied
		}
		c, err = s.confirm(ctx, actor, c)
		if err != nil {
			return err
		}
	}
	if c.Resolved {
		return nil
	}
	// Query attempted starts first. A missing established Run is never sent
	// again, even if its history has disappeared during this test stage.
	if c.StartState != "start_unattempted" {
		err = s.observe(ctx, c)
		if err == nil {
			return nil
		}
		if c.RunID != "" || c.CancelRequested || !time.Now().Before(c.QueueExpiresAt) || !isNotFound(err) {
			return s.store.LocalObservationUnavailable(ctx, c, "LOCAL_RESULT_UNAVAILABLE")
		}
	}
	if !known {
		return ErrDenied
	}
	decision, err := s.authorization.AuthorizeLocal(ctx, actor, "", disclosure.LocalCheckCreate)
	if err != nil || !decision.Allowed {
		return s.store.LocalObservationUnavailable(ctx, c, "LOCAL_START_UNRESOLVED")
	}
	if err := s.checkRetention(ctx, c.ID); err != nil {
		return s.store.LocalObservationUnavailable(ctx, c, "LOCAL_START_UNRESOLVED")
	}
	marked, send, err := s.store.MarkLocalStart(ctx, c, decision.FreshUntil)
	if err != nil {
		return s.store.LocalObservationUnavailable(ctx, c, "LOCAL_START_UNRESOLVED")
	}
	if !send {
		return nil
	}
	c = marked
	// SDK retries and duplicate responses remain inside the original identity.
	// The return value alone is not accepted as proof of the input binding.
	if err := s.start(ctx, c); err != nil && !isAlreadyStarted(err) {
		return s.store.LocalObservationUnavailable(ctx, c, "LOCAL_START_UNRESOLVED")
	}
	if err := s.observe(ctx, c); err != nil {
		return s.store.LocalObservationUnavailable(ctx, c, "LOCAL_RESULT_UNAVAILABLE")
	}
	return nil
}
