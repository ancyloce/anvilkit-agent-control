package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Artifacts implements the scoped artifact transfer of DD-02 §6 over the
// eight artifact classes of the contract: Begin binds scope, class,
// expected digest and size and deadline and hands out an opaque handle
// (and, to the trusted caller, the upload capability); Resolve turns a
// candidate's handle into the capability for the current physical
// instance of the transfer's attempt; Finalize reads the exact object
// version the uploader named, outside every lock, verifies its bytes,
// size and digest (and the containment of an archive), then revalidates
// the scope, epochs and deadline under the transfer lock and commits; Get
// reads inside the caller's scope. Every object key stays in this service
// and its store; callers and logs see handles and transfer ids only.
type Artifacts struct {
	store   Store
	objects ArtifactStore
	limits  ArtifactLimits
	clock   domain.Clock
	log     *slog.Logger
}

// ArtifactLimits are the reviewed bounds of the artifact service.
type ArtifactLimits struct {
	MaxObjectBytes int64
	MaxWindow      time.Duration
	CapabilityTTL  time.Duration
	Archive        domain.ArchiveLimits
}

// NewArtifacts builds the service; a nil objects store leaves every
// transfer call answering domain.ErrUnavailable (the store is a
// deployment input).
func NewArtifacts(store Store, objects ArtifactStore, limits ArtifactLimits, clock domain.Clock, log *slog.Logger) *Artifacts {
	if limits.MaxObjectBytes <= 0 {
		limits.MaxObjectBytes = 64 << 20
	}
	if limits.MaxWindow <= 0 {
		limits.MaxWindow = 24 * time.Hour
	}
	if limits.CapabilityTTL <= 0 {
		limits.CapabilityTTL = 15 * time.Minute
	}
	if limits.Archive.MaxEntries <= 0 {
		limits.Archive.MaxEntries = 10000
	}
	if limits.Archive.MaxUncompressed <= 0 {
		limits.Archive.MaxUncompressed = 4 * limits.MaxObjectBytes
	}
	return &Artifacts{store: store, objects: objects, limits: limits, clock: clock, log: log}
}

func (s *Artifacts) available() error {
	if s.objects == nil {
		return fmt.Errorf("%w: no artifact store is configured", domain.ErrUnavailable)
	}
	return nil
}

// capability issues the upload capability of a begun transfer, bounded by
// the transfer deadline and the reviewed TTL; nothing is persisted and no
// request is made.
func (s *Artifacts) capability(ctx context.Context, t *domain.Transfer) (*UploadCapability, error) {
	if t.State != domain.TransferBegun {
		return nil, nil
	}
	now := s.clock.Now()
	expires := now.Add(s.limits.CapabilityTTL)
	if expires.After(t.Deadline) {
		expires = t.Deadline
	}
	if !expires.After(now) {
		return nil, nil
	}
	cap, err := s.objects.UploadCapability(ctx, t.ObjectKey, t.MediaType, t.ExpectedSize, expires)
	if err != nil {
		return nil, fmt.Errorf("%w: upload capability for transfer %s: %v", domain.ErrUnavailable, t.ID, err)
	}
	return &cap, nil
}

// Begin records a transfer under the caller's verified scope. The same
// command returns the original transfer; a changed request under the same
// command conflicts. A bound transfer is checked against its operation
// and attempt under their locks (ranks 2 and 3) with the clock read after
// them; the capability is issued afterwards, outside the transaction. A
// reentered begin receives the capability again only after the same
// checks pass for the persisted transfer as of now: after a cancel, a
// moved epoch, a closed attempt or a passed deadline the original
// transfer state is answered without fresh upload authority.
func (s *Artifacts) Begin(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, req domain.TransferRequest) (t *domain.Transfer, existing bool, cap *UploadCapability, err error) {
	if err := s.available(); err != nil {
		return nil, false, nil, err
	}
	if scope.TenantID != cmd.TenantID {
		return nil, false, nil, fmt.Errorf("%w: command tenant differs from scope", domain.ErrInvalid)
	}
	issue := false
	for attempt := 0; attempt < 2; attempt++ {
		err = s.store.Tx(ctx, func(r Repo) error {
			found, err := r.GetTransferByCommand(ctx, cmd.TenantID, cmd.CommandID)
			switch {
			case err == nil:
				if err := found.Binds(scope, req, cmd.RequestDigest); err != nil {
					return err
				}
				t, existing = found, true
				issue, err = s.reissuable(ctx, r, found)
				return err
			case !errors.Is(err, domain.ErrNotFound):
				return err
			}
			c := domain.TransferContext{Limits: domain.TransferLimits{MaxObjectBytes: s.limits.MaxObjectBytes, MaxWindow: s.limits.MaxWindow}}
			if req.OperationID != "" {
				op, err := r.LockOperation(ctx, req.OperationID)
				if err != nil {
					return err
				}
				if op.TenantID != scope.TenantID {
					return domain.ErrNotFound
				}
				c.Operation = op
				if req.AttemptID != "" {
					at, err := r.LockAttempt(ctx, req.AttemptID)
					if err != nil {
						return err
					}
					c.Attempt = at
				}
			}
			c.Now = s.clock.Now()
			fresh, err := domain.NewTransfer(cmd, scope, req, c)
			if err != nil {
				return err
			}
			if err := r.InsertTransfer(ctx, fresh); err != nil {
				return err
			}
			t, existing, issue = fresh, false, true
			return nil
		})
		if errors.Is(err, ErrDuplicateKey) && attempt == 0 {
			continue // the same command raced on another replica; reload it
		}
		break
	}
	if err != nil {
		return nil, false, nil, err
	}
	if !issue {
		return t, existing, nil, nil
	}
	cap, err = s.capability(ctx, t)
	if err != nil {
		return nil, false, nil, err
	}
	return t, existing, cap, nil
}

// reissuable rechecks, inside the begin transaction and under the
// operation (rank 2) and attempt (rank 3) locks of a bound transfer, that
// the persisted transfer may still be uploaded: the scope, execution
// state, epochs and deadline are the ones a fresh begin would be admitted
// under. A transfer that may not answers the original state with no
// capability.
func (s *Artifacts) reissuable(ctx context.Context, r Repo, t *domain.Transfer) (bool, error) {
	if t.State != domain.TransferBegun {
		return false, nil
	}
	c := domain.ScopeContext{}
	if t.OperationID != "" {
		op, err := r.LockOperation(ctx, t.OperationID)
		if err != nil {
			return false, err
		}
		c.Operation = op
		if t.AttemptID != "" {
			if c.Attempt, err = r.LockAttempt(ctx, t.AttemptID); err != nil {
				return false, err
			}
		}
	}
	c.Now = s.clock.Now()
	if err := t.Reissuable(c); err != nil {
		s.log.Info("upload capability not reissued", "transferId", t.ID, "reason", err.Error())
		return false, nil
	}
	return true, nil
}

// Resolve turns a handle into the upload capability for the physical
// instance that presents it: only the current instance of the transfer's
// attempt, under the current epochs and inside the deadline, receives it.
// The read is scoped by the instance's own binding, never by a tenant the
// caller claims.
func (s *Artifacts) Resolve(ctx context.Context, handle, instanceID string) (*domain.Transfer, *UploadCapability, error) {
	if err := s.available(); err != nil {
		return nil, nil, err
	}
	var t *domain.Transfer
	var c domain.ScopeContext
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		if t, err = r.GetTransferByHandle(ctx, handle); err != nil {
			return err
		}
		inst, err := r.LockInstance(ctx, instanceID) // a read on the pool connection; no transaction, no lock held
		if err != nil {
			return err
		}
		c.Instance = inst
		if t.OperationID != "" {
			if c.Operation, err = r.GetOperationScoped(ctx, t.OperationID, t.TenantID); err != nil {
				return err
			}
		}
		if t.AttemptID != "" {
			if c.Attempt, err = r.GetAttempt(ctx, t.AttemptID); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	c.Now = s.clock.Now()
	if err := t.Resolvable(c); err != nil {
		return nil, nil, err
	}
	cap, err := s.capability(ctx, t)
	if err != nil {
		return nil, nil, err
	}
	if cap == nil {
		return nil, nil, fmt.Errorf("%w: transfer %s can no longer be uploaded", domain.ErrStaleExecution, t.ID)
	}
	return t, cap, nil
}

// Get reads a transfer by id or by handle inside the caller's scope.
func (s *Artifacts) Get(ctx context.Context, scope domain.Scope, transferID, handle string) (*domain.Transfer, error) {
	var t *domain.Transfer
	err := s.store.Read(ctx, func(r Repo) error {
		var err error
		switch {
		case transferID != "":
			t, err = r.GetTransfer(ctx, transferID)
		case handle != "":
			t, err = r.GetTransferByHandle(ctx, handle)
		default:
			return fmt.Errorf("%w: a transfer id or a handle is required", domain.ErrInvalid)
		}
		if err != nil {
			return err
		}
		if !t.InScope(scope.TenantID) {
			return domain.ErrNotFound
		}
		return nil
	})
	return t, err
}

// Finalize verifies and commits an upload.
//
//  1. The transfer is read inside the caller's tenant (by id or handle); a
//     finalized transfer answers the original outcome to the same command
//     and version and conflicts otherwise; a rejected or expired one is
//     stale; a begun one past its deadline is expired here.
//  2. Outside every lock the named object version is read from the store
//     and its size and digest established from the bytes themselves; a
//     source artifact, and any bytes carrying an archive signature, are
//     inspected for containment from the bytes, never from the declared
//     media type. A version that does not exist leaves the transfer begun
//     (the upload is incomplete), any verified mismatch rejects it
//     permanently.
//  3. Under the transfer lock (and the operation and attempt locks of a
//     bound transfer, in rank order) the scope, epochs and deadline are
//     revalidated as of now and the verified facts are committed. A stale
//     scope rejects; a deadline that passed meanwhile expires; nothing
//     external happens under the locks.
func (s *Artifacts) Finalize(ctx context.Context, cmd domain.CommandIdentity, transferID, handle, objectVersion, instanceID string) (t *domain.Transfer, existing bool, err error) {
	if err := s.available(); err != nil {
		return nil, false, err
	}
	if objectVersion == "" {
		return nil, false, fmt.Errorf("%w: the object version is required", domain.ErrInvalid)
	}
	// Step 1: the transfer and its reentry, read inside the tenant.
	var peek *domain.Transfer
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		switch {
		case transferID != "":
			peek, err = r.GetTransfer(ctx, transferID)
		case handle != "":
			peek, err = r.GetTransferByHandle(ctx, handle)
		default:
			return fmt.Errorf("%w: a transfer id or a handle is required", domain.ErrInvalid)
		}
		if err != nil {
			return err
		}
		if !peek.InScope(cmd.TenantID) {
			return domain.ErrNotFound
		}
		return nil
	}); err != nil {
		return nil, false, err
	}
	switch peek.State {
	case domain.TransferFinalized:
		if err := peek.Reenters(cmd, objectVersion); err != nil {
			return nil, false, err
		}
		return peek, true, nil
	case domain.TransferBegun:
	default:
		return nil, false, fmt.Errorf("%w: transfer %s is %s (%s)", domain.ErrStaleExecution, peek.ID, peek.State, peek.ReasonCode)
	}
	if instanceID != "" && peek.AttemptID != "" {
		// A finalizing instance must be the current owner of the attempt.
		if err := s.store.Read(ctx, func(r Repo) error {
			inst, err := r.LockInstance(ctx, instanceID)
			if err != nil {
				return err
			}
			if inst.AttemptID != peek.AttemptID || !inst.Current {
				return fmt.Errorf("%w: instance %s is not the current owner of attempt %s", domain.ErrStaleExecution, instanceID, peek.AttemptID)
			}
			return nil
		}); err != nil {
			return nil, false, err
		}
	}
	if !s.clock.Now().Before(peek.Deadline) {
		return s.settle(ctx, cmd, peek.ID, objectVersion, nil, fmt.Errorf("%w: transfer %s deadline %s passed", domain.ErrStaleExecution, peek.ID, peek.Deadline.UTC().Format(time.RFC3339)), domain.ReasonDeadlineExceeded)
	}

	// Step 2: the object, outside every lock.
	body, facts, err := s.objects.Read(ctx, peek.ObjectKey, objectVersion, peek.ExpectedSize)
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return nil, false, fmt.Errorf("%w: transfer %s: object version %s is not present; the upload is not complete", domain.ErrInvalid, peek.ID, objectVersion)
	case errors.Is(err, domain.ErrInvalid):
		// The object is larger than declared: a verified mismatch.
		return s.settle(ctx, cmd, peek.ID, objectVersion, nil, err, domain.ReasonSizeMismatch)
	case err != nil:
		return nil, false, fmt.Errorf("%w: reading transfer %s: %v", domain.ErrUnavailable, peek.ID, err)
	}
	var verification error
	reason := ""
	if facts.Size != peek.ExpectedSize || facts.Digest != peek.ExpectedDigest {
		verification = nil // Finalize itself classifies size and digest mismatches
	} else if err := domain.CheckArchive(peek.Class, peek.MediaType, body, s.limits.Archive); err != nil {
		verification, reason = err, archiveReason(err)
	}

	// Step 3: commit under the locks.
	return s.settle(ctx, cmd, peek.ID, objectVersion, &facts, verification, reason)
}

// settle commits the outcome of a finalize under the transfer lock (rank
// 6) after the operation (2) and attempt (3) locks of a bound transfer,
// with the clock read after them. verification, when set, is a failure
// established outside the locks that rejects the transfer with reason.
func (s *Artifacts) settle(ctx context.Context, cmd domain.CommandIdentity, transferID, objectVersion string, facts *domain.ObjectFacts, verification error, reason string) (t *domain.Transfer, existing bool, err error) {
	var outcome error
	err = s.store.Tx(ctx, func(r Repo) error {
		peek, err := r.GetTransfer(ctx, transferID)
		if err != nil {
			return err
		}
		c := domain.ScopeContext{}
		if peek.OperationID != "" {
			if c.Operation, err = r.LockOperation(ctx, peek.OperationID); err != nil {
				return err
			}
			if peek.AttemptID != "" {
				if c.Attempt, err = r.LockAttempt(ctx, peek.AttemptID); err != nil {
					return err
				}
			}
		}
		locked, err := r.LockTransfer(ctx, transferID)
		if err != nil {
			return err
		}
		t = locked
		c.Now = s.clock.Now()
		if locked.State == domain.TransferFinalized {
			existing = true
			outcome = locked.Reenters(cmd, objectVersion)
			return nil
		}
		if err := locked.Finalizable(c); err != nil {
			outcome = err
			return r.UpdateTransfer(ctx, locked) // an expiry or a stale scope is recorded
		}
		if verification != nil {
			locked.Reject(reason, c.Now)
			outcome = verification
			return r.UpdateTransfer(ctx, locked)
		}
		if facts == nil {
			outcome = fmt.Errorf("%w: transfer %s has nothing verified to commit", domain.ErrInvalid, locked.ID)
			return nil
		}
		if err := locked.Finalize(cmd, *facts, c.Now); err != nil {
			outcome = err
		}
		return r.UpdateTransfer(ctx, locked)
	})
	if err != nil {
		return nil, false, err
	}
	if outcome != nil {
		return nil, false, outcome
	}
	return t, existing, nil
}

func archiveReason(err error) string {
	msg := err.Error()
	for _, code := range []string{domain.ReasonPathEscape, domain.ReasonArchiveInvalid} {
		if len(msg) > 0 && containsWord(msg, code) {
			return code
		}
	}
	return domain.ReasonArchiveInvalid
}

func containsWord(s, w string) bool {
	return len(w) > 0 && len(s) >= len(w) && (indexOf(s, w) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// BindOutputs verifies, inside the acceptance transaction, that the
// result manifest's outputs are what the reviewed profile prescribes for
// the verdict (domain.ResultManifest.Delivers: a certified fixture result
// names the fixed result exactly once, so an empty list or a substitute
// output is refused before anything is bound) and every output present
// against the finalized transfer its handle refers to (scope, class,
// digest, size, object version, epochs), and returns the stage artifacts
// to record. An output without a handle is accepted only as the fixed
// embedded result of a qualification fixture profile
// (domain.JobProfile.EmbeddedOutput); any other output that names no
// finalized object refuses the result. It reads only; the caller holds
// the operation and attempt locks.
func BindOutputs(ctx context.Context, r Repo, op *domain.Operation, at *domain.Attempt, m domain.ResultManifest, profile domain.JobProfile) ([]domain.StageArtifact, error) {
	if err := m.Delivers(profile); err != nil {
		return nil, err
	}
	var bound []domain.StageArtifact
	seen := map[string]bool{}
	for _, out := range m.Outputs {
		if out.Handle == "" {
			// Delivers already refused a fixed result named twice.
			if err := profile.EmbeddedOutput(out, m.Verdict); err != nil {
				return nil, err
			}
			continue
		}
		if seen[out.Handle] {
			return nil, fmt.Errorf("%w: output handle %q named twice", domain.ErrInvalid, out.Handle)
		}
		seen[out.Handle] = true
		t, err := r.GetTransferByHandle(ctx, out.Handle)
		if errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("%w: output %q names no transfer of this scope", domain.ErrStaleExecution, out.Handle)
		}
		if err != nil {
			return nil, err
		}
		a, err := domain.BindArtifact(out, t, op, at)
		if err != nil {
			return nil, err
		}
		bound = append(bound, a)
	}
	return bound, nil
}
