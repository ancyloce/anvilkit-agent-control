package artifacts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
)

var (
	ErrDenied     = errors.New("PERMISSION_DENIED: artifact capability")
	ErrExpired    = errors.New("FAILED_PRECONDITION: artifact capability expired")
	ErrIncomplete = errors.New("DEPENDENCY_UNAVAILABLE: obligation inventory enumeration incomplete")
)

// transferLifetime mirrors storage's development default for a capability.
const transferLifetime = 300 * time.Second

// Inventory is the obligation inventory the adapter writes business-write
// records to before a capability exists: the intake filesystem in local work.
type Inventory interface {
	Persist(key string, body []byte) (string, error)
	List(prefix string) ([]string, error)
}

// Capability is a restricted transfer grant: one operation on one object key
// for one tenant, operation, Attempt, physical instance and execution
// generation, with an explicit byte ceiling and expiry. The secret travels to
// the sidecar only and is never persisted or logged; the store keeps its digest.
type Capability struct {
	TransferID, EffectID, Operation, Kind, RefID, ObjectKey string
	storage.TransferBinding
	ByteCeiling uint64
	ExpiresAt   time.Time
	Secret      string
	State       string
}

// IssueRequest is what a trusted caller (the execution adapter for a job's
// outputs, the sidecar exchange for inputs) asks for.
type IssueRequest struct {
	storage.TransferBinding
	CommandID, Operation, Kind, RefID, Purpose string
	DeclaredSizeBytes                          uint64
	DeclaredContentDigest                      string
}

// Adapter composes the store transactions, the object store and the obligation
// inventory. Every storage or inventory call happens between transactions,
// never under a database lock.
type Adapter struct {
	store       *storage.Store
	objects     *Store
	inventory   Inventory
	environment string
	now         func() time.Time
}

func NewAdapter(store *storage.Store, objects *Store, inventory Inventory, environment string) (*Adapter, error) {
	if store == nil || objects == nil || inventory == nil || environment == "" {
		return nil, errors.New("artifact adapter requires its store, object store, inventory and environment")
	}
	return &Adapter{store: store, objects: objects, inventory: inventory, environment: environment, now: time.Now}, nil
}

func secretDigest(secret string) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(secret)))
}

// Issue performs the four-phase write around the capability: prepare the
// effect and obligation row (transaction A), persist the inventory record
// outside every lock, recheck the binding and confirm (transaction B), and only
// then hand out the capability. Failed or unknown persistence returns an error
// and no capability; the prepared identity is reconciled by a replay of the
// same scoped command, which reaches the same object and confirms it.
func (a *Adapter) Issue(ctx context.Context, r IssueRequest) (Capability, error) {
	key, err := Key(r.TenantID, r.OperationID, r.Kind, r.RefID)
	if err != nil {
		return Capability{}, err
	}
	ceiling := ByteCeiling(r.Kind)
	if r.DeclaredSizeBytes > ceiling {
		return Capability{}, ErrTooLarge
	}
	if r.Operation == "read" {
		// A read capability names bytes that already exist under the tenant.
		if _, err := a.objects.Stat(key, ceiling); err != nil {
			return Capability{}, err
		}
	}
	secret := rand.Text() + rand.Text()
	request := storage.TransferRequest{TransferBinding: r.TransferBinding, CommandID: r.CommandID, Operation: r.Operation, Kind: r.Kind, RefID: r.RefID, Purpose: r.Purpose, ObjectKey: key,
		DeclaredSizeBytes: r.DeclaredSizeBytes, DeclaredContentDigest: r.DeclaredContentDigest, SecretDigest: secretDigest(secret)}
	prepared, err := a.store.PrepareArtifactTransfer(ctx, request, ceiling, a.environment)
	if err != nil {
		return Capability{}, err
	}
	if prepared.Existing && prepared.DispatchState != "registered" {
		// A capability that was handed out is not recoverable by design: the
		// replay reconciles the obligation and reports the transfer's state,
		// and a caller that still needs bytes to move asks for a new transfer
		// under a new command. A never-dispatched replay carries this fresh
		// secret instead.
		secret = ""
	}
	body, err := prepared.ObligationBody()
	if err != nil {
		return Capability{}, err
	}
	version, err := a.inventory.Persist(prepared.ObligationKey, body)
	if err != nil {
		return Capability{}, fmt.Errorf("obligation record not persisted; no capability issued: %w", err)
	}
	confirmed, err := a.store.ConfirmArtifactTransfer(ctx, prepared, version)
	if err != nil {
		return Capability{}, err
	}
	return Capability{TransferID: confirmed.EffectID, EffectID: confirmed.EffectID, Operation: confirmed.Operation, Kind: confirmed.Kind, RefID: confirmed.RefID, ObjectKey: confirmed.ObjectKey,
		TransferBinding: confirmed.TransferBinding, ByteCeiling: confirmed.ByteCeiling, ExpiresAt: confirmed.ExpiresAt, Secret: secret, State: confirmed.TransferState(a.now())}, nil
}

// authorize checks a presented capability against the durable transfer: the
// secret's digest, the binding, the operation, the expiry and the state. It
// reads without locking and grants nothing by itself; the accepting
// transaction rechecks the binding again.
func (a *Adapter) authorize(ctx context.Context, c Capability, operation string, states ...string) (storage.Transfer, error) {
	t, err := a.store.ReadArtifactTransfer(ctx, c.TransferID)
	if err != nil {
		return t, err
	}
	presented := secretDigest(c.Secret)
	if c.Secret == "" || subtle.ConstantTimeCompare([]byte(presented), []byte(t.SecretDigest)) != 1 || t.TransferBinding != c.TransferBinding || t.Operation != operation || t.ObjectKey != c.ObjectKey {
		return t, ErrDenied
	}
	if !slices.Contains(states, t.DispatchState) {
		return t, storage.ErrTransferState
	}
	if !a.now().Before(t.ExpiresAt) {
		return t, ErrExpired
	}
	return t, nil
}

// Upload stores the bytes a write capability admits. With a remote backend the
// sidecar writes to the capability URL directly; the local backend accepts the
// bytes here under the same checks (capability, ceiling, immutable key).
func (a *Adapter) Upload(ctx context.Context, c Capability, body []byte) (Object, error) {
	t, err := a.authorize(ctx, c, "write", "dispatched")
	if err != nil {
		return Object{}, err
	}
	return a.objects.Write(t.ObjectKey, body, t.ByteCeiling)
}

// Finalize verifies the stored bytes against the holder's claim and the
// declared content, then asks the store to accept the reference while the
// binding is still current. The returned reference is the only form in which
// the artifact is accepted anywhere else.
func (a *Adapter) Finalize(ctx context.Context, c Capability, claim Reference) (Reference, error) {
	// A finalization may be replayed against an already accepted transfer;
	// the store answers it with the same receipt or a conflict.
	t, err := a.authorize(ctx, c, "write", "dispatched", "confirmed")
	if err != nil {
		return Reference{}, err
	}
	if claim.Kind != t.Kind || claim.RefID != t.RefID {
		return Reference{}, ErrDenied
	}
	// Outside every lock: the bytes actually stored decide.
	stored, err := a.objects.Verify(t.TenantID, t.OperationID, claim)
	if err != nil {
		return Reference{}, err
	}
	if _, err := a.store.AcceptArtifactTransfer(ctx, t, storage.StoredArtifact{ContentDigest: stored.ContentDigest, ObjectVersion: stored.ObjectVersion, SizeBytes: stored.SizeBytes}, claim.SubjectDigest); err != nil {
		return Reference{}, err
	}
	return Reference{Kind: t.Kind, RefID: t.RefID, SubjectDigest: claim.SubjectDigest, ContentDigest: stored.ContentDigest, SizeBytes: stored.SizeBytes, ObjectVersion: stored.ObjectVersion}, nil
}

// Download returns the bytes of a verified reference under a read capability.
// The reference must verify against the stored bytes; altered bytes, another
// tenant's object or a version the store never issued return nothing.
func (a *Adapter) Download(ctx context.Context, c Capability, ref Reference) ([]byte, Object, error) {
	t, err := a.authorize(ctx, c, "read", "dispatched")
	if err != nil {
		return nil, Object{}, err
	}
	if ref.Kind != t.Kind || ref.RefID != t.RefID {
		return nil, Object{}, ErrDenied
	}
	return a.objects.Read(t.TenantID, t.OperationID, ref)
}

// Discovery is the reconciliation set of one class for one window: inventory
// objects whose identity has no surviving row. It is evidence that an
// obligation may exist and carries no authority to dispatch anything.
type Discovery struct {
	Class      string
	Enumerated int
	Surviving  int
	Missing    []string
}

// Discover enumerates the inventory hour by hour from `from` to `to` and
// subtracts the identities the database still holds. An enumeration that does
// not complete yields ErrIncomplete and no conclusion, never an empty set.
func (a *Adapter) Discover(ctx context.Context, class string, from, to time.Time) (Discovery, error) {
	if class != "intake" && class != "job-launch" && class != "model-dispatch" && class != "business-write" {
		return Discovery{}, storage.ErrInvalid
	}
	from, to = from.UTC().Truncate(time.Hour), to.UTC()
	if !from.Before(to) {
		return Discovery{}, storage.ErrInvalid
	}
	var keys []string
	for hour := from; hour.Before(to); hour = hour.Add(time.Hour) {
		listed, err := a.inventory.List(fmt.Sprintf("obligations/%s/%s", a.environment, hour.Format("2006/01/02/15")))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			// An hour with no directory holds no records; any other failure
			// leaves the enumeration incomplete.
			return Discovery{}, fmt.Errorf("%w: %v", ErrIncomplete, err)
		}
		keys = append(keys, listed...)
	}
	surviving, err := a.store.ObligationIdentities(ctx, class, from, to)
	if err != nil {
		return Discovery{}, err
	}
	d := Discovery{Class: class, Surviving: len(surviving)}
	for _, key := range keys {
		// obligations/<environment>/<yyyy>/<mm>/<dd>/<hh>/<tenantId>/<class>/<identity>
		parts := strings.Split(key, "/")
		if len(parts) != 9 || parts[7] != class {
			continue
		}
		d.Enumerated++
		if _, ok := surviving[parts[8]]; !ok {
			d.Missing = append(d.Missing, parts[8])
		}
	}
	return d, nil
}
