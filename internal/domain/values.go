// Package domain holds Control's pure business rules: identities, the
// operation/attempt state machines and the errors the transport maps to
// gRPC codes. It imports no Fx, gRPC or SQL driver, and its rules read no
// clock: the time is an argument (SystemClock is the Clock the application
// layer is given).
package domain

import (
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var (
	// ErrNotFound covers records outside the caller's verified scope too:
	// existence is never disclosed across tenants.
	ErrNotFound = errors.New("not found")
	// ErrIdempotencyConflict: same command identity with a different digest.
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	// ErrRevisionConflict: expected revision does not match the current one.
	// Where both revisions are known it is a *RevisionConflict.
	ErrRevisionConflict = errors.New("revision conflict")
	// ErrStaleExecution: the operation, attempt or instance is fenced, not
	// current, or terminal for the requested action.
	ErrStaleExecution = errors.New("stale execution")
	// ErrProfileUnqualified: no reviewed profile authorizes the request.
	ErrProfileUnqualified = errors.New("profile unqualified")
	// ErrInvalid: a value violates a domain rule the schema could not express.
	ErrInvalid = errors.New("invalid argument")
	// ErrEffectUncertain: an external write (inventory) may or may not have
	// happened; the caller reenters the same identity, never a new one.
	ErrEffectUncertain = errors.New("effect uncertain")
	// ErrCapacityExhausted: the reserved capacity pool is full.
	ErrCapacityExhausted = errors.New("capacity exhausted")
	// ErrIntegrity: stored state contradicts its own record (bytes that no
	// longer verify against their recorded digest, an inventory record that
	// does not decode or names another binding, a release target that is no
	// effect of the release); it is never served or reconciled as valid.
	ErrIntegrity = errors.New("integrity failure")
	// ErrBudgetExhausted: an allocation or its pool cannot cover the amount.
	ErrBudgetExhausted = errors.New("budget exhausted")
	// ErrForbidden: current authority (commercial evidence, grant policy)
	// denies the action.
	ErrForbidden = errors.New("forbidden")
	// ErrEvidenceInsufficient: the evidence offered does not settle the
	// obligation (a not-sent confirmation that is not independently
	// checkable, a definite outcome without its usage or receipt); the call
	// or effect keeps its state.
	ErrEvidenceInsufficient = errors.New("evidence insufficient")
	// ErrUnavailable: a dependency the request needs (an object store, an
	// upstream) is not configured or cannot answer; nothing was decided.
	ErrUnavailable = errors.New("dependency unavailable")
)

// RevisionConflict is an ErrRevisionConflict that knows both revisions: the
// one the caller expected and the one the record is at. The transport carries
// them to the public error's details.expectedRevision and
// details.currentRevision (RB-A23); the message is the text the plain error
// had ("revision conflict: " and the detail).
type RevisionConflict struct {
	Expected, Current uint64
	msg               string
}

// NewRevisionConflict builds the conflict; format and args are the detail
// after "revision conflict: ".
func NewRevisionConflict(expected, current uint64, format string, args ...any) error {
	return &RevisionConflict{Expected: expected, Current: current, msg: ErrRevisionConflict.Error() + ": " + fmt.Sprintf(format, args...)}
}

func (e *RevisionConflict) Error() string { return e.msg }

// Unwrap makes errors.Is(err, ErrRevisionConflict) hold.
func (e *RevisionConflict) Unwrap() error { return ErrRevisionConflict }

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Digest is "sha256:" plus 64 lowercase hexadecimal characters.
type Digest string

func ParseDigest(s string) (Digest, error) {
	if !digestPattern.MatchString(s) {
		return "", fmt.Errorf("%w: digest %q", ErrInvalid, s)
	}
	return Digest(s), nil
}

// Revision is a canonical unsigned decimal string on the wire and a uint64 here.
type Revision uint64

func ParseRevision(s string) (Revision, error) {
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return 0, fmt.Errorf("%w: revision %q", ErrInvalid, s)
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: revision %q", ErrInvalid, s)
	}
	return Revision(v), nil
}

func (r Revision) String() string { return strconv.FormatUint(uint64(r), 10) }

// CommandIdentity makes a durable command reentrant.
type CommandIdentity struct {
	TenantID      string
	CommandID     string
	ActorID       string
	RequestDigest Digest
}

// Scope is the verified caller scope, never a caller claim. Roles are the
// user's roles as the API verified them (P0.3); the transport keeps them
// only when the verified caller is the API workload.
type Scope struct {
	TenantID  string
	ProjectID string
	ActorID   string
	Roles     []string
}

// RoleOperator is the role of a user who may run recovery, dispose
// obligations, read the tenant's finance records and act on operations of
// every project of the tenant.
const RoleOperator = "operator"

// RolePlatformOperator may begin a recovery run of every tenant ("*"),
// publish prices, fund pools and set cost caps, and read the finance
// records of every tenant.
const RolePlatformOperator = "platform_operator"

// HasRole reports whether the scope holds the verified role.
func (s Scope) HasRole(role string) bool {
	for _, r := range s.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// Sees reports whether an operation is inside the scope: the same tenant
// and, for a scope that names a project, that project unless the scope
// holds the operator role. A scope without a project is the tenant-wide
// scope of a platform workload (the Workflow); the API always names the
// user's project.
func (s Scope) Sees(op *Operation) bool {
	return op.TenantID == s.TenantID && (s.ProjectID == "" || op.ProjectID == s.ProjectID || s.HasRole(RoleOperator))
}

// NewID returns a prefixed, URL-safe random identifier such as "op_7k3…".
// IDs are opaque; ordering comes from created_at and sequences.
func NewID(prefix string) string {
	var b [15]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + "_" + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:])
}

// Clock is the only time source the application layer uses so tests can pin it.
type Clock interface{ Now() time.Time }

// SystemClock truncates to microseconds, PostgreSQL's timestamptz
// precision, so a record built in memory and the same record reloaded by
// another replica serialize identically (inventory bodies must agree).
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }
