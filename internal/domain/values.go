// Package domain holds Control's pure business rules: identities, the
// operation/attempt state machines and the errors the transport maps to
// gRPC codes. It imports no Fx, gRPC, SQL driver or clock source.
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
	// ErrIntegrity: stored bytes no longer verify against their recorded
	// digest; the record must not be served as the accepted result.
	ErrIntegrity = errors.New("integrity failure")
	// ErrBudgetExhausted: an allocation or its pool cannot cover the amount.
	ErrBudgetExhausted = errors.New("budget exhausted")
	// ErrForbidden: current authority (commercial evidence, grant policy)
	// denies the action.
	ErrForbidden = errors.New("forbidden")
	// ErrEvidenceInsufficient: the evidence offered for a not-sent
	// confirmation is not independently checkable; the call stays unknown.
	ErrEvidenceInsufficient = errors.New("evidence insufficient")
	// ErrUnavailable: a dependency the request needs (an object store, an
	// upstream) is not configured or cannot answer; nothing was decided.
	ErrUnavailable = errors.New("dependency unavailable")
)

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

// Scope is the verified caller scope, never a caller claim.
type Scope struct {
	TenantID  string
	ProjectID string
	ActorID   string
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
