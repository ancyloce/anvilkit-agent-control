// Package disclosure supplies CONTROL-03's bounded local authorization decisions.
// Its fixture files confer no production SSO, Pagix or execution authority.
package disclosure

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const APIService = "anvilkit-agent-api"
const OperationRead = "operation.read"
const fileMaxBytes = 65536

type Principal struct {
	ActorID, TenantID string
	ExpiresAt         time.Time
}

type credential struct {
	digest    [sha256.Size]byte
	principal Principal
}
type permission struct{ Role, ResourceKind, Action string }
type binding struct {
	OperationID, ActorID, TenantID, ServiceIdentity string
	ServiceAuthorized                               bool
	ValidUntil                                      time.Time
}
type profile struct {
	credentials               []credential
	permissions               []permission
	bindings                  map[string]binding
	localBindings             map[string]binding
	sourceCredentialExpiresAt time.Time
}
type membership struct {
	MemberRoles map[string][]string
	ValidUntil  time.Time
}
type membershipSource interface {
	Read(context.Context, string) (membership, error)
}
type fileSource struct {
	path   string
	schema *jsonschema.Schema
}
type noRemoteSchemas struct{}

func (noRemoteSchemas) Load(string) (any, error) { return nil, errors.New("schema is not retained") }

// NewLocal loads a fixed identity/permission profile. Membership is read from
// the separate evidence file only by a persisted refresh-lease holder.
func NewLocal(store *storage.Store, profilePath, evidencePath string) (*Service, error) {
	if store == nil || profilePath == "" || evidencePath == "" {
		return nil, errors.New("disclosure requires PostgreSQL and both fixture paths")
	}
	inputs, err := contracts.VerifiedInputs()
	if err != nil {
		return nil, err
	}
	var limits struct {
		Entries []struct {
			Key   string
			Value json.RawMessage
		}
	}
	if err := json.Unmarshal(inputs["pilot-limits-v1.json"], &limits); err != nil {
		return nil, err
	}
	matchedLimits := 0
	for _, limit := range limits.Entries {
		if (limit.Key == "authorization.freshnessSeconds" && string(limit.Value) == "30") || (limit.Key == "pagix.read.totalTimeoutMs" && string(limit.Value) == "10000") {
			matchedLimits++
		}
		if limit.Key == "clock.maxInterServiceErrorSeconds" && string(limit.Value) == "2" {
			matchedLimits++
		}
	}
	if matchedLimits != 3 {
		return nil, errors.New("retained disclosure timing limits disagree")
	}
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(noRemoteSchemas{})
	compiler.AssertFormat()
	for _, name := range []string{"common-v1.schema.json", "controlled-disclosure-v1.schema.json"} {
		var document map[string]any
		if err := json.Unmarshal(inputs[name], &document); err != nil {
			return nil, err
		}
		if err := compiler.AddResource(document["$id"].(string), document); err != nil {
			return nil, err
		}
	}
	profileSchema, err := compiler.Compile("urn:anvilkit:controlled-disclosure:v1#/$defs/profile")
	if err != nil {
		return nil, err
	}
	evidenceSchema, err := compiler.Compile("urn:anvilkit:controlled-disclosure:v1#/$defs/evidence")
	if err != nil {
		return nil, err
	}
	var document struct {
		SourceCredentialExpiresAt time.Time
		Identities                []struct {
			Token, ActorID, TenantID string
			ExpiresAt                time.Time
		}
		RolePermissions    []permission
		OperationBindings  []binding
		LocalCheckBindings []binding
	}
	if err := readDocument(profilePath, profileSchema, &document); err != nil {
		return nil, errors.New("invalid controlled disclosure profile")
	}
	p := profile{permissions: document.RolePermissions, bindings: make(map[string]binding), localBindings: make(map[string]binding), sourceCredentialExpiresAt: document.SourceCredentialExpiresAt}
	actors, tenants, tokens := map[string]bool{}, map[string]bool{}, map[[sha256.Size]byte]bool{}
	for _, identity := range document.Identities {
		digest := sha256.Sum256([]byte(identity.Token))
		if !strings.HasPrefix(identity.ActorID, "fixture-") || !strings.HasPrefix(identity.TenantID, "fixture-") || actors[identity.ActorID] || tenants[identity.TenantID] || tokens[digest] {
			return nil, errors.New("profile must bind two distinct synthetic identities")
		}
		actors[identity.ActorID], tenants[identity.TenantID], tokens[digest] = true, true, true
		p.credentials = append(p.credentials, credential{digest, Principal{identity.ActorID, identity.TenantID, identity.ExpiresAt}})
	}
	boundActors := map[string]bool{}
	for _, b := range document.OperationBindings {
		if _, duplicate := p.bindings[b.OperationID]; duplicate {
			return nil, errors.New("duplicate fixture operation binding")
		}
		matched := false
		for _, c := range p.credentials {
			if c.principal.ActorID == b.ActorID && c.principal.TenantID == b.TenantID {
				matched = true
			}
		}
		if !matched || boundActors[b.ActorID] {
			return nil, errors.New("operation binding has no fixture identity")
		}
		boundActors[b.ActorID] = true
		p.bindings[b.OperationID] = b
	}
	for _, b := range document.LocalCheckBindings {
		if _, duplicate := p.localBindings[b.ActorID]; duplicate {
			return nil, errors.New("duplicate local fixture binding")
		}
		matched := false
		for _, c := range p.credentials {
			if c.principal.ActorID == b.ActorID && c.principal.TenantID == b.TenantID {
				matched = true
			}
		}
		if !matched {
			return nil, errors.New("local binding has no fixture identity")
		}
		p.localBindings[b.ActorID] = b
	}
	service := newService(store, p, fileSource{evidencePath, evidenceSchema})
	service.idSchema, err = compiler.Compile("urn:anvilkit:values:v1#/$defs/id")
	return service, err
}

func (s *Service) ValidID(value string) bool {
	return s.idSchema != nil && s.idSchema.Validate(value) == nil
}

// Authenticate matches a process-configured credential, never body claims.
func (s *Service) Authenticate(header string) (Principal, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || len(token) < 32 || len(token) > 256 {
		return Principal{}, false
	}
	digest := sha256.Sum256([]byte(token))
	for _, c := range s.profile.credentials {
		if subtle.ConstantTimeCompare(digest[:], c.digest[:]) == 1 && s.now().Before(c.principal.ExpiresAt) {
			return c.principal, true
		}
	}
	return Principal{}, false
}

func readDocument(path string, schema *jsonschema.Schema, into any) error {
	f, err := os.Open(path)
	if err != nil {
		return ErrUnavailable
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return ErrUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(f, fileMaxBytes+1))
	if err != nil {
		return ErrUnavailable
	}
	document, err := contracts.DecodeObject(raw, fileMaxBytes)
	if err != nil || schema.Validate(document) != nil {
		return ErrUnavailable
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (s fileSource) Read(ctx context.Context, tenant string) (membership, error) {
	if ctx.Err() != nil {
		return membership{}, ctx.Err()
	}
	var document struct {
		Tenants []struct {
			TenantID            string
			Complete, Available bool
			ValidUntil          time.Time
			MemberRoles         map[string][]string
		}
	}
	if err := readDocument(s.path, s.schema, &document); err != nil {
		return membership{}, ErrUnavailable
	}
	seen := map[string]bool{}
	var result membership
	for _, row := range document.Tenants {
		if !strings.HasPrefix(row.TenantID, "fixture-") || seen[row.TenantID] {
			return membership{}, ErrUnavailable
		}
		seen[row.TenantID] = true
		if row.TenantID == tenant {
			if !row.Complete || !row.Available {
				return membership{}, ErrUnavailable
			}
			result = membership{row.MemberRoles, row.ValidUntil}
		}
	}
	if ctx.Err() != nil {
		return membership{}, ctx.Err()
	}
	if result.MemberRoles == nil {
		return membership{}, ErrUnavailable
	}
	return result, nil
}
