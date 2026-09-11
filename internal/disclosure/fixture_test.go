package disclosure

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
)

func TestFixedFixtureCredentialProfile(t *testing.T) {
	until := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	tokenA, tokenB := "fixture-credential-a-0000000000000000", "fixture-credential-b-0000000000000000"
	p := map[string]any{"schemaVersion": 1, "profile": "control-03-fixture", "sourceCredentialExpiresAt": until,
		"identities":        []any{map[string]any{"token": tokenA, "actorId": "fixture-actor-a", "tenantId": "fixture-tenant-a", "expiresAt": until}, map[string]any{"token": tokenB, "actorId": "fixture-actor-b", "tenantId": "fixture-tenant-b", "expiresAt": until}},
		"rolePermissions":   []any{map[string]any{"role": "fixture-reader", "action": OperationRead, "resourceKind": "operation"}},
		"operationBindings": []any{map[string]any{"operationId": "fixture-op-a", "actorId": "fixture-actor-a", "tenantId": "fixture-tenant-a", "serviceIdentity": APIService, "serviceAuthorized": true, "validUntil": until}, map[string]any{"operationId": "fixture-op-b", "actorId": "fixture-actor-b", "tenantId": "fixture-tenant-b", "serviceIdentity": APIService, "serviceAuthorized": true, "validUntil": until}}}
	path := filepath.Join(t.TempDir(), "profile.json")
	write := func() {
		t.Helper()
		raw, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	// Configuration and credential checks do not call any database method.
	s, err := NewLocal(&storage.Store{}, path, "unused-evidence.json")
	if err != nil {
		t.Fatal(err)
	}
	a, ok := s.Authenticate("Bearer " + tokenA)
	if !ok || a.ActorID != "fixture-actor-a" || a.TenantID != "fixture-tenant-a" {
		t.Fatal("credential did not map to its exact identity")
	}
	if _, ok := s.Authenticate("Bearer fixture-unmapped-000000000000000000"); ok {
		t.Fatal("unmapped bearer authenticated")
	}
	p["identities"].([]any)[0].(map[string]any)["token"] = "fixture-replacement-0000000000000000"
	write()
	if _, ok := s.Authenticate("Bearer " + tokenA); !ok {
		t.Fatal("file replacement silently changed active credentials")
	}
	if _, ok := s.Authenticate("Bearer fixture-replacement-0000000000000000"); ok {
		t.Fatal("new credential activated without process configuration reload")
	}
	s.now = func() time.Time { return a.ExpiresAt }
	if _, ok := s.Authenticate("Bearer " + tokenA); ok {
		t.Fatal("expired credential authenticated")
	}
	p["identities"].([]any)[0].(map[string]any)["token"] = tokenB
	write()
	if _, err := NewLocal(&storage.Store{}, path, "unused"); err == nil || strings.Contains(err.Error(), tokenB) {
		t.Fatal("ambiguous credential accepted or exposed")
	}
	p["identities"].([]any)[0].(map[string]any)["token"] = tokenA
	p["unexpectedAuthority"] = true
	write()
	if _, err := NewLocal(&storage.Store{}, path, "unused"); err == nil {
		t.Fatal("unknown authority field accepted")
	}
	if _, err := NewLocal(nil, path, "unused"); err == nil {
		t.Fatal("disclosure enabled without PostgreSQL")
	}
}
