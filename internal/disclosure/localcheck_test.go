package disclosure

import "testing"

func TestLocalFixturePermissionsStayIndependent(t *testing.T) {
	for _, action := range []string{LocalCheckCreate, OperationRead, OperationCancel} {
		t.Run(action, func(t *testing.T) {
			s, store, actor, now := decisionFixture()
			s.profile.localBindings = map[string]binding{actor.ActorID: {ActorID: actor.ActorID, TenantID: actor.TenantID, ServiceIdentity: APIService, ServiceAuthorized: true, ValidUntil: actor.ExpiresAt}}
			resource, operation := "operation", "generated-operation"
			if action == LocalCheckCreate {
				resource, operation = "local-check", ""
			}
			s.profile.permissions = []permission{{"fixture-reader", resource, action}}
			decision, err := s.AuthorizeLocal(t.Context(), actor, operation, action)
			if err != nil || !decision.Allowed || !decision.FreshUntil.Equal(now.Add(freshness)) {
				t.Fatal("explicit local permission was not honored", err)
			}
			for _, other := range []string{LocalCheckCreate, OperationRead, OperationCancel} {
				if other == action {
					continue
				}
				id := "generated-operation"
				if other == LocalCheckCreate {
					id = ""
				}
				decision, err := s.AuthorizeLocal(t.Context(), actor, id, other)
				if err != nil || decision.Allowed {
					t.Fatal("one action implied another", other, err)
				}
			}
			if action != LocalCheckCreate {
				store.matches = false
				decision, err := s.AuthorizeLocal(t.Context(), actor, operation, action)
				if err != nil || decision.Allowed {
					t.Fatal("read/cancel bypassed persisted local ownership", err)
				}
			}
			actor.TenantID = "fixture-other-tenant"
			decision, err = s.AuthorizeLocal(t.Context(), actor, operation, action)
			if err != nil || decision.Allowed {
				t.Fatal("cross-tenant binding authorized", err)
			}
		})
	}
}

func TestExplicitDisclosureBindingOverridesLocalScope(t *testing.T) {
	s, _, actor, _ := decisionFixture()
	s.profile.localBindings = map[string]binding{actor.ActorID: {ActorID: actor.ActorID, TenantID: actor.TenantID, ServiceIdentity: APIService, ServiceAuthorized: true, ValidUntil: actor.ExpiresAt}}
	b := s.profile.bindings["op-a"]
	b.ServiceAuthorized = false
	s.profile.bindings["op-a"] = b
	decision, err := s.Authorize(t.Context(), actor, "op-a")
	if err != nil || decision.Allowed {
		t.Fatal("local fallback overrode an explicit denial", err)
	}
}
