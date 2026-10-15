package main

import "testing"

func TestARollbackNeedsTheDestructiveFlag(t *testing.T) {
	if err := rollbackAllowed(3, false); err == nil {
		t.Fatal("-to without -destructive was accepted")
	}
	if err := rollbackAllowed(3, true); err != nil {
		t.Fatal(err)
	}
	if err := rollbackAllowed(-1, false); err != nil {
		t.Fatal("an upgrade needs no flag:", err)
	}
}
