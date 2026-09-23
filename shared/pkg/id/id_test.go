package id

import (
	"testing"
)

func TestNewUniqueNonEmpty(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		v := New()
		if v == "" {
			t.Fatal("New() returned empty id")
		}
		if seen[v] {
			t.Fatalf("collision after %d ids: %q", i, v)
		}
		seen[v] = true
		if !Valid(v) {
			t.Fatalf("New() output %q must be Valid", v)
		}
	}
}

func TestValidRejectsEmpty(t *testing.T) {
	if Valid("") {
		t.Fatal("Valid(\"\") must be false")
	}
	if !Valid("anything-nonempty") {
		t.Fatal("Valid(nonempty) must be true")
	}
}
