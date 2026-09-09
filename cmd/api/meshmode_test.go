package main

import (
	"testing"
)

// parseMeshMode fail-closed 语义（ADR-0040 §24，W0-2）：未知值必须报错，
// 与 agentd 同行为；拼写错误不得静默回落 disabled。
func TestParseMeshMode(t *testing.T) {
	cases := []struct {
		in      string
		enabled bool
		wantErr bool
	}{
		{"disabled", false, false},
		{"", false, false},
		{"eastwest", true, false},
		{"EASTWEST", true, false},
		{" eastwest ", true, false},
		{"full", false, true},
		{"east-west", false, true},
		{"mesh", false, true},
	}
	for _, c := range cases {
		got, err := parseMeshMode(c.in)
		if c.wantErr && err == nil {
			t.Fatalf("parseMeshMode(%q): expected error, got enabled=%v", c.in, got)
		}
		if !c.wantErr && (err != nil || got != c.enabled) {
			t.Fatalf("parseMeshMode(%q) = (%v, %v), want (%v, nil)", c.in, got, err, c.enabled)
		}
	}
}
