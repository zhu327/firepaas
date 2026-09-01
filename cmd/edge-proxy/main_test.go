package main

import "testing"

func TestParseExtraPorts(t *testing.T) {
	if got, _ := parseExtraPorts(""); len(got) != 0 {
		t.Fatalf("empty spec: %v", got)
	}
	got, err := parseExtraPorts("8081, 9000-9002")
	if err != nil || len(got) != 4 || got[0] != 8081 || got[3] != 9002 {
		t.Fatalf("parse: %v %v", got, err)
	}
	if _, err := parseExtraPorts("70000"); err == nil {
		t.Fatal("out-of-range port must be rejected")
	}
	if _, err := parseExtraPorts("9002-9000"); err == nil {
		t.Fatal("inverted range must be rejected")
	}
}

func TestListenerPorts(t *testing.T) {
	got := listenerPorts("8080", ":8443", []int{9000})
	for _, port := range []int{8080, 8443, 9000} {
		if !got[port] {
			t.Fatalf("port %d missing: %v", port, got)
		}
	}
}
