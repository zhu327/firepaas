package main

import (
	"strings"
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

func TestOperationJSONRedactsUnstructuredError(t *testing.T) {
	out := opJSON(
		store.OperationTrace{Operation: store.Operation{Error: "GET https://source.internal/path?token=secret failed"}},
	)
	got, _ := out["error"].(string)
	if strings.Contains(got, "source.internal") || strings.Contains(got, "secret") {
		t.Fatalf("operation error leaked: %q", got)
	}
}

func TestWaitTerminalStateSets(t *testing.T) {
	for _, status := range []string{"SUCCEEDED", "FAILED", "CANCELLED", "SUPERSEDED", "TIMED_OUT"} {
		if !operationTerminal(status) {
			t.Errorf("operation status %s must be terminal", status)
		}
	}
	if operationTerminal("CLAIMED") {
		t.Fatal("CLAIMED must not be terminal")
	}

	for _, status := range []string{"COMPLETE", "FAILED", "CANCELLED", "SUPERSEDED"} {
		if !rolloutTerminal(status) {
			t.Errorf("rollout status %s must be terminal", status)
		}
	}
	if rolloutTerminal("CUTOVER") {
		t.Fatal("CUTOVER must not be terminal")
	}
}
