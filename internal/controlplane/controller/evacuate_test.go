package controller

import "testing"

func TestEvacuationOperationIDSuffixIsSafeForShortExecution(t *testing.T) {
	for _, exec := range []string{"", "x", "exec-1", "12345678", "123456789"} {
		got := evacuationDeleteOperationID("machine", exec)
		if len(got) < len("op-evac-machine-") {
			t.Fatalf("execution %q produced malformed operation ID %q", exec, got)
		}
	}
	if got := evacuationDeleteOperationID("machine", "123456789"); got != "op-evac-machine-23456789" {
		t.Fatalf("long execution suffix = %q", got)
	}
}
