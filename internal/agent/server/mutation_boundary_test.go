package server

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/mutation"
	"github.com/zhu327/firepaas/internal/agent/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The migrated handlers must not regress to manually composing persistence and
// fencing primitives. Those calls belong to internal/agent/mutation.
func TestMutationErrorMappingPreservesFamilyCodes(t *testing.T) {
	tests := []struct {
		name     string
		mapError func(error) error
		err      error
		want     codes.Code
	}{
		{"snapshot machine missing", mapSnapshotError, machine.ErrMachineNotFound, codes.NotFound},
		{"snapshot missing", mapSnapshotError, machine.ErrSnapshotNotFound, codes.NotFound},
		{"snapshot unsupported", mapSnapshotError, machine.ErrSnapshotUnsupported, codes.Unimplemented},
		{"snapshot stale", mapSnapshotError, state.ErrStaleGeneration, codes.FailedPrecondition},
		{"snapshot conflict", mapSnapshotError, mutation.ErrConflict, codes.AlreadyExists},
		{
			"snapshot status preserved",
			mapSnapshotError,
			status.Error(codes.PermissionDenied, "denied"),
			codes.PermissionDenied,
		},
		{"volume machine missing", mapVolumeMutationError, machine.ErrMachineNotFound, codes.NotFound},
		{"volume stale execution", mapVolumeMutationError, machine.ErrStaleExecution, codes.FailedPrecondition},
		{"volume stale generation", mapVolumeMutationError, state.ErrStaleGeneration, codes.FailedPrecondition},
		{"volume conflict", mapVolumeMutationError, mutation.ErrConflict, codes.AlreadyExists},
		{
			"volume status preserved",
			mapVolumeMutationError,
			status.Error(codes.ResourceExhausted, "full"),
			codes.ResourceExhausted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := status.Code(tt.mapError(tt.err)); got != tt.want {
				t.Fatalf("code=%v want=%v", got, tt.want)
			}
		})
	}
	if status.Code(mapSnapshotError(errors.New("raw"))) != codes.Internal ||
		status.Code(mapVolumeMutationError(errors.New("raw"))) != codes.Internal {
		t.Fatal("unknown raw errors must map to Internal")
	}
}

func TestMutationHandlersUseProtocolBoundary(t *testing.T) {
	for _, name := range []string{"server.go", "snapshots.go", "volumes.go", "runtimeops.go"} {
		raw, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatal(err)
		}
		if match := regexp.MustCompile(`s\.(ledger|fences)\.(Check|Get|Begin|Complete|Put|Claim|WithMachine|LockMachine|Advance)\(`).Find(raw); match != nil {
			t.Fatalf("%s manually sequences mutation primitive %q", name, match)
		}
	}
}
