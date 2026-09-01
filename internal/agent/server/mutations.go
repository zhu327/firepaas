package server

import (
	"encoding/json"
	"errors"

	"github.com/zhu327/firepaas/internal/agent/mutation"
	"github.com/zhu327/firepaas/internal/agent/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func protoCodec[T proto.Message](newValue func() T) mutation.Codec[T] {
	return mutation.Codec[T]{
		Encode: func(value T) (json.RawMessage, error) { return protojson.Marshal(value) },
		Decode: func(raw json.RawMessage) (T, error) {
			value := newValue()
			if err := protojson.Unmarshal(raw, value); err != nil {
				var zero T
				return zero, err
			}
			return value, nil
		},
	}
}

func mutationError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, mutation.ErrConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, state.ErrStaleGeneration):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
