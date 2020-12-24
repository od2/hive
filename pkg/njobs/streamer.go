package njobs

import (
	"context"
	"fmt"
	"time"

	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Streamer accepts connections from a worker and pushes down assignments.
//
// Re-connecting and running multiple connections is also supported.
// When the worker is absent for too long, the streamer shuts down.
type Streamer struct {
	*RedisClient

	types.UnimplementedAssignmentsServer
}

// OpenAssignmentsStream creates a new njobs session.
func (s *Streamer) OpenAssignmentsStream(
	ctx context.Context,
	_ *types.OpenAssignmentsStreamRequest,
) (*types.OpenAssignmentsStreamResponse, error) {
	worker, err := auth.FromGRPCContext(ctx)
	if err != nil {
		return nil, err
	}
	session, err := s.EvalStartSession(ctx, worker.WorkerID)
	if err != nil {
		return nil, err
	}
	return &types.OpenAssignmentsStreamResponse{
		StreamId: session,
	}, nil
}

// StopWork halts the message stream making the worker shut down.
func (s *Streamer) CloseAssignmentsStream(
	ctx context.Context,
	_ *types.CloseAssignmentsStreamRequest,
) (*types.CloseAssignmentsStreamResponse, error) {
	worker, err := auth.FromGRPCContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.EvalStopSession(ctx, worker.WorkerID); err != nil {
		return nil, err
	}
	return &types.CloseAssignmentsStreamResponse{}, nil
}

// WantAssignments adds more quota to the worker stream.
func (s *Streamer) WantAssignments(
	ctx context.Context,
	req *types.WantAssignmentsRequest,
) (*types.WantAssignmentsResponse, error) {
	worker, err := auth.FromGRPCContext(ctx)
	if err != nil {
		return nil, err
	}
	newQuota, err := s.AddSessionQuota(ctx, worker.WorkerID, req.StreamId, int64(req.AddWatermark))
	if err != nil {
		return nil, fmt.Errorf("failed to run addSessionQuota: %w", err)
	}
	return &types.WantAssignmentsResponse{
		Watermark: int32(newQuota),
	}, nil
}

// StreamAssignments sends task assignments from the server to the client.
func (s *Streamer) StreamAssignments(
	req *types.StreamAssignmentsRequest,
	outStream types.Assignments_StreamAssignmentsServer,
) error {
	ctx := outStream.Context()
	worker, err := auth.FromGRPCContext(ctx)
	if err != nil {
		return err
	}
	refreshSessionTicker := time.NewTicker(s.SessionRefreshInterval)
	for {
		// If context is done, exit.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-refreshSessionTicker.C:
			if err := s.RefreshSession(ctx, worker.WorkerID, req.StreamId); err == ErrSessionNotFound {
				return status.Error(codes.NotFound, "session expired or not found")
			} else if err != nil {
				return err
			}
		default:
			break // continue
		}
	}
}
