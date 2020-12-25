package njobs

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-redis/redis/v8"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/types"
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
	req *types.CloseAssignmentsStreamRequest,
) (*types.CloseAssignmentsStreamResponse, error) {
	worker, err := auth.FromGRPCContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.EvalStopSession(ctx, worker.WorkerID, req.StreamId); err != nil {
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
		Watermark:      int32(newQuota),
		AddedWatermark: int32(newQuota), // TODO Check if that's the actual number added
	}, nil
}

// StreamAssignments sends task assignments from the server to the client.
func (s *Streamer) StreamAssignments(
	req *types.StreamAssignmentsRequest,
	outStream types.Assignments_StreamAssignmentsServer,
) error {
	ctx := outStream.Context()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker, err := auth.FromGRPCContext(ctx)
	if err != nil {
		return err
	}
	// Check if stream ID exists.
	_, zscoreErr := s.Redis.ZScore(ctx, s.PartitionKeys.SessionExpires,
		redisSessionKey(worker.WorkerID, req.StreamId)).Result()
	if errors.Is(zscoreErr, redis.Nil) {
		return ErrSessionNotFound
	} else if zscoreErr != nil {
		return fmt.Errorf("failed to check if session exists: %w", zscoreErr)
	}
	// Create new Redis Streams consumer session.
	assignmentsC := make(chan []*types.Assignment)
	session := Session{
		RedisClient: s.RedisClient,
		Worker:      worker.WorkerID,
		Session:     req.StreamId,
	}
	sessionErrC := make(chan error, 1)
	go func() {
		defer cancel()
		defer close(sessionErrC)
		sessionErrC <- session.Run(ctx, assignmentsC)
	}()
	// Loop through Redis Streams results.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sessionErr := <-sessionErrC:
			return fmt.Errorf("session terminated: %w", sessionErr)
		case batch := <-assignmentsC:
			if err := outStream.Send(&types.AssignmentBatch{Assignments: batch}); err != nil {
				return err
			}
		default:
			break // continue
		}
	}
}

// ReportAssignments reports about completed tasks.
func (s *Streamer) ReportAssignments(
	ctx context.Context,
	req *types.ReportAssignmentsRequest,
) (*types.ReportAssignmentsResponse, error) {
	worker, err := auth.FromGRPCContext(ctx)
	if err != nil {
		return nil, err
	}
	count, err := s.EvalAck(ctx, worker.WorkerID, req.Results)
	if err != nil {
		return nil, err
	}
	return &types.ReportAssignmentsResponse{Acknowledged: int64(count)}, nil
}
