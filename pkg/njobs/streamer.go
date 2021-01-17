package njobs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/topology"
	"go.od2.network/hive/pkg/topology/redisshard"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Streamer accepts connections from a worker and pushes down assignments.
//
// Re-connecting and running multiple connections is also supported.
// When the worker is absent for too long, the streamer shuts down.
type Streamer struct {
	Topology *topology.Config
	Factory  redisshard.Factory
	Log      *zap.Logger

	types.UnimplementedAssignmentsServer
}

func (s *Streamer) getRedis(collection string) (*RedisClient, error) {
	// TODO Support partitions
	if collection == "" {
		return nil, status.Error(codes.InvalidArgument, "missing collection param")
	}
	coll := s.Topology.GetCollection(collection)
	if coll == nil {
		return nil, status.Error(codes.NotFound, "no such collection: "+collection)
	}
	shard := topology.Shard{Collection: collection}
	rd, err := s.Factory.GetShard(shard)
	if err != nil {
		return nil, err
	}
	return NewRedisClient(rd, &shard, coll)
}

// OpenAssignmentsStream creates a new njobs session.
func (s *Streamer) OpenAssignmentsStream(
	ctx context.Context,
	req *types.OpenAssignmentsStreamRequest,
) (*types.OpenAssignmentsStreamResponse, error) {
	worker, err := auth.WorkerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rc, err := s.getRedis(req.GetCollection())
	if err != nil {
		return nil, err
	}
	nowUnix := time.Now().Unix()
	session, err := rc.EvalStartSession(ctx, worker.WorkerID, nowUnix)
	if err != nil {
		return nil, err
	}
	s.Log.Info("Success OpenAssignmentsStream()",
		zap.Int64("worker", worker.WorkerID), zap.Int64("session", session))
	return &types.OpenAssignmentsStreamResponse{
		StreamId: session,
	}, nil
}

// CloseAssignmentsStream halts the message stream making the worker shut down.
func (s *Streamer) CloseAssignmentsStream(
	ctx context.Context,
	req *types.CloseAssignmentsStreamRequest,
) (*types.CloseAssignmentsStreamResponse, error) {
	worker, err := auth.WorkerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rc, err := s.getRedis(req.GetCollection())
	if err != nil {
		return nil, err
	}
	if err := rc.EvalStopSession(ctx, worker.WorkerID, req.StreamId); err != nil {
		return nil, err
	}
	s.Log.Info("Success CloseAssignmentsStream()",
		zap.Int64("worker", worker.WorkerID), zap.Int64("session", req.StreamId))
	return &types.CloseAssignmentsStreamResponse{}, nil
}

// WantAssignments adds more quota to the worker stream.
func (s *Streamer) WantAssignments(
	ctx context.Context,
	req *types.WantAssignmentsRequest,
) (*types.WantAssignmentsResponse, error) {
	worker, err := auth.WorkerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.AddWatermark <= 0 {
		return nil, status.Error(codes.InvalidArgument, "negative assignment add count")
	}
	rc, err := s.getRedis(req.GetCollection())
	if err != nil {
		return nil, err
	}
	newQuota, err := rc.AddSessionQuota(ctx, worker.WorkerID, req.StreamId, int64(req.AddWatermark))
	if err != nil {
		return nil, fmt.Errorf("failed to run addSessionQuota: %w", err)
	}
	return &types.WantAssignmentsResponse{
		Watermark:      int32(newQuota),
		AddedWatermark: int32(newQuota), // TODO Check if that's the actual number added
	}, nil
}

// SurrenderAssignments tells the server to reset all pending assignments.
func (s *Streamer) SurrenderAssignments(
	ctx context.Context,
	req *types.SurrenderAssignmentsRequest,
) (*types.SurrenderAssignmentsResponse, error) {
	worker, err := auth.WorkerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rc, err := s.getRedis(req.GetCollection())
	if err != nil {
		return nil, err
	}
	removedQuota, err := rc.ResetSessionQuota(ctx, worker.WorkerID, req.StreamId)
	if err != nil {
		return nil, fmt.Errorf("failed to run resetSessionQuota: %w", err)
	}
	return &types.SurrenderAssignmentsResponse{
		RemovedWatermark: int32(removedQuota),
	}, nil
}

// GetPendingAssignmentsCount returns the number of task assignments
// that are not delivered to the client yet, but which are queued for the near future.
func (s *Streamer) GetPendingAssignmentsCount(
	ctx context.Context,
	req *types.GetPendingAssignmentsCountRequest,
) (*types.PendingAssignmentsCount, error) {
	worker, err := auth.WorkerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var sessionKey [16]byte
	binary.BigEndian.PutUint64(sessionKey[:8], uint64(worker.WorkerID))
	binary.BigEndian.PutUint64(sessionKey[8:], uint64(req.StreamId))
	rc, err := s.getRedis(req.GetCollection())
	if err != nil {
		return nil, err
	}
	count, err := rc.Redis.HGet(ctx, rc.PartitionKeys.SessionQuota, string(sessionKey[:])).Int64()
	if errors.Is(err, redis.Nil) {
		count = 0
	} else if err != nil {
		return nil, err
	}
	return &types.PendingAssignmentsCount{Watermark: int32(count)}, nil
}

// StreamAssignments sends task assignments from the server to the client.
func (s *Streamer) StreamAssignments(
	req *types.StreamAssignmentsRequest,
	outStream types.Assignments_StreamAssignmentsServer,
) error {
	ctx := outStream.Context()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker, err := auth.WorkerFromContext(ctx)
	if err != nil {
		return err
	}
	rc, err := s.getRedis(req.GetCollection())
	if err != nil {
		return err
	}
	// Check if stream ID exists.
	_, zscoreErr := rc.Redis.ZScore(ctx, rc.PartitionKeys.SessionExpires,
		redisSessionKey(worker.WorkerID, req.StreamId)).Result()
	if errors.Is(zscoreErr, redis.Nil) {
		return ErrSessionNotFound
	} else if zscoreErr != nil {
		return fmt.Errorf("failed to check if session exists: %w", zscoreErr)
	}
	// Create new Redis Streams consumer session.
	assignmentsC := make(chan []*types.Assignment)
	session := Session{
		RedisClient: rc,
		Worker:      worker.WorkerID,
		Session:     req.StreamId,
	}
	sessionErrC := make(chan error, 1)
	go func() {
		defer cancel()
		defer close(sessionErrC)
		sessionErrC <- session.Run(ctx, assignmentsC)
	}()
	log := s.Log.With(zap.Int64("worker", worker.WorkerID), zap.Int64("session", req.StreamId))
	log.Info("Started StreamAssignments")
	defer log.Info("Stopped StreamAssignments")
	// Loop through Redis Streams results.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sessionErr := <-sessionErrC:
			return fmt.Errorf("session terminated: %w", sessionErr)
		case batch := <-assignmentsC:
			log.Info("Delivering assignments", zap.Int("num_assignments", len(batch)))
			if err := outStream.Send(&types.AssignmentBatch{Assignments: batch}); err != nil {
				return err
			}
		}
	}
}

// ReportAssignments reports about completed tasks.
func (s *Streamer) ReportAssignments(
	ctx context.Context,
	req *types.ReportAssignmentsRequest,
) (*types.ReportAssignmentsResponse, error) {
	worker, err := auth.WorkerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rc, err := s.getRedis(req.GetCollection())
	if err != nil {
		return nil, err
	}
	count, err := rc.EvalAck(ctx, worker.WorkerID, req.Reports)
	if err != nil {
		return nil, err
	}
	s.Log.Info("Got report",
		zap.Int64("worker", worker.WorkerID),
		zap.Int("num_assignments", len(req.Reports)),
		zap.Uint("num_acknowledged", count))
	return &types.ReportAssignmentsResponse{Acknowledged: int64(count)}, nil
}
