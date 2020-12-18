package tasks

import (
	"context"
	"strconv"

	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/redisqueue"
	"go.od2.network/hive/pkg/types"
)

// Server provides the tasks API.
type Server struct {
	Consumers    *redisqueue.Consumers
	MaxClaimSize uint
}

var _ types.TasksServer = (*Server)(nil)

// GetTasks claims a bunch of tasks for the worker.
func (s *Server) GetTasks(ctx context.Context, request *types.GetTasksRequest) (*types.GetTasksResponse, error) {
	authCtx, err := auth.FromGRPCContext(ctx)
	if err != nil {
		// TODO Return bad request
		return nil, err
	}
	if request.NumTasks <= 0 || request.NumTasks > int32(s.MaxClaimSize) {
		// TODO Return bad request
		return nil, err
	}
	claimer := strconv.FormatInt(authCtx.WorkerID, 10)
	taskIDs, err := s.Consumers.ClaimTasks(ctx, claimer, uint(authCtx.WorkerID))
	if err != nil {
		// TODO Return internal server error.
		return nil, err
	}
	// TODO Error check
	return nil, nil
}
