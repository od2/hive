package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap"
)

// TODO Handle server reporting stream as not found
// TODO Catch panics in workers
// TODO Limit overall worker failures
// TODO Naming: Rename "stream ID" to "session ID"
// TODO Consider backoff for open/close

// Simple is a works off items one-by-one from multiple routines.
// It is resilient to network failures and auto-heals failed gRPC connections.
type Simple struct {
	Assignments types.AssignmentsClient
	Log         *zap.Logger
	Handler     SimpleHandler

	// Options
	Routines      uint            // Number of worker routines
	Prefetch      uint            // Assignment prefetch count
	GracePeriod   time.Duration   // Duration given to shut down stream cleanly
	FillRate      time.Duration   // Max rate to fill assignments
	StreamBackoff backoff.BackOff // Stream connection error backoff
	APIBackoff    backoff.BackOff // API call backoff
}

// SimpleHandler works off individual tasks, returning task statuses for each.
// The handler needs to handle calls concurrently from multiple goroutines.
type SimpleHandler interface {
	WorkAssignment(context.Context, *types.Assignment) types.TaskStatus
}

// session describes a logical worker session spanning multiple gRPC stream.
// A session dies when the worker failed to run a stream in a predefined duration.
type session struct {
	*Simple
	streamID int64 // server-assigned session ID

	softCtx    context.Context
	softCancel context.CancelFunc
	hardCtx    context.Context

	inflight  int32 // number of received tasks pending completion [atomic]
	needsFill int32 // flag whether fill algorithm needs to run [atomic]
}

// Run runs the worker until the context is cancelled.
// After the context is cancelled, the outstanding items are worked off before the function returns.
// The function only returns an error if anything goes wrong during startup,
// and might return nil if the error occurs at a vital component in the pipeline at a later stage.
func (w *Simple) Run(outerCtx context.Context) error {
	// Create contexts:
	// The soft context finishes when the pipeline initiates shutdown.
	// The hard context briefly outlives the soft context during the grace period.
	// The graceful shutdown aims to complete all outstanding promises to the server, to maintain overall hive health.
	// When the hard context is done, every component shuts down immediately.
	softCtx, softCancel := context.WithCancel(outerCtx)
	defer softCancel()
	hardCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()
	// Allocate stream.
	openStream, err := w.Assignments.OpenAssignmentsStream(softCtx, &types.OpenAssignmentsStreamRequest{})
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	w.Log.Info("Opened stream")
	defer func() {
		_, err := w.Assignments.CloseAssignmentsStream(hardCtx, &types.CloseAssignmentsStreamRequest{
			StreamId: openStream.StreamId,
		})
		if err != nil {
			w.Log.Error("Error closing stream", zap.Error(err))
		}
		w.Log.Info("Closed stream")
	}()
	// Set up concurrent system.
	assigns := make(chan *types.Assignment) // Incoming assignments
	sess := &session{
		Simple:     w,
		streamID:   openStream.StreamId,
		softCtx:    softCtx,
		softCancel: softCancel,
		hardCtx:    hardCtx,
		needsFill:  1,
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	// Start reading items from stream.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer softCancel()
		if err := sess.pull(assigns); err != nil {
			w.Log.Error("Session pull failed permanently", zap.Error(err))
		}
	}()
	// Allocate routines.
	wg.Add(int(w.Routines))
	for i := uint(0); i < w.Routines; i++ {
		go func(i uint) {
			defer wg.Done()
			defer softCancel()
			if err := sess.worker(assigns); err != nil {
				w.Log.Error("Too many worker errors, shutting down", zap.Uint("worker_serial", i))
			}
		}(i)
	}
	w.Log.Info("Started worker routines", zap.Uint("routines", w.Routines))
	// Fill pipeline with assignments.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer softCancel()
		if err := sess.fill(); err != nil {
			w.Log.Error("Session fill failed permanently")
		}
	}()
	return nil
}

// fill continually tells the server to push more tasks.
func (s *session) fill() error {
	s.Log.Info("Starting session filler")
	defer s.Log.Info("Stopping session filler")
	ticker := time.NewTicker(s.FillRate)
	defer ticker.Stop()
	for {
		// Wait for something to happen.
		select {
		case <-s.softCtx.Done():
			return nil
		case <-ticker.C:
			break // select
		}
		// Check if action needs to be taken.
		if atomic.LoadInt32(&s.needsFill) == 0 {
			continue
		}
		numPending, err := s.numPending(s.softCtx)
		if err != nil {
			return fmt.Errorf("failed to get number of pending tasks: %w", err)
		}
		delta := int32(s.Prefetch) - numPending
		if delta <= 0 {
			continue
		}
		// Request more assignments.
		s.Log.Debug("Want more assignments", zap.Int32("add_watermark", delta))
		if err := backoff.RetryNotify(func() error {
			if err := s.fillOnce(delta); errors.Is(err, context.Canceled) {
				return backoff.Permanent(err)
			} else if err != nil {
				return err
			}
			return nil
		}, s.APIBackoff, func(err error, dur time.Duration) {
			s.Log.Warn("Error requesting more assignments, retrying", zap.Error(err), zap.Duration("backoff", dur))
		}); err != nil {
			s.Log.Error("Failed to request more assignments", zap.Error(err))
		}
	}
}

// fillOnce requests the server to push more assignments with a single gRPC call.
func (s *session) fillOnce(delta int32) (err error) {
	_, err = s.Assignments.WantAssignments(s.softCtx, &types.WantAssignmentsRequest{
		StreamId:     s.streamID,
		AddWatermark: delta,
	})
	return err
}

// worker runs a single worker loop routine.
func (s *session) worker(assigns <-chan *types.Assignment) error {
	for {
		select {
		case <-s.hardCtx.Done():
			return s.hardCtx.Err()
		case assign, ok := <-assigns:
			if !ok {
				return nil
			}
			// Hand off task to handler for processing.
			status := s.Handler.WorkAssignment(s.hardCtx, assign)
			// Update pipeline state.
			atomic.StoreInt32(&s.needsFill, 1)
			atomic.AddInt32(&s.inflight, -1)
			// Notify server with result.
			if err := backoff.RetryNotify(func() error {
				return s.reportAssignment(assign, status)
			}, s.APIBackoff, func(err error, dur time.Duration) {
				s.Log.Warn("Error reporting assignment result", zap.Error(err), zap.Duration("backoff", dur))
			}); err != nil {
				s.Log.Error("Failed to report assignment result", zap.Error(err))
			}
		}
	}
}

// reportAssignment reports the status of a single assignment.
// TODO If this becomes a bottleneck, batch together assignment reports.
func (s *session) reportAssignment(assign *types.Assignment, status types.TaskStatus) error {
	_, err := s.Assignments.ReportAssignments(s.hardCtx, &types.ReportAssignmentsRequest{
		Reports: []*types.AssignmentReport{
			{
				KafkaPointer: assign.GetKafkaPointer(),
				Status:       status,
			},
		},
	})
	return err
}

// addInflight increases the number of tasks in-flight.
func (s *session) addInflight(num int32) {
	atomic.AddInt32(&s.inflight, num)
}

// numPending fetches and returns an approximation of the number of tasks pending and in-flight.
// Until the soft context is cancelled, the returned value is just a hint.
// Afterwards, return value is guaranteed to be equal or greater than the immediate/accurate value.
func (s *session) numPending(ctx context.Context) (int32, error) {
	var watermark int32
	if err := backoff.RetryNotify(func() error {
		pendingAssignCountRes, err := s.Assignments.GetPendingAssignmentsCount(ctx, &types.GetPendingAssignmentsCountRequest{
			StreamId: s.streamID,
		})
		if errors.Is(err, context.Canceled) {
			return backoff.Permanent(err)
		} else if err != nil {
			return err
		}
		watermark = pendingAssignCountRes.GetWatermark()
		return nil
	}, s.APIBackoff, func(err error, dur time.Duration) {
		s.Log.Warn("Error getting pending assignments, retrying", zap.Error(err), zap.Duration("backoff", dur))
	}); err != nil {
		s.Log.Error("Failed to get pending assignments", zap.Error(err))
	}
	return watermark + atomic.LoadInt32(&s.inflight), nil
}

// pull consumes assignments from a session across multiple streams.
// It reconnects as necessary and is resilient to network failures.
// This function closes the assigns channel when it exits.
func (s *session) pull(assigns chan<- *types.Assignment) error {
	defer close(assigns)
	return backoff.RetryNotify(func() error {
		// Connect to stream.
		grpcStream, err := s.Assignments.StreamAssignments(s.hardCtx, &types.StreamAssignmentsRequest{
			StreamId: s.streamID,
		})
		if err != nil {
			return err
		}
		defer grpcStream.CloseSend()
		sessStream := &stream{
			session: s,
			stream:  grpcStream,
		}
		// Check if outer context is done so we close the session.
		if ctxErr := s.softCtx.Err(); ctxErr != nil {
			// Gracefully shut down and fetch all pending tasks.
			if s.GracePeriod > 0 {
				sessStream.drain(assigns)
				return nil
			}
			if errors.Is(ctxErr, context.Canceled) {
				return nil
			}
			return backoff.Permanent(ctxErr)
		}
		// Pull items from stream.
		return sessStream.pull(s.softCtx, assigns)
	}, s.StreamBackoff, func(err error, dur time.Duration) {
		s.Log.Error("Stream failed, retrying", zap.Error(err), zap.Duration("backoff", dur))
	})
}

// stream is a single gRPC stream in a worker session.
// It dies when the stream exits.
type stream struct {
	*session
	stream types.Assignments_StreamAssignmentsClient
}

// pull consumes assignments from a single stream and exits early on context cancel.
func (s *stream) pull(ctx context.Context, assigns chan<- *types.Assignment) error {
	s.Log.Info("Pulling items from stream")
	defer s.Log.Info("Shutting down pulling")
	for {
		// Check for stream liveness.
		select {
		case <-ctx.Done():
			s.Log.Info("Pull context done")
			return nil
		case <-s.stream.Context().Done():
			return fmt.Errorf("stream context: %w", s.stream.Context().Err())
		default:
			break // select
		}
		// Fan out stream items.
		assignBatchRes, err := s.stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to recv assigns: %w", err)
		}
		assignBatch := assignBatchRes.GetAssignments()
		s.Log.Debug("Fanning out items", zap.Int("num_assigns", len(assignBatch)))
		for _, assign := range assignBatch {
			assigns <- assign
		}
		s.Log.Debug("Finished fanning out items")
	}
}

// drainStream runs a grace period to pull the stream dry.
// The stream is determined to be dry when the server reports no more pending assignments.
// It exits prematurely if the context is cancelled.
func (s *stream) drain(assigns chan<- *types.Assignment) {
	s.Log.Info("Draining stream from pending assignments")
	for {
		// Check for stream liveness.
		select {
		case <-s.hardCtx.Done():
			s.Log.Error("Drain context done", zap.Error(s.hardCtx.Err()))
		case <-s.stream.Context().Done():
			s.Log.Error("Stream context done while draining", zap.Error(s.stream.Context().Err()))
			return
		default:
			break // select
		}
		// Check how many assignments are left.
		totalPending, err := s.numPending(s.hardCtx)
		if err != nil {
			s.Log.Error("Failed to get pending assignments while draining", zap.Error(err))
			return
		}
		if totalPending <= 0 {
			break
		}
		// Fan out stream items.
		assignBatchRes, err := s.stream.Recv()
		if err != nil {
			s.Log.Error("Failed to recv assigns while draining", zap.Error(err))
			return
		}
		assignBatch := assignBatchRes.GetAssignments()
		s.Log.Debug("Fanning out assignments (drain)", zap.Int("num_assigns", len(assignBatch)))
		for _, assign := range assignBatch {
			assigns <- assign
		}
		s.Log.Debug("Finished fanning out assignments (drain)")
	}
	s.Log.Info("Finished all assignments")
}