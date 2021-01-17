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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TODO Catch panics in workers
// TODO Limit overall worker failures
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
	ReportRate    time.Duration   // Rate at which to report results
	ReportBatch   uint            // Max batch size for a report
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
	sessionID int64 // server-assigned session ID

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
	assigns := make(chan *types.Assignment)                      // Incoming assignments
	reports := make(chan *types.AssignmentReport, w.ReportBatch) // Outgoing result reports
	sess := &session{
		Simple:     w,
		sessionID:  openStream.StreamId,
		softCtx:    softCtx,
		softCancel: softCancel,
		hardCtx:    hardCtx,
		needsFill:  1,
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	// Start reporting assignments.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer softCancel()
		r := reporter{
			session: sess,
		}
		r.run(reports)
	}()
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
	workerRoutines := int32(w.Routines)
	for i := uint(0); i < w.Routines; i++ {
		go func(i uint) {
			defer wg.Done()
			defer softCancel()
			defer func() {
				// Close the results channel when all writer routines exited.
				if atomic.AddInt32(&workerRoutines, -1) <= 0 {
					close(reports)
				}
			}()
			if err := sess.worker(reports, assigns); err != nil {
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
	loopBack := make(chan struct{}, 1)
	defer close(loopBack)
	loopBack <- struct{}{}
	for {
		// Wait for something to happen.
		select {
		case <-s.softCtx.Done():
			return nil
		case <-ticker.C:
			break // select
		case <-loopBack:
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

type reporter struct {
	*session
	batch []*types.AssignmentReport
}

// run continually reports task assignment results.
func (r *reporter) run(reports <-chan *types.AssignmentReport) {
	ticker := time.NewTicker(r.ReportRate)
	defer ticker.Stop()
	for {
		select {
		case <-r.hardCtx.Done():
			return
		case <-ticker.C:
			r.flush()
		case report, ok := <-reports:
			if !ok {
				r.flush()
				return
			}
			r.batch = append(r.batch, report)
			if uint(len(r.batch)) >= r.ReportBatch {
				r.flush()
			}
		}
	}
}

func (r *reporter) flush() {
	if len(r.batch) <= 0 {
		return
	}
	if err := backoff.RetryNotify(func() error {
		_, err := r.Assignments.ReportAssignments(r.hardCtx, &types.ReportAssignmentsRequest{
			Reports: r.batch,
		})
		return err
	}, r.APIBackoff, func(err error, dur time.Duration) {
		r.Log.Warn("Error reporting results, retrying", zap.Error(err), zap.Duration("backoff", dur))
	}); err != nil {
		r.Log.Error("Failed to request more assignments", zap.Error(err))
		r.softCancel()
		return
	}
	r.Log.Debug("Flushed results", zap.Int("result_count", len(r.batch)))
	r.batch = nil
	return
}

// fillOnce requests the server to push more assignments with a single gRPC call.
func (s *session) fillOnce(delta int32) (err error) {
	_, err = s.Assignments.WantAssignments(s.softCtx, &types.WantAssignmentsRequest{
		StreamId:     s.sessionID,
		AddWatermark: delta,
	})
	return err
}

// worker runs a single worker loop routine.
func (s *session) worker(reports chan<- *types.AssignmentReport, assigns <-chan *types.Assignment) error {
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
			if atomic.AddInt32(&s.inflight, -1) < 0 {
				panic("negative in-flight counter")
			}
			reports <- &types.AssignmentReport{
				KafkaPointer: assign.GetKafkaPointer(),
				Status:       status,
			}
		}
	}
}

// numPending fetches and returns an approximation of the number of tasks pending and in-flight.
// Until the soft context is cancelled, the returned value is just a hint.
// Afterwards, return value is guaranteed to be equal or greater than the immediate/accurate value.
func (s *session) numPending(ctx context.Context) (int32, error) {
	var watermark int32
	if err := backoff.RetryNotify(func() error {
		pendingAssignCountRes, err := s.Assignments.GetPendingAssignmentsCount(ctx, &types.GetPendingAssignmentsCountRequest{
			StreamId: s.sessionID,
		})
		if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
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
	inflight := atomic.LoadInt32(&s.inflight)
	s.Log.Debug("Pending count",
		zap.Int32("pending", watermark),
		zap.Int32("inflight", inflight))
	return watermark + inflight, nil
}

// pull consumes assignments from a session across multiple streams.
// It reconnects as necessary and is resilient to network failures.
// This function closes the assigns channel when it exits.
func (s *session) pull(assigns chan<- *types.Assignment) error {
	defer close(assigns)
	return backoff.RetryNotify(func() error {
		// Connect to stream.
		grpcStream, err := s.Assignments.StreamAssignments(s.hardCtx, &types.StreamAssignmentsRequest{
			StreamId: s.sessionID,
		})
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return backoff.Permanent(fmt.Errorf("stream closed"))
		} else if err != nil {
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
		atomic.AddInt32(&s.inflight, int32(len(assignBatch)))
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
	// Cut off stream from any more assignments.
	res, err := s.Assignments.SurrenderAssignments(s.hardCtx, &types.SurrenderAssignmentsRequest{
		StreamId: s.session.sessionID,
	})
	if err != nil {
		s.Log.Warn("Failed to surrender assignments", zap.Error(err))
	} else {
		if res.GetRemovedWatermark() > 0 {
			s.Log.Info("Surrendered tasks", zap.Int32("surrendered_counts", res.GetRemovedWatermark()))
		}
	}
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
