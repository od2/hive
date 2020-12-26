package njobs

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive/pkg/redistest"
	"go.od2.network/hive/pkg/saramamock"
	"go.od2.network/hive/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func BenchmarkNJobs(b *testing.B) {
	// N million jobs => 3*N million assignments
	runBenchmark(b, &benchOptions{
		Options:  DefaultOptions,
		Workers:  32,
		Sessions: 2,
		Rate:     5,
		Jobs:     uint(b.N),
	})
}

type benchOptions struct {
	// Pipeline settings
	Options
	Topic     string
	Partition int32
	// Benchmark settings
	Workers  uint    // Number of concurrent workers
	Sessions uint    // Number of sessions per worker
	QoS      uint    // Minimum number of pre-sent assignments
	Rate     float32 // Work rate per worker
	Jobs     uint    // Number of jobs in total
}

type benchStack struct {
	// Environment
	B           *testing.B
	Redis       *redistest.Redis
	RedisClient *RedisClient
	Session     *saramamock.ConsumerGroupSession
	Claim       *saramamock.ConsumerGroupClaim
	opts        *benchOptions
	ctx         context.Context
	cancel      context.CancelFunc
	// Modules
	assigner *Assigner
	streamer *Streamer
	listener *bufconn.Listener
	server   *grpc.Server
	// Status
	batches int64
	assigns int64
}

func newBenchStack(b *testing.B, opts *benchOptions) *benchStack {
	ctx, cancel := context.WithCancel(context.Background())
	redis := redistest.NewRedis(ctx, b)

	// Build fake Kafka consumer session.
	session := &saramamock.ConsumerGroupSession{MContext: ctx}
	claim := &saramamock.ConsumerGroupClaim{
		MTopic:               opts.Topic,
		MPartition:           opts.Partition,
		MInitialOffset:       0,
		MHighWaterMarkOffset: 0,
	}
	claim.Init()
	msgCount := int64(0)
	claim.NextMessage = func() *sarama.ConsumerMessage {
		var itemID [8]byte
		binary.BigEndian.PutUint64(itemID[:], uint64(0x100)+uint64(msgCount))
		return &sarama.ConsumerMessage{
			Timestamp: time.Now(),
			Key:       itemID[:],
			Value:     nil,
			Topic:     claim.MTopic,
			Partition: claim.MPartition,
			Offset:    msgCount * 16,
		}
	}
	// Build njobs Redis client.
	scripts, err := LoadScripts(ctx, redis.Client)
	require.NoError(b, err)
	rc := &RedisClient{
		Redis:         redis.Client,
		Options:       &opts.Options,
		PartitionKeys: NewPartitionKeys(claim.MTopic, claim.MPartition),
		Scripts:       scripts,
	}
	// Build assigner.
	assigner := &Assigner{
		Redis:   redis.Client,
		Options: &opts.Options,
	}
	// Build streamer.
	streamer := &Streamer{
		RedisClient: rc,
	}
	// Build fake network listener.
	lis := bufconn.Listen(1024 * 1024)
	go func() {
		<-ctx.Done()
		lis.Close()
	}()
	// Build gRPC server.
	server := grpc.NewServer()
	types.RegisterAssignmentsServer(server, streamer)
	// Connect to Redis.
	return &benchStack{
		// Environment
		B:           b,
		Redis:       redis,
		RedisClient: rc,
		Session:     session,
		Claim:       claim,
		opts:        opts,
		ctx:         ctx,
		cancel:      cancel,
		// Modules
		assigner: assigner,
		streamer: streamer,
		listener: lis,
		server:   server,
		batches:  0,
		assigns:  0,
	}
}

func runBenchmark(b *testing.B, opts *benchOptions) {
	stack := newBenchStack(b, opts)
	defer stack.cancel()
	var wg sync.WaitGroup
	// Run message generator.
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := stack.Claim.Run(stack.ctx)
		require.EqualError(b, err, "context canceled", "mock sarama claim failed")
	}()
	// Run task assigner.
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(b, stack.assigner.Setup(stack.Session))
		err := stack.assigner.ConsumeClaim(stack.Session, stack.Claim)
		require.EqualError(b, err, "context canceled", "sarama claim consumer")
		assert.NoError(b, stack.assigner.Cleanup(stack.Session))
	}()
	// Start streamer server.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = stack.server.Serve(stack.listener)
	}()
	b.ResetTimer()
	// Start job workers.
	var workerWg sync.WaitGroup
	for i := uint(0); i < stack.opts.Workers; i++ {
		workerWg.Add(int(stack.opts.Sessions))
		for j := uint(0); j < stack.opts.Sessions; j++ {
			go func() {
				defer workerWg.Done()
				stack.runClient(stack.ctx, int64(i))
			}()
		}
	}
	b.Log("Test running")
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stack.ctx.Done():
				return
			case <-t.C:
				b.Logf("Stats: assigns=%d batches=%d",
					atomic.LoadInt64(&stack.assigns),
					atomic.LoadInt64(&stack.batches))
			}
		}
	}()
	workerWg.Wait()
	b.StopTimer()
	b.Log("Waiting for shutdown")
	wg.Wait()
	b.Log("Run finished")
}

func (stack *benchStack) runClient(ctx context.Context, workerID int64) {
	client, closer := stack.newClient(workerID)
	defer closer.Close()
	// Grab stream.
	streamRes, err := client.OpenAssignmentsStream(ctx, &types.OpenAssignmentsStreamRequest{})
	require.NoError(stack.B, err)
	defer func() {
		_, err = client.CloseAssignmentsStream(ctx, &types.CloseAssignmentsStreamRequest{StreamId: streamRes.StreamId})
		require.NoError(stack.B, err)
	}()
	var clientPending int32
	// Keep the quota count up.
	go func() {
		for {
			// Check if we have enough assignments pending.
			pendingRes, err := client.GetPendingAssignmentsCount(ctx, &types.GetPendingAssignmentsCountRequest{StreamId: streamRes.StreamId})
			require.NoError(stack.B, err)
			totalPending := pendingRes.Watermark + atomic.LoadInt32(&clientPending)
			if totalPending >= int32(stack.opts.QoS) {
				continue
			}
			// We need more assignments.
			_, err = client.WantAssignments(ctx, &types.WantAssignmentsRequest{
				StreamId:     streamRes.StreamId,
				AddWatermark: int32(stack.opts.QoS) - totalPending,
			})
			require.NoError(stack.B, err)
		}
	}()
	// Process items.
	stream, err := client.StreamAssignments(stack.ctx, &types.StreamAssignmentsRequest{StreamId: streamRes.StreamId})
	require.NoError(stack.B, err)
	for {
		// Got new assignments.
		batch, err := stream.Recv()
		require.NoError(stack.B, err)
		atomic.AddInt64(&stack.batches, 1)
		results := make([]*types.AssignmentResult, len(batch.Assignments))
		for i, a := range batch.Assignments {
			results[i] = &types.AssignmentResult{
				KafkaPointer: a.KafkaPointer,
				Status:       types.TaskStatus_SUCCESS,
			}
		}
		// Acknowledge assignments.
		_, err = client.ReportAssignments(ctx, &types.ReportAssignmentsRequest{Results: results})
		require.NoError(stack.B, err)
		// Benchmark progress
		atomic.AddInt64(&stack.assigns, int64(len(batch.Assignments)))
	}
}

func (stack *benchStack) newClient(workerID int64) (types.AssignmentsClient, io.Closer) {
	dialer := func(context.Context, string) (net.Conn, error) {
		return stack.listener.Dial()
	}
	conn, err := grpc.DialContext(stack.ctx, "bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(withClientAuthUnary(workerID)),
		grpc.WithStreamInterceptor(withClientAuthStream(workerID)))
	require.NoError(stack.B, err, "failed to connect to gRPC")
	return types.NewAssignmentsClient(conn), conn
}

func withClientAuthUnary(workerID int64) grpc.UnaryClientInterceptor {
	workerIDStr := strconv.FormatInt(workerID, 10)
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", workerIDStr)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func withClientAuthStream(workerID int64) grpc.StreamClientInterceptor {
	workerIDStr := strconv.FormatInt(workerID, 10)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", workerIDStr)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
