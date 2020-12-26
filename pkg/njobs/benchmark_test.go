package njobs

import (
	"context"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestBenchmark(t *testing.T) {
	if !runHeavyTests {
		t.Skip("Heavy tests disabled")
	}

	type testCaseDef struct {
		name string
		opts *benchOptions
	}
	cases := []testCaseDef{
		{
			name: "Warmup",
			opts: &benchOptions{
				Options:  DefaultOptions,
				Workers:  32,
				Sessions: 2,
				Assigns:  50000,
				QoS:      128,
			},
		},
		{
			name: "Assign100000_Sessions64_Batch512",
			opts: &benchOptions{
				Options: Options{
					TaskAssignments:        3,
					AssignInterval:         250 * time.Millisecond,
					AssignBatch:            512,
					SessionTimeout:         5 * time.Minute,
					SessionRefreshInterval: 3 * time.Second,
					SessionExpireInterval:  10 * time.Second,
					SessionExpireBatch:     16,
					TaskTimeout:            time.Minute,
					TaskExpireInterval:     2 * time.Second,
					TaskExpireBatch:        128,
					DeliverBatch:           512,
				},
				Workers:  32,
				Sessions: 2,
				Assigns:  100000,
				QoS:      512,
			},
		},
		{
			name: "Assign100000_Sessions64_Batch1024",
			opts: &benchOptions{
				Options: Options{
					TaskAssignments:        3,
					AssignInterval:         250 * time.Millisecond,
					AssignBatch:            1024,
					SessionTimeout:         5 * time.Minute,
					SessionRefreshInterval: 3 * time.Second,
					SessionExpireInterval:  10 * time.Second,
					SessionExpireBatch:     16,
					TaskTimeout:            time.Minute,
					TaskExpireInterval:     2 * time.Second,
					TaskExpireBatch:        128,
					DeliverBatch:           1024,
				},
				Workers:  32,
				Sessions: 2,
				Assigns:  100000,
				QoS:      1024,
			},
		},
		{
			name: "Sessions64_Batch2048",
			opts: &benchOptions{
				Options: Options{
					TaskAssignments:        3,
					AssignInterval:         250 * time.Millisecond,
					AssignBatch:            2048,
					SessionTimeout:         5 * time.Minute,
					SessionRefreshInterval: 3 * time.Second,
					SessionExpireInterval:  10 * time.Second,
					SessionExpireBatch:     16,
					TaskTimeout:            time.Minute,
					TaskExpireInterval:     2 * time.Second,
					TaskExpireBatch:        128,
					DeliverBatch:           2048,
				},
				Workers:  32,
				Sessions: 2,
				Assigns:  100000,
				QoS:      2048,
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			runBenchmark(t, testCase.opts)
		})
	}
}

type benchOptions struct {
	// Pipeline settings
	Options
	Topic     string
	Partition int32
	// Benchmark settings
	Workers  uint // Number of concurrent workers
	Sessions uint // Number of sessions per worker
	QoS      uint // Minimum number of pre-sent assignments
	Assigns  uint // Number of jobs in total
}

type benchStack struct {
	// Environment
	T           *testing.T
	Redis       *redistest.Redis
	RedisClient *RedisClient
	Session     *saramamock.ConsumerGroupSession
	Claim       *saramamock.ConsumerGroupClaim
	opts        *benchOptions
	ctx         context.Context
	cancel      context.CancelFunc
	innerCtx    context.Context
	innerCancel context.CancelFunc
	// Modules
	assigner *Assigner
	listener *bufconn.Listener
	server   *grpc.Server
	// Status
	batches int64
	assigns int64
}

func newBenchStack(t *testing.T, opts *benchOptions) *benchStack {
	ctx, cancel := context.WithCancel(context.Background())
	redis := redistest.NewRedis(ctx, t)
	innerCtx, innerCancel := context.WithCancel(ctx)
	// Build fake Kafka consumer session.
	session := &saramamock.ConsumerGroupSession{MContext: innerCtx}
	claim := &saramamock.ConsumerGroupClaim{
		MTopic:               opts.Topic,
		MPartition:           opts.Partition,
		MInitialOffset:       0,
		MHighWaterMarkOffset: 0,
	}
	claim.Init()
	msgCount := int64(0)
	claim.NextMessage = func() *sarama.ConsumerMessage {
		msg := &sarama.ConsumerMessage{
			Timestamp: time.Now(),
			Key:       []byte(strconv.FormatUint(uint64(0x100)+uint64(msgCount), 16)),
			Value:     nil,
			Topic:     claim.MTopic,
			Partition: claim.MPartition,
			Offset:    msgCount * 16,
		}
		msgCount++
		return msg
	}
	// Build njobs Redis client.
	scripts, err := LoadScripts(ctx, redis.Client)
	require.NoError(t, err)
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
		T:           t,
		Redis:       redis,
		RedisClient: rc,
		Session:     session,
		Claim:       claim,
		opts:        opts,
		ctx:         ctx,
		cancel:      cancel,
		innerCtx:    innerCtx,
		innerCancel: innerCancel,
		// Modules
		assigner: assigner,
		listener: lis,
		server:   server,
		batches:  0,
		assigns:  0,
	}
}

func runBenchmark(t *testing.T, opts *benchOptions) {
	stack := newBenchStack(t, opts)
	defer stack.cancel()
	var wg sync.WaitGroup
	// Run message generator.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stack.innerCancel()
		err := stack.Claim.Run(stack.innerCtx)
		require.EqualError(t, err, "context canceled", "mock sarama claim failed")
	}()
	// Run task assigner.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stack.innerCancel()
		require.NoError(t, stack.assigner.Setup(stack.Session))
		err := stack.assigner.ConsumeClaim(stack.Session, stack.Claim)
		t.Log("Sarama claim consumer exited with:", err)
		assert.NoError(t, stack.assigner.Cleanup(stack.Session))
	}()
	// Start streamer server.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stack.innerCancel()
		_ = stack.server.Serve(stack.listener)
	}()
	// Start job workers.
	var workerWg sync.WaitGroup
	for i := uint(0); i < stack.opts.Workers; i++ {
		workerWg.Add(int(stack.opts.Sessions))
		for j := uint(0); j < stack.opts.Sessions; j++ {
			go func(i uint) {
				defer workerWg.Done()
				stack.runClient(stack.innerCtx, int64(i))
			}(i)
		}
	}
	t.Log("Test running")
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stack.innerCtx.Done():
				return
			case <-ticker.C:
				t.Logf("Stats: assigns=%d batches=%d",
					atomic.LoadInt64(&stack.assigns),
					atomic.LoadInt64(&stack.batches))
			}
		}
	}()
	workerWg.Wait()
	stack.cancel()
	t.Log("Waiting for shutdown")
	wg.Wait()
	t.Log("Run finished")
}

func (stack *benchStack) runClient(ctx context.Context, workerID int64) {
	client, closer := stack.newClient(workerID)
	defer closer.Close()
	// Grab stream.
	streamRes, err := client.OpenAssignmentsStream(stack.ctx, &types.OpenAssignmentsStreamRequest{})
	require.NoError(stack.T, err)
	defer func() {
		_, err = client.CloseAssignmentsStream(stack.ctx,
			&types.CloseAssignmentsStreamRequest{StreamId: streamRes.StreamId})
		require.NoError(stack.T, err)
	}()
	var clientPending int32
	// Keep the quota count up.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			// Check if we have enough assignments pending.
			pendingRes, err := client.GetPendingAssignmentsCount(ctx, &types.GetPendingAssignmentsCountRequest{StreamId: streamRes.StreamId})
			if errStatus, ok := status.FromError(err); ok && errStatus.Code() == codes.Canceled {
				break // cancel gracefully
			}
			require.NoError(stack.T, err)
			totalPending := pendingRes.Watermark + atomic.LoadInt32(&clientPending)
			if totalPending >= int32(stack.opts.QoS) {
				continue
			}
			// We need more assignments.
			_, err = client.WantAssignments(ctx, &types.WantAssignmentsRequest{
				StreamId:     streamRes.StreamId,
				AddWatermark: int32(stack.opts.QoS) - totalPending,
			})
			if errStatus, ok := status.FromError(err); ok && errStatus.Code() == codes.Canceled {
				break // cancel gracefully
			}
			require.NoError(stack.T, err)
			// Sleep a bit.
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				break // continue
			}
		}
	}()
	// Process items.
	stream, err := client.StreamAssignments(stack.innerCtx,
		&types.StreamAssignmentsRequest{StreamId: streamRes.StreamId})
	require.NoError(stack.T, err)
	for {
		// Got new assignments.
		batch, err := stream.Recv()
		if errStatus, ok := status.FromError(err); ok && errStatus.Code() == codes.Canceled {
			break // cancel gracefully
		}
		require.NoError(stack.T, err)
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
		if errStatus, ok := status.FromError(err); ok && errStatus.Code() == codes.Canceled {
			break // cancel gracefully
		}
		require.NoError(stack.T, err)
		// Benchmark progress
		totalAssigns := atomic.AddInt64(&stack.assigns, int64(len(batch.Assignments)))
		if totalAssigns > int64(stack.opts.Assigns) {
			stack.innerCancel()
		}
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
	require.NoError(stack.T, err, "failed to connect to gRPC")
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
