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
	"github.com/stretchr/testify/require"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/authgw"
	"go.od2.network/hive/pkg/redistest"
	"go.od2.network/hive/pkg/token"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestBenchmark(t *testing.T) {
	type testCaseDef struct {
		name  string
		short bool
		opts  *benchOptions
	}
	cases := []testCaseDef{
		{
			name:  "Tiny",
			short: true,
			opts: &benchOptions{
				Options:  DefaultOptions,
				Workers:  1,
				Sessions: 1,
				Assigns:  2,
				QoS:      4,
			},
		},
		{
			name:  "Warmup",
			short: false,
			opts: &benchOptions{
				Options:  DefaultOptions,
				Workers:  32,
				Sessions: 2,
				Assigns:  50000,
				QoS:      128,
			},
		},
		{
			name:  "Assign100000_Sessions64_Batch512",
			short: false,
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
			name:  "Assign100000_Sessions64_Batch1024",
			short: false,
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
			name:  "Assign100000_Sessions64_Batch1024",
			short: false,
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
		{
			name:  "Assign100000_Sessions1024_Batch32",
			short: false,
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
					DeliverBatch:           32,
				},
				Workers:  512,
				Sessions: 2,
				Assigns:  100000,
				QoS:      32,
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if !testCase.short {
				t.Skip("Skipping benchmark in short mode")
			}
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
	opts        *benchOptions
	ctx         context.Context
	cancel      context.CancelFunc
	innerCtx    context.Context
	innerCancel context.CancelFunc
	// Modules
	signer   token.Signer
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
	const topic, partition = "test", int32(2)

	// Build njobs Redis client.
	scripts, err := LoadScripts(ctx, redis.Client)
	require.NoError(t, err)
	rc := &RedisClient{
		Redis:         redis.Client,
		Options:       &opts.Options,
		PartitionKeys: NewPartitionKeys(topic, partition),
		Scripts:       scripts,
	}
	// Build assigner.
	assigner := &Assigner{
		RedisClient: rc,
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
	signer := token.NewSimpleSigner(new([32]byte))
	interceptor := auth.WorkerAuthInterceptor{
		Backend: simpleTokenBackend{},
		Signer:  signer,
		Log:     zaptest.NewLogger(t),
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.Unary()),
		grpc.StreamInterceptor(interceptor.Stream()),
	)
	types.RegisterAssignmentsServer(server, streamer)
	// Connect to Redis.
	return &benchStack{
		// Environment
		T:           t,
		Redis:       redis,
		RedisClient: rc,
		signer:      signer,
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
	// Build fake Kafka message stream.
	msgs := make(chan *sarama.ConsumerMessage)
	wg.Add(1)
	go func() {
		defer wg.Done()
		msgCount := int64(0)
		for {
			msg := &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Key:       []byte(strconv.FormatUint(uint64(0x100)+uint64(msgCount), 16)),
				Value:     nil,
				Topic:     "test",
				Partition: 2,
				Offset:    msgCount * 16,
			}
			msgCount++
			select {
			case <-stack.innerCtx.Done():
				return
			case msgs <- msg:
				break // continue
			}
		}
	}()
	// Run task assigner.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stack.innerCancel()
		err := stack.assigner.Run(msgs)
		t.Log("Sarama claim consumer exited with:", err)
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
	exp, err := token.TimeToExp(time.Now().Add(16 * time.Hour))
	require.NoError(stack.T, err)
	payload := token.Payload{
		Exp: exp,
	}
	// Put the worker ID into the first 8 bytes.
	binary.BigEndian.PutUint64(payload.ID[:], uint64(workerID))
	sp, err := stack.signer.Sign(payload)
	require.NoError(stack.T, err)
	workerCredentials := auth.WorkerCredentials{Token: token.Marshal(sp)}
	conn, err := grpc.DialContext(stack.ctx, "bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(&workerCredentials))
	require.NoError(stack.T, err, "failed to connect to gRPC")
	return types.NewAssignmentsClient(conn), conn
}

type simpleTokenBackend struct{}

func (simpleTokenBackend) LookupToken(_ context.Context, id token.ID) (*authgw.TokenInfo, error) {
	workerID := int64(binary.BigEndian.Uint64(id[:]))
	return &authgw.TokenInfo{
		WorkerID: workerID,
		Valid:    true,
	}, nil
}
