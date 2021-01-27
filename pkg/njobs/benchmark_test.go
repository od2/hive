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
	"github.com/Shopify/sarama/mocks"
	"github.com/cenkalti/backoff/v4"
	"github.com/pelletier/go-toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive-api"
	"go.od2.network/hive-worker"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/authgw"
	"go.od2.network/hive/pkg/redistest"
	"go.od2.network/hive/pkg/saramamock"
	"go.od2.network/hive/pkg/token"
	"go.od2.network/hive/pkg/topology"
	"go.od2.network/hive/pkg/topology/redisshard"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
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
				Collection: `Name = "test"`,
				Workers:    6,
				Sessions:   1,
				Assigns:    2,
				QoS:        4,
			},
		},
		{
			name:  "Warmup",
			short: false,
			opts: &benchOptions{
				Collection: `Name = "test"`,
				Workers:    32,
				Sessions:   2,
				Assigns:    50000,
				QoS:        128,
			},
		},
		{
			name:  "Assign100000_Sessions64_Batch512",
			short: false,
			opts: &benchOptions{
				Collection: `
Name = "test"
PKType = "BIGINT"
AssignBatch = 512
`,
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
				Collection: `
Name = "test"
PKType = "BIGINT"
AssignBatch = 1024
`,
				Workers:  32,
				Sessions: 2,
				Assigns:  100000,
				QoS:      1024,
			},
		},
		{
			name:  "Assign100000_Sessions64_Batch2048",
			short: false,
			opts: &benchOptions{
				Collection: `
Name = "test"
PKType = "BIGINT"
AssignBatch = 2048
`,
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
				Collection: `
Name = "test"
PKType = "BIGINT"
AssignBatch = 32
`,
				Workers:  512,
				Sessions: 2,
				Assigns:  100000,
				QoS:      32,
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if !testCase.short && testing.Short() {
				t.Skip("Skipping benchmark in short mode")
			}
			runBenchmark(t, testCase.opts)
		})
	}
}

type benchOptions struct {
	// Pipeline settings
	Collection string
	Topic      string
	Partition  int32
	// Benchmark settings
	Workers  uint // Number of concurrent workers
	Sessions uint // Number of sessions per worker
	QoS      uint // Minimum number of pre-sent assignments
	Assigns  uint // Number of jobs in total
}

type benchStack struct {
	// Environment
	T           *testing.T
	Log         *zap.Logger
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
	assigns int64
}

func newBenchStack(t *testing.T, opts *benchOptions) *benchStack {
	ctx, cancel := context.WithCancel(context.Background())
	redis := redistest.NewRedis(ctx, t)
	innerCtx, innerCancel := context.WithCancel(ctx)

	// Fake topology.
	redisFactory := &redisshard.StandaloneFactory{Redis: redis.Client}
	shard := topology.Shard{Collection: "test", Partition: int32(2)}
	rd, err := redisFactory.GetShard(shard)
	require.NoError(t, err)
	collection := new(topology.Collection)
	require.NoError(t, toml.Unmarshal([]byte(opts.Collection), collection), "Collection config")
	require.NotEmpty(t, collection.Name, "Collection name")
	topo := &topology.Config{
		Collections: []*topology.Collection{collection},
		RedisShardFactory: &topology.RedisShardFactory{
			Type:       "Standalone",
			Standalone: topology.RedisShardFactoryStandalone{Client: rd},
		},
	}

	// Build assigner.
	assigner := &Assigner{
		RedisFactory: redisFactory,
		Producer:     mocks.NewSyncProducer(t, sarama.NewConfig()),
		Topology:     topo,
		Log:          zaptest.NewLogger(t),
	}
	// Build streamer.
	streamer := &Streamer{
		Topology: topo,
		Factory:  redisFactory,
		Log:      zap.NewNop(),
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
		Log:     zaptest.NewLogger(t, zaptest.Level(zap.InfoLevel)),
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.Unary()),
		grpc.StreamInterceptor(interceptor.Stream()),
	)
	hive.RegisterAssignmentsServer(server, streamer)
	// Connect to Redis.
	return &benchStack{
		// Environment
		T:     t,
		Log:   zaptest.NewLogger(t),
		Redis: redis,
		RedisClient: &RedisClient{
			Redis:         rd,
			Collection:    collection,
			PartitionKeys: PartitionKeys{},
		},
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
		claim := &saramamock.ConsumerGroupClaimChan{
			MsgChan: msgs,
			MTopic:  "test.tasks",
		}
		session := &saramamock.ConsumerGroupSession{}
		err := stack.assigner.ConsumeClaim(session, claim)
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
				t.Logf("Stats: assigns=%d", atomic.LoadInt64(&stack.assigns))
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

	var simpleWorker = &worker.Simple{
		Assignments:   client,
		Log:           zap.NewNop(),
		Handler:       &noopHandler{stack},
		Collection:    "test",
		Routines:      1,
		Prefetch:      4,
		GracePeriod:   10 * time.Second,
		FillRate:      1, // fill as fast as possible
		StreamBackoff: new(backoff.StopBackOff),
		ReportRate:    1 * time.Second,
		ReportBatch:   128,
	}
	assert.NoError(stack.T, simpleWorker.Run(ctx))
}

type noopHandler struct {
	*benchStack
}

// WorkAssignment does nothing and returns TaskStatus_SUCCESS.
func (n *noopHandler) WorkAssignment(context.Context, *hive.Assignment) hive.TaskStatus {
	if atomic.AddInt64(&n.assigns, 1) > int64(n.opts.Assigns) {
		n.cancel()
	}
	return hive.TaskStatus_SUCCESS
}

func (stack *benchStack) newClient(workerID int64) (hive.AssignmentsClient, io.Closer) {
	dialer := func(context.Context, string) (net.Conn, error) {
		return stack.listener.Dial()
	}
	var tokenID [12]byte
	// Put the worker ID into the first 8 bytes.
	binary.BigEndian.PutUint64(tokenID[:], uint64(workerID))
	sp, err := stack.signer.Sign(tokenID)
	require.NoError(stack.T, err)
	workerCredentials := hive.WorkerCredentials{Token: token.Marshal(sp)}
	conn, err := grpc.DialContext(stack.ctx, "bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(&workerCredentials))
	require.NoError(stack.T, err, "failed to connect to gRPC")
	return hive.NewAssignmentsClient(conn), conn
}

type simpleTokenBackend struct{}

func (simpleTokenBackend) LookupToken(_ context.Context, id token.ID) (*authgw.TokenInfo, error) {
	workerID := int64(binary.BigEndian.Uint64(id[:]))
	return &authgw.TokenInfo{
		WorkerID: workerID,
		Valid:    true,
	}, nil
}
