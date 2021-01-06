package njobs

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/redistest"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestNJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	authCtx := &auth.WorkerContext{WorkerID: 1}
	ctx = auth.WithWorkerContext(ctx, authCtx)
	rd := redistest.NewRedis(ctx, t)
	defer rd.Close()

	const topic = "test"
	const partition = int32(1)
	const worker1 = int64(1)
	const unixTime1 = int64(1)

	// Build njobs Redis client.
	scripts, err := LoadScripts(ctx, rd.Client)
	require.NoError(t, err)
	rc := RedisClient{
		Redis:         rd.Client,
		Options:       new(Options),
		PartitionKeys: NewPartitionKeys(topic, partition),
		Scripts:       scripts,
	}
	*rc.Options = DefaultOptions
	rc.Options.TaskAssignments = 1
	// Build task streamer server.
	streamer := Streamer{
		RedisClient: &rc,
		Log:         zaptest.NewLogger(t),
	}
	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()
	serv := grpc.NewServer(
		grpc.UnaryInterceptor(func(
			ctx context.Context,
			req interface{},
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (resp interface{}, err error) {
			return handler(auth.WithWorkerContext(ctx, authCtx), req)
		}),
		grpc.StreamInterceptor(func(
			srv interface{},
			ss grpc.ServerStream,
			info *grpc.StreamServerInfo,
			handler grpc.StreamHandler,
		) error {
			return handler(srv, &serverStream{
				ServerStream: ss,
				ctx:          auth.WithWorkerContext(ctx, authCtx),
			})
		}),
	)
	types.RegisterAssignmentsServer(serv, &streamer)
	go serv.Serve(lis)
	// Build assignments client.
	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(dialer), grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()
	client := types.NewAssignmentsClient(conn)

	// Build mock Kafka consumer.
	//consumer := mocks.NewConsumer(t, &sarama.Config{})
	//consumer.ExpectConsumePartition("test", 1, 0).YieldMessage()
	//consumer.ConsumePartition("test", 1, 0)

	// Try consuming messages on non-existent session.
	assignStream1, err := client.StreamAssignments(ctx, &types.StreamAssignmentsRequest{StreamId: 1})
	require.NoError(t, err)
	_, err = assignStream1.Recv()
	require.EqualError(t, err, "rpc error: code = NotFound desc = session not found")

	// Create a new session.
	sessionID1, err := streamer.EvalStartSession(ctx, worker1, unixTime1)
	require.NoError(t, err)
	t.Logf("Started worker=%d session=%d", worker1, sessionID1)

	// Allow 5 tasks.
	res, err := client.WantAssignments(ctx, &types.WantAssignmentsRequest{
		StreamId:     sessionID1,
		AddWatermark: 5,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(5), res.AddedWatermark)
	assert.Equal(t, int32(5), res.Watermark)
	t.Logf("WantAssignments: worker=%d session=%d watermark=%d", worker1, sessionID1, res.Watermark)

	// Check that client was moved to active set.
	require.NoError(t, streamer.Redis.ZScore(ctx, rc.PartitionKeys.ActiveWorkers,
		redisWorkerKey(worker1)).Err(), "worker not activated")

	// Check the pending assignment count.
	pending1Res, err := client.GetPendingAssignmentsCount(ctx, &types.GetPendingAssignmentsCountRequest{
		StreamId: sessionID1,
	})
	require.NoError(t, err)
	assert.Equal(t, pending1Res.Watermark, int32(5))

	// Check the pending assignment count for a non-existent session.
	pending2Res, err := client.GetPendingAssignmentsCount(ctx, &types.GetPendingAssignmentsCountRequest{
		StreamId: 9999,
	})
	require.NoError(t, err)
	assert.Equal(t, pending2Res.Watermark, int32(0))

	// Push 16 messages.
	msgBatch := make([]*sarama.ConsumerMessage, 16)
	for i := range msgBatch {
		msgBatch[i] = &sarama.ConsumerMessage{
			Key:       []byte(fmt.Sprintf("item_%d", i)),
			Value:     nil,
			Topic:     topic,
			Partition: partition,
			Offset:    128 + int64(i),
		}
	}
	offset, err := streamer.evalAssignTasks(ctx, msgBatch)
	assert.EqualError(t, err, "no workers available")
	require.Equal(t, int64(132), offset, "wrong tasks assigned")

	// Try consuming messages on an existing session.
	assignStream2, err := client.StreamAssignments(ctx, &types.StreamAssignmentsRequest{StreamId: sessionID1})
	require.NoError(t, err)
	t.Log("Started assignments stream")
	batch, err := assignStream2.Recv()
	require.NoError(t, err)
	assert.Len(t, batch.Assignments, 5)
	for i, assignment := range batch.Assignments {
		assert.Equal(t, int64(128+i), assignment.KafkaPointer.Offset)
	}
	t.Logf("Received batch of %d", len(batch.Assignments))
	require.NoError(t, assignStream2.CloseSend())

	// Acknowledge a few messages.
	reports := make([]*types.AssignmentReport, 4)
	for i, a := range batch.Assignments[:4] {
		var status types.TaskStatus
		if i%2 == 0 {
			status = types.TaskStatus_SUCCESS
		} else {
			status = types.TaskStatus_CLIENT_FAILURE
		}
		reports[i] = &types.AssignmentReport{
			KafkaPointer: a.KafkaPointer,
			Status:       status,
		}
	}
	_, err = client.ReportAssignments(ctx, &types.ReportAssignmentsRequest{
		Reports: reports,
	})
	require.NoError(t, err)

	// Read all results from Redis.
	results1, err := rc.Redis.XRange(ctx, rc.PartitionKeys.Results, "-", "+").Result()
	require.NoError(t, err)
	for i := range results1 {
		require.NoError(t, rc.Redis.XDel(ctx, rc.PartitionKeys.Results, results1[i].ID).Err())
		results1[i].ID = ""
		delete(results1[i].Values, "exp_time")
	}
	assert.Equal(t, []redis.XMessage{
		{Values: map[string]interface{}{
			"item":   "item_0",
			"offset": "128",
			"status": "0",
			"worker": "1",
		}},
		{Values: map[string]interface{}{
			"item":   "item_1",
			"offset": "129",
			"status": "1",
			"worker": "1",
		}},
		{Values: map[string]interface{}{
			"item":   "item_2",
			"offset": "130",
			"status": "0",
			"worker": "1",
		}},
		{Values: map[string]interface{}{
			"item":   "item_3",
			"offset": "131",
			"status": "1",
			"worker": "1",
		}},
	}, results1)

	// Read worker queue.
	queue1, err := rc.Redis.XRange(ctx, rc.PartitionKeys.WorkerQueue(worker1), "-", "+").Result()
	require.NoError(t, err)
	for i := range queue1 {
		delete(queue1[i].Values, "exp_time")
	}
	assert.Equal(t, []redis.XMessage{
		{
			ID: "132-1",
			Values: map[string]interface{}{
				"item": "item_4",
			},
		},
	}, queue1)

	// Shut down session.
	require.NoError(t, streamer.EvalStopSession(ctx, worker1, sessionID1))
	t.Logf("Stopped session %d for worker %d", sessionID1, worker1)

	// Read all results from Redis.
	results3, err := rc.Redis.XRange(ctx, rc.PartitionKeys.Results, "-", "+").Result()
	require.NoError(t, err)
	for i := range results3 {
		require.NoError(t, rc.Redis.XDel(ctx, rc.PartitionKeys.Results, results3[i].ID).Err())
		results3[i].ID = ""
		delete(results3[i].Values, "exp_time")
	}
	assert.Equal(t, []redis.XMessage{
		{Values: map[string]interface{}{
			"item":   "item_4",
			"offset": "132",
			"status": "2",
			"worker": "1",
		}},
	}, results3)
}

type serverStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the embedded context.
func (s *serverStream) Context() context.Context {
	return s.ctx
}
