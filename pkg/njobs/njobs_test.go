package njobs

import (
	"context"
	"encoding/binary"
	"net"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive/pkg/redistest"
	"go.od2.network/hive/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestNJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", "1")
	rd := redistest.NewRedis(ctx, t)
	defer rd.Close()

	const topic = "test"
	const partition = int32(1)
	const worker1 = int64(1)

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
	rc.Options.N = 1
	// Build task streamer server.
	streamer := Streamer{
		RedisClient: &rc,
	}
	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()
	serv := grpc.NewServer()
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
	sessionID1, err := streamer.EvalStartSession(ctx, worker1)
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

	// Push 16 messages.
	msgBatch := make([]*sarama.ConsumerMessage, 16)
	for i := range msgBatch {
		msgBatch[i] = &sarama.ConsumerMessage{
			Key:       kafkaMessageKey(32 + int64(i)),
			Value:     nil,
			Topic:     topic,
			Partition: partition,
			Offset:    32 + int64(i),
		}
	}
	offset, err := streamer.evalAssignTasks(ctx, msgBatch)
	assert.EqualError(t, err, "no workers available")
	require.Equal(t, int64(36), offset, "wrong tasks assigned")

	// Try consuming messages on an existing session.
	assignStream2, err := client.StreamAssignments(ctx, &types.StreamAssignmentsRequest{StreamId: sessionID1})
	require.NoError(t, err)
	t.Log("Started assignments stream")
	batch, err := assignStream2.Recv()
	require.NoError(t, err)
	assert.Len(t, batch.Assignments, 5)
	for i, assignment := range batch.Assignments {
		assert.Equal(t, int64(32+i), assignment.KafkaOffset)
	}
	t.Logf("Received batch of %d", len(batch.Assignments))
	require.NoError(t, assignStream2.CloseSend())

	// Shut down session.
	require.NoError(t, streamer.EvalStopSession(ctx, worker1, sessionID1))
	t.Logf("Stopped session %d for worker %d", sessionID1, worker1)
}

func kafkaMessageKey(itemID int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(itemID))
	return buf
}
