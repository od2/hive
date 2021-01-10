package admin_tool

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/cobra"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/njobs"
)

var assignerCmd = cobra.Command{
	Use:   "assigner",
	Short: "Debug njobs assigner",
}

func init() {
	Cmd.AddCommand(&assignerCmd)
}

var assignerDumpRedisCmd = cobra.Command{
	Use:   "dump-redis <topic> <partition>",
	Short: "Dump Redis content",
	Args:  cobra.ExactArgs(2),
	Run:   providers.NewCmd(runAssignerDumpRedis),
}

func init() {
	assignerCmd.AddCommand(&assignerDumpRedisCmd)
}

func runAssignerDumpRedis(
	args []string,
	redisClient *redis.Client,
) {
	topic := args[0]
	partition64, err := strconv.ParseInt(args[1], 10, 32)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Invalid partition:", err)
		os.Exit(1)
	}
	partition := int32(partition64)
	partitionKeys := njobs.NewPartitionKeys(topic, partition)
	fmt.Println("Topic:", topic)
	fmt.Println("Partition:", partition)
	ctx := context.TODO()

	fmt.Println("SessionSerial:", hex.EncodeToString([]byte(partitionKeys.SessionSerial)))
	sessionSerial, err := redisClient.HGetAll(ctx, partitionKeys.SessionSerial).Result()
	if err != nil {
		fmt.Println("\tFailed to dump:", err)
	} else {
		for k, v := range sessionSerial {
			workerID := int64(binary.BigEndian.Uint64([]byte(k)))
			fmt.Printf("\t%d: %s\n", workerID, v)
		}
	}

	fmt.Println("SessionCount:", hex.EncodeToString([]byte(partitionKeys.SessionCount)))
	sessionCount, err := redisClient.HGetAll(ctx, partitionKeys.SessionCount).Result()
	if err != nil {
		fmt.Println("\tFailed to dump:", err)
	} else {
		for k, v := range sessionCount {
			workerID := int64(binary.BigEndian.Uint64([]byte(k)))
			fmt.Printf("\t%d: %s\n", workerID, v)
		}
	}

	fmt.Println("SessionExpires:", hex.EncodeToString([]byte(partitionKeys.SessionExpires)))
	sessionExpires, err := redisClient.ZRangeWithScores(ctx, partitionKeys.SessionExpires, 0, -1).Result()
	if err != nil {
		fmt.Println("\tFailed to dump:", err)
	} else {
		for _, z := range sessionExpires {
			member := []byte(z.Member.(string))
			worker := binary.BigEndian.Uint64(member[:8])
			sessionID := binary.BigEndian.Uint64(member[8:])
			fmt.Printf("\t%d/%d: %f\n", worker, sessionID, z.Score)
		}
	}

	fmt.Println("WorkerQuota:", hex.EncodeToString([]byte(partitionKeys.WorkerQuota)))
	workerQuota, err := redisClient.HGetAll(ctx, partitionKeys.WorkerQuota).Result()
	if err != nil {
		fmt.Println("\tFailed to dump:", err)
	} else {
		for k, v := range workerQuota {
			workerID := int64(binary.BigEndian.Uint64([]byte(k)))
			fmt.Printf("\t%d: %s\n", workerID, v)
		}
	}

	fmt.Println("SessionQuota:", hex.EncodeToString([]byte(partitionKeys.SessionQuota)))
	sessionQuota, err := redisClient.HGetAll(ctx, partitionKeys.SessionQuota).Result()
	if err != nil {
		fmt.Println("\tFailed to dump:", err)
	} else {
		for k, v := range sessionQuota {
			key := []byte(k)
			worker := binary.BigEndian.Uint64(key[:8])
			sessionID := binary.BigEndian.Uint64(key[8:])
			fmt.Printf("\t%d/%d: %s\n", worker, sessionID, v)
		}
	}

	fmt.Println("Offset:", hex.EncodeToString([]byte(partitionKeys.Offset)))
	offset, err := redisClient.Get(ctx, partitionKeys.Offset).Result()
	if err != nil {
		fmt.Println("\tFailed to dump:", err)
	} else {
		fmt.Printf("\t%s\n", offset)
	}

	fmt.Println("TaskAssigns:", hex.EncodeToString([]byte(partitionKeys.TaskAssigns)))
	taskAssigns, err := redisClient.HGetAll(ctx, partitionKeys.TaskAssigns).Result()
	if err != nil {
		fmt.Println("\tFailed to dump:", err)
	} else {
		for k, v := range taskAssigns {
			fmt.Printf("\t%s: %s\n", k, v)
		}
	}

	fmt.Println("ActiveWorkers:", hex.EncodeToString([]byte(partitionKeys.ActiveWorkers)))
	activeWorkers, err := redisClient.ZRangeWithScores(ctx, partitionKeys.ActiveWorkers, 0, -1).Result()
	if err != nil {
		fmt.Println("\tFailed to dump:", err)
	} else {
		for _, z := range activeWorkers {
			member := []byte(z.Member.(string))
			worker := binary.BigEndian.Uint64(member[:8])
			fmt.Printf("\t%d: %f\n", worker, z.Score)
		}
	}

	fmt.Println("WorkerOffsets:", hex.EncodeToString([]byte(partitionKeys.WorkerOffsets)))
	workerOffsets, err := redisClient.HGetAll(ctx, partitionKeys.WorkerOffsets).Result()
	if err != nil {
		fmt.Println("\tFailed to dump:", err)
	} else {
		for k, v := range workerOffsets {
			key := []byte(k)
			worker := binary.BigEndian.Uint64(key[:8])
			offset := binary.BigEndian.Uint64([]byte(v))
			fmt.Printf("\t%d: %d\n", worker, offset)
		}
	}

	fmt.Println("Results:", hex.EncodeToString([]byte(partitionKeys.Results)))
	resultStream, err := redisClient.XRange(ctx, partitionKeys.Results, "-", "+").Result()
	if err != nil {
		fmt.Println("\tFailed to dump:", err)
	} else {
		buf, err := json.MarshalIndent(resultStream, "\t", "  ")
		if err != nil {
			fmt.Println("\tFailed to dump:", err)
		} else {
			fmt.Print("\t")
			fmt.Println(string(buf))
		}
	}

	// TODO Worker Streams
}

var assignerDumpKafkaCmd = cobra.Command{
	Use:   "dump-kafka <topic> <partition>",
	Short: "Dump Kafka content",
	Args:  cobra.ExactArgs(2),
	Run:   providers.NewCmd(runAssignerDumpKafka),
}

func init() {
	assignerCmd.AddCommand(&assignerDumpKafkaCmd)
}

func runAssignerDumpKafka(
	args []string,
	client sarama.Client,
) {
	topic := args[0]
	partition64, err := strconv.ParseInt(args[1], 10, 32)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Invalid partition:", err)
		os.Exit(1)
	}
	partition := int32(partition64)
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create consumer:", err)
		os.Exit(1)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "Failed to close consumer:", err)
		}
	}()
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create partition consumer:", err)
		os.Exit(1)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "Failed to close partition consumer:", err)
		}
	}()
	type message struct {
		Offset int64  `json:"offset"`
		Key    string `json:"key"`
	}
	enc := json.NewEncoder(os.Stdout)
	for msg := range partitionConsumer.Messages() {
		if err := enc.Encode(&message{
			Offset: msg.Offset,
			Key:    string(msg.Key),
		}); err != nil {
			panic(err)
		}
	}
}
