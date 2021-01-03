package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rs/xid"
	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/authgw"
	"go.od2.network/hive/pkg/njobs"
	"go.od2.network/hive/pkg/token"
)

var adminCmd = cobra.Command{
	Use:   "admin-tool",
	Short: "Debug utility for administering hive components",
}

func init() {
	rootCmd.AddCommand(&adminCmd)
}

var adminWorkerCmd = cobra.Command{
	Use:   "worker",
	Short: "Manage workers",
}

func init() {
	adminCmd.AddCommand(&adminWorkerCmd)
}

var adminWorkerTokenCmd = cobra.Command{
	Use:   "token",
	Short: "Manage worker tokens",
}

func init() {
	adminWorkerCmd.AddCommand(&adminWorkerTokenCmd)
}

var adminWorkerTokenCreateCmd = cobra.Command{
	Use:   "create <worker>",
	Short: "Create worker token",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		workerStr := args[0]
		workerID, err := strconv.ParseInt(workerStr, 10, 64)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Invalid worker ID:", workerID)
			os.Exit(1)
		}
		db, err := openDB()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		signer := getSigner()
		// TODO Duplicate from management API
		id := xid.New()
		ts := time.Now().Add(365 * 24 * time.Hour)
		exp, err := token.TimeToExp(ts)
		if err != nil {
			panic(err)
		}
		payload := token.Payload{
			Exp: exp,
		}
		copy(payload.ID[:], id[:])
		signedPayload, err := signer.Sign(payload)
		if err != nil {
			panic(err)
		}
		_, err = db.ExecContext(context.Background(), `
		INSERT INTO auth_tokens (id, worker_id, expires_at) VALUES (?, ?, ?);`,
			id[:], workerID, ts)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to create token:", err)
			os.Exit(1)
		}
		fmt.Println(token.Marshal(signedPayload))
	},
}

func init() {
	adminWorkerTokenCmd.AddCommand(&adminWorkerTokenCreateCmd)
}

var adminWorkerTokenVerifyCmd = cobra.Command{
	Use:   "verify <token>",
	Short: "Verify worker token",
	Args:  cobra.ExactArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		authToken := args[0]
		signedPayload := token.Unmarshal(authToken)
		if signedPayload == nil {
			fmt.Println("REJECT: Invalid token")
			return
		}
		signer := getSigner()
		if !signer.VerifyTag(signedPayload) {
			fmt.Println("REJECT: Invalid MAC signature")
			return
		}
		db, err := openDB()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		authgwDB := authgw.Database{DB: db.DB}
		tokenInfo, err := authgwDB.LookupToken(context.Background(), signedPayload.Payload.ID)
		if errors.Is(err, authgw.ErrUnknown) {
			fmt.Println("REJECT: Unknown token")
			return
		} else if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if time.Now().After(tokenInfo.ExpiresAt) {
			fmt.Println("REJECT: Token expired")
			return
		}
		fmt.Println("OK")
	},
}

func init() {
	adminWorkerTokenCmd.AddCommand(&adminWorkerTokenVerifyCmd)
}

var adminAssignerCmd = cobra.Command{
	Use:   "assigner",
	Short: "Debug njobs assigner",
}

func init() {
	adminCmd.AddCommand(&adminAssignerCmd)
}

var adminAssignerDumpRedisCmd = cobra.Command{
	Use:   "dump-redis <topic> <partition>",
	Short: "Dump Redis content",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		topic := args[0]
		partition64, err := strconv.ParseInt(args[1], 10, 32)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Invalid partition:", err)
			os.Exit(1)
		}
		partition := int32(partition64)
		partitionKeys := njobs.NewPartitionKeys(topic, partition)
		redisClient := redisClientFromEnv()
		fmt.Println("Topic:", topic)
		fmt.Println("Partition:", partition)
		ctx := context.TODO()

		fmt.Println("SessionSerial:", hex.EncodeToString([]byte(partitionKeys.SessionSerial)))
		sessionSerial, err := redisClient.HGetAll(ctx, partitionKeys.SessionSerial).Result()
		if err != nil {
			fmt.Println("\tFailed to dump", err)
		} else {
			for k, v := range sessionSerial {
				workerID := int64(binary.BigEndian.Uint64([]byte(k)))
				fmt.Printf("\t%d: %s\n", workerID, v)
			}
		}

		fmt.Println("SessionCount:", hex.EncodeToString([]byte(partitionKeys.SessionCount)))
		sessionCount, err := redisClient.HGetAll(ctx, partitionKeys.SessionCount).Result()
		if err != nil {
			fmt.Println("\tFailed to dump", err)
		} else {
			for k, v := range sessionCount {
				workerID := int64(binary.BigEndian.Uint64([]byte(k)))
				fmt.Printf("\t%d: %s\n", workerID, v)
			}
		}

		fmt.Println("SessionExpires:", hex.EncodeToString([]byte(partitionKeys.SessionExpires)))
		sessionExpires, err := redisClient.ZRangeWithScores(ctx, partitionKeys.SessionExpires, 0, -1).Result()
		if err != nil {
			fmt.Println("\tFailed to dump", err)
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
			fmt.Println("\tFailed to dump", err)
		} else {
			for k, v := range workerQuota {
				workerID := int64(binary.BigEndian.Uint64([]byte(k)))
				fmt.Printf("\t%d: %s\n", workerID, v)
			}
		}

		fmt.Println("SessionQuota:", hex.EncodeToString([]byte(partitionKeys.SessionQuota)))
		sessionQuota, err := redisClient.HGetAll(ctx, partitionKeys.SessionQuota).Result()
		if err != nil {
			fmt.Println("\tFailed to dump", err)
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
			fmt.Println("\tFailed to dump", err)
		} else {
			fmt.Printf("\t%s\n", offset)
		}

		fmt.Println("TaskAssigns:", hex.EncodeToString([]byte(partitionKeys.TaskAssigns)))
		taskAssigns, err := redisClient.HGetAll(ctx, partitionKeys.TaskAssigns).Result()
		if err != nil {
			fmt.Println("\tFailed to dump", err)
		} else {
			for k, v := range taskAssigns {
				fmt.Printf("\t%s: %s\n", k, v)
			}
		}

		fmt.Println("ActiveWorkers:", hex.EncodeToString([]byte(partitionKeys.ActiveWorkers)))
		activeWorkers, err := redisClient.ZRangeWithScores(ctx, partitionKeys.ActiveWorkers, 0, -1).Result()
		if err != nil {
			fmt.Println("\tFailed to dump", err)
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
			fmt.Println("\tFailed to dump", err)
		} else {
			for k, v := range workerOffsets {
				key := []byte(k)
				worker := binary.BigEndian.Uint64(key[:8])
				offset := binary.BigEndian.Uint64([]byte(v))
				fmt.Printf("\t%d: %d\n", worker, offset)
			}
		}

		// TODO Streams
	},
}

func init() {
	adminAssignerCmd.AddCommand(&adminAssignerDumpRedisCmd)
}
