package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rs/xid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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

var workerTokenCmd = cobra.Command{
	Use:   "token",
	Short: "Manage worker tokens",
}

func init() {
	adminWorkerCmd.AddCommand(&workerTokenCmd)
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
		db, err := sql.Open("mysql", viper.GetString(ConfMySQLDSN))
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
	workerTokenCmd.AddCommand(&adminWorkerTokenCreateCmd)
}
