package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rs/xid"
	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/authgw"
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
