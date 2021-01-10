package admin_tool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/xid"
	"github.com/spf13/cobra"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/authgw"
	"go.od2.network/hive/pkg/token"
)

var workerCmd = cobra.Command{
	Use:   "worker",
	Short: "Manage workers",
}

func init() {
	Cmd.AddCommand(&workerCmd)
}

var workerTokenCmd = cobra.Command{
	Use:   "token",
	Short: "Manage worker tokens",
}

func init() {
	workerCmd.AddCommand(&workerTokenCmd)
}

var workerTokenCreateCmd = cobra.Command{
	Use:   "create <worker>",
	Short: "Create worker token",
	Args:  cobra.ExactArgs(1),
	Run:   providers.NewCmd(runWorkerTokenCreate),
}

func init() {
	workerTokenCmd.AddCommand(&workerTokenCreateCmd)
}

func runWorkerTokenCreate(
	args []string,
	db *sqlx.DB,
	signer token.Signer,
) {
	workerStr := args[0]
	workerID, err := strconv.ParseInt(workerStr, 10, 64)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Invalid worker ID:", workerID)
		os.Exit(1)
	}
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
}

var workerTokenVerifyCmd = cobra.Command{
	Use:   "verify <token>",
	Short: "Verify worker token",
	Args:  cobra.ExactArgs(1),
	Run:   providers.NewCmd(runWorkerTokenVerify),
}

func init() {
	workerTokenCmd.AddCommand(&workerTokenVerifyCmd)
}

func runWorkerTokenVerify(
	args []string,
	db *sqlx.DB,
	signer token.Signer,
) {
	authToken := args[0]
	signedPayload := token.Unmarshal(authToken)
	if signedPayload == nil {
		fmt.Println("REJECT: Invalid token")
		return
	}
	if !signer.VerifyTag(signedPayload) {
		fmt.Println("REJECT: Invalid MAC signature")
		return
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
}
