package admintool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
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

var workerVerifyTokenCmd = cobra.Command{
	Use:   "verify_token <token>",
	Short: "Verify worker token",
	Args:  cobra.ExactArgs(1),
	Run:   providers.NewCmd(runWorkerVerifyToken),
}

func init() {
	workerCmd.AddCommand(&workerVerifyTokenCmd)
}

func runWorkerVerifyToken(args []string, db *sqlx.DB, signer token.Signer) {
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
	tokenInfo, err := authgwDB.LookupToken(context.Background(), signedPayload.ID)
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
