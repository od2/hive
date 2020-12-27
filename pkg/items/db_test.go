package items

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

var sqlConnStr string

func TestMain(m *testing.M) {
	flag.StringVar(&sqlConnStr, "sql-conn", "", "SQL connection string")
	flag.Parse()
	os.Exit(m.Run())
}

func connect(t *testing.T) *sqlx.DB {
	if sqlConnStr == "" {
		t.Skip("SQL connection flag not set, skipping")
		return nil
	}
	// Force Go-compatible time handling.
	cfg, err := mysql.ParseDSN(sqlConnStr)
	require.NoError(t, err, "Invalid DSN")
	cfg.ParseTime = true
	cfg.Loc = time.Local
	// Connect
	db, err := sqlx.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	return db
}
