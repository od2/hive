package items

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var predefinedDB *sqlx.DB

func TestMain(m *testing.M) {
	sqlConnStr := flag.String("sql-conn", "", "SQL connection string")
	flag.Parse()
	if *sqlConnStr != "" {
		cfg, err := mysql.ParseDSN(*sqlConnStr)
		if err != nil {
			panic(err)
		}
		cfg.ParseTime = true
		cfg.Loc = time.Local
		predefinedDB, err = sqlx.Open("mysql", cfg.FormatDSN())
		if err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}
