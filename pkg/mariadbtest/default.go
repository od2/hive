// Package mariadbtest constructs short-lived MariaDB instances for unit-testing.
//
// Available backends: Subprocess (local mysqld), Docker.
package mariadbtest

import (
	"database/sql"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// Backend is an available MariaDB test backend.
type Backend interface {
	MySQLConfig() *mysql.Config
	DB(name string) (*sql.DB, error)
	Close(t testing.TB)
}

// Default constructs a MariaDB server/client session
// from the fastest available backend.
func Default(t testing.TB) Backend {
	if SupportsSubprocess() {
		t.Log("mariadbtest: MySQL server installed, using subprocess")
		return NewSubprocess(t)
	}
	t.Log("mariadbtest: Falling back to Docker")
	return NewDocker(t)
}
