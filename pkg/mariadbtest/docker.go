package mariadbtest

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Docker is a test configuration with MariaDB 10.3 running in a Docker container,
// and a local client authenticated and attached to the DB.
type Docker struct {
	Resource *dockertest.Resource
	config   *mysql.Config
}

// Assert Docker implements Backend.
var _ Backend = (*Docker)(nil)

// NewDocker creates and starts a Docker test configuration.
// It terminates the test if creation fails.
func NewDocker(t testing.TB) *Docker {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Connection to Docker")
	t.Log("Connected to Docker")
	pool.MaxWait = 2 * time.Minute
	var passBytes [16]byte
	_, err = rand.Read(passBytes[:])
	require.NoError(t, err, "Getting random password bytes")
	password := hex.EncodeToString(passBytes[:])
	runOpts := &dockertest.RunOptions{
		Repository: "mariadb",
		Tag:        "10.3-focal",
		Env: []string{
			"MYSQL_DATABASE=mariadbtest",
			"MYSQL_USER=root",
			"MYSQL_ROOT_PASSWORD=" + password,
		},
	}
	resource, err := pool.RunWithOptions(runOpts, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err, "Creating MariaDB")
	t.Log("Created MariaDB Docker container")
	sqlConfig := mysql.NewConfig()
	sqlConfig.User = "root"
	sqlConfig.Passwd = password
	sqlConfig.Net = "tcp"
	sqlConfig.Addr = "localhost:" + resource.GetPort("3306/tcp")
	sqlConfig.DBName = "mariadbtest"
	sqlConfig.AllowNativePasswords = true
	dsn := sqlConfig.FormatDSN()
	t.Log("Connecting to MySQL:", dsn)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Retry(func() error {
		if err := db.Ping(); err != nil {
			t.Log("Ping failed, retrying:", err)
			return err
		}
		return nil
	}), "Connection to MariaDB")
	return &Docker{
		Resource: resource,
		config:   sqlConfig,
	}
}

// MySQLConfig returns the base config for connecting to Dockerized MySQL.
func (m *Docker) MySQLConfig() *mysql.Config {
	return m.config
}

// DB opens the specified database.
// An empty string opens the default database.
func (m *Docker) DB(name string) (*sql.DB, error) {
	config := m.config
	config.DBName = name
	return sql.Open("mysql", config.FormatDSN())
}

// Close force removes the MariaDB container and destroys all data.
func (m *Docker) Close(t testing.TB) {
	assert.NoError(t, m.Resource.Close(), "Removing container")
}
