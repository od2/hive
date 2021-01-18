package mariadbtest

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Docker is a test configuration with MariaDB 10.3 running in a Docker container,
// and a local client authenticated and attached to the DB.
type Docker struct {
	Resource *dockertest.Resource
	DB       *sqlx.DB
}

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
			"MYSQL_DATABASE=hive",
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
	sqlConfig := mysql.Config{
		User:   "root",
		Passwd: password,
		Net:    "tcp",
		Addr:   "localhost:" + resource.GetPort("3306/tcp"),
		DBName: "hive",

		ParseTime:            true,
		Loc:                  time.Local,
		AllowNativePasswords: true,
	}
	dsn := sqlConfig.FormatDSN()
	t.Log("Connecting to MySQL:", dsn)
	db, err := sqlx.Open("mysql", dsn)
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
		DB:       db,
	}
}

// Close force removes the MariaDB container and destroys all data.
func (m *Docker) Close(t testing.TB) {
	assert.NoError(t, m.Resource.Close(), "Removing container")
}
