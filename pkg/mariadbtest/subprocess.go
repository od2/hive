package mariadbtest

import (
	"database/sql"
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive/pkg/exectest"
)

// SupportsSubprocess checks if the system supports running MySQL subprocess unit tests.
func SupportsSubprocess() bool {
	_, err := os.Stat("/usr/sbin/mysqld")
	if err != nil {
		return false
	}
	_, err = os.Stat("/usr/bin/mysql_install_db")
	if err != nil {
		return false
	}
	return true
}

// Subprocess runs a local MariaDB server in a temp directory.
type Subprocess struct {
	Dir    string
	BG     *exectest.Background
	config *mysql.Config
}

// Assert Subprocess implements Backend.
var _ Backend = (*Subprocess)(nil)

// NewSubprocess spawns the test MariaDB server in the background.
func NewSubprocess(t testing.TB) *Subprocess {
	// Create temp dir.
	dir, err := ioutil.TempDir("", "mariadbtest-*")
	require.NoError(t, err, "Creating temp dir")
	// Create data dir.
	dataDir := filepath.Join(dir, "data")
	err = os.Mkdir(dataDir, 0750)
	require.NoError(t, err, "Creating data dir")
	// Read current user.
	// TODO This is not portable, but I only care about Linux.
	user := os.Getenv("USER")
	require.NotEmpty(t, user, "Reading $USER")
	// Bootstrap the MySQL data dir.
	installCmd := exec.Command("/usr/bin/mysql_install_db",
		"--user="+user,
		"--datadir="+dataDir,
		"--auth-root-authentication-method=socket",
		"--auth-root-socket-user="+user,
		"--skip-test-db",
		// "--skip-auth-anonymous-user", TODO Not supported in MariaDB 10.3
		"--skip-name-resolve",
		"--force")
	installCmd.Stdout = &exectest.PipeCapture{
		TB:     t,
		Prefix: "mysql_install_db: ",
	}
	installCmd.Stderr = &exectest.PipeCapture{
		TB:     t,
		Prefix: "mysql_install_db (stderr): ",
	}
	err = installCmd.Run()
	require.NoError(t, err, "Running mysql_install_db")
	t.Log("mariadbtest: DB path:", dataDir)
	// Start the MySQL server in the background.
	socketPath := filepath.Join(dir, "mysql.sock")
	cmd := exec.Command("/usr/sbin/mysqld",
		"--no-defaults",
		"--datadir", dataDir,
		"--skip-networking",
		"--socket", socketPath)
	bg := exectest.NewBackground(t, cmd)
	bg.Name = "mysql"
	bg.LogStdout = true
	bg.LogStderr = true
	bg.Start()
	// Formulate client config.
	config := mysql.NewConfig()
	config.Net = "unix"
	config.Addr = socketPath
	config.User = user
	// Quickly connect to ping instance for startup probes.
	startupClient, err := sql.Open("mysql", config.FormatDSN())
	require.NoError(t, err, "Client for startup probes")
	startupTicker := time.NewTicker(100 * time.Millisecond)
	defer startupTicker.Stop()
	var pingErr error
tryLoop:
	for try := 0; try < 30; try++ {
		if try > 0 {
			select {
			case <-startupTicker.C:
				break
			case <-bg.Done():
				break tryLoop
			}
		}
		pingErr = startupClient.Ping()
		if errors.Is(pingErr, os.ErrNotExist) {
			continue // MySQL hasn't opened socket yet
		} else if pingErr != nil {
			t.Fatal("Failed to ping MySQL:", pingErr.Error())
		}
		t.Log("mariadbtest: MySQL is up")
		goto started
	}
	if err := bg.Err(); err != nil {
		t.Fatal("Subprocess failed:", err)
	}
	t.Fatal("Failed to ping MySQL:", pingErr)
started:
	_, err = startupClient.Exec("CREATE DATABASE root;")
	require.NoError(t, err, "Creating initial database")
	return &Subprocess{
		Dir:    dir,
		BG:     bg,
		config: config,
	}
}

// DB opens the specified database.
// An empty string opens the default database.
func (s *Subprocess) DB(name string) (*sql.DB, error) {
	config := s.config
	if name == "" {
		name = "root"
	}
	config.DBName = name
	return sql.Open("mysql", config.FormatDSN())
}

// MySQLConfig returns the base config for connecting to the local MySQL server.
func (s *Subprocess) MySQLConfig() *mysql.Config {
	return s.config
}

// Close kills the subprocess and removes the temp dir.
func (s *Subprocess) Close(t testing.TB) {
	t.Log("mariadbtest: Removing", s.Dir)
	s.BG.Close()
	_ = os.RemoveAll(s.Dir)
}
