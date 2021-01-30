package mariadbtest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubprocess(t *testing.T) {
	_, err := os.Stat("/usr/sbin/mysqld")
	if os.IsNotExist(err) {
		t.Skip("No MySQL server program found")
	}
	sub := NewSubprocess(t)
	defer sub.Close(t)
	db, err := sub.DB("")
	require.NoError(t, err)
	require.NoError(t, db.Ping())
}
