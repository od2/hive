package management

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive-api"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/mariadbtest"
	"go.od2.network/hive/pkg/token"
	"go.uber.org/zap/zaptest"
)

func TestHandler(t *testing.T) {
	mdb := mariadbtest.NewDocker(t)
	defer mdb.Close(t)
	signer := token.NewSimpleSigner(new([32]byte))
	log := zaptest.NewLogger(t)
	h := &Handler{
		DB:     mdb.DB.DB,
		Signer: signer,
		Log:    log,
	}
	ctx := context.Background()
	var user = auth.GitHubUser{ID: 123, Login: "terorie"}
	ctx = auth.WithWebContext(ctx, &user)
	// Create table.
	_, goFile, _, _ := runtime.Caller(0)
	sqlInit, err := ioutil.ReadFile(filepath.Join(goFile, "../../../sql/0001-create-tokens-db.sql"))
	require.NoError(t, err)
	_, err = h.DB.Exec(string(sqlInit))
	require.NoError(t, err)
	// Create four tokens.
	tokenIDs := make([]string, 4)
	for i := 0; i < 4; i++ {
		req := &hive.CreateWorkerTokenRequest{
			Description: fmt.Sprintf("Token %d", i),
		}
		createToken, err := h.CreateWorkerToken(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, createToken)
		require.NotNil(t, createToken.Token)
		t.Logf("Created token %d: %s", i, createToken.Key)
		assert.Len(t, createToken.Key, token.MarshalledSize)
		assert.Len(t, createToken.Token.Id, 16)
		assert.Equal(t, createToken.Token.Description, req.Description)
		assert.Len(t, createToken.Token.TokenBit, 4)
		assert.NotEqual(t, int64(0), createToken.Token.GetCreatedAt().Seconds)
		assert.Equal(t, int64(0), createToken.Token.GetLastUsedAt().Seconds)
		tokenIDs[i] = createToken.Token.Id
	}
	// Delete one token.
	revoke, err := h.RevokeWorkerToken(ctx, &hive.RevokeWorkerTokenRequest{TokenId: tokenIDs[3]})
	require.NoError(t, err)
	require.NotNil(t, revoke)
	assert.True(t, revoke.GetFound())
	t.Log("Removed token 3")
	// Revoke for revoked token should fail.
	revoke, err = h.RevokeWorkerToken(ctx, &hive.RevokeWorkerTokenRequest{TokenId: tokenIDs[3]})
	require.NoError(t, err)
	require.NotNil(t, revoke)
	assert.False(t, revoke.GetFound())
	t.Log("Confirmed token 3 does not exist")
	// Mark one token as used.
	lastUsedUpdate, err := h.DB.Exec("UPDATE auth_tokens SET last_used_at = ? WHERE id = FROM_BASE64(?);",
		time.Now(), tokenIDs[0])
	require.NoError(t, err)
	lastUsedUpdateRows, err := lastUsedUpdate.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), lastUsedUpdateRows)
	// List tokens.
	tokenList, err := h.ListWorkerTokens(ctx, &hive.ListWorkerTokensRequest{})
	require.NoError(t, err)
	require.NotNil(t, tokenList)
	assert.Len(t, tokenList.GetTokens(), 3)
	for _, workerToken := range tokenList.GetTokens() {
		assert.Len(t, workerToken.Id, 16)
		assert.NotEmpty(t, workerToken.Description)
		assert.Len(t, workerToken.TokenBit, 4)
		assert.NotEqual(t, int64(0), workerToken.GetCreatedAt().Seconds)
		if workerToken.Id != tokenIDs[0] {
			assert.Equal(t, int64(0), workerToken.GetLastUsedAt().Seconds)
		} else {
			assert.NotEqual(t, int64(0), workerToken.GetLastUsedAt().Seconds)
		}
	}
	t.Log("Listed the existing tokens")
	// Revoke all tokens.
	_, err = h.RevokeAllWorkerTokens(ctx, &hive.RevokeAllWorkerTokensRequest{})
	require.NoError(t, err)
	t.Log("Revoked all tokens")
	// List tokens (should be empty).
	tokenList, err = h.ListWorkerTokens(ctx, &hive.ListWorkerTokensRequest{})
	require.NoError(t, err)
	require.NotNil(t, tokenList)
	assert.Len(t, tokenList.GetTokens(), 0)
	t.Log("Confirmed no tokens exist anymore")
}
