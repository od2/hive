package management

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive-api"
	"go.od2.network/hive-api/web"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/items"
	"go.od2.network/hive/pkg/mariadbtest"
	"go.od2.network/hive/pkg/token"
	"go.od2.network/hive/pkg/topology"
	"go.uber.org/zap/zaptest"
)

func TestHandler(t *testing.T) {
	// Bootstrap database.
	testDB := mariadbtest.Default(t)
	defer testDB.Close(t)
	testDBConfig := testDB.MySQLConfig()
	testDBConfig.ParseTime = true
	testDBConfig.Loc = time.Local
	db, err := testDB.DB("")
	require.NoError(t, err)
	dbx := sqlx.NewDb(db, "mysql")
	signer := token.NewSimpleSigner(new([32]byte))
	log := zaptest.NewLogger(t)
	// Build topology.
	topo := &topology.Config{}
	topo.Collections = []*topology.Collection{
		{
			Name:   "yt.videos",
			PKType: "BIGINT",
		},
		{
			Name:   "some.collection",
			PKType: "INT",
		},
	}
	factory := items.Factory{
		DB:       dbx,
		Topology: topo,
	}
	for i, coll := range topo.Collections {
		store, err := factory.GetStore(coll.Name)
		require.NoError(t, err)
		require.NoError(t, store.CreateTable(context.Background()))
		// Insert a bunch of rows.
		count := (i + 1) * 128
		ptrs := make([]*hive.ItemPointer, count)
		for j := 0; j < count; j++ {
			ptrs[j] = &hive.ItemPointer{
				Dst: &hive.ItemLocator{
					Collection: coll.Name,
					Id:         strconv.FormatInt(int64(j), 10),
				},
				Timestamp: ptypes.TimestampNow(),
			}
		}
		require.NoError(t, store.InsertDiscovered(context.Background(), ptrs))
		t.Logf("Inserted %d items to %s", count, coll.Name)
	}
	h := &Handler{
		DB:       dbx,
		Signer:   signer,
		Log:      log,
		Topology: topo,
		Items:    &factory,
	}
	ctx := context.Background()
	var user = auth.WebIdentity{ID: 123, Name: "terorie"}
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
		req := &web.CreateWorkerTokenRequest{
			Description: fmt.Sprintf("Token %d", i),
		}
		createToken, err := h.CreateWorkerToken(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, createToken)
		require.NotNil(t, createToken.Token)
		t.Logf("Created token %d ID %s: %s", i, createToken.Token.Id, createToken.Key)
		assert.Len(t, createToken.Key, token.MarshalledSize)
		assert.Len(t, createToken.Token.Id, 22)
		assert.Equal(t, createToken.Token.Description, req.Description)
		assert.Len(t, createToken.Token.TokenBit, 4)
		assert.NotEqual(t, int64(0), createToken.Token.GetCreatedAt().Seconds)
		assert.Equal(t, int64(0), createToken.Token.GetLastUsedAt().Seconds)
		tokenIDs[i] = createToken.Token.Id
	}
	// Delete one token.
	t.Logf("Revoking token %s", tokenIDs[3])
	revoke, err := h.RevokeWorkerToken(ctx, &web.RevokeWorkerTokenRequest{TokenId: tokenIDs[3]})
	require.NoError(t, err)
	require.NotNil(t, revoke)
	assert.True(t, revoke.GetFound())
	t.Log("Removed token 3")
	// Revoke for revoked token should fail.
	revoke, err = h.RevokeWorkerToken(ctx, &web.RevokeWorkerTokenRequest{TokenId: tokenIDs[3]})
	require.NoError(t, err)
	require.NotNil(t, revoke)
	assert.False(t, revoke.GetFound())
	t.Log("Confirmed token 3 does not exist")
	// Mark one token as used.
	t.Log("Marking token as used", tokenIDs[0])
	var tokenIDBin0 token.ID
	n, err := base64.RawStdEncoding.Decode(tokenIDBin0[:], []byte(tokenIDs[0]))
	require.NoError(t, err)
	assert.Equal(t, 16, n)
	lastUsedUpdate, err := h.DB.Exec("UPDATE auth_tokens SET last_used_at = ? WHERE id = ?;",
		time.Now(), tokenIDBin0[:])
	require.NoError(t, err)
	lastUsedUpdateRows, err := lastUsedUpdate.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), lastUsedUpdateRows)
	// List tokens.
	tokenList, err := h.ListWorkerTokens(ctx, &web.ListWorkerTokensRequest{})
	require.NoError(t, err)
	require.NotNil(t, tokenList)
	assert.Len(t, tokenList.GetTokens(), 3)
	for _, workerToken := range tokenList.GetTokens() {
		assert.Len(t, workerToken.Id, 22)
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
	_, err = h.RevokeAllWorkerTokens(ctx, &web.RevokeAllWorkerTokensRequest{})
	require.NoError(t, err)
	t.Log("Revoked all tokens")
	// List tokens (should be empty).
	tokenList, err = h.ListWorkerTokens(ctx, &web.ListWorkerTokensRequest{})
	require.NoError(t, err)
	require.NotNil(t, tokenList)
	assert.Len(t, tokenList.GetTokens(), 0)
	t.Log("Confirmed no tokens exist anymore")
	// Get collections with row counts.
	t.Run("GetCollections", func(t *testing.T) {
		collectionsRes, err := h.GetCollections(ctx, &web.GetCollectionsRequest{})
		require.NoError(t, err)
		assert.Equal(t, &web.GetCollectionsResponse{
			Collections: []*web.Collection{
				{
					Name:      "yt.videos",
					ItemCount: 128,
				},
				{
					Name:      "some.collection",
					ItemCount: 256,
				},
			},
		}, collectionsRes)
		t.Log("Got collections")
	})
}
