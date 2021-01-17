package items

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.od2.network/hive/pkg/topology"
)

// Factory creates new stores given a collection name.
type Factory struct {
	DB       *sqlx.DB
	Topology *topology.Config
}

// GetStore returns a store for a given collection name.
func (f *Factory) GetStore(collectionName string) (*Store, error) {
	coll := f.Topology.GetCollection(collectionName)
	if coll == nil {
		return nil, fmt.Errorf("no such collection in topology: %s", collectionName)
	}
	s := &Store{
		DB:        f.DB,
		TableName: strings.Replace(collectionName, ".", "_", 1) + "_items",
		PKType:    coll.PKType,
	}
	return s, nil
}
