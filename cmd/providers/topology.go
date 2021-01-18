package providers

import (
	"os"

	"github.com/jmoiron/sqlx"
	"github.com/pelletier/go-toml"
	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/items"
	"go.od2.network/hive/pkg/topology"
	"go.uber.org/zap"
)

// Topology config keys.
const (
	ConfTopologyConfigFile = "topology.config_file"
)

func init() {
	viper.SetDefault(ConfTopologyConfigFile, "")
}

func NewTopologyConfig(log *zap.Logger) (*topology.Config, error) {
	configFilePath := viper.GetString(ConfTopologyConfigFile)
	if configFilePath == "" {
		log.Fatal("Missing var " + ConfTopologyConfigFile)
	}
	log.Info("Reading topology config",
		zap.String(ConfTopologyConfigFile, configFilePath))
	config := new(topology.Config)
	f, err := os.Open(configFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := toml.NewDecoder(f)
	if err := dec.Decode(config); err != nil {
		return nil, err
	}
	return config, nil
}

func NewItemsFactory(db *sqlx.DB, topo *topology.Config) *items.Factory {
	return &items.Factory{
		DB:       db,
		Topology: topo,
	}
}
