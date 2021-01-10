package providers

import (
	"context"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// MySQL config keys.
const (
	ConfMySQLDSN = "mysql.dsn"
)

func init() {
	viper.SetDefault(ConfMySQLDSN, "")
}

func NewMySQL(ctx context.Context, log *zap.Logger, lc fx.Lifecycle) (*sqlx.DB, error) {
	// Force Go-compatible time handling.
	cfg, err := mysql.ParseDSN(viper.GetString(ConfMySQLDSN))
	if err != nil {
		return nil, err
	}
	cfg.ParseTime = true
	cfg.Loc = time.Local
	log.Info("Connecting to MySQL DB",
		zap.String("mysql.net", cfg.Net),
		zap.String("mysql.addr", cfg.Addr),
		zap.String("mysql.db_name", cfg.DBName),
		zap.String("mysql.user", cfg.User))
	// Connect
	db, err := sqlx.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return db.Close()
		},
	})
	return db, nil
}
