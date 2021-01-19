module go.od2.network/hive

go 1.15

require (
	github.com/Shopify/sarama v1.27.2
	github.com/cenkalti/backoff/v4 v4.1.0
	github.com/deathowl/go-metrics-prometheus v0.0.0-20200518174047-74482eab5bfb
	github.com/go-redis/redis/v8 v8.4.8
	github.com/go-sql-driver/mysql v1.5.0
	github.com/golang/protobuf v1.4.3
	github.com/hashicorp/golang-lru v0.5.4
	github.com/jmoiron/sqlx v1.2.1-0.20201120164427-00c6e74d816a
	github.com/ory/dockertest/v3 v3.6.3
	github.com/pelletier/go-toml v1.8.1
	github.com/prometheus/client_golang v1.9.0
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	github.com/rs/xid v1.2.1
	github.com/spf13/cobra v1.1.1
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/otel v0.16.0
	go.opentelemetry.io/otel/exporters/metric/prometheus v0.16.0
	go.uber.org/fx v1.13.1
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a
	google.golang.org/grpc v1.35.0
	google.golang.org/protobuf v1.25.0
)

// https://github.com/pelletier/go-toml/pull/464
replace github.com/pelletier/go-toml => github.com/terorie/go-toml v1.8.2-0.20210118184423-69117aeb7d8a
