package main

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/appctx"
	"go.od2.network/hive/pkg/dedup"
	"go.od2.network/hive/pkg/discovery"
	"go.od2.network/hive/pkg/items"
	"go.uber.org/zap"
)

var discoveryCmd = cobra.Command{
	Use:   "discovery",
	Short: "Run item discovery service.",
	Args:  cobra.NoArgs,
	Run:   runDiscovery,
}

func init() {
	flags := discoveryCmd.Flags()
	flags.String("consumer-group", "discovery", "Consumer group name")
	flags.String("collection", "", "Item collection to watch")
	flags.String("pk-type", "", "Primary key type of item collection")

	rootCmd.AddCommand(&discoveryCmd)
}

func runDiscovery(cmd *cobra.Command, _ []string) {
	ctx, cancel := context.WithCancel(appctx.Context())
	defer cancel()

	flags := cmd.Flags()
	collection, err := flags.GetString("collection")
	if err != nil {
		panic(err)
	}
	if collection == "" {
		log.Fatal("Empty --collection")
	}
	pkType, err := flags.GetString("pk-type")
	if err != nil {
		panic(err)
	}
	if pkType == "" {
		log.Fatal("Empty --pk-type")
	}
	consumerGroupName, err := flags.GetString("consumer-group")
	if err != nil {
		panic(err)
	}

	tasksTopic := collection
	discoveryTopic := collection + ".discovered"

	// Connect to Kafka.
	saramaClient := saramaClientFromEnv()
	defer func() {
		if err := saramaClient.Close(); err != nil {
			log.Error("Failed to close sarama client", zap.Error(err))
		}
	}()

	// Create Kafka producer.
	log.Info("Creating Kafka producer")
	producer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		log.Fatal("Failed to build Kafka producer", zap.Error(err))
	}
	defer func() {
		log.Info("Closing Kafka producer")
		if err := producer.Close(); err != nil {
			log.Error("Failed to close Kafka producer", zap.Error(err))
		}
	}()

	// Create Kafka group consumer.
	log.Info("Binding to Kafka consumer group",
		zap.String("kafka.consumer_group", consumerGroupName))
	consumerGroup, err := sarama.NewConsumerGroupFromClient(consumerGroupName, saramaClient)
	if err != nil {
		log.Fatal("Failed to build Kafka consumer", zap.Error(err))
	}
	defer func() {
		log.Info("Closing Kafka consumer group client")
		if err := consumerGroup.Close(); err != nil {
			log.Error("Failed to close Kafka consumer group", zap.Error(err))
		}
	}()

	// Connect to MySQL database.
	log.Info("Connecting to MySQL")
	db, err := openDB()
	if err != nil {
		log.Fatal("Failed to connect to MySQL", zap.Error(err))
	}
	defer func() {
		log.Info("Closing MySQL client")
		if err := db.Close(); err != nil {
			log.Error("Failed to MySQL client", zap.Error(err))
		}
	}()
	// Connect to items store.
	store := &items.Store{
		DB:        db,
		TableName: collection,
		PKType:    pkType,
	}
	log.Info("Connecting to items store",
		zap.String("items.collection", tasksTopic),
		zap.String("items.pkType", pkType))

	// Connect to dedup.
	// TODO Support bitmap dedup.
	deduper := &dedup.SQL{Store: store}

	// Set up SQL dedup.
	worker := &discovery.Worker{
		Dedup:     deduper,
		MaxDelay:  2 * time.Second, // TODO Configurable
		BatchSize: 32,              // TODO Configurable

		ItemStore: store,
		KafkaSink: &discovery.KafkaSink{
			Producer: producer,
			Topic:    tasksTopic,
		},
		Log: log,
	}
	log.Info("Producing new tasks",
		zap.String("kafka.topic", tasksTopic))
	log.Info("Consuming discovered items",
		zap.String("kafka.discovered", discoveryTopic))
	if err := consumerGroup.Consume(ctx, []string{discoveryTopic}, worker); err != nil {
		log.Error("Consumer group exited", zap.Error(err))
	}
}
