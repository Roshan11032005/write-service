package kafkautil

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"write-service/model"
)

// CreateConsumer creates a confluent Kafka consumer from the given config.
func CreateConsumer(cfg *model.KafkaConfig) (*kafka.Consumer, error) {
	kcfg := &kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(cfg.BootstrapServers, ","),
		"group.id":           cfg.GroupId,
		"enable.auto.commit": false,
		"auto.offset.reset":  cfg.AutoOffsetReset,
	}

	return kafka.NewConsumer(kcfg)
}
