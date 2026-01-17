package infra

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func DiscoverTopics(brokers []string, pattern string) ([]string, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no Kafka brokers configured")
	}

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers[0],
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client: %w", err)
	}
	defer admin.Close()

	timeoutMs := int((10 * time.Second).Milliseconds())

	metadata, err := admin.GetMetadata(nil, true, timeoutMs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	matcher, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}

	topics := make([]string, 0)

	for topicName, topic := range metadata.Topics {
		if strings.HasPrefix(topicName, "__") {
			continue
		}
		if topic.Error.Code() != kafka.ErrNoError {
			continue
		}
		if matcher.MatchString(topicName) {
			topics = append(topics, topicName)
		}
	}

	if len(topics) == 0 {
		return nil, fmt.Errorf("no topics found matching pattern: %s", pattern)
	}

	return topics, nil
}
