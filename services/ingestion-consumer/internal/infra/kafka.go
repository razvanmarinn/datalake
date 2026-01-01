package infra

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/segmentio/kafka-go"
)

func DiscoverTopics(brokers []string, pattern string) ([]string, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	conn, err := dialer.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to dial broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions: %w", err)
	}

	topicMap := make(map[string]struct{})
	matcher, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}

	for _, p := range partitions {
		if matcher.MatchString(p.Topic) {
			topicMap[p.Topic] = struct{}{}
		}
	}

	var topics []string
	for t := range topicMap {
		topics = append(topics, t)
	}

	if len(topics) == 0 {
		return nil, fmt.Errorf("no topics found matching pattern: %s", pattern)
	}

	return topics, nil
}
