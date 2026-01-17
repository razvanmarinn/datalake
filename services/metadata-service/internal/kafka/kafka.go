package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type KafkaWriter struct {
	Writer  *kafka.Writer
	Brokers []string
}

type Provisioner struct {
	K8sClient dynamic.Interface
}

func NewKafkaWriter(brokers []string) *KafkaWriter {

	return &KafkaWriter{

		Writer: kafka.NewWriter(kafka.WriterConfig{

			Brokers: brokers,

			Balancer: &kafka.LeastBytes{},

			Async: false,

			BatchSize: 10,

			BatchTimeout: 1 * time.Second,

			RequiredAcks: 1,
		}),

		Brokers: brokers,
	}

}

func (k *KafkaWriter) EnsureTopicExists(ctx context.Context, topic string) (bool, error) {

	conn, err := kafka.Dial("tcp", k.Brokers[0])

	if err != nil {

		return false, fmt.Errorf("failed to connect to Kafka: %w", err)

	}

	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)

	if err == nil && len(partitions) > 0 {

		return true, nil

	}

	return false, nil

}

func (k *KafkaWriter) WriteMessageForSchema(ctx context.Context, projectName string, message kafka.Message) error {

	topic := "create-resources"

	topicMessage := message

	topicMessage.Topic = topic

	return k.Writer.WriteMessages(ctx, topicMessage)

}

func NewProvisioner() (*Provisioner, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Provisioner{K8sClient: client}, nil
}

func CreateKafkaTopicResource(projectName string, schemaName string) *unstructured.Unstructured {
	resourceName := strings.ToLower(fmt.Sprintf("%s-%s", projectName, schemaName))
	resourceName = strings.ReplaceAll(resourceName, "_", "-")

	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "kafka.strimzi.io/v1beta2",
			"kind":       "KafkaTopic",
			"metadata": map[string]interface{}{
				"name":      resourceName,
				"namespace": "kafka",
				"labels": map[string]interface{}{
					"strimzi.io/cluster": "my-kafka-cluster",
				},
			},
			"spec": map[string]interface{}{
				"partitions": 3,
				"replicas":   1,
				"config": map[string]interface{}{
					"retention.ms":   "604800000",
					"cleanup.policy": "delete",
				},
			},
		},
	}
}

func (p *Provisioner) ProvisionSchemaTopic(ctx context.Context, projectName, schemaName string) error {
	gvr := schema.GroupVersionResource{
		Group:    "kafka.strimzi.io",
		Version:  "v1beta2",
		Resource: "kafkatopics",
	}

	resource := CreateKafkaTopicResource(projectName, schemaName)

	_, err := p.K8sClient.Resource(gvr).Namespace("kafka").Create(
		ctx,
		resource,
		metav1.CreateOptions{},
	)

	if errors.IsAlreadyExists(err) {
		return nil
	}

	return err
}
