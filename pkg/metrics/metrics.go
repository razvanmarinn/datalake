package metrics

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ServiceMetrics struct {
	HTTPRequestsTotal      *prometheus.CounterVec
	HTTPRequestDuration    *prometheus.HistogramVec
	ActiveConnections      prometheus.Gauge
	CustomCounters         map[string]*prometheus.CounterVec
	CustomHistograms       map[string]*prometheus.HistogramVec
	CustomGauges           map[string]prometheus.Gauge
}

func NewServiceMetrics(serviceName string) *ServiceMetrics {
	return &ServiceMetrics{
		HTTPRequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Total number of HTTP requests processed",
				ConstLabels: prometheus.Labels{"service": serviceName},
			},
			[]string{"method", "endpoint", "status_code"},
		),

		HTTPRequestDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "http_request_duration_seconds",
				Help: "Duration of HTTP requests in seconds",
				ConstLabels: prometheus.Labels{"service": serviceName},
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "endpoint"},
		),

		ActiveConnections: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "active_connections",
				Help: "Number of active connections",
				ConstLabels: prometheus.Labels{"service": serviceName},
			},
		),

		CustomCounters:   make(map[string]*prometheus.CounterVec),
		CustomHistograms: make(map[string]*prometheus.HistogramVec),
		CustomGauges:     make(map[string]prometheus.Gauge),
	}
}

func (sm *ServiceMetrics) AddCustomCounter(name, help string, labels []string, serviceName string) *prometheus.CounterVec {
	counter := promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: name,
			Help: help,
			ConstLabels: prometheus.Labels{"service": serviceName},
		},
		labels,
	)
	sm.CustomCounters[name] = counter
	return counter
}

func (sm *ServiceMetrics) AddCustomHistogram(name, help string, labels []string, buckets []float64, serviceName string) *prometheus.HistogramVec {
	if buckets == nil {
		buckets = prometheus.DefBuckets
	}
	histogram := promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: name,
			Help: help,
			ConstLabels: prometheus.Labels{"service": serviceName},
			Buckets: buckets,
		},
		labels,
	)
	sm.CustomHistograms[name] = histogram
	return histogram
}

func (sm *ServiceMetrics) AddCustomGauge(name, help string, serviceName string) prometheus.Gauge {
	gauge := promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: name,
			Help: help,
			ConstLabels: prometheus.Labels{"service": serviceName},
		},
	)
	sm.CustomGauges[name] = gauge
	return gauge
}

func (sm *ServiceMetrics) PrometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Increment active connections
		sm.ActiveConnections.Inc()
		defer sm.ActiveConnections.Dec()

		c.Next()

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())

		// Record metrics
		sm.HTTPRequestsTotal.WithLabelValues(
			c.Request.Method,
			c.FullPath(),
			status,
		).Inc()

		sm.HTTPRequestDuration.WithLabelValues(
			c.Request.Method,
			c.FullPath(),
		).Observe(duration)
	}
}

func SetupMetricsEndpoint(router *gin.Engine) {
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
}

type StreamingMetrics struct {
	*ServiceMetrics
	KafkaMessagesProduced   *prometheus.CounterVec
	KafkaMessagesBatchSize  *prometheus.HistogramVec
	KafkaProduceErrors      *prometheus.CounterVec
	DataIngestionBytes      *prometheus.CounterVec
	ActiveIngestionSessions prometheus.Gauge
}

func NewStreamingMetrics(serviceName string) *StreamingMetrics {
	sm := NewServiceMetrics(serviceName)
	return &StreamingMetrics{
		ServiceMetrics: sm,
		KafkaMessagesProduced: sm.AddCustomCounter(
			"kafka_messages_produced_total",
			"Total number of messages produced to Kafka",
			[]string{"topic"},
			serviceName,
		),
		KafkaMessagesBatchSize: sm.AddCustomHistogram(
			"kafka_messages_batch_size",
			"Size of Kafka message batches",
			[]string{"topic"},
			[]float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
			serviceName,
		),
		KafkaProduceErrors: sm.AddCustomCounter(
			"kafka_produce_errors_total",
			"Total number of Kafka produce errors",
			[]string{"topic", "error_type"},
			serviceName,
		),
		DataIngestionBytes: sm.AddCustomCounter(
			"data_ingestion_bytes_total",
			"Total bytes of data ingested",
			[]string{"project", "data_type"},
			serviceName,
		),
		ActiveIngestionSessions: sm.AddCustomGauge(
			"active_ingestion_sessions",
			"Number of active data ingestion sessions",
			serviceName,
		),
	}
}

type GatewayMetrics struct {
	*ServiceMetrics
	ProxyRequestsTotal    *prometheus.CounterVec
	ProxyRequestDuration  *prometheus.HistogramVec
	AuthRequestsTotal     *prometheus.CounterVec
}

func NewGatewayMetrics(serviceName string) *GatewayMetrics {
	sm := NewServiceMetrics(serviceName)
	return &GatewayMetrics{
		ServiceMetrics: sm,
		ProxyRequestsTotal: sm.AddCustomCounter(
			"proxy_requests_total",
			"Total number of requests proxied to downstream services",
			[]string{"service", "method", "status_code"},
			serviceName,
		),
		ProxyRequestDuration: sm.AddCustomHistogram(
			"proxy_request_duration_seconds",
			"Duration of proxy requests to downstream services in seconds",
			[]string{"service", "method"},
			nil,
			serviceName,
		),
		AuthRequestsTotal: sm.AddCustomCounter(
			"auth_requests_total",
			"Total number of authentication requests",
			[]string{"status"},
			serviceName,
		),
	}
}