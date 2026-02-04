package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Counter metrics
	BlockWritesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dfs_block_writes_total",
			Help: "Total number of block write operations",
		},
		[]string{"status"},
	)

	BlockReadsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dfs_block_reads_total",
			Help: "Total number of block read operations",
		},
		[]string{"status"},
	)

	ChecksumVerificationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dfs_checksum_verifications_total",
			Help: "Total number of checksum verifications",
		},
		[]string{"result"},
	)

	BlockCorruptionTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dfs_block_corruption_total",
			Help: "Total number of corrupted blocks detected",
		},
		[]string{"worker_id"},
	)

	IntegrityChecksTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dfs_integrity_checks_total",
			Help: "Total number of integrity check cycles completed",
		},
		[]string{"status"},
	)

	// Histogram metrics
	ChecksumCalculationDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dfs_checksum_calculation_duration_seconds",
			Help:    "Time taken to calculate checksums during write",
			Buckets: prometheus.DefBuckets,
		},
	)

	ChecksumVerificationDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dfs_checksum_verification_duration_seconds",
			Help:    "Time taken to verify checksums during read",
			Buckets: prometheus.DefBuckets,
		},
	)

	IntegrityCheckDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dfs_integrity_check_duration_seconds",
			Help:    "Time taken to complete full integrity check cycle",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600},
		},
	)

	BlockWriteSizeBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dfs_block_write_size_bytes",
			Help:    "Size of blocks written",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 20),
		},
	)

	BlockReadSizeBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dfs_block_read_size_bytes",
			Help:    "Size of blocks read",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 20),
		},
	)

	// Gauge metrics
	BlocksStoredTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dfs_blocks_stored_total",
			Help: "Total number of blocks currently stored",
		},
		[]string{"worker_id"},
	)

	CorruptedBlocksCurrent = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dfs_corrupted_blocks_current",
			Help: "Current number of corrupted blocks detected",
		},
		[]string{"worker_id"},
	)

	StorageBytesUsed = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dfs_storage_bytes_used",
			Help: "Total storage space used in bytes",
		},
		[]string{"worker_id"},
	)

	LastIntegrityCheckTimestamp = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dfs_last_integrity_check_timestamp",
			Help: "Unix timestamp of last completed integrity check",
		},
	)
)
