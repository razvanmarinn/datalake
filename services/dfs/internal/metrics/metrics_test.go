package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricsRegistration(t *testing.T) {
	t.Run("block writes counter exists", func(t *testing.T) {
		assert.NotNil(t, BlockWritesTotal)
	})

	t.Run("block reads counter exists", func(t *testing.T) {
		assert.NotNil(t, BlockReadsTotal)
	})

	t.Run("checksum verifications counter exists", func(t *testing.T) {
		assert.NotNil(t, ChecksumVerificationsTotal)
	})

	t.Run("block corruption counter exists", func(t *testing.T) {
		assert.NotNil(t, BlockCorruptionTotal)
	})

	t.Run("integrity checks counter exists", func(t *testing.T) {
		assert.NotNil(t, IntegrityChecksTotal)
	})
}

func TestHistogramMetrics(t *testing.T) {
	t.Run("checksum calculation duration exists", func(t *testing.T) {
		assert.NotNil(t, ChecksumCalculationDuration)
	})

	t.Run("checksum verification duration exists", func(t *testing.T) {
		assert.NotNil(t, ChecksumVerificationDuration)
	})

	t.Run("integrity check duration exists", func(t *testing.T) {
		assert.NotNil(t, IntegrityCheckDuration)
	})

	t.Run("block write size exists", func(t *testing.T) {
		assert.NotNil(t, BlockWriteSizeBytes)
	})

	t.Run("block read size exists", func(t *testing.T) {
		assert.NotNil(t, BlockReadSizeBytes)
	})
}

func TestGaugeMetrics(t *testing.T) {
	t.Run("blocks stored total exists", func(t *testing.T) {
		assert.NotNil(t, BlocksStoredTotal)
	})

	t.Run("corrupted blocks current exists", func(t *testing.T) {
		assert.NotNil(t, CorruptedBlocksCurrent)
	})

	t.Run("storage bytes used exists", func(t *testing.T) {
		assert.NotNil(t, StorageBytesUsed)
	})

	t.Run("last integrity check timestamp exists", func(t *testing.T) {
		assert.NotNil(t, LastIntegrityCheckTimestamp)
	})
}

func TestMetricLabels(t *testing.T) {
	t.Run("counter with status label", func(t *testing.T) {
		// Verify we can use the counter with labels
		BlockWritesTotal.WithLabelValues("success")
		BlockWritesTotal.WithLabelValues("failure")
	})

	t.Run("counter with result label", func(t *testing.T) {
		ChecksumVerificationsTotal.WithLabelValues("valid")
		ChecksumVerificationsTotal.WithLabelValues("corrupted")
		ChecksumVerificationsTotal.WithLabelValues("missing")
		ChecksumVerificationsTotal.WithLabelValues("error")
	})

	t.Run("gauge with worker_id label", func(t *testing.T) {
		BlocksStoredTotal.WithLabelValues("worker-1")
		CorruptedBlocksCurrent.WithLabelValues("worker-1")
		StorageBytesUsed.WithLabelValues("worker-1")
	})
}

func TestMetricOperations(t *testing.T) {
	t.Run("increment counter", func(t *testing.T) {
		BlockWritesTotal.WithLabelValues("test").Inc()
	})

	t.Run("observe histogram", func(t *testing.T) {
		ChecksumCalculationDuration.Observe(0.5)
		ChecksumVerificationDuration.Observe(0.1)
		IntegrityCheckDuration.Observe(10.0)
	})

	t.Run("set gauge", func(t *testing.T) {
		BlocksStoredTotal.WithLabelValues("test-worker").Set(100)
		StorageBytesUsed.WithLabelValues("test-worker").Set(1024 * 1024)
	})

	t.Run("add to counter", func(t *testing.T) {
		BlockCorruptionTotal.WithLabelValues("test-worker").Add(1)
	})
}
