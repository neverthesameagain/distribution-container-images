package storage

import (
	"sync"

	prometheus "github.com/distribution/distribution/v3/metrics"
	"github.com/docker/go-metrics"
)

var (
	// blobUploadMetrics holds all metrics related to blob upload operations
	blobUploadMetrics struct {
		once sync.Once

		// sessionsCreated counts the total number of upload sessions created
		sessionsCreated metrics.Counter

		// sessionsDeduplicatedByDigest counts sessions skipped because blob already exists
		sessionsDeduplicatedByDigest metrics.Counter

		// sessionsDeduplicatedByIdempotencyKey counts sessions resumed via idempotency key
		sessionsDeduplicatedByIdempotencyKey metrics.Counter

		// commitsTotal counts the total number of blob commits attempted
		commitsTotal metrics.Counter

		// commitsDeduplicatedAtMove counts commits where moveBlob found blob already exists
		commitsDeduplicatedAtMove metrics.Counter

		// digestLocksAcquired counts successful digest lock acquisitions
		digestLocksAcquired metrics.Counter

		// digestLockWaitTime tracks time spent waiting for digest locks
		digestLockWaitTime metrics.Timer

		// commitDuration tracks the total time to commit a blob
		commitDuration metrics.Timer
	}
)

// initBlobUploadMetrics initializes the blob upload metrics.
// This is called lazily to avoid registration issues during testing.
func initBlobUploadMetrics() {
	blobUploadMetrics.once.Do(func() {
		blobUploadMetrics.sessionsCreated = prometheus.StorageNamespace.NewCounter(
			"blob_upload_sessions_created_total",
			"Total number of blob upload sessions created",
		)

		blobUploadMetrics.sessionsDeduplicatedByDigest = prometheus.StorageNamespace.NewCounter(
			"blob_upload_sessions_deduplicated_digest_total",
			"Total number of upload sessions skipped because blob already exists (digest check)",
		)

		blobUploadMetrics.sessionsDeduplicatedByIdempotencyKey = prometheus.StorageNamespace.NewCounter(
			"blob_upload_sessions_deduplicated_idempotency_key_total",
			"Total number of upload sessions resumed via idempotency key",
		)

		blobUploadMetrics.commitsTotal = prometheus.StorageNamespace.NewCounter(
			"blob_commits_total",
			"Total number of blob commit operations attempted",
		)

		blobUploadMetrics.commitsDeduplicatedAtMove = prometheus.StorageNamespace.NewCounter(
			"blob_commits_deduplicated_at_move_total",
			"Total number of commits where blob already existed at destination (concurrent upload resolution)",
		)

		blobUploadMetrics.digestLocksAcquired = prometheus.StorageNamespace.NewCounter(
			"blob_digest_locks_acquired_total",
			"Total number of digest locks successfully acquired",
		)

		blobUploadMetrics.digestLockWaitTime = prometheus.StorageNamespace.NewTimer(
			"blob_digest_lock_wait_seconds",
			"Time spent waiting to acquire digest locks",
		)

		blobUploadMetrics.commitDuration = prometheus.StorageNamespace.NewTimer(
			"blob_commit_duration_seconds",
			"Total time to commit a blob (including lock acquisition, move, link, cleanup)",
		)
	})
}

// MetricsSessionCreated increments the session created counter.
func MetricsSessionCreated() {
	initBlobUploadMetrics()
	blobUploadMetrics.sessionsCreated.Inc(1)
}

// MetricsSessionDeduplicatedByDigest increments the digest deduplication counter.
func MetricsSessionDeduplicatedByDigest() {
	initBlobUploadMetrics()
	blobUploadMetrics.sessionsDeduplicatedByDigest.Inc(1)
}

// MetricsSessionDeduplicatedByIdempotencyKey increments the idempotency key deduplication counter.
func MetricsSessionDeduplicatedByIdempotencyKey() {
	initBlobUploadMetrics()
	blobUploadMetrics.sessionsDeduplicatedByIdempotencyKey.Inc(1)
}

// MetricsCommitStarted increments the commit counter and returns a function
// to record the commit duration when called.
func MetricsCommitStarted() func() {
	initBlobUploadMetrics()
	blobUploadMetrics.commitsTotal.Inc(1)
	return metrics.StartTimer(blobUploadMetrics.commitDuration)
}

// MetricsCommitDeduplicatedAtMove increments the counter for commits that
// found the blob already existed at the destination.
func MetricsCommitDeduplicatedAtMove() {
	initBlobUploadMetrics()
	blobUploadMetrics.commitsDeduplicatedAtMove.Inc(1)
}

// MetricsDigestLockAcquired increments the digest lock counter and returns
// a function to record the wait time when called.
func MetricsDigestLockAcquired() func() {
	initBlobUploadMetrics()
	blobUploadMetrics.digestLocksAcquired.Inc(1)
	return metrics.StartTimer(blobUploadMetrics.digestLockWaitTime)
}
