package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/registry/storage/cache/memory"
	"github.com/distribution/distribution/v3/registry/storage/driver/inmemory"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// TestDigestLockerBasic tests basic digest locking functionality.
func TestDigestLockerBasic(t *testing.T) {
	locker := NewInMemoryDigestLocker()
	ctx := context.Background()
	dgst := digest.FromString("test content")

	// Should be able to acquire lock
	lock1, err := locker.Lock(ctx, dgst)
	if err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}

	// Second lock attempt should block
	acquired := make(chan bool, 1)
	go func() {
		lock2, err := locker.Lock(ctx, dgst)
		if err != nil {
			acquired <- false
			return
		}
		lock2.Unlock()
		acquired <- true
	}()

	// Give the goroutine time to start blocking
	select {
	case <-acquired:
		t.Fatal("second lock should not have been acquired while first is held")
	case <-time.After(100 * time.Millisecond):
		// Expected - second lock is blocking
	}

	// Release first lock
	lock1.Unlock()

	// Now second lock should acquire
	select {
	case success := <-acquired:
		if !success {
			t.Fatal("second lock acquisition failed after first was released")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("second lock was not acquired after first was released")
	}
}

// TestDigestLockerConcurrent tests that digest locking serializes concurrent commits.
func TestDigestLockerConcurrent(t *testing.T) {
	locker := NewInMemoryDigestLocker()
	ctx := context.Background()
	dgst := digest.FromString("concurrent test content")

	const numGoroutines = 10
	var wg sync.WaitGroup
	var concurrentHolders int32
	var maxConcurrent int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock, err := locker.Lock(ctx, dgst)
			if err != nil {
				t.Errorf("failed to acquire lock: %v", err)
				return
			}

			// Track concurrent holders
			current := atomic.AddInt32(&concurrentHolders, 1)
			for {
				max := atomic.LoadInt32(&maxConcurrent)
				if current <= max {
					break
				}
				if atomic.CompareAndSwapInt32(&maxConcurrent, max, current) {
					break
				}
			}

			// Simulate some work
			time.Sleep(10 * time.Millisecond)

			atomic.AddInt32(&concurrentHolders, -1)
			lock.Unlock()
		}()
	}

	wg.Wait()

	// With proper locking, max concurrent should be 1
	if atomic.LoadInt32(&maxConcurrent) > 1 {
		t.Errorf("digest lock allowed %d concurrent holders, expected 1", maxConcurrent)
	}
}

// TestDigestLockerDifferentDigests tests that different digests can be locked concurrently.
func TestDigestLockerDifferentDigests(t *testing.T) {
	locker := NewInMemoryDigestLocker()
	ctx := context.Background()
	dgst1 := digest.FromString("content 1")
	dgst2 := digest.FromString("content 2")

	// Lock first digest
	lock1, err := locker.Lock(ctx, dgst1)
	if err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}

	// Should be able to lock different digest concurrently
	lock2, err := locker.Lock(ctx, dgst2)
	if err != nil {
		t.Fatalf("failed to acquire second lock for different digest: %v", err)
	}

	lock1.Unlock()
	lock2.Unlock()
}

// TestDigestLockerContextCancellation tests that locks respect context cancellation.
func TestDigestLockerContextCancellation(t *testing.T) {
	locker := NewInMemoryDigestLocker()
	dgst := digest.FromString("cancel test")

	// Acquire the lock
	ctx1 := context.Background()
	lock1, err := locker.Lock(ctx1, dgst)
	if err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}

	// Try to acquire with a context that we'll cancel
	ctx2, cancel := context.WithCancel(context.Background())

	errChan := make(chan error, 1)
	go func() {
		_, err := locker.Lock(ctx2, dgst)
		errChan <- err
	}()

	// Give goroutine time to start blocking
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// Should receive context cancelled error
	select {
	case err := <-errChan:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("lock acquisition did not respect context cancellation")
	}

	lock1.Unlock()
}

// TestSessionDeduplicator tests the session deduplication functionality.
func TestSessionDeduplicator(t *testing.T) {
	sd := NewSessionDeduplicator(1 * time.Hour)
	dgst := digest.FromString("session test")

	// Register a session
	sd.RegisterSession("session-1", "idempotency-key-1", dgst)

	// Should be able to look up by idempotency key
	sessionID, found := sd.LookupByIdempotencyKey("idempotency-key-1")
	if !found {
		t.Fatal("expected to find session by idempotency key")
	}
	if sessionID != "session-1" {
		t.Errorf("expected session-1, got %s", sessionID)
	}

	// Unknown key should not be found
	_, found = sd.LookupByIdempotencyKey("unknown-key")
	if found {
		t.Error("expected unknown key to not be found")
	}

	// Remove the session
	sd.RemoveSession("session-1", "idempotency-key-1", dgst)

	// Should no longer be found
	_, found = sd.LookupByIdempotencyKey("idempotency-key-1")
	if found {
		t.Error("expected removed session to not be found")
	}
}

// TestIdempotencyKeyIntegration tests the full idempotency key flow through the storage layer.
func TestIdempotencyKeyIntegration(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("test/idempotency-key")
	driver := inmemory.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider(memory.UnlimitedSize)), EnableDelete, EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		t.Fatalf("unexpected error getting repo: %v", err)
	}
	bs := repository.Blobs(ctx)

	// First request with idempotency key should create a session
	wr1, err := bs.Create(ctx, WithIdempotencyKey("test-key-123"))
	if err != nil {
		t.Fatalf("first create with idempotency key failed: %v", err)
	}
	sessionID1 := wr1.ID()

	// Second request with same idempotency key should return ErrBlobUploadResumed
	_, err = bs.Create(ctx, WithIdempotencyKey("test-key-123"))
	if err == nil {
		t.Fatal("expected ErrBlobUploadResumed, got nil")
	}
	ebr, ok := err.(distribution.ErrBlobUploadResumed)
	if !ok {
		t.Fatalf("expected ErrBlobUploadResumed, got %T: %v", err, err)
	}
	if ebr.SessionID != sessionID1 {
		t.Errorf("expected session ID %s, got %s", sessionID1, ebr.SessionID)
	}

	// Different idempotency key should create a new session
	wr2, err := bs.Create(ctx, WithIdempotencyKey("different-key-456"))
	if err != nil {
		t.Fatalf("create with different idempotency key failed: %v", err)
	}
	if wr2.ID() == sessionID1 {
		t.Error("different idempotency key should create different session")
	}

	// Clean up - cancel both uploads
	wr1.Cancel(ctx)
	wr2.Cancel(ctx)
}

// TestSessionDeduplicatorExpiry tests that sessions expire after TTL.
func TestSessionDeduplicatorExpiry(t *testing.T) {
	sd := NewSessionDeduplicator(50 * time.Millisecond)
	dgst := digest.FromString("expiry test")

	// Register a session
	sd.RegisterSession("session-1", "idempotency-key-1", dgst)

	// Should be found immediately
	_, found := sd.LookupByIdempotencyKey("idempotency-key-1")
	if !found {
		t.Fatal("expected to find session immediately after registration")
	}

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Should no longer be found
	_, found = sd.LookupByIdempotencyKey("idempotency-key-1")
	if found {
		t.Error("expected session to be expired")
	}
}

// TestConcurrentBlobCommits tests that concurrent commits for the same digest
// are properly serialized and only one actually performs the move.
func TestConcurrentBlobCommits(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("test/concurrent-commits")
	driver := inmemory.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider(memory.UnlimitedSize)), EnableDelete, EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		t.Fatalf("unexpected error getting repo: %v", err)
	}
	bs := repository.Blobs(ctx)

	// Create test blob content
	blobContent := []byte("concurrent commit test blob content")
	dgst := digest.FromBytes(blobContent)

	const numConcurrentUploads = 5
	var wg sync.WaitGroup
	var successCount int32
	var alreadyExistsCount int32

	// Start multiple concurrent uploads
	for i := 0; i < numConcurrentUploads; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			wr, err := bs.Create(ctx)
			if err != nil {
				t.Errorf("goroutine %d: failed to create upload: %v", idx, err)
				return
			}

			_, err = wr.Write(blobContent)
			if err != nil {
				t.Errorf("goroutine %d: failed to write: %v", idx, err)
				return
			}

			_, err = wr.Commit(ctx, v1.Descriptor{Digest: dgst})
			if err != nil {
				t.Errorf("goroutine %d: commit failed: %v", idx, err)
				return
			}

			atomic.AddInt32(&successCount, 1)
		}(i)
	}

	wg.Wait()

	// All commits should succeed (due to idempotent moveBlob)
	if atomic.LoadInt32(&successCount) != numConcurrentUploads {
		t.Errorf("expected all %d commits to succeed, got %d", numConcurrentUploads, successCount)
	}

	// Verify blob exists and has correct content
	reader, err := bs.Open(ctx, dgst)
	if err != nil {
		t.Fatalf("failed to open committed blob: %v", err)
	}
	defer reader.Close()

	readContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read blob: %v", err)
	}

	if !bytes.Equal(readContent, blobContent) {
		t.Error("blob content does not match")
	}

	// Additional test: try to create new uploads with digest check
	// These should all return ErrBlobAlreadyExists
	for i := 0; i < 3; i++ {
		_, err := bs.Create(ctx, WithDigestCheck(dgst))
		if err == nil {
			t.Error("expected ErrBlobAlreadyExists for subsequent upload with digest check")
			continue
		}
		if _, ok := err.(distribution.ErrBlobAlreadyExists); ok {
			atomic.AddInt32(&alreadyExistsCount, 1)
		} else {
			t.Errorf("expected ErrBlobAlreadyExists, got %T: %v", err, err)
		}
	}

	if atomic.LoadInt32(&alreadyExistsCount) != 3 {
		t.Errorf("expected 3 ErrBlobAlreadyExists, got %d", alreadyExistsCount)
	}
}

// TestConcurrentSessionCreationWithDigestCheck tests that concurrent session
// creation with digest check properly detects existing blobs.
func TestConcurrentSessionCreationWithDigestCheck(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("test/concurrent-session-creation")
	driver := inmemory.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider(memory.UnlimitedSize)), EnableDelete, EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		t.Fatalf("unexpected error getting repo: %v", err)
	}
	bs := repository.Blobs(ctx)

	// First, upload a blob
	blobContent := []byte("existing blob for concurrent session test")
	dgst := digest.FromBytes(blobContent)

	wr, err := bs.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create upload: %v", err)
	}
	if _, err := wr.Write(blobContent); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if _, err := wr.Commit(ctx, v1.Descriptor{Digest: dgst}); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Now try concurrent session creations with digest check
	const numConcurrentAttempts = 10
	var wg sync.WaitGroup
	var alreadyExistsCount int32

	for i := 0; i < numConcurrentAttempts; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			_, err := bs.Create(ctx, WithDigestCheck(dgst))
			if err == nil {
				t.Errorf("goroutine %d: expected error, got nil", idx)
				return
			}
			if _, ok := err.(distribution.ErrBlobAlreadyExists); ok {
				atomic.AddInt32(&alreadyExistsCount, 1)
			} else {
				t.Errorf("goroutine %d: expected ErrBlobAlreadyExists, got %T: %v", idx, err, err)
			}
		}(i)
	}

	wg.Wait()

	if atomic.LoadInt32(&alreadyExistsCount) != numConcurrentAttempts {
		t.Errorf("expected %d ErrBlobAlreadyExists, got %d", numConcurrentAttempts, alreadyExistsCount)
	}
}

// TestPartialCommitRecovery tests that if commit fails partway through,
// a retry will correctly complete.
func TestPartialCommitRecovery(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("test/partial-commit")
	driver := inmemory.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider(memory.UnlimitedSize)), EnableDelete, EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		t.Fatalf("unexpected error getting repo: %v", err)
	}
	bs := repository.Blobs(ctx)

	// Upload blob 1
	blobContent1 := []byte("first blob for partial commit test")
	dgst1 := digest.FromBytes(blobContent1)

	wr1, err := bs.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create first upload: %v", err)
	}
	if _, err := wr1.Write(blobContent1); err != nil {
		t.Fatalf("failed to write first blob: %v", err)
	}
	desc1, err := wr1.Commit(ctx, v1.Descriptor{Digest: dgst1})
	if err != nil {
		t.Fatalf("failed to commit first blob: %v", err)
	}

	// Now simulate a "retry" by uploading the same blob again
	// This tests the idempotent behavior of moveBlob and linkBlob
	wr2, err := bs.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create second upload: %v", err)
	}
	if _, err := wr2.Write(blobContent1); err != nil {
		t.Fatalf("failed to write second blob: %v", err)
	}
	desc2, err := wr2.Commit(ctx, v1.Descriptor{Digest: dgst1})
	if err != nil {
		t.Fatalf("failed to commit second blob (retry): %v", err)
	}

	// Both descriptors should be identical
	if desc1.Digest != desc2.Digest {
		t.Errorf("descriptors don't match: %v != %v", desc1.Digest, desc2.Digest)
	}

	// Verify blob is accessible
	_, err = bs.Stat(ctx, dgst1)
	if err != nil {
		t.Errorf("failed to stat blob after retry: %v", err)
	}
}

// TestLinkBlobIdempotency tests that linkBlob can be called multiple times
// for the same blob without error.
func TestLinkBlobIdempotency(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("test/link-idempotency")
	driver := inmemory.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider(memory.UnlimitedSize)), EnableDelete, EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		t.Fatalf("unexpected error getting repo: %v", err)
	}
	bs := repository.Blobs(ctx)

	// Upload a blob
	blobContent := []byte("blob for link idempotency test")
	dgst := digest.FromBytes(blobContent)

	wr, err := bs.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create upload: %v", err)
	}
	if _, err := wr.Write(blobContent); err != nil {
		t.Fatalf("failed to write blob: %v", err)
	}
	_, err = wr.Commit(ctx, v1.Descriptor{Digest: dgst})
	if err != nil {
		t.Fatalf("failed to commit blob: %v", err)
	}

	// Upload the same blob again - this exercises linkBlob idempotency
	wr2, err := bs.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create second upload: %v", err)
	}
	if _, err := wr2.Write(blobContent); err != nil {
		t.Fatalf("failed to write second blob: %v", err)
	}
	_, err = wr2.Commit(ctx, v1.Descriptor{Digest: dgst})
	if err != nil {
		t.Fatalf("second commit failed (linkBlob should be idempotent): %v", err)
	}

	// Verify blob is still accessible
	desc, err := bs.Stat(ctx, dgst)
	if err != nil {
		t.Fatalf("failed to stat blob: %v", err)
	}
	if desc.Digest != dgst {
		t.Errorf("unexpected digest: %v != %v", desc.Digest, dgst)
	}
}

// TestMoveBlobIdempotency tests that moveBlob correctly handles an already-existing blob.
func TestMoveBlobIdempotency(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("test/move-idempotency")
	driver := inmemory.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider(memory.UnlimitedSize)), EnableDelete, EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		t.Fatalf("unexpected error getting repo: %v", err)
	}
	bs := repository.Blobs(ctx)

	// Upload the same blob twice (simulating retry or concurrent upload)
	blobContent := []byte("blob content for move idempotency test")
	dgst := digest.FromBytes(blobContent)

	for i := 0; i < 3; i++ {
		wr, err := bs.Create(ctx)
		if err != nil {
			t.Fatalf("iteration %d: failed to create upload: %v", i, err)
		}
		if _, err := wr.Write(blobContent); err != nil {
			t.Fatalf("iteration %d: failed to write: %v", i, err)
		}
		_, err = wr.Commit(ctx, v1.Descriptor{Digest: dgst})
		if err != nil {
			t.Fatalf("iteration %d: commit failed: %v", i, err)
		}
	}

	// Verify blob exists and content is correct
	reader, err := bs.Open(ctx, dgst)
	if err != nil {
		t.Fatalf("failed to open blob: %v", err)
	}
	defer reader.Close()

	readContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read blob: %v", err)
	}

	if !bytes.Equal(readContent, blobContent) {
		t.Error("blob content does not match after multiple commits")
	}
}

// TestCancelUploadCleanup tests that cancelling an upload properly cleans up resources.
func TestCancelUploadCleanup(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("test/cancel-cleanup")
	driver := inmemory.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider(memory.UnlimitedSize)), EnableDelete, EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		t.Fatalf("unexpected error getting repo: %v", err)
	}
	bs := repository.Blobs(ctx)

	// Start an upload
	blobContent := []byte("blob that will be cancelled")
	wr, err := bs.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create upload: %v", err)
	}

	// Write some content
	if _, err := wr.Write(blobContent); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Get the upload ID for later verification
	uploadID := wr.ID()

	// Cancel the upload
	if err := wr.Cancel(ctx); err != nil {
		t.Fatalf("failed to cancel upload: %v", err)
	}

	// Trying to resume the cancelled upload should fail
	_, err = bs.Resume(ctx, uploadID)
	if err != distribution.ErrBlobUploadUnknown {
		t.Errorf("expected ErrBlobUploadUnknown after cancel, got: %v", err)
	}
}

// TestMultipleBlobsNoCrossContamination ensures multiple different blobs
// don't interfere with each other.
func TestMultipleBlobsNoCrossContamination(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("test/no-cross-contamination")
	driver := inmemory.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider(memory.UnlimitedSize)), EnableDelete, EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		t.Fatalf("unexpected error getting repo: %v", err)
	}
	bs := repository.Blobs(ctx)

	const numBlobs = 10
	blobs := make([][]byte, numBlobs)
	digests := make([]digest.Digest, numBlobs)

	// Create and upload multiple blobs concurrently
	var wg sync.WaitGroup
	for i := 0; i < numBlobs; i++ {
		blobs[i] = []byte(fmt.Sprintf("unique blob content %d with random data", i))
		digests[i] = digest.FromBytes(blobs[i])

		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			wr, err := bs.Create(ctx)
			if err != nil {
				t.Errorf("blob %d: failed to create upload: %v", idx, err)
				return
			}
			if _, err := wr.Write(blobs[idx]); err != nil {
				t.Errorf("blob %d: failed to write: %v", idx, err)
				return
			}
			_, err = wr.Commit(ctx, v1.Descriptor{Digest: digests[idx]})
			if err != nil {
				t.Errorf("blob %d: commit failed: %v", idx, err)
				return
			}
		}(i)
	}

	wg.Wait()

	// Verify each blob has correct content
	for i := 0; i < numBlobs; i++ {
		reader, err := bs.Open(ctx, digests[i])
		if err != nil {
			t.Errorf("blob %d: failed to open: %v", i, err)
			continue
		}

		content, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			t.Errorf("blob %d: failed to read: %v", i, err)
			continue
		}

		if !bytes.Equal(content, blobs[i]) {
			t.Errorf("blob %d: content mismatch", i)
		}
	}
}

// TestMidUploadRetryDeduplication tests that retrying a POST while an upload
// is in progress returns the existing session instead of creating a duplicate.
// This is the key scenario: client starts upload, network hiccup causes retry,
// but original upload is still in progress. Without this fix, a second session
// would be created, leading to duplicate uploads and orphaned sessions.
func TestMidUploadRetryDeduplication(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("test/mid-upload-retry")
	driver := inmemory.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider(memory.UnlimitedSize)), EnableDelete, EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		t.Fatalf("unexpected error getting repo: %v", err)
	}
	bs := repository.Blobs(ctx)

	// Prepare blob content and digest
	blobContent := []byte("test blob content for mid-upload retry scenario")
	dgst := digest.FromBytes(blobContent)

	// Start first upload with digest check option
	wr1, err := bs.Create(ctx, WithDigestCheck(dgst))
	if err != nil {
		t.Fatalf("failed to create first upload: %v", err)
	}
	session1ID := wr1.ID()

	// Write partial content (simulating mid-upload state)
	// Note: We're simulating the scenario where a client has started uploading
	// but hasn't committed yet
	partialContent := blobContent[:len(blobContent)/2]
	if _, err := wr1.Write(partialContent); err != nil {
		t.Fatalf("failed to write partial content: %v", err)
	}

	// Now simulate a client retry - another POST with the same digest
	// This should return the existing session, not create a new one
	wr2, err := bs.Create(ctx, WithDigestCheck(dgst))

	// Should get ErrBlobUploadResumed pointing to the existing session
	if err == nil {
		t.Fatalf("expected error for duplicate session, got new session: %s", wr2.ID())
	}

	resumed, ok := err.(distribution.ErrBlobUploadResumed)
	if !ok {
		t.Fatalf("expected ErrBlobUploadResumed, got: %T: %v", err, err)
	}

	if resumed.SessionID != session1ID {
		t.Errorf("expected to resume session %s, got %s", session1ID, resumed.SessionID)
	}

	// Resume the session to complete the upload
	resumedWriter, err := bs.Resume(ctx, session1ID)
	if err != nil {
		t.Fatalf("failed to resume session: %v", err)
	}

	// Write the full content to the resumed session
	// In real scenario, client would use ReadFrom or continue from offset
	// For this test, we cancel and create a fresh upload to verify the mechanism
	resumedWriter.Cancel(ctx)

	// Create a fresh upload now that the old one is cancelled
	wr3, err := bs.Create(ctx, WithDigestCheck(dgst))
	if err != nil {
		t.Fatalf("failed to create upload after cancel: %v", err)
	}

	// Write full content and commit
	if _, err := wr3.Write(blobContent); err != nil {
		t.Fatalf("failed to write content: %v", err)
	}

	_, err = wr3.Commit(ctx, v1.Descriptor{Digest: dgst})
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify blob exists with correct content
	reader, err := bs.Open(ctx, dgst)
	if err != nil {
		t.Fatalf("failed to open committed blob: %v", err)
	}
	defer reader.Close()

	readContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read blob: %v", err)
	}

	if !bytes.Equal(readContent, blobContent) {
		t.Error("blob content does not match")
	}
}

// TestLookupByDigest tests the SessionDeduplicator's LookupByDigest functionality.
func TestLookupByDigest(t *testing.T) {
	sd := NewSessionDeduplicator(time.Hour)
	dgst := digest.FromString("test content")

	// Initially should not find anything
	if _, found := sd.LookupByDigest(dgst); found {
		t.Error("should not find session for unregistered digest")
	}

	// Register a session with digest
	sd.RegisterSession("session-1", "", dgst)

	// Should now find the session
	sessionID, found := sd.LookupByDigest(dgst)
	if !found {
		t.Error("should find session for registered digest")
	}
	if sessionID != "session-1" {
		t.Errorf("expected session-1, got %s", sessionID)
	}

	// Register another session for the same digest
	sd.RegisterSession("session-2", "", dgst)

	// Should still return the first session (oldest)
	sessionID, found = sd.LookupByDigest(dgst)
	if !found {
		t.Error("should find session for registered digest")
	}
	if sessionID != "session-1" {
		t.Errorf("expected session-1 (first registered), got %s", sessionID)
	}

	// Remove first session
	sd.RemoveSession("session-1", "", dgst)

	// Should now return session-2
	sessionID, found = sd.LookupByDigest(dgst)
	if !found {
		t.Error("should find session-2 after removing session-1")
	}
	if sessionID != "session-2" {
		t.Errorf("expected session-2, got %s", sessionID)
	}

	// Remove second session
	sd.RemoveSession("session-2", "", dgst)

	// Should not find anything now
	if _, found := sd.LookupByDigest(dgst); found {
		t.Error("should not find session after all removed")
	}
}

// TestConcurrentMidUploadRetries tests multiple concurrent retry attempts
// while an upload is in progress. All retries should get the same session.
func TestConcurrentMidUploadRetries(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("test/concurrent-mid-upload")
	driver := inmemory.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider(memory.UnlimitedSize)), EnableDelete, EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		t.Fatalf("unexpected error getting repo: %v", err)
	}
	bs := repository.Blobs(ctx)

	blobContent := []byte("test blob for concurrent retries")
	dgst := digest.FromBytes(blobContent)

	// Start original upload
	wr1, err := bs.Create(ctx, WithDigestCheck(dgst))
	if err != nil {
		t.Fatalf("failed to create first upload: %v", err)
	}
	originalSessionID := wr1.ID()

	// Write some content to simulate mid-upload
	if _, err := wr1.Write(blobContent[:10]); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Launch multiple concurrent retry attempts
	const numRetries = 10
	var wg sync.WaitGroup
	results := make(chan string, numRetries)
	errors := make(chan error, numRetries)

	for i := 0; i < numRetries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := bs.Create(ctx, WithDigestCheck(dgst))
			if err != nil {
				if resumed, ok := err.(distribution.ErrBlobUploadResumed); ok {
					results <- resumed.SessionID
				} else {
					errors <- err
				}
			} else {
				errors <- fmt.Errorf("expected error, got new session")
			}
		}()
	}

	wg.Wait()
	close(results)
	close(errors)

	// Check for unexpected errors
	for err := range errors {
		t.Errorf("unexpected error: %v", err)
	}

	// All retries should have gotten the same session ID
	for sessionID := range results {
		if sessionID != originalSessionID {
			t.Errorf("retry got different session %s, expected %s", sessionID, originalSessionID)
		}
	}
}
