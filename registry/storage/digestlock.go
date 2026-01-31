package storage

import (
	"context"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
)

// DigestLocker provides digest-scoped locking to prevent race conditions
// during concurrent blob commits. This interface allows for different
// implementations (in-memory for single-node, distributed for multi-node).
type DigestLocker interface {
	// Lock acquires a lock for the given digest. The lock is held until
	// Unlock is called or the context is cancelled.
	Lock(ctx context.Context, dgst digest.Digest) (DigestLock, error)
}

// DigestLock represents a held lock on a digest.
type DigestLock interface {
	// Unlock releases the lock. Must be called when done with the lock.
	Unlock()
}

// InMemoryDigestLocker provides in-memory digest locking suitable for
// single-node deployments. For multi-node deployments, use a distributed
// implementation (e.g., Redis-based).
type InMemoryDigestLocker struct {
	mu    sync.Mutex
	locks map[digest.Digest]*digestLockEntry
}

type digestLockEntry struct {
	mu       sync.Mutex
	refCount int
}

// NewInMemoryDigestLocker creates a new in-memory digest locker.
func NewInMemoryDigestLocker() *InMemoryDigestLocker {
	return &InMemoryDigestLocker{
		locks: make(map[digest.Digest]*digestLockEntry),
	}
}

// Lock acquires a lock for the given digest.
func (l *InMemoryDigestLocker) Lock(ctx context.Context, dgst digest.Digest) (DigestLock, error) {
	// Get or create the lock entry for this digest
	l.mu.Lock()
	entry, ok := l.locks[dgst]
	if !ok {
		entry = &digestLockEntry{}
		l.locks[dgst] = entry
	}
	entry.refCount++
	l.mu.Unlock()

	// Try to acquire the digest-specific lock with context cancellation support
	acquired := make(chan struct{})
	go func() {
		entry.mu.Lock()
		close(acquired)
	}()

	select {
	case <-acquired:
		return &inMemoryDigestLock{
			locker: l,
			dgst:   dgst,
			entry:  entry,
		}, nil
	case <-ctx.Done():
		// Context cancelled, clean up
		l.mu.Lock()
		entry.refCount--
		if entry.refCount == 0 {
			delete(l.locks, dgst)
		}
		l.mu.Unlock()
		return nil, ctx.Err()
	}
}

type inMemoryDigestLock struct {
	locker *InMemoryDigestLocker
	dgst   digest.Digest
	entry  *digestLockEntry
}

func (lock *inMemoryDigestLock) Unlock() {
	lock.entry.mu.Unlock()

	lock.locker.mu.Lock()
	lock.entry.refCount--
	if lock.entry.refCount == 0 {
		delete(lock.locker.locks, lock.dgst)
	}
	lock.locker.mu.Unlock()
}

// NoopDigestLocker is a no-op implementation for backward compatibility
// or when locking is disabled.
type NoopDigestLocker struct{}

func (NoopDigestLocker) Lock(ctx context.Context, dgst digest.Digest) (DigestLock, error) {
	return noopLock{}, nil
}

type noopLock struct{}

func (noopLock) Unlock() {}

// SessionDeduplicator tracks active upload sessions to prevent duplicate
// sessions for the same blob or idempotency key.
type SessionDeduplicator struct {
	mu sync.RWMutex
	// byIdempotencyKey maps client-provided idempotency keys to session IDs
	byIdempotencyKey map[string]sessionEntry
	// byDigest maps digests to active session IDs (for deduplication)
	byDigest map[digest.Digest][]string
	// sessionTTL is how long to keep entries after session creation
	sessionTTL time.Duration
}

type sessionEntry struct {
	sessionID string
	createdAt time.Time
	digest    digest.Digest // optional, may be empty for chunked uploads
}

// NewSessionDeduplicator creates a new session deduplicator.
func NewSessionDeduplicator(sessionTTL time.Duration) *SessionDeduplicator {
	if sessionTTL == 0 {
		sessionTTL = 24 * time.Hour // default TTL
	}
	return &SessionDeduplicator{
		byIdempotencyKey: make(map[string]sessionEntry),
		byDigest:         make(map[digest.Digest][]string),
		sessionTTL:       sessionTTL,
	}
}

// RegisterSession registers a new upload session, optionally with an
// idempotency key and expected digest.
func (sd *SessionDeduplicator) RegisterSession(sessionID string, idempotencyKey string, dgst digest.Digest) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	entry := sessionEntry{
		sessionID: sessionID,
		createdAt: time.Now(),
		digest:    dgst,
	}

	if idempotencyKey != "" {
		sd.byIdempotencyKey[idempotencyKey] = entry
	}

	if dgst != "" {
		sd.byDigest[dgst] = append(sd.byDigest[dgst], sessionID)
	}
}

// LookupByIdempotencyKey returns the session ID for a given idempotency key
// if it exists and hasn't expired.
func (sd *SessionDeduplicator) LookupByIdempotencyKey(idempotencyKey string) (string, bool) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	entry, ok := sd.byIdempotencyKey[idempotencyKey]
	if !ok {
		return "", false
	}

	// Check if expired
	if time.Since(entry.createdAt) > sd.sessionTTL {
		return "", false
	}

	return entry.sessionID, true
}

// LookupByDigest returns the first active session ID for a given digest
// if one exists. This allows detecting when a session is already in progress
// for the same blob content, preventing duplicate sessions during client retries.
func (sd *SessionDeduplicator) LookupByDigest(dgst digest.Digest) (string, bool) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	sessions, ok := sd.byDigest[dgst]
	if !ok || len(sessions) == 0 {
		return "", false
	}

	// Return the first (oldest) session for this digest
	return sessions[0], true
}

// RemoveSession removes a session from tracking (called on commit or cancel).
// If idempotencyKey and dgst are empty, it will search all entries to find and remove the session.
func (sd *SessionDeduplicator) RemoveSession(sessionID string, idempotencyKey string, dgst digest.Digest) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	// If we have the key/digest, use them directly for efficient removal
	if idempotencyKey != "" {
		delete(sd.byIdempotencyKey, idempotencyKey)
	}

	if dgst != "" {
		sd.removeSessionFromDigest(sessionID, dgst)
	}

	// If we don't have the key/digest (e.g., resumed session), search for the session
	if idempotencyKey == "" && dgst == "" {
		// Search byIdempotencyKey for the session
		for key, entry := range sd.byIdempotencyKey {
			if entry.sessionID == sessionID {
				delete(sd.byIdempotencyKey, key)
				// Also remove from byDigest if we found the digest
				if entry.digest != "" {
					sd.removeSessionFromDigest(sessionID, entry.digest)
				}
				return
			}
		}

		// If not found by idempotency key, search byDigest
		for d, sessions := range sd.byDigest {
			for i, id := range sessions {
				if id == sessionID {
					sd.byDigest[d] = append(sessions[:i], sessions[i+1:]...)
					if len(sd.byDigest[d]) == 0 {
						delete(sd.byDigest, d)
					}
					return
				}
			}
		}
	}
}

// removeSessionFromDigest is a helper to remove a session from the byDigest map.
// Caller must hold the lock.
func (sd *SessionDeduplicator) removeSessionFromDigest(sessionID string, dgst digest.Digest) {
	sessions := sd.byDigest[dgst]
	for i, id := range sessions {
		if id == sessionID {
			sd.byDigest[dgst] = append(sessions[:i], sessions[i+1:]...)
			break
		}
	}
	if len(sd.byDigest[dgst]) == 0 {
		delete(sd.byDigest, dgst)
	}
}

// Cleanup removes expired entries. Should be called periodically.
func (sd *SessionDeduplicator) Cleanup() {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	now := time.Now()
	for key, entry := range sd.byIdempotencyKey {
		if now.Sub(entry.createdAt) > sd.sessionTTL {
			delete(sd.byIdempotencyKey, key)
		}
	}
}
