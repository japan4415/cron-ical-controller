/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// DefaultFeedBindAddress is the default address for the feed HTTP server.
	DefaultFeedBindAddress = ":8082"

	// FeedPathPrefix is the URL prefix for feed endpoints.
	FeedPathPrefix = "/feeds/"

	// contentTypeICal is the Content-Type for iCalendar responses.
	contentTypeICal = "text/calendar; charset=utf-8"

	// shutdownTimeout is the time to wait for graceful shutdown.
	shutdownTimeout = 5 * time.Second

	// DefaultCacheMaxBytes bounds the total size of all cached feeds (64 MiB).
	//
	// Rationale: with MaxEventsPerFeed=10000 a single feed serializes to at
	// most ~2.4 MB (~240 B/event), so 64 MiB holds ~26 worst-case feeds or
	// hundreds of typically-sized ones (a default window=7d hourly feed is a
	// few tens of KB). It stays well below the manager's memory limit,
	// leaving room for generation-time working memory and the controller
	// runtime baseline. See README.md ("Sizing guidelines") for details.
	DefaultCacheMaxBytes = 64 << 20 // 64 MiB
)

// FeedCache provides thread-safe access to cached iCal feed data.
// Keys are in the format "/feeds/{namespace}/{name}.ics".
// The total size of cached data is bounded by a byte budget; inserting beyond
// it evicts the least-recently-updated entries (see Set).
type FeedCache struct {
	mu         sync.RWMutex
	feeds      map[string][]byte
	updated    map[string]time.Time
	totalBytes int
	maxBytes   int

	// nowFunc returns the time used to order entries for eviction. When nil,
	// time.Now is used. Injected in tests to make eviction order deterministic.
	nowFunc func() time.Time
}

// now returns the current eviction-order timestamp, honoring an injectable
// clock for tests.
func (c *FeedCache) now() time.Time {
	if c.nowFunc != nil {
		return c.nowFunc()
	}
	return time.Now()
}

// NewFeedCache creates a new empty feed cache with the default total size
// limit (DefaultCacheMaxBytes).
func NewFeedCache() *FeedCache {
	return NewFeedCacheWithLimit(DefaultCacheMaxBytes)
}

// NewFeedCacheWithLimit creates a new empty feed cache that keeps at most
// maxBytes bytes of feed data in total.
func NewFeedCacheWithLimit(maxBytes int) *FeedCache {
	return &FeedCache{
		feeds:    make(map[string][]byte),
		updated:  make(map[string]time.Time),
		maxBytes: maxBytes,
	}
}

// Set stores iCal data for the given path, taking ownership of the slice.
//
// If storing data would exceed the cache's byte budget, other entries are
// evicted oldest-update-first until it fits. This policy was chosen because:
//   - feeds are only rewritten by reconciliation, so the least-recently-
//     updated entry is the least likely to be regenerated again soon;
//     recently regenerated feeds are exactly the ones still receiving traffic
//   - the number of entries equals the number of CronICalFeed objects (small),
//     so an O(n) scan per eviction is negligible compared to serving cost
//   - it is deterministic and dependency-free (no LRU list bookkeeping)
//
// Ties on update time are broken by lexicographic path order so behavior is
// fully deterministic even within one clock tick.
//
// A single payload larger than the entire budget can never fit; it is refused
// (returning false) rather than served half-cached. Callers surface this via
// logs/events; the stale previous entry (if any) is dropped as well so that a
// too-large feed fails visibly instead of silently serving outdated content.
func (c *FeedCache) Set(path string, data []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()

	// Charge for the entry being replaced before evaluating the budget.
	if old, ok := c.feeds[path]; ok {
		c.totalBytes -= len(old)
		delete(c.feeds, path)
		delete(c.updated, path)
	}

	if len(data) > c.maxBytes {
		return false
	}

	for c.totalBytes+len(data) > c.maxBytes {
		victim := ""
		var oldest time.Time
		for p, ts := range c.updated {
			if victim == "" || ts.Before(oldest) || (ts.Equal(oldest) && p < victim) {
				victim, oldest = p, ts
			}
		}
		if victim == "" {
			break
		}
		c.totalBytes -= len(c.feeds[victim])
		delete(c.feeds, victim)
		delete(c.updated, victim)
	}

	c.feeds[path] = data
	c.updated[path] = now
	c.totalBytes += len(data)
	return true
}

// Get retrieves cached iCal data for the given path.
// Returns nil and false if the path is not cached.
func (c *FeedCache) Get(path string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	data, ok := c.feeds[path]
	return data, ok
}

// Delete removes cached data for the given path.
func (c *FeedCache) Delete(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.feeds[path]; ok {
		c.totalBytes -= len(old)
		delete(c.feeds, path)
		delete(c.updated, path)
	}
}

// TotalBytes returns the total number of bytes currently cached.
func (c *FeedCache) TotalBytes() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totalBytes
}

// FeedPath returns the canonical feed path for the given namespace and name.
func FeedPath(namespace, name string) string {
	return fmt.Sprintf("/feeds/%s/%s.ics", namespace, name)
}

// FeedServer serves iCal feeds over HTTP. It implements
// manager.Runnable and manager.LeaderElectionRunnable for integration
// with controller-runtime's manager lifecycle.
type FeedServer struct {
	bindAddress string
	cache       *FeedCache
	server      *http.Server
}

// NewFeedServer creates a new FeedServer with the given bind address and cache.
func NewFeedServer(bindAddress string, cache *FeedCache) *FeedServer {
	fs := &FeedServer{
		bindAddress: bindAddress,
		cache:       cache,
	}

	mux := http.NewServeMux()
	mux.HandleFunc(FeedPathPrefix, fs.handleFeed)

	fs.server = &http.Server{
		Addr:              bindAddress,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	return fs
}

// handleFeed serves cached iCal data for the requested path.
func (s *FeedServer) handleFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	data, ok := s.cache.Get(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", contentTypeICal)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// Start implements manager.Runnable. It starts the HTTP server and blocks
// until the context is cancelled.
func (s *FeedServer) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("feed-server")

	ln, err := net.Listen("tcp", s.bindAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.bindAddress, err)
	}

	log.Info("Starting feed server", "address", ln.Addr().String())

	// Run server in a goroutine
	errCh := make(chan error, 1)
	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	// Wait for context cancellation
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Info("Shutting down feed server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return s.server.Shutdown(shutdownCtx)
	}
}

// NeedLeaderElection implements manager.LeaderElectionRunnable.
// Feed server only runs on the leader to avoid serving stale data.
func (s *FeedServer) NeedLeaderElection() bool {
	return true
}
