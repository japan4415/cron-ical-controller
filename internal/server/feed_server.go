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
)

// FeedCache provides thread-safe access to cached iCal feed data.
// Keys are in the format "/feeds/{namespace}/{name}.ics".
type FeedCache struct {
	mu    sync.RWMutex
	feeds map[string][]byte
}

// NewFeedCache creates a new empty feed cache.
func NewFeedCache() *FeedCache {
	return &FeedCache{
		feeds: make(map[string][]byte),
	}
}

// Set stores iCal data for the given path.
func (c *FeedCache) Set(path string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.feeds[path] = data
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
	delete(c.feeds, path)
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
