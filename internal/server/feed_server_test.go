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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFeedCache_SetAndGet(t *testing.T) {
	cache := NewFeedCache()

	path := FeedPath("default", "my-feed")
	data := []byte("BEGIN:VCALENDAR\nEND:VCALENDAR")

	// Get before Set should return not found
	_, ok := cache.Get(path)
	if ok {
		t.Error("expected not found before Set")
	}

	// Set and Get
	cache.Set(path, data)
	got, ok := cache.Get(path)
	if !ok {
		t.Fatal("expected found after Set")
	}
	if string(got) != string(data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestFeedCache_Delete(t *testing.T) {
	cache := NewFeedCache()

	path := FeedPath("ns", "name")
	cache.Set(path, []byte("data"))

	cache.Delete(path)
	_, ok := cache.Get(path)
	if ok {
		t.Error("expected not found after Delete")
	}
}

// newTestCache creates a cache with the given byte budget and a monotonic
// injected clock so that eviction order is fully deterministic.
func newTestCache(maxBytes int) *FeedCache {
	cache := NewFeedCacheWithLimit(maxBytes)
	tick := 0
	cache.nowFunc = func() time.Time {
		tick++
		return time.Unix(int64(tick), 0)
	}
	return cache
}

func TestFeedCache_EvictsLeastRecentlyUpdatedWhenOverLimit(t *testing.T) {
	// Budget fits exactly two 10-byte entries; inserting a third must evict
	// the least-recently-updated one (a), not b or c.
	cache := newTestCache(20)

	a := FeedPath("ns", "a")
	b := FeedPath("ns", "b")
	c := FeedPath("ns", "c")

	if !cache.Set(a, make([]byte, 10)) {
		t.Fatal("expected first Set to succeed")
	}
	if !cache.Set(b, make([]byte, 10)) {
		t.Fatal("expected second Set to succeed")
	}
	// Touch a so it becomes more recent than b.
	cache.Set(a, make([]byte, 10))

	if _, ok := cache.Get(b); !ok {
		t.Fatal("expected b to be cached before exceeding the budget")
	}

	if !cache.Set(c, make([]byte, 10)) {
		t.Fatal("expected third Set to succeed")
	}

	// b is now the least-recently-updated entry and must have been evicted.
	if _, ok := cache.Get(b); ok {
		t.Error("expected least-recently-updated entry b to be evicted")
	}
	for _, p := range []string{a, c} {
		if _, ok := cache.Get(p); !ok {
			t.Errorf("expected recently updated entry %s to survive", p)
		}
	}
	if got := cache.TotalBytes(); got != 20 {
		t.Errorf("TotalBytes = %d, want 20", got)
	}
}

func TestFeedCache_ReplacingSamePathDoesNotGrowBudgetUse(t *testing.T) {
	cache := newTestCache(100)

	path := FeedPath("default", "feed")
	if !cache.Set(path, make([]byte, 80)) {
		t.Fatal("expected Set to succeed")
	}
	// Overwrite with a smaller payload: the replaced bytes must be released.
	if !cache.Set(path, make([]byte, 30)) {
		t.Fatal("expected overwrite to succeed")
	}
	if got := cache.TotalBytes(); got != 30 {
		t.Errorf("TotalBytes = %d, want 30 (replaced entry must release its bytes)", got)
	}

	// The freed space allows another feed without eviction.
	other := FeedPath("default", "other")
	if !cache.Set(other, make([]byte, 50)) {
		t.Fatalf("expected second feed to fit after replacement, TotalBytes=%d", cache.TotalBytes())
	}
	if _, ok := cache.Get(path); !ok {
		t.Error("expected original feed to remain cached")
	}
}

func TestFeedCache_RefusesPayloadLargerThanEntireBudget(t *testing.T) {
	cache := newTestCache(16)

	huge := FeedPath("default", "huge")
	if cache.Set(huge, make([]byte, 17)) {
		t.Error("expected Set of an oversized payload to be refused")
	}
	if _, ok := cache.Get(huge); ok {
		t.Error("oversized payload must not be cached")
	}
	if got := cache.TotalBytes(); got != 0 {
		t.Errorf("TotalBytes = %d, want 0", got)
	}

	// Exactly at the limit still fits.
	exact := FeedPath("default", "exact")
	if !cache.Set(exact, make([]byte, 16)) {
		t.Error("expected payload equal to the budget to be cached")
	}
}

func TestFeedCache_DefaultLimitApplied(t *testing.T) {
	cache := NewFeedCache()
	if cache.maxBytes != DefaultCacheMaxBytes {
		t.Errorf("maxBytes = %d, want DefaultCacheMaxBytes (%d)", cache.maxBytes, DefaultCacheMaxBytes)
	}
}

func TestFeedCache_TotalBytesTracksDeletes(t *testing.T) {
	cache := newTestCache(1024)

	path := FeedPath("default", "feed")
	cache.Set(path, make([]byte, 123))
	if got := cache.TotalBytes(); got != 123 {
		t.Errorf("TotalBytes = %d, want 123", got)
	}

	cache.Delete(path)
	if got := cache.TotalBytes(); got != 0 {
		t.Errorf("TotalBytes after Delete = %d, want 0", got)
	}
}

func TestFeedPath(t *testing.T) {
	tests := []struct {
		namespace string
		name      string
		want      string
	}{
		{"default", "my-feed", "/feeds/default/my-feed.ics"},
		{"kube-system", "cron-feed", "/feeds/kube-system/cron-feed.ics"},
	}

	for _, tt := range tests {
		got := FeedPath(tt.namespace, tt.name)
		if got != tt.want {
			t.Errorf("FeedPath(%q, %q) = %q, want %q", tt.namespace, tt.name, got, tt.want)
		}
	}
}

func TestFeedServer_HandleFeed_CachedData(t *testing.T) {
	cache := NewFeedCache()
	icalData := []byte("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR")
	feedPath := FeedPath("default", "test-feed")
	cache.Set(feedPath, icalData)

	server := NewFeedServer(":0", cache)

	req := httptest.NewRequest(http.MethodGet, feedPath, nil)
	rec := httptest.NewRecorder()

	server.handleFeed(rec, req)

	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if ct != contentTypeICal {
		t.Errorf("expected Content-Type %q, got %q", contentTypeICal, ct)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != string(icalData) {
		t.Errorf("response body mismatch: got %q", body)
	}
}

func TestFeedServer_HandleFeed_NotFound(t *testing.T) {
	cache := NewFeedCache()
	server := NewFeedServer(":0", cache)

	req := httptest.NewRequest(http.MethodGet, "/feeds/default/nonexistent.ics", nil)
	rec := httptest.NewRecorder()

	server.handleFeed(rec, req)

	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestFeedServer_HandleFeed_MethodNotAllowed(t *testing.T) {
	cache := NewFeedCache()
	server := NewFeedServer(":0", cache)

	req := httptest.NewRequest(http.MethodPost, "/feeds/default/test.ics", nil)
	rec := httptest.NewRecorder()

	server.handleFeed(rec, req)

	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.StatusCode)
	}
}

func TestFeedServer_NeedLeaderElection(t *testing.T) {
	server := NewFeedServer(":0", NewFeedCache())
	if !server.NeedLeaderElection() {
		t.Error("FeedServer should need leader election")
	}
}
