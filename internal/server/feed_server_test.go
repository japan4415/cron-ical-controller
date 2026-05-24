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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

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
