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

package ical

import (
	"strings"
	"testing"
	"time"
)

func TestParseDurationLabel(t *testing.T) {
	defaultDur := 5 * time.Minute

	tests := []struct {
		name         string
		labelValue   string
		wantDuration time.Duration
		wantWarning  bool
	}{
		{
			name:         "empty label returns default",
			labelValue:   "",
			wantDuration: defaultDur,
			wantWarning:  false,
		},
		{
			name:         "valid duration 5m",
			labelValue:   "5m",
			wantDuration: 5 * time.Minute,
			wantWarning:  false,
		},
		{
			name:         "valid duration 1h30m",
			labelValue:   "1h30m",
			wantDuration: 90 * time.Minute,
			wantWarning:  false,
		},
		{
			name:         "valid duration 30s",
			labelValue:   "30s",
			wantDuration: 30 * time.Second,
			wantWarning:  false,
		},
		{
			name:         "invalid duration returns default with warning",
			labelValue:   "invalid",
			wantDuration: defaultDur,
			wantWarning:  true,
		},
		{
			name:         "invalid duration numeric only",
			labelValue:   "123",
			wantDuration: defaultDur,
			wantWarning:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dur, warn := ParseDurationLabel(tt.labelValue, defaultDur)
			if dur != tt.wantDuration {
				t.Errorf("ParseDurationLabel(%q) duration = %v, want %v", tt.labelValue, dur, tt.wantDuration)
			}
			if warn != tt.wantWarning {
				t.Errorf("ParseDurationLabel(%q) warning = %v, want %v", tt.labelValue, warn, tt.wantWarning)
			}
		})
	}
}

func TestGenerateFeed_NoCronJobs(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	result := GenerateFeed(nil, 7, now)

	if len(result.Warnings) != 0 {
		t.Errorf("expected no warnings, got %d", len(result.Warnings))
	}

	// Should produce a valid VCALENDAR with no VEVENT
	if !strings.Contains(result.ICalData, "BEGIN:VCALENDAR") {
		t.Error("expected VCALENDAR in output")
	}
	if strings.Contains(result.ICalData, "BEGIN:VEVENT") {
		t.Error("expected no VEVENT for empty job list")
	}
}

func TestGenerateFeed_SingleCronJob(t *testing.T) {
	// Every hour on the hour
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	jobs := []CronJobInfo{
		{
			Name:      "test-job",
			Namespace: "default",
			Schedule:  "0 * * * *",
			TimeZone:  "UTC",
			Duration:  30 * time.Minute,
		},
	}

	result := GenerateFeed(jobs, 1, now) // 1 day window

	if len(result.Warnings) != 0 {
		t.Errorf("expected no warnings, got %d: %v", len(result.Warnings), result.Warnings)
	}

	// 24 hours, each hour => 24 events (hour 0 is after now, so 1:00-23:00 = 23, plus 0:00 next day... wait)
	// now = 2026-01-01 00:00:00 UTC, window ends 2026-01-02 00:00:00 UTC
	// Next after 00:00 is 01:00, 02:00, ..., 23:00 = 23 events
	eventCount := strings.Count(result.ICalData, "BEGIN:VEVENT")
	if eventCount != 23 {
		t.Errorf("expected 23 events, got %d", eventCount)
	}

	// Check SUMMARY contains job name
	if !strings.Contains(result.ICalData, "SUMMARY:test-job") {
		t.Error("expected SUMMARY:test-job in output")
	}
}

func TestGenerateFeed_InvalidCronExpression(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	jobs := []CronJobInfo{
		{
			Name:      "bad-cron-job",
			Namespace: "default",
			Schedule:  "invalid cron",
			TimeZone:  "UTC",
			Duration:  5 * time.Minute,
		},
	}

	result := GenerateFeed(jobs, 7, now)

	if len(result.Warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d", len(result.Warnings))
	}
	if result.Warnings[0].CronJobName != "bad-cron-job" {
		t.Errorf("expected warning for bad-cron-job, got %s", result.Warnings[0].CronJobName)
	}
	if !strings.Contains(result.Warnings[0].Message, "failed to parse cron expression") {
		t.Errorf("expected parse error warning, got: %s", result.Warnings[0].Message)
	}
}

func TestGenerateFeed_InvalidTimeZone(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	jobs := []CronJobInfo{
		{
			Name:      "tz-job",
			Namespace: "default",
			Schedule:  "0 12 * * *",
			TimeZone:  "Invalid/Timezone",
			Duration:  5 * time.Minute,
		},
	}

	result := GenerateFeed(jobs, 1, now)

	// Should produce a warning about invalid timezone
	foundTZWarning := false
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "invalid timezone") {
			foundTZWarning = true
			break
		}
	}
	if !foundTZWarning {
		t.Error("expected timezone warning")
	}

	// Should still generate events (using UTC fallback)
	if !strings.Contains(result.ICalData, "BEGIN:VEVENT") {
		t.Error("expected events even with invalid timezone (UTC fallback)")
	}
}

func TestGenerateFeed_TimeZoneConversion(t *testing.T) {
	// CronJob runs at 09:00 Asia/Tokyo (= 00:00 UTC)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	jobs := []CronJobInfo{
		{
			Name:      "tokyo-job",
			Namespace: "default",
			Schedule:  "0 9 * * *",
			TimeZone:  "Asia/Tokyo",
			Duration:  30 * time.Minute,
		},
	}

	result := GenerateFeed(jobs, 2, now)

	if len(result.Warnings) != 0 {
		t.Errorf("expected no warnings, got %d", len(result.Warnings))
	}

	// 09:00 JST = 00:00 UTC. now is 00:00 UTC, so next fire is 09:00 JST on Jan 1 = 00:00 UTC Jan 1.
	// But since now==00:00 UTC Jan 1, Next() should return the next occurrence after now,
	// which for "0 9 * * *" in Asia/Tokyo from 2026-01-01 09:00:00 JST...
	// now in JST is 2026-01-01 09:00:00, so next is 2026-01-02 09:00:00 JST = 2026-01-02 00:00:00 UTC
	eventCount := strings.Count(result.ICalData, "BEGIN:VEVENT")
	if eventCount != 1 {
		t.Errorf("expected 1 event (next day), got %d", eventCount)
	}
}

func TestGenerateFeed_MultipleJobs(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	jobs := []CronJobInfo{
		{
			Name:      "daily-job",
			Namespace: "ns1",
			Schedule:  "0 12 * * *",
			TimeZone:  "UTC",
			Duration:  1 * time.Hour,
		},
		{
			Name:      "hourly-job",
			Namespace: "ns2",
			Schedule:  "0 * * * *",
			TimeZone:  "UTC",
			Duration:  15 * time.Minute,
		},
	}

	result := GenerateFeed(jobs, 1, now)

	if len(result.Warnings) != 0 {
		t.Errorf("expected no warnings, got %d", len(result.Warnings))
	}

	// daily-job: 12:00 = 1 event
	// hourly-job: 01:00-23:00 = 23 events
	// Total = 24
	eventCount := strings.Count(result.ICalData, "BEGIN:VEVENT")
	if eventCount != 24 {
		t.Errorf("expected 24 events, got %d", eventCount)
	}

	if !strings.Contains(result.ICalData, "SUMMARY:daily-job") {
		t.Error("expected SUMMARY:daily-job")
	}
	if !strings.Contains(result.ICalData, "SUMMARY:hourly-job") {
		t.Error("expected SUMMARY:hourly-job")
	}
}

func TestGenerateFeed_ZeroDuration(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	jobs := []CronJobInfo{
		{
			Name:      "zero-dur-job",
			Namespace: "default",
			Schedule:  "0 12 * * *",
			TimeZone:  "UTC",
			Duration:  0,
		},
	}

	result := GenerateFeed(jobs, 1, now)

	// DTSTART and DTEND should be the same
	if !strings.Contains(result.ICalData, "BEGIN:VEVENT") {
		t.Error("expected VEVENT even with zero duration")
	}
}

func TestGenerateFeed_UIDFormat(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	jobs := []CronJobInfo{
		{
			Name:      "uid-test",
			Namespace: "myns",
			Schedule:  "0 12 * * *",
			TimeZone:  "UTC",
			Duration:  5 * time.Minute,
		},
	}

	result := GenerateFeed(jobs, 1, now)

	// UID format: namespace-name-DTSTART(RFC3339)@cron-ical.discord.jp
	expectedUID := "myns-uid-test-2026-01-01T12:00:00Z@cron-ical.discord.jp"
	if !strings.Contains(result.ICalData, expectedUID) {
		t.Errorf("expected UID %q in output, got:\n%s", expectedUID, result.ICalData)
	}
}

func TestGenerateFeed_VCALENDAR_Properties(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	result := GenerateFeed(nil, 7, now)

	if !strings.Contains(result.ICalData, "VERSION:2.0") {
		t.Error("expected VERSION:2.0")
	}
	if !strings.Contains(result.ICalData, "PRODID:"+productID) {
		t.Error("expected PRODID:" + productID)
	}
	if !strings.Contains(result.ICalData, "CALSCALE:GREGORIAN") {
		t.Error("expected CALSCALE:GREGORIAN")
	}
}
