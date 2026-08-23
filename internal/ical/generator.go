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
	"bytes"
	"fmt"
	"time"

	ics "github.com/arran4/golang-ical"
	"github.com/robfig/cron/v3"
)

const (
	// DurationLabelKey is the CronJob label key for specifying average duration.
	DurationLabelKey = "cron-ical.discord.jp/avg-duration"

	// icalDomain is used as the domain part of VEVENT UIDs.
	icalDomain = "cron-ical.discord.jp"

	// productID identifies this software in the VCALENDAR PRODID property.
	productID = "-//cron-ical-controller//EN"

	// MaxEventsPerFeed caps the number of VEVENTs a single feed may contain.
	//
	// Rationale (measured with golang-ical v0.3.5 / robfig cron v3.0.1):
	// a serialized VEVENT costs roughly 240 B, so the cap bounds a feed at
	// ~2.4 MB and the in-memory enumeration peak (~1.5-2 KB per event while
	// building the calendar) at ~15-25 MB per generation. Without a cap, a
	// legitimate window=90d x every-minute CronJob yields ~130k events
	// (~31 MB serialized, 180-290 MB peak heap), which OOMKills the manager
	// under its memory limit. The cap leaves ample headroom for low-to-medium
	// frequency schedules (e.g. hourly x window=90d only yields ~2.2k events),
	// while extreme combinations such as an every-minute schedule hit the cap
	// and are truncated (window=7d alone already produces ~10,080 firings;
	// see the sizing table in README.md).
	MaxEventsPerFeed = 10000
)

// CronJobInfo holds the information extracted from a CronJob needed to generate
// iCal events.
type CronJobInfo struct {
	// Name is the CronJob's name (used as VEVENT SUMMARY).
	Name string
	// Namespace is the CronJob's namespace.
	Namespace string
	// Schedule is the cron expression (5-field standard format).
	Schedule string
	// TimeZone is the IANA timezone for the schedule. Empty means UTC.
	TimeZone string
	// Duration is the expected runtime of the job.
	Duration time.Duration
}

// ParseWarning describes a non-fatal issue encountered during generation.
type ParseWarning struct {
	// CronJobName identifies the CronJob that caused the warning.
	CronJobName string
	// CronJobNamespace is the namespace of the CronJob.
	CronJobNamespace string
	// Message describes the issue.
	Message string
}

// GenerateResult contains the generated iCal data and any warnings.
type GenerateResult struct {
	// ICalData is the serialized iCalendar content. It is produced directly
	// from the serializer's output buffer (no intermediate string), so callers
	// may take ownership of the slice without an extra copy.
	ICalData []byte
	// Warnings contains non-fatal issues encountered during generation.
	Warnings []ParseWarning
	// EventCount is the number of VEVENTs contained in ICalData.
	EventCount int
	// Truncated reports whether enumeration was cut short because
	// MaxEventsPerFeed was reached. When true, events beyond the cap are
	// missing from ICalData and EventCount equals MaxEventsPerFeed.
	Truncated bool
}

// ParseDurationLabel parses the duration label value. If the value is empty or
// invalid (including negative values), it returns the defaultDuration and a
// boolean indicating whether a warning should be emitted (true if the value
// was non-empty but invalid/negative).
func ParseDurationLabel(labelValue string, defaultDuration time.Duration) (time.Duration, bool) {
	if labelValue == "" {
		return defaultDuration, false
	}
	d, err := time.ParseDuration(labelValue)
	if err != nil {
		return defaultDuration, true
	}
	if d < 0 {
		return defaultDuration, true
	}
	return d, false
}

// GenerateFeed creates an iCalendar feed from the given CronJob information.
// It generates VEVENTs for each scheduled firing within the time window
// [now, now+windowDays). The now parameter is used as the reference time,
// including for DTSTAMP, so callers must pass a stable (e.g. truncated)
// generation time: identical inputs always produce byte-identical output.
// This determinism is what allows the controller to detect "no content
// change" and skip redundant status updates. Callers should truncate now to
// a coarse granularity (see requeueInterval in internal/controller).
//
// Enumeration stops at MaxEventsPerFeed events (result.Truncated reports
// whether the cap was hit). Jobs are visited in input order, so once the cap
// is reached later jobs are dropped entirely; this keeps output deterministic
// and bounds memory regardless of how many high-frequency jobs match.
func GenerateFeed(jobs []CronJobInfo, windowDays int, now time.Time) GenerateResult {
	cal := ics.NewCalendar()
	cal.SetProductId(productID)
	cal.SetVersion("2.0")
	cal.SetCalscale("GREGORIAN")
	cal.SetMethod(ics.MethodPublish)

	windowEnd := now.AddDate(0, 0, windowDays)

	var warnings []ParseWarning
	eventCount := 0
	truncated := false

	// Support both standard 5-field cron and predefined descriptors (@hourly, @daily, etc.)
	// as Kubernetes CronJob accepts both formats.
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

jobLoop:
	for _, job := range jobs {
		// Determine timezone
		loc := time.UTC
		if job.TimeZone != "" {
			parsedLoc, err := time.LoadLocation(job.TimeZone)
			if err != nil {
				warnings = append(warnings, ParseWarning{
					CronJobName:      job.Name,
					CronJobNamespace: job.Namespace,
					Message:          fmt.Sprintf("invalid timezone %q, using UTC: %v", job.TimeZone, err),
				})
			} else {
				loc = parsedLoc
			}
		}

		// Parse cron expression with timezone support
		schedule, err := parser.Parse(job.Schedule)
		if err != nil {
			warnings = append(warnings, ParseWarning{
				CronJobName:      job.Name,
				CronJobNamespace: job.Namespace,
				Message:          fmt.Sprintf("failed to parse cron expression %q: %v", job.Schedule, err),
			})
			continue
		}

		// Enumerate firings within the half-open window [now, windowEnd).
		// Use the reference time converted to the job's timezone for correct scheduling.
		t := now.In(loc)
		for {
			t = schedule.Next(t)
			if !t.Before(windowEnd) || t.IsZero() {
				break
			}
			if eventCount >= MaxEventsPerFeed {
				truncated = true
				break jobLoop
			}

			// Convert to UTC for iCal
			startUTC := t.UTC()
			endUTC := startUTC.Add(job.Duration)

			uid := fmt.Sprintf("%s-%s-%s@%s",
				job.Namespace, job.Name, startUTC.Format(time.RFC3339), icalDomain)

			event := cal.AddEvent(uid)
			event.SetDtStampTime(now.UTC())
			event.SetStartAt(startUTC)
			event.SetEndAt(endUTC)
			event.SetSummary(job.Name)
			descTZ := job.TimeZone
			if descTZ == "" {
				descTZ = "UTC"
			}
			event.SetDescription(fmt.Sprintf("Namespace: %s\nSchedule: %s\nTimeZone: %s",
				job.Namespace, job.Schedule, descTZ))
			eventCount++
		}
	}

	// Serialize straight into a byte buffer instead of going through
	// Serialize()'s string: that avoids allocating an intermediate string
	// copy of the whole calendar on top of the bytes we hand to the cache
	// (~31 MB duplicated for an uncapped 90d x every-minute feed). The only
	// error SerializeTo can report is a write failure from the underlying
	// writer, which bytes.Buffer never produces — same reasoning as the
	// library's own Serialize() implementation.
	var buf bytes.Buffer
	_ = cal.SerializeTo(&buf)

	return GenerateResult{
		ICalData:   buf.Bytes(),
		Warnings:   warnings,
		EventCount: eventCount,
		Truncated:  truncated,
	}
}
