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
	// ICalData is the serialized iCalendar content.
	ICalData string
	// Warnings contains non-fatal issues encountered during generation.
	Warnings []ParseWarning
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
// [now, now+windowDays). The now parameter is used as the reference time.
func GenerateFeed(jobs []CronJobInfo, windowDays int, now time.Time) GenerateResult {
	cal := ics.NewCalendar()
	cal.SetProductId(productID)
	cal.SetVersion("2.0")
	cal.SetCalscale("GREGORIAN")
	cal.SetMethod(ics.MethodPublish)

	windowEnd := now.AddDate(0, 0, windowDays)

	var warnings []ParseWarning

	// Support both standard 5-field cron and predefined descriptors (@hourly, @daily, etc.)
	// as Kubernetes CronJob accepts both formats.
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

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
		}
	}

	return GenerateResult{
		ICalData: cal.Serialize(),
		Warnings: warnings,
	}
}
