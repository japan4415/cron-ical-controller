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

package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cronicalv1alpha1 "github.com/japan4415/cron-ical-controller/api/v1alpha1"
)

func TestTruncateGenerationTime(t *testing.T) {
	tests := []struct {
		name string
		in   time.Time
		want time.Time
	}{
		{
			name: "truncates sub-hour components",
			in:   time.Date(2026, 8, 23, 10, 17, 45, 123, time.UTC),
			want: time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC),
		},
		{
			name: "already truncated time is unchanged",
			in:   time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC),
			want: time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC),
		},
		{
			name: "rounding is anchored to UTC boundaries",
			in:   time.Date(2026, 8, 23, 9, 59, 59, 0, time.FixedZone("CET", 3600)),
			want: time.Date(2026, 8, 23, 9, 0, 0, 0, time.FixedZone("CET", 3600)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := truncateGenerationTime(tt.in); !got.Equal(tt.want) {
				t.Errorf("truncateGenerationTime(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestGenerationTimesEqual(t *testing.T) {
	base := metav1.NewTime(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	sameInstant := metav1.NewTime(time.Date(2026, 8, 23, 19, 0, 0, 0, time.FixedZone("JST", 9*3600)))
	different := metav1.NewTime(time.Date(2026, 8, 23, 11, 0, 0, 0, time.UTC))

	tests := []struct {
		name string
		a, b *metav1.Time
		want bool
	}{
		{name: "both nil", a: nil, b: nil, want: true},
		{name: "one nil", a: &base, b: nil, want: false},
		{name: "other nil", a: nil, b: &base, want: false},
		{name: "same instant different zone", a: &base, b: &sameInstant, want: true},
		{name: "different instants", a: &base, b: &different, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generationTimesEqual(tt.a, tt.b); got != tt.want {
				t.Errorf("generationTimesEqual(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestConditionsEquivalent(t *testing.T) {
	condition := func(typ, status, reason, message string, observedGeneration int64, ltt time.Time) metav1.Condition {
		return metav1.Condition{
			Type:               typ,
			Status:             metav1.ConditionStatus(status),
			ObservedGeneration: observedGeneration,
			LastTransitionTime: metav1.NewTime(ltt),
			Reason:             reason,
			Message:            message,
		}
	}
	base := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)

	a := []metav1.Condition{
		condition("Ready", "True", "FeedGenerated", "ok", 3, base),
		condition(cronicalv1alpha1.ConditionDegraded, "False", "AsExpected", "Generated 5 event(s)", 3, base),
	}

	tests := []struct {
		name string
		b    []metav1.Condition
		want bool
	}{
		{
			name: "identical conditions",
			b:    append([]metav1.Condition(nil), a...),
			want: true,
		},
		{
			name: "only LastTransitionTime differs",
			b: []metav1.Condition{
				condition("Ready", "True", "FeedGenerated", "ok", 3, base.Add(time.Minute)),
				condition(cronicalv1alpha1.ConditionDegraded, "False", "AsExpected", "Generated 5 event(s)", 3, base.Add(time.Hour)),
			},
			want: true,
		},
		{
			name: "status changed",
			b: []metav1.Condition{
				condition("Ready", "False", "ListFailed", "ok", 3, base),
				condition(cronicalv1alpha1.ConditionDegraded, "False", "AsExpected", "Generated 5 event(s)", 3, base),
			},
			want: false,
		},
		{
			name: "message changed",
			b: []metav1.Condition{
				condition("Ready", "True", "FeedGenerated", "ok", 3, base),
				condition(cronicalv1alpha1.ConditionDegraded, "False", "AsExpected", "Generated 7 event(s)", 3, base),
			},
			want: false,
		},
		{
			name: "observed generation changed",
			b: []metav1.Condition{
				condition("Ready", "True", "FeedGenerated", "ok", 4, base),
				condition(cronicalv1alpha1.ConditionDegraded, "False", "AsExpected", "Generated 5 event(s)", 3, base),
			},
			want: false,
		},
		{
			name: "length differs",
			b: []metav1.Condition{
				condition("Ready", "True", "FeedGenerated", "ok", 3, base),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := conditionsEquivalent(a, tt.b); got != tt.want {
				t.Errorf("conditionsEquivalent(...) = %v, want %v", got, tt.want)
			}
		})
	}

	if conditionsEquivalent(a, nil) {
		t.Error("expected nil conditions list to differ from populated list")
	}
	if !conditionsEquivalent(nil, nil) {
		t.Error("expected two nil lists to be equivalent")
	}
	if !conditionsEquivalent([]metav1.Condition{}, nil) {
		t.Error("expected empty and nil lists to be equivalent")
	}
}

func TestStatusEquivalent(t *testing.T) {
	genTime := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)

	newStatus := func() *cronicalv1alpha1.CronICalFeedStatus {
		return &cronicalv1alpha1.CronICalFeedStatus{
			LastGeneratedAt: ptrTime(genTime),
			CronJobCount:    2,
			EventCount:      14,
			FeedURL:         "/feeds/default/test-feed.ics",
			Conditions: []metav1.Condition{
				{
					Type:               cronicalv1alpha1.ConditionReady,
					Status:             metav1.ConditionTrue,
					Reason:             "FeedGenerated",
					Message:            "Feed generated",
					ObservedGeneration: 1,
				},
			},
		}
	}

	t.Run("deep copy is equivalent", func(t *testing.T) {
		prev := newStatus().DeepCopy()
		cur := newStatus()
		// Mutating the current snapshot must not affect the previous one.
		cur.LastGeneratedAt = ptrTime(genTime.Add(time.Hour))
		cur.EventCount++
		if !statusEquivalent(prev, newStatus()) {
			t.Error("expected fresh status to be equivalent to its earlier deep copy")
		}
	})

	t.Run("event count change alone triggers update", func(t *testing.T) {
		prev := newStatus()
		cur := newStatus()
		cur.EventCount++
		if statusEquivalent(prev, cur) {
			t.Error("expected eventCount change to be detected")
		}
	})

	t.Run("last generated at change alone triggers update", func(t *testing.T) {
		prev := newStatus()
		cur := newStatus()
		cur.LastGeneratedAt = ptrTime(genTime.Add(requeueInterval))
		if statusEquivalent(prev, cur) {
			t.Error("expected LastGeneratedAt advance to be detected")
		}
	})

	t.Run("nil vs populated", func(t *testing.T) {
		if statusEquivalent(nil, newStatus()) {
			t.Error("expected nil previous status to differ")
		}
		if statusEquivalent(newStatus(), nil) {
			t.Error("expected nil current status to differ")
		}
		if !statusEquivalent(nil, nil) {
			t.Error("expected two nil statuses to be equivalent")
		}
	})
}

func ptrTime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}
