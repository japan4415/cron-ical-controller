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
	"context"
	"fmt"
	"slices"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cronicalv1alpha1 "github.com/japan4415/cron-ical-controller/api/v1alpha1"
	"github.com/japan4415/cron-ical-controller/internal/ical"
	"github.com/japan4415/cron-ical-controller/internal/server"
)

const (
	// requeueInterval is the default interval for periodic reconciliation
	// to slide the event generation window.
	requeueInterval = 1 * time.Hour
)

// CronICalFeedReconciler reconciles a CronICalFeed object
type CronICalFeedReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	FeedCache *server.FeedCache

	// NowFunc returns the reference time used for feed generation. When nil,
	// time.Now is used. Injected in tests to make generation windows deterministic.
	NowFunc func() time.Time
}

// now returns the current generation reference time, honoring an injectable
// clock for tests.
func (r *CronICalFeedReconciler) now() time.Time {
	if r.NowFunc != nil {
		return r.NowFunc()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=cron-ical.discord.jp,resources=cronicalfeeds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cron-ical.discord.jp,resources=cronicalfeeds/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cron-ical.discord.jp,resources=cronicalfeeds/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile generates an iCal feed from CronJobs matching the CronICalFeed's selector
// and stores the result in the in-memory cache for HTTP serving.
func (r *CronICalFeedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1. Fetch the CronICalFeed resource
	var feed cronicalv1alpha1.CronICalFeed
	if err := r.Get(ctx, req.NamespacedName, &feed); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Resource was deleted; clean up cache
			feedPath := server.FeedPath(req.Namespace, req.Name)
			r.FeedCache.Delete(feedPath)
			log.Info("CronICalFeed deleted, removed from cache", "path", feedPath)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Determine window and defaults
	windowDays := int(7)
	if feed.Spec.Window != nil {
		windowDays = int(*feed.Spec.Window)
	}

	defaultDuration := time.Duration(0)
	if feed.Spec.DefaultDuration != "" {
		d, err := time.ParseDuration(feed.Spec.DefaultDuration)
		if err != nil {
			log.Error(err, "invalid defaultDuration, using 0s", "value", feed.Spec.DefaultDuration)
			r.Recorder.Eventf(&feed, "Warning", "InvalidDefaultDuration",
				"Invalid defaultDuration %q: %v, using 0s", feed.Spec.DefaultDuration, err)
		} else if d < 0 {
			log.Info("negative defaultDuration not allowed, using 0s", "value", feed.Spec.DefaultDuration)
			r.Recorder.Eventf(&feed, "Warning", "InvalidDefaultDuration",
				"Negative defaultDuration %q not allowed, using 0s", feed.Spec.DefaultDuration)
		} else {
			defaultDuration = d
		}
	}

	defaultTimeZone := "UTC"
	if feed.Spec.DefaultTimeZone != "" {
		defaultTimeZone = feed.Spec.DefaultTimeZone
	}

	// 3. List CronJobs matching the selector
	cronJobs, err := r.listCronJobs(ctx, &feed)
	if err != nil {
		log.Error(err, "failed to list CronJobs")
		prevConditions := append([]metav1.Condition(nil), feed.Status.Conditions...)
		meta.SetStatusCondition(&feed.Status.Conditions, metav1.Condition{
			Type:               cronicalv1alpha1.ConditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: feed.Generation,
			Reason:             "ListFailed",
			Message:            fmt.Sprintf("Failed to list CronJobs: %v", err),
		})
		meta.SetStatusCondition(&feed.Status.Conditions, metav1.Condition{
			Type:               cronicalv1alpha1.ConditionCronJobsFound,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: feed.Generation,
			Reason:             "ListFailed",
			Message:            fmt.Sprintf("Unable to determine CronJob count: %v", err),
		})
		// Only write the status when the conditions actually changed; repeated
		// failures with an identical error would otherwise keep generating
		// watch events for our own object.
		if !conditionsEquivalent(prevConditions, feed.Status.Conditions) {
			if statusErr := r.Status().Update(ctx, &feed); statusErr != nil {
				log.Error(statusErr, "failed to update status")
			}
		}
		return ctrl.Result{}, err
	}

	// 4. Build CronJobInfo list
	var jobInfos []ical.CronJobInfo
	for i := range cronJobs {
		cj := &cronJobs[i]

		// Determine timezone
		tz := defaultTimeZone
		if cj.Spec.TimeZone != nil && *cj.Spec.TimeZone != "" {
			tz = *cj.Spec.TimeZone
		}

		// Parse duration from label
		duration := defaultDuration
		if labelVal, ok := cj.Labels[ical.DurationLabelKey]; ok {
			d, warn := ical.ParseDurationLabel(labelVal, defaultDuration)
			duration = d
			if warn {
				r.Recorder.Eventf(&feed, "Warning", "InvalidDurationLabel",
					"CronJob %s/%s has invalid duration label %q, using default %v",
					cj.Namespace, cj.Name, labelVal, defaultDuration)
			}
		}

		jobInfos = append(jobInfos, ical.CronJobInfo{
			Name:      cj.Name,
			Namespace: cj.Namespace,
			Schedule:  cj.Spec.Schedule,
			TimeZone:  tz,
			Duration:  duration,
		})
	}

	// 5. Generate iCal feed
	// Truncate the generation time to the requeue interval so that both the
	// generated calendar bytes (DTSTAMP) and LastGeneratedAt stay constant
	// within a generation window. This keeps the feed byte-for-byte
	// deterministic across reconciles in the same window, which is what makes
	// the no-op status update skip below effective.
	now := truncateGenerationTime(r.now())
	result := ical.GenerateFeed(jobInfos, windowDays, now)

	// Record warnings from generation
	for _, w := range result.Warnings {
		r.Recorder.Eventf(&feed, "Warning", "GenerationWarning",
			"CronJob %s/%s: %s", w.CronJobNamespace, w.CronJobName, w.Message)
	}

	// 6. Store in cache (path is always /feeds/{namespace}/{name}.ics)
	feedPath := server.FeedPath(feed.Namespace, feed.Name)
	r.FeedCache.Set(feedPath, []byte(result.ICalData))

	// 7. Update status only when the observed state meaningfully changed.
	// An unconditional update would emit a watch event for our own object on
	// every reconcile (extra etcd writes, and before GenerationChangedPredicate
	// existed, a self-induced reconcile loop). LastGeneratedAt alone never
	// triggers a write because it is truncated to the requeue interval: within
	// one generation window it does not change.
	prevStatus := feed.Status.DeepCopy()

	nowMeta := metav1.NewTime(now)
	feed.Status.LastGeneratedAt = &nowMeta
	feed.Status.CronJobCount = int32(len(cronJobs))
	feed.Status.EventCount = int32(result.EventCount)
	feed.Status.FeedURL = feedPath

	// Set conditions
	cronJobsFound := len(cronJobs) > 0
	cronJobsFoundStatus := metav1.ConditionFalse
	cronJobsFoundMsg := "No CronJobs matched the selector"
	if cronJobsFound {
		cronJobsFoundStatus = metav1.ConditionTrue
		cronJobsFoundMsg = fmt.Sprintf("Found %d CronJob(s)", len(cronJobs))
	}
	meta.SetStatusCondition(&feed.Status.Conditions, metav1.Condition{
		Type:               cronicalv1alpha1.ConditionCronJobsFound,
		Status:             cronJobsFoundStatus,
		ObservedGeneration: feed.Generation,
		Reason:             "Checked",
		Message:            cronJobsFoundMsg,
	})
	meta.SetStatusCondition(&feed.Status.Conditions, metav1.Condition{
		Type:               cronicalv1alpha1.ConditionReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: feed.Generation,
		Reason:             "FeedGenerated",
		Message: fmt.Sprintf("Feed generated with %d event(s) from %d CronJob(s)",
			result.EventCount, len(cronJobs)),
	})

	// Degraded flags the (defensive) case where CronJobs matched but none of
	// them produced events. The API server validates cron expressions at
	// admission time, so reaching this state should be nearly impossible; it
	// exists so that such failures are observable via status instead of being
	// silently served as an empty calendar.
	degradedStatus := metav1.ConditionFalse
	degradedReason := "AsExpected"
	var degradedMessage string
	switch {
	case len(cronJobs) > 0 && result.EventCount == 0 && len(result.Warnings) > 0:
		degradedStatus = metav1.ConditionTrue
		degradedReason = "CronJobsUnparseable"
		degradedMessage = fmt.Sprintf("%d CronJob(s) matched but no events were generated; see generation warning events",
			len(cronJobs))
	case result.EventCount > 0:
		degradedMessage = fmt.Sprintf("Generated %d event(s)", result.EventCount)
	case len(cronJobs) == 0:
		degradedMessage = "No CronJobs matched the selector"
	default:
		degradedMessage = "Matched CronJob(s) produced no events within the window"
	}
	meta.SetStatusCondition(&feed.Status.Conditions, metav1.Condition{
		Type:               cronicalv1alpha1.ConditionDegraded,
		Status:             degradedStatus,
		ObservedGeneration: feed.Generation,
		Reason:             degradedReason,
		Message:            degradedMessage,
	})

	if !statusEquivalent(prevStatus, &feed.Status) {
		if err := r.Status().Update(ctx, &feed); err != nil {
			log.Error(err, "failed to update CronICalFeed status")
			return ctrl.Result{}, err
		}
	} else {
		log.V(1).Info("status unchanged; skipping status update",
			"cronJobCount", len(cronJobs),
			"eventCount", result.EventCount,
		)
	}

	log.Info("Reconcile complete",
		"cronJobCount", len(cronJobs),
		"eventCount", result.EventCount,
		"feedPath", feedPath,
		"windowDays", windowDays,
	)

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// truncateGenerationTime rounds t down to the requeueInterval boundary so that
// the generation reference time only advances once per interval. The rounding
// is anchored to UTC hour boundaries for the default 1h interval.
func truncateGenerationTime(t time.Time) time.Time {
	return t.Truncate(requeueInterval)
}

// statusEquivalent reports whether two status snapshots are equivalent for
// the purpose of deciding whether to persist an update. LastTransitionTime is
// deliberately ignored: it is managed by meta.SetStatusCondition and must not
// influence the comparison.
func statusEquivalent(a, b *cronicalv1alpha1.CronICalFeedStatus) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !generationTimesEqual(a.LastGeneratedAt, b.LastGeneratedAt) {
		return false
	}
	if a.CronJobCount != b.CronJobCount ||
		a.EventCount != b.EventCount ||
		a.FeedURL != b.FeedURL {
		return false
	}
	return conditionsEquivalent(a.Conditions, b.Conditions)
}

// generationTimesEqual compares optional timestamps by instant. Sub-second
// precision is irrelevant here because generation times are truncated before
// being stored.
func generationTimesEqual(a, b *metav1.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Time.Equal(b.Time)
}

// conditionsEquivalent compares condition lists field by field, ignoring
// LastTransitionTime.
func conditionsEquivalent(a, b []metav1.Condition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type ||
			a[i].Status != b[i].Status ||
			a[i].Reason != b[i].Reason ||
			a[i].Message != b[i].Message ||
			a[i].ObservedGeneration != b[i].ObservedGeneration {
			return false
		}
	}
	return true
}

// listCronJobs lists CronJobs matching the feed's selector.
func (r *CronICalFeedReconciler) listCronJobs(ctx context.Context, feed *cronicalv1alpha1.CronICalFeed) ([]batchv1.CronJob, error) {
	// Determine namespaces to search
	namespaces := feed.Spec.Selector.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{feed.Namespace}
	}

	// Build label selector
	var selector labels.Selector
	if feed.Spec.Selector.LabelSelector != nil {
		var err error
		selector, err = metav1.LabelSelectorAsSelector(feed.Spec.Selector.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid label selector: %w", err)
		}
	}

	var allCronJobs []batchv1.CronJob
	for _, ns := range namespaces {
		var cronJobList batchv1.CronJobList
		listOpts := []client.ListOption{client.InNamespace(ns)}
		if selector != nil {
			listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: selector})
		}
		if err := r.List(ctx, &cronJobList, listOpts...); err != nil {
			return nil, fmt.Errorf("failed to list CronJobs in namespace %s: %w", ns, err)
		}
		allCronJobs = append(allCronJobs, cronJobList.Items...)
	}

	return allCronJobs, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronICalFeedReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cronicalv1alpha1.CronICalFeed{},
			// Ignore update events that only touch status/metadata. Status
			// writes by this very reconciler bump the resourceVersion, which
			// would otherwise re-enqueue us in a self-induced reconcile loop.
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&batchv1.CronJob{}, handler.EnqueueRequestsFromMapFunc(r.mapCronJobToFeeds)).
		Named("cronicalfeed").
		Complete(r)
}

// mapCronJobToFeeds maps a CronJob change to all CronICalFeeds that might
// include it, triggering reconciliation.
func (r *CronICalFeedReconciler) mapCronJobToFeeds(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logf.FromContext(ctx)

	var feedList cronicalv1alpha1.CronICalFeedList
	if err := r.List(ctx, &feedList); err != nil {
		log.Error(err, "failed to list CronICalFeeds for CronJob mapping")
		return nil
	}

	cronJob, ok := obj.(*batchv1.CronJob)
	if !ok {
		return nil
	}

	var requests []reconcile.Request
	for i := range feedList.Items {
		feed := &feedList.Items[i]
		if r.cronJobMatchesFeed(cronJob, feed) {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      feed.Name,
					Namespace: feed.Namespace,
				},
			})
		}
	}

	return requests
}

// cronJobMatchesFeed checks if a CronJob is in scope for a given CronICalFeed.
func (r *CronICalFeedReconciler) cronJobMatchesFeed(cronJob *batchv1.CronJob, feed *cronicalv1alpha1.CronICalFeed) bool {
	// Check namespace
	namespaces := feed.Spec.Selector.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{feed.Namespace}
	}
	if !slices.Contains(namespaces, cronJob.Namespace) {
		return false
	}

	// Check label selector
	if feed.Spec.Selector.LabelSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(feed.Spec.Selector.LabelSelector)
		if err != nil {
			return false
		}
		if !selector.Matches(labels.Set(cronJob.Labels)) {
			return false
		}
	}

	return true
}
