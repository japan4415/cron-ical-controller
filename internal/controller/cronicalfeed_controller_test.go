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
	"bytes"
	"context"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cronicalv1alpha1 "github.com/japan4415/cron-ical-controller/api/v1alpha1"
	"github.com/japan4415/cron-ical-controller/internal/ical"
	"github.com/japan4415/cron-ical-controller/internal/server"
)

var _ = Describe("CronICalFeed Controller", func() {
	const (
		feedName      = "test-feed"
		feedNamespace = "default"
		cronJobName   = "test-cronjob"
	)

	var (
		feedCache  *server.FeedCache
		reconciler *CronICalFeedReconciler
		recorder   *record.FakeRecorder
	)

	feedKey := types.NamespacedName{Name: feedName, Namespace: feedNamespace}

	BeforeEach(func() {
		feedCache = server.NewFeedCache()
		recorder = record.NewFakeRecorder(10)
		reconciler = &CronICalFeedReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			FeedCache: feedCache,
			Recorder:  recorder,
		}
	})

	AfterEach(func() {
		// Clean up CronICalFeed
		feed := &cronicalv1alpha1.CronICalFeed{}
		if err := k8sClient.Get(context.Background(), feedKey, feed); err == nil {
			Expect(k8sClient.Delete(context.Background(), feed)).To(Succeed())
		}

		// Clean up CronJobs in default namespace
		cronJobList := &batchv1.CronJobList{}
		Expect(k8sClient.List(context.Background(), cronJobList)).To(Succeed())
		for i := range cronJobList.Items {
			Expect(k8sClient.Delete(context.Background(), &cronJobList.Items[i])).To(Succeed())
		}
	})

	createFeed := func(spec cronicalv1alpha1.CronICalFeedSpec) {
		feed := &cronicalv1alpha1.CronICalFeed{
			ObjectMeta: metav1.ObjectMeta{
				Name:      feedName,
				Namespace: feedNamespace,
			},
			Spec: spec,
		}
		Expect(k8sClient.Create(context.Background(), feed)).To(Succeed())
	}

	createCronJob := func(name string, schedule string, labels map[string]string, timeZone *string) {
		cj := &batchv1.CronJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: feedNamespace,
				Labels:    labels,
			},
			Spec: batchv1.CronJobSpec{
				Schedule: schedule,
				TimeZone: timeZone,
				JobTemplate: batchv1.JobTemplateSpec{
					Spec: batchv1.JobSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:    "test",
										Image:   "busybox",
										Command: []string{"echo", "hello"},
									},
								},
								RestartPolicy: corev1.RestartPolicyNever,
							},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(context.Background(), cj)).To(Succeed())
	}

	doReconcile := func() (reconcile.Result, error) {
		return reconciler.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: feedKey,
		})
	}

	Context("When CronICalFeed is deleted", func() {
		It("should return without error", func() {
			result, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})
	})

	Context("When no CronJobs exist", func() {
		BeforeEach(func() {
			createFeed(cronicalv1alpha1.CronICalFeedSpec{
				DefaultDuration: "5m",
			})
		})

		It("should generate an empty feed and set CronJobsFound=False", func() {
			result, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(requeueInterval))

			// Check cache has data
			feedPath := server.FeedPath(feedNamespace, feedName)
			data, ok := feedCache.Get(feedPath)
			Expect(ok).To(BeTrue())
			Expect(string(data)).To(ContainSubstring("BEGIN:VCALENDAR"))
			Expect(string(data)).NotTo(ContainSubstring("BEGIN:VEVENT"))

			// Check status
			var feed cronicalv1alpha1.CronICalFeed
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())
			Expect(feed.Status.CronJobCount).To(Equal(int32(0)))
			Expect(feed.Status.FeedURL).To(Equal(feedPath))
			Expect(feed.Status.LastGeneratedAt).NotTo(BeNil())

			// Check conditions
			readyCond := findCondition(feed.Status.Conditions, cronicalv1alpha1.ConditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))

			cronJobsCond := findCondition(feed.Status.Conditions, cronicalv1alpha1.ConditionCronJobsFound)
			Expect(cronJobsCond).NotTo(BeNil())
			Expect(cronJobsCond.Status).To(Equal(metav1.ConditionFalse))
		})
	})

	Context("When CronJobs exist", func() {
		BeforeEach(func() {
			createCronJob(cronJobName, "0 * * * *", nil, nil)
			createFeed(cronicalv1alpha1.CronICalFeedSpec{
				Window:          ptr.To[int32](1),
				DefaultDuration: "10m",
			})
		})

		It("should generate a feed with events and set CronJobsFound=True", func() {
			result, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(requeueInterval))

			// Check cache
			feedPath := server.FeedPath(feedNamespace, feedName)
			data, ok := feedCache.Get(feedPath)
			Expect(ok).To(BeTrue())
			Expect(string(data)).To(ContainSubstring("BEGIN:VEVENT"))
			Expect(string(data)).To(ContainSubstring("SUMMARY:" + cronJobName))

			// Check status
			var feed cronicalv1alpha1.CronICalFeed
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())
			Expect(feed.Status.CronJobCount).To(Equal(int32(1)))

			cronJobsCond := findCondition(feed.Status.Conditions, cronicalv1alpha1.ConditionCronJobsFound)
			Expect(cronJobsCond).NotTo(BeNil())
			Expect(cronJobsCond.Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Context("When CronJob has invalid duration label", func() {
		BeforeEach(func() {
			createCronJob(cronJobName, "0 12 * * *", map[string]string{
				ical.DurationLabelKey: "invalid",
			}, nil)
			createFeed(cronicalv1alpha1.CronICalFeedSpec{
				DefaultDuration: "15m",
			})
		})

		It("should use default duration and emit a warning event", func() {
			_, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())

			// Should still generate events
			feedPath := server.FeedPath(feedNamespace, feedName)
			data, ok := feedCache.Get(feedPath)
			Expect(ok).To(BeTrue())
			Expect(string(data)).To(ContainSubstring("BEGIN:VEVENT"))

			// Check for warning event
			Eventually(func() bool {
				select {
				case event := <-recorder.Events:
					return strings.Contains(event, "InvalidDurationLabel")
				default:
					return false
				}
			}).Should(BeTrue())
		})
	})

	// Note: Invalid cron expression test is omitted because the Kubernetes API
	// server validates cron expressions at admission time. This scenario is
	// covered by internal/ical unit tests instead.

	Context("When the feed event cap is exceeded", func() {
		BeforeEach(func() {
			// An every-minute schedule over a 7-day window yields ~10,080
			// firings, just past MaxEventsPerFeed: the cheapest legitimate
			// input that exercises truncation end to end.
			createCronJob(cronJobName, "* * * * *", nil, nil)
			createFeed(cronicalv1alpha1.CronICalFeedSpec{
				Window:          ptr.To[int32](7),
				DefaultDuration: "5m",
			})
		})

		It("should stop at the cap, serve a complete calendar, and surface the truncation", func() {
			result, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(requeueInterval))

			// Status reports exactly the capped number of events.
			var feed cronicalv1alpha1.CronICalFeed
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())
			Expect(feed.Status.EventCount).To(Equal(int32(ical.MaxEventsPerFeed)))

			// The Ready condition records the truncation.
			readyCond := findCondition(feed.Status.Conditions, cronicalv1alpha1.ConditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCond.Reason).To(Equal("FeedTruncated"))
			Expect(readyCond.Message).To(ContainSubstring("truncated"))

			// A warning event notifies users watching the resource.
			Eventually(func() bool {
				select {
				case event := <-recorder.Events:
					return strings.Contains(event, "EventLimitExceeded")
				default:
					return false
				}
			}).Should(BeTrue())

			// The served feed holds exactly the capped number of VEVENTs and
			// remains well-formed (OOM safety must not corrupt output).
			feedPath := server.FeedPath(feedNamespace, feedName)
			data, ok := feedCache.Get(feedPath)
			Expect(ok).To(BeTrue())
			Expect(bytes.Count(data, []byte("BEGIN:VEVENT"))).To(Equal(ical.MaxEventsPerFeed))
			Expect(data).To(ContainSubstring("END:VCALENDAR"))
		})
	})

	Context("When feed is deleted", func() {
		It("should clean up the cache", func() {
			createFeed(cronicalv1alpha1.CronICalFeedSpec{})

			// First reconcile to populate cache
			_, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())

			feedPath := server.FeedPath(feedNamespace, feedName)
			_, ok := feedCache.Get(feedPath)
			Expect(ok).To(BeTrue())

			// Delete feed
			feed := &cronicalv1alpha1.CronICalFeed{}
			Expect(k8sClient.Get(context.Background(), feedKey, feed)).To(Succeed())
			Expect(k8sClient.Delete(context.Background(), feed)).To(Succeed())

			// Wait for deletion
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), feedKey, feed)
				return errors.IsNotFound(err)
			}).Should(BeTrue())

			// Reconcile after deletion
			_, err = doReconcile()
			Expect(err).NotTo(HaveOccurred())

			// Cache should be cleaned
			_, ok = feedCache.Get(feedPath)
			Expect(ok).To(BeFalse())
		})
	})

	Context("With label selector", func() {
		BeforeEach(func() {
			// CronJob with matching label
			createCronJob("matching-job", "0 * * * *", map[string]string{
				"team": "alpha",
			}, nil)
			// CronJob without matching label
			createCronJob("non-matching-job", "0 * * * *", map[string]string{
				"team": "beta",
			}, nil)

			createFeed(cronicalv1alpha1.CronICalFeedSpec{
				Selector: cronicalv1alpha1.CronJobSelector{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"team": "alpha"},
					},
				},
			})
		})

		It("should only include matching CronJobs", func() {
			_, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())

			var feed cronicalv1alpha1.CronICalFeed
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())
			Expect(feed.Status.CronJobCount).To(Equal(int32(1)))

			feedPath := server.FeedPath(feedNamespace, feedName)
			data, _ := feedCache.Get(feedPath)
			Expect(string(data)).To(ContainSubstring("SUMMARY:matching-job"))
			Expect(string(data)).NotTo(ContainSubstring("SUMMARY:non-matching-job"))
		})
	})

	Context("Status update consistency", func() {
		BeforeEach(func() {
			createCronJob("job-a", "0 6 * * *", nil, nil)
			createCronJob("job-b", "30 12 * * *", nil, nil)
			createFeed(cronicalv1alpha1.CronICalFeedSpec{
				Window:          ptr.To[int32](7),
				DefaultDuration: "30m",
				DefaultTimeZone: "UTC",
			})
		})

		It("should update all status fields correctly", func() {
			_, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())

			var feed cronicalv1alpha1.CronICalFeed
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())

			Expect(feed.Status.CronJobCount).To(Equal(int32(2)))
			Expect(feed.Status.FeedURL).To(Equal(server.FeedPath(feedNamespace, feedName)))
			Expect(feed.Status.LastGeneratedAt).NotTo(BeNil())
			Expect(feed.Status.Conditions).To(HaveLen(3))

			readyCond := findCondition(feed.Status.Conditions, cronicalv1alpha1.ConditionReady)
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCond.Reason).To(Equal("FeedGenerated"))

			cronJobsCond := findCondition(feed.Status.Conditions, cronicalv1alpha1.ConditionCronJobsFound)
			Expect(cronJobsCond.Status).To(Equal(metav1.ConditionTrue))

			degradedCond := findCondition(feed.Status.Conditions, cronicalv1alpha1.ConditionDegraded)
			Expect(degradedCond).NotTo(BeNil())
			Expect(degradedCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(degradedCond.Reason).To(Equal("AsExpected"))
		})
	})

	Context("When the generation state repeats across reconciles", func() {
		var baseTime time.Time

		BeforeEach(func() {
			baseTime = time.Date(2026, 8, 23, 10, 17, 45, 0, time.UTC)
			createCronJob(cronJobName, "0 * * * *", nil, nil)
			createFeed(cronicalv1alpha1.CronICalFeedSpec{
				Window:          ptr.To[int32](1),
				DefaultDuration: "10m",
			})
			reconciler.NowFunc = func() time.Time { return baseTime }
		})

		It("should not issue a status update when nothing changed", func() {
			_, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())

			var feed cronicalv1alpha1.CronICalFeed
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())
			resourceVersionAfterFirst := feed.ResourceVersion
			lastGeneratedAfterFirst := feed.Status.LastGeneratedAt
			Expect(lastGeneratedAfterFirst).NotTo(BeNil())

			// Second reconcile within the same generation window: identical
			// inputs must produce identical output and therefore no write.
			result, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(requeueInterval))

			feed = cronicalv1alpha1.CronICalFeed{}
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())
			Expect(feed.ResourceVersion).To(Equal(resourceVersionAfterFirst),
				"resourceVersion changed although nothing changed: a status update was issued")
			Expect(feed.Status.LastGeneratedAt.Time).To(Equal(lastGeneratedAfterFirst.Time))
		})

		It("should not even issue Status().Update() on a no-change reconcile", func() {
			// The API server silently drops updates whose resulting object is
			// byte-identical (no resourceVersion bump), so observing the
			// resourceVersion alone cannot prove that the reconciler stopped
			// issuing writes. Wrap the client to count Status().Update() calls
			// directly.
			counter := &statusUpdateCounter{Client: k8sClient}
			countingReconciler := &CronICalFeedReconciler{
				Client:    counter,
				Scheme:    k8sClient.Scheme(),
				FeedCache: feedCache,
				Recorder:  recorder,
				NowFunc:   func() time.Time { return baseTime },
			}
			reconcileReq := reconcile.Request{NamespacedName: feedKey}

			_, err := countingReconciler.Reconcile(context.Background(), reconcileReq)
			Expect(err).NotTo(HaveOccurred())

			_, err = countingReconciler.Reconcile(context.Background(), reconcileReq)
			Expect(err).NotTo(HaveOccurred())

			_, err = countingReconciler.Reconcile(context.Background(), reconcileReq)
			Expect(err).NotTo(HaveOccurred())

			Expect(counter.statusUpdates).To(Equal(1),
				"only the first reconcile must write status; no-change reconciles must skip it")
		})

		It("should issue a status update when the generation window advances", func() {
			_, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())

			var feed cronicalv1alpha1.CronICalFeed
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())
			resourceVersionAfterFirst := feed.ResourceVersion
			lastGeneratedAfterFirst := feed.Status.LastGeneratedAt

			// Advance past the next hour boundary: LastGeneratedAt alone moves,
			// which is allowed to trigger exactly one write per window.
			reconciler.NowFunc = func() time.Time { return baseTime.Add(requeueInterval + time.Minute) }
			_, err = doReconcile()
			Expect(err).NotTo(HaveOccurred())

			feed = cronicalv1alpha1.CronICalFeed{}
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())
			Expect(feed.ResourceVersion).NotTo(Equal(resourceVersionAfterFirst))
			Expect(feed.Status.LastGeneratedAt.Time).To(Equal(
				lastGeneratedAfterFirst.Add(requeueInterval)))
			// Content metrics stay untouched by the clock advance.
			Expect(feed.Status.CronJobCount).To(Equal(int32(1)))
			Expect(feed.Status.EventCount).To(BeNumerically(">", 0))
		})

		It("should regenerate deterministic bytes within the same window", func() {
			_, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())

			feedPath := server.FeedPath(feedNamespace, feedName)
			dataAfterFirst, ok := feedCache.Get(feedPath)
			Expect(ok).To(BeTrue())

			_, err = doReconcile()
			Expect(err).NotTo(HaveOccurred())

			dataAfterSecond, ok := feedCache.Get(feedPath)
			Expect(ok).To(BeTrue())
			Expect(dataAfterSecond).To(Equal(dataAfterFirst),
				"two generations in the same window must be byte-identical (deterministic DTSTAMP)")
		})

		It("should update eventCount when matched CronJobs change", func() {
			_, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())

			var feed cronicalv1alpha1.CronICalFeed
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())
			resourceVersionAfterFirst := feed.ResourceVersion
			eventCountAfterFirst := feed.Status.EventCount
			Expect(eventCountAfterFirst).To(BeNumerically(">", 0))

			// Add another matching CronJob: meaningful change → update.
			createCronJob("second-cronjob", "30 * * * *", nil, nil)
			_, err = doReconcile()
			Expect(err).NotTo(HaveOccurred())

			feed = cronicalv1alpha1.CronICalFeed{}
			Expect(k8sClient.Get(context.Background(), feedKey, &feed)).To(Succeed())
			Expect(feed.ResourceVersion).NotTo(Equal(resourceVersionAfterFirst))
			Expect(feed.Status.CronJobCount).To(Equal(int32(2)))
			Expect(feed.Status.EventCount).To(BeNumerically(">", eventCountAfterFirst))
		})
	})

	Context("With a running manager and GenerationChangedPredicate", func() {
		const timeout = 20 * time.Second
		const pollingInterval = 250 * time.Millisecond

		It("should not re-enqueue on its own status updates", func() {
			By("starting a manager with the controller wired through SetupWithManager")
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:                 k8sClient.Scheme(),
				Metrics:                metricsserver.Options{BindAddress: "0"},
				HealthProbeBindAddress: "0",
			})
			Expect(err).NotTo(HaveOccurred())

			managerCache := server.NewFeedCache()
			managerReconciler := &CronICalFeedReconciler{
				Client:    mgr.GetClient(),
				Scheme:    mgr.GetScheme(),
				Recorder:  mgr.GetEventRecorderFor("cronicalfeed-controller-test"), //nolint:staticcheck // TODO: migrate to GetEventRecorder (events API)
				FeedCache: managerCache,
			}
			Expect(managerReconciler.SetupWithManager(mgr)).To(Succeed())

			mgrCtx, mgrCancel := context.WithCancel(context.Background())
			defer mgrCancel()
			go func() {
				_ = mgr.Start(mgrCtx)
			}()

			By("creating a CronJob and a CronICalFeed")
			createCronJob(cronJobName, "0 * * * *", nil, nil)
			createFeed(cronicalv1alpha1.CronICalFeedSpec{})

			By("waiting for the controller to converge on the initial reconcile")
			Eventually(func(g Gomega) {
				feed := &cronicalv1alpha1.CronICalFeed{}
				g.Expect(k8sClient.Get(context.Background(), feedKey, feed)).To(Succeed())
				g.Expect(feed.Status.LastGeneratedAt).NotTo(BeNil())
			}, timeout, pollingInterval).Should(Succeed())

			By("writing an outdated lastGeneratedAt via the status subresource")
			feed := &cronicalv1alpha1.CronICalFeed{}
			Expect(k8sClient.Get(context.Background(), feedKey, feed)).To(Succeed())
			staleTime := metav1.NewTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
			feed.Status.LastGeneratedAt = &staleTime
			Expect(k8sClient.Status().Update(context.Background(), feed)).To(Succeed())

			feed = &cronicalv1alpha1.CronICalFeed{}
			Expect(k8sClient.Get(context.Background(), feedKey, feed)).To(Succeed())
			resourceVersionAfterTouch := feed.ResourceVersion

			By("verifying that no reconcile is triggered by the status-only event")
			// A status-only update does not bump metadata.generation, so the
			// GenerationChangedPredicate must swallow it. If the predicate were
			// missing, this very write would re-enqueue the object and the
			// reconciler would restore lastGeneratedAt, changing the RV.
			Consistently(func(g Gomega) string {
				current := &cronicalv1alpha1.CronICalFeed{}
				g.Expect(k8sClient.Get(context.Background(), feedKey, current)).To(Succeed())
				return current.ResourceVersion
			}, "3s", pollingInterval).Should(Equal(resourceVersionAfterTouch))
		})
	})
})

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

// statusUpdateCounter wraps a client.Client and counts how many times
// Status().Update() is invoked, so tests can assert that no status write is
// issued at all.
type statusUpdateCounter struct {
	client.Client
	statusUpdates int
}

func (c *statusUpdateCounter) Status() client.SubResourceWriter {
	return &countingStatusWriter{SubResourceWriter: c.Client.Status(), counter: c}
}

type countingStatusWriter struct {
	client.SubResourceWriter
	counter *statusUpdateCounter
}

func (w *countingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	w.counter.statusUpdates++
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}
