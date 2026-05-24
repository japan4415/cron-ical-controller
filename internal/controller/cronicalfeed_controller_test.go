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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cronicalv1alpha1 "github.com/japan4415/cron-ical-controller/api/v1alpha1"
	"github.com/japan4415/cron-ical-controller/internal/ical"
	"github.com/japan4415/cron-ical-controller/internal/server"
)

func int32Ptr(i int32) *int32 { return &i }
func strPtr(s string) *string { return &s }

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
				Window:          int32Ptr(1),
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
				Window:          int32Ptr(7),
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
			Expect(feed.Status.Conditions).To(HaveLen(2))

			readyCond := findCondition(feed.Status.Conditions, cronicalv1alpha1.ConditionReady)
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCond.Reason).To(Equal("FeedGenerated"))

			cronJobsCond := findCondition(feed.Status.Conditions, cronicalv1alpha1.ConditionCronJobsFound)
			Expect(cronJobsCond.Status).To(Equal(metav1.ConditionTrue))
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
