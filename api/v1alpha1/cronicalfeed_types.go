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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CronJobSelector defines the criteria for selecting target CronJobs.
type CronJobSelector struct {
	// namespaces is a list of namespaces to search for CronJobs.
	// If empty, the CronICalFeed's own namespace is used.
	// +optional
	Namespaces []string `json:"namespaces,omitempty"`

	// labelSelector filters CronJobs by labels.
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`
}

// CronICalFeedSpec defines the desired state of CronICalFeed.
type CronICalFeedSpec struct {
	// selector specifies the criteria for selecting target CronJobs.
	// +optional
	Selector CronJobSelector `json:"selector,omitempty"`

	// window is the number of days into the future to generate iCal events for.
	// +kubebuilder:default=7
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=90
	// +optional
	Window *int32 `json:"window,omitempty"`

	// defaultDuration is the default duration for CronJob events when the
	// cron-ical.discord.jp/avg-duration label is not set.
	// Must be a valid Go time.Duration string (e.g. "5m", "1h30m").
	// Negative values are not allowed.
	// +kubebuilder:default="0s"
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	// +optional
	DefaultDuration string `json:"defaultDuration,omitempty"`

	// defaultTimeZone is the IANA timezone name to use when a CronJob does not
	// have its .spec.timeZone field set.
	// +kubebuilder:default="UTC"
	// +optional
	DefaultTimeZone string `json:"defaultTimeZone,omitempty"`
}

// CronICalFeedStatus defines the observed state of CronICalFeed.
type CronICalFeedStatus struct {
	// lastGeneratedAt is the timestamp when the .ics feed was last generated.
	// +optional
	LastGeneratedAt *metav1.Time `json:"lastGeneratedAt,omitempty"`

	// cronJobCount is the number of CronJobs currently matched by the selector.
	// +optional
	CronJobCount int32 `json:"cronJobCount,omitempty"`

	// feedURL is the HTTP path where this feed is served.
	// +optional
	FeedURL string `json:"feedURL,omitempty"`

	// conditions represent the latest available observations of the CronICalFeed's state.
	//
	// Condition types:
	// - "Ready": the feed is successfully generated and available for serving
	// - "CronJobsFound": at least one target CronJob was found
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition type constants for CronICalFeed.
const (
	// ConditionReady indicates whether the feed is successfully generated and servable.
	ConditionReady = "Ready"
	// ConditionCronJobsFound indicates whether any target CronJobs were found.
	ConditionCronJobsFound = "CronJobsFound"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="CronJobs",type=integer,JSONPath=`.status.cronJobCount`,description="Number of matched CronJobs"
// +kubebuilder:printcolumn:name="Feed URL",type=string,JSONPath=`.status.feedURL`,description="Feed endpoint path"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`,description="Whether the feed is ready"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CronICalFeed is the Schema for the cronicalfeeds API.
// It exports CronJob schedules as iCalendar (.ics) feeds.
type CronICalFeed struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of CronICalFeed
	// +required
	Spec CronICalFeedSpec `json:"spec"`

	// status defines the observed state of CronICalFeed
	// +optional
	Status CronICalFeedStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CronICalFeedList contains a list of CronICalFeed
type CronICalFeedList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CronICalFeed `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CronICalFeed{}, &CronICalFeedList{})
}
