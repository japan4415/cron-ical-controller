# cron-ical-controller

cron-ical-controller is a Kubernetes operator that exposes the schedules of
CronJobs as iCalendar (.ics) feeds. A `CronICalFeed` custom resource selects
CronJobs by namespace and labels; the operator enumerates each selected
CronJob's firings over a time window and serves them as individual VEVENTs via
an HTTP endpoint (`/feeds/{namespace}/{name}.ics`). It is strictly read-only:
it never modifies CronJobs.

## Sizing guidelines

The controller bounds its memory usage structurally so that legitimate but
dense inputs (e.g. a `* * * * *` schedule over `window: 90`) cannot OOMKill it.

### Per-feed event cap (`MaxEventsPerFeed = 10000`)

Each feed contains at most 10,000 VEVENTs. Enumeration stops at the cap and
the truncation is surfaced through:

- a `Warning` event with reason `EventLimitExceeded`, and
- the `Ready` condition switching to reason `FeedTruncated` with a message
  recording the truncation, plus `status.eventCount` showing the emitted count.

Measured cost (golang-ical v0.3.5): one serialized VEVENT is roughly 240 B,
so a capped feed serializes to about 2.4 MB and building it peaks around
15-25 MB of heap. Typical counts per schedule/window combination:

| Schedule        | Window | Firings  | Result                    |
| --------------- | ------ | -------- | ------------------------- |
| hourly          | 7 days | 168      | fits                      |
| every 5 minutes | 7 days | 2,016    | fits                      |
| every minute    | 7 days | ~10,080  | truncated at 10,000       |
| hourly          | 90 days | 2,160   | fits                      |
| every minute    | 90 days | ~129,600 | truncated at 10,000       |

If your feed gets truncated, narrow the window or match fewer/higher-interval
CronJobs; events beyond the cap are not served.

### Feed cache budget (`DefaultCacheMaxBytes = 64 MiB`)

All generated feeds are held in an in-memory cache capped at 64 MiB total.
When the budget would be exceeded, the least-recently-updated feeds are evicted
first (they will be regenerated on the next reconcile). At most ~2.4 MB per
feed, this holds ~26 worst-case feeds or hundreds of typically sized ones.

### Manager memory settings

The deployment ships with `memory: 256Mi` limit / `128Mi` request and
`GOMEMLIMIT=230MiB`. The limit covers the worst-case combination of cache
budget (64 MiB), generation-time working memory (~25 MB) and the
controller-runtime/client-go baseline, with headroom; `GOMEMLIMIT` sits at
about 90% of the limit so the garbage collector ramps up before the container
limit is reached. If you raise the event cap or cache budget for your cluster,
revisit these values accordingly.
