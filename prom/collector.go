// Package prom exposes a podshare Store's Stats() as a
// prometheus.Collector. Optional — only import this subpackage if you
// want Prometheus metrics. The main podshare module otherwise has no
// dependency on github.com/prometheus/client_golang.
//
// Wire it up:
//
//	store, _ := podshare.New[Flag](ctx, "feature-flags", tr)
//	collector := prom.NewCollector(store.Stats, prometheus.Labels{
//	    "topic": "feature-flags",
//	    "pod":   os.Getenv("POD_NAME"),
//	})
//	prometheus.MustRegister(collector)
//
// One Collector per Store. The Stats() method is O(1), so scrape cost
// is negligible — register at startup and forget about it.
package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanhphuchuynh/podshare"
)

// Collector implements prometheus.Collector for a single podshare Store.
// Construct with NewCollector; register with prometheus.MustRegister.
type Collector struct {
	statsFn func() podshare.Stats

	writesLocal      *prometheus.Desc
	writesApplied    *prometheus.Desc
	writesRejected   *prometheus.Desc
	reads            *prometheus.Desc
	eventsDispatched *prometheus.Desc
	watchersActive   *prometheus.Desc
	watchersDropped  *prometheus.Desc
	snapshots        *prometheus.Desc
	snapshotBytes    *prometheus.Desc
	keys             *prometheus.Desc
	tombstones       *prometheus.Desc
	tombstonesGCed   *prometheus.Desc
}

// NewCollector wraps a Stats provider as a prometheus.Collector.
// statsFn is typically store.Stats — a method value that produces a
// fresh Stats snapshot on each call. labels are applied as constant
// labels on every emitted metric (good for "topic", "pod", etc.).
//
// statsFn must be cheap and safe for concurrent use. podshare's
// Stats() is O(1) and safe.
func NewCollector(statsFn func() podshare.Stats, labels prometheus.Labels) *Collector {
	if statsFn == nil {
		panic("prom: statsFn is nil")
	}
	if labels == nil {
		labels = prometheus.Labels{}
	}
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(name, help, nil, labels)
	}
	return &Collector{
		statsFn:          statsFn,
		writesLocal:      desc("podshare_writes_local_total", "Local Set/Delete/SetMany/DeleteMany invocations."),
		writesApplied:    desc("podshare_writes_applied_total", "Writes that changed the data map (local plus peer-originated, post-LWW)."),
		writesRejected:   desc("podshare_writes_rejected_total", "Writes that lost LWW reconciliation."),
		reads:            desc("podshare_reads_total", "Get calls."),
		eventsDispatched: desc("podshare_events_dispatched_total", "Watch events dispatched (including those filtered server-side)."),
		watchersActive:   desc("podshare_watchers_active", "Currently attached Watch consumers."),
		watchersDropped:  desc("podshare_watchers_dropped_total", "Slow consumers dropped because their buffer was full."),
		snapshots:        desc("podshare_snapshots_total", "Snapshots successfully persisted via the Transport."),
		snapshotBytes:    desc("podshare_snapshot_bytes", "Size of the most recently persisted snapshot, in bytes."),
		keys:             desc("podshare_keys", "Live (non-tombstoned) keys in the local replica."),
		tombstones:       desc("podshare_tombstones", "Tombstoned keys currently retained pending TTL."),
		tombstonesGCed:   desc("podshare_tombstones_gced_total", "Tombstones compacted past their TTL."),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.writesLocal, c.writesApplied, c.writesRejected,
		c.reads, c.eventsDispatched,
		c.watchersActive, c.watchersDropped,
		c.snapshots, c.snapshotBytes,
		c.keys, c.tombstones, c.tombstonesGCed,
	} {
		ch <- d
	}
}

// Collect implements prometheus.Collector. Fired on every Prometheus
// scrape — takes one cheap Stats() snapshot and emits 12 metrics.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	s := c.statsFn()
	emit := func(d *prometheus.Desc, kind prometheus.ValueType, v float64) {
		ch <- prometheus.MustNewConstMetric(d, kind, v)
	}
	emit(c.writesLocal, prometheus.CounterValue, float64(s.WritesLocal))
	emit(c.writesApplied, prometheus.CounterValue, float64(s.WritesApplied))
	emit(c.writesRejected, prometheus.CounterValue, float64(s.WritesRejected))
	emit(c.reads, prometheus.CounterValue, float64(s.Reads))
	emit(c.eventsDispatched, prometheus.CounterValue, float64(s.EventsDispatched))
	emit(c.watchersActive, prometheus.GaugeValue, float64(s.WatchersActive))
	emit(c.watchersDropped, prometheus.CounterValue, float64(s.WatchersDropped))
	emit(c.snapshots, prometheus.CounterValue, float64(s.Snapshots))
	emit(c.snapshotBytes, prometheus.GaugeValue, float64(s.SnapshotBytes))
	emit(c.keys, prometheus.GaugeValue, float64(s.Keys))
	emit(c.tombstones, prometheus.GaugeValue, float64(s.Tombstones))
	emit(c.tombstonesGCed, prometheus.CounterValue, float64(s.TombstonesGCed))
}
