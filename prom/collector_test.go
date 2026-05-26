package prom_test

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/prom"
	"github.com/thanhphuchuynh/podshare/transport"
)

type cfg struct {
	N int `json:"n"`
}

func TestCollectorRegistersAndScrapes(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	store, err := podshare.New[cfg](ctx, "metrics", tr,
		podshare.WithNodeID[cfg]("pod-a"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	reg := prometheus.NewRegistry()
	c := prom.NewCollector(store.Stats, prometheus.Labels{
		"topic": "metrics",
		"pod":   "pod-a",
	})
	if err := reg.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Generate activity so metrics are non-zero.
	if err := store.Set(ctx, "k1", cfg{N: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ctx, "k2", cfg{N: 2}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		_, _ = store.Get("k2")
	}

	// Lint: descriptors valid, label names match, no collisions.
	lints, err := testutil.GatherAndLint(reg)
	if err != nil {
		t.Fatalf("lint: %v", err)
	}
	if len(lints) > 0 {
		t.Fatalf("lint problems: %v", lints)
	}

	// We expect exactly the 12 metrics declared by the Collector.
	if count := testutil.CollectAndCount(c); count != 12 {
		t.Fatalf("collected %d metrics, want 12", count)
	}

	// Spot-check the actual numbers via testutil.ToFloat64 on filtered
	// single-metric helpers. We re-register the same collector against a
	// fresh registry per metric so ToFloat64's "exactly one" check passes.
	check := func(metricName string, want float64) {
		t.Helper()
		val := testutil.ToFloat64(singleMetric{c: c, name: metricName})
		if val != want {
			t.Errorf("%s = %v, want %v", metricName, val, want)
		}
	}
	check("podshare_writes_local_total", 3) // 2 sets + 1 delete
	check("podshare_reads_total", 5)
	check("podshare_keys", 1)       // k2 lives
	check("podshare_tombstones", 1) // k1 tombstoned

	// And: the labels we passed are present in the output.
	mfs, _ := reg.Gather()
	if !labelOnEvery(mfs, "topic", "metrics") {
		t.Error(`"topic" label missing on at least one metric`)
	}
	if !labelOnEvery(mfs, "pod", "pod-a") {
		t.Error(`"pod" label missing on at least one metric`)
	}
}

func TestCollectorWithNilLabels(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()
	store, _ := podshare.New[cfg](ctx, "nolabels", tr)
	defer store.Close()

	c := prom.NewCollector(store.Stats, nil)
	if count := testutil.CollectAndCount(c); count != 12 {
		t.Fatalf("collected %d metrics, want 12", count)
	}
}

func TestNewCollectorPanicsOnNilFn(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil statsFn")
		}
	}()
	_ = prom.NewCollector(nil, nil)
}

// --- helpers ----------------------------------------------------------

// singleMetric narrows a multi-metric Collector to a single named one
// so testutil.ToFloat64 can read its value. (ToFloat64 requires the
// Collector to emit exactly one metric.)
type singleMetric struct {
	c    prometheus.Collector
	name string
}

func (s singleMetric) Describe(ch chan<- *prometheus.Desc) {
	inner := make(chan *prometheus.Desc, 32)
	go func() {
		s.c.Describe(inner)
		close(inner)
	}()
	for d := range inner {
		if strings.Contains(d.String(), `"`+s.name+`"`) {
			ch <- d
		}
	}
}

func (s singleMetric) Collect(ch chan<- prometheus.Metric) {
	inner := make(chan prometheus.Metric, 32)
	go func() {
		s.c.Collect(inner)
		close(inner)
	}()
	for m := range inner {
		if strings.Contains(m.Desc().String(), `"`+s.name+`"`) {
			ch <- m
		}
	}
}

// labelOnEvery reports whether every metric in mfs carries label=value.
func labelOnEvery(mfs []*dto.MetricFamily, label, value string) bool {
	for _, fam := range mfs {
		for _, m := range fam.GetMetric() {
			found := false
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}
