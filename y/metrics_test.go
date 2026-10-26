/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"expvar"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLatencyHistogramPercentiles(t *testing.T) {
	h := newLatencyHistogram()
	// Feed 100 samples between 1ms and 100ms.
	for i := 0; i < 100; i++ {
		h.observe(time.Duration(1+i) * time.Millisecond)
	}
	h.mu.Lock()
	s := h.snapshotLocked()
	h.mu.Unlock()
	require.Equal(t, int64(100), s.Count)
	require.Equal(t, time.Millisecond, s.Min)
	require.Equal(t, 100*time.Millisecond, s.Max)
	require.True(t, s.Mean > 50*time.Millisecond)

	// Quantiles must be monotonic and within [min, max].
	p50 := s.Percentiles["p50"]
	p90 := s.Percentiles["p90"]
	p99 := s.Percentiles["p99"]
	p999 := s.Percentiles["p999"]
	require.True(t, p50 > 0 && p50 <= p90)
	require.True(t, p90 <= p99)
	require.True(t, p99 <= p999)
	require.True(t, p999 <= s.Max)
	require.Equal(t, s.Max, s.Percentile(1.0))
	require.Equal(t, time.Duration(0), s.Percentile(0.0))

	// Bucket counts must sum to the total count.
	var sum int64
	for _, c := range s.Buckets {
		sum += c
	}
	require.Equal(t, s.Count, sum)
}

func TestLatencyHistogramEmpty(t *testing.T) {
	h := newLatencyHistogram()
	h.mu.Lock()
	s := h.snapshotLocked()
	h.mu.Unlock()
	require.Equal(t, int64(0), s.Count)
	require.Equal(t, time.Duration(0), s.Percentile(0.99))
	require.Equal(t, time.Duration(0), s.Percentiles["p99"])
}

func TestRegistryDuplicateDirAndRelease(t *testing.T) {
	dir := t.Name()
	m1 := NewMetrics(dir, dir)
	m2 := NewMetrics(dir, dir) // same directory name, concurrent instance
	require.NotEqual(t, m1.id, m2.id)

	// Both instances must be registered independently; same-named opens must
	// not overwrite each other.
	snaps := AllMetricsSnapshots()
	ids := map[uint64]bool{}
	for _, s := range snaps {
		ids[s.InstanceID] = true
	}
	require.True(t, ids[m1.id])
	require.True(t, ids[m2.id])

	// Updates stay attributed to their own instance.
	m1.NumPutsAdd(3)
	m2.NumPutsAdd(5)
	require.Equal(t, int64(3), m1.Snapshot().Counters[MetricPutNumUser])
	require.Equal(t, int64(5), m2.Snapshot().Counters[MetricPutNumUser])

	// Closing one instance releases only its own entry.
	m1.Close()
	snaps = AllMetricsSnapshots()
	ids = map[uint64]bool{}
	for _, s := range snaps {
		ids[s.InstanceID] = true
	}
	require.False(t, ids[m1.id])
	require.True(t, ids[m2.id])

	// Close is idempotent.
	m1.Close()
	m2.Close()
	for _, s := range AllMetricsSnapshots() {
		require.NotEqual(t, m2.id, s.InstanceID)
	}

	// A reopen of the same directory gets a fresh identity and registers
	// cleanly after both prior instances went away.
	m3 := NewMetrics(dir, dir)
	require.NotEqual(t, m1.id, m3.id)
	require.NotEqual(t, m2.id, m3.id)
	require.True(t, m3.ownerLegacyGauges, "sole live opener should own legacy gauges")
	m3.Close()
}

func TestSnapshotIsIndependentDeepCopy(t *testing.T) {
	m := NewMetrics(t.Name(), t.Name())
	defer m.Close()

	m.NumGetsAdd(2)
	m.NumLSMGetsAdd("l6", 4)
	m.ObserveGet(5 * time.Millisecond)

	s := m.Snapshot()
	require.Equal(t, int64(2), s.Counters[MetricGetNumUser])
	require.Equal(t, int64(4), s.Maps[MetricGetNumLSM]["l6"])
	require.Equal(t, int64(1), s.Histograms[MetricLatencyGetUser].Count)

	// Mutate the live plane after the snapshot; the captured snapshot must
	// remain an immutable point-in-time view.
	m.NumGetsAdd(100)
	m.NumLSMGetsAdd("l6", 100)
	require.Equal(t, int64(2), s.Counters[MetricGetNumUser])
	require.Equal(t, int64(4), s.Maps[MetricGetNumLSM]["l6"])
	require.Equal(t, int64(1), s.Histograms[MetricLatencyGetUser].Count)
}

func TestDisabledMetricsNilSafe(t *testing.T) {
	var m *Metrics // the handle held when Options.MetricsEnabled is false
	// None of these may panic or touch expvar.
	m.NumGetsAdd(1)
	m.NumPutsAdd(1)
	m.NumBytesCompactionWrittenAdd("l6", 1)
	m.LSMSizeSet(10)
	m.PendingWritesSet(10)
	m.ObserveGet(time.Millisecond)
	m.ObserveVlogGC(time.Millisecond)
	m.ObserveDiagnostic(DiagnosticFlatten, time.Millisecond)
	m.Close()
	_, ok := m.LSMSize()
	require.False(t, ok)
	require.Equal(t, MetricsSnapshot{}, m.Snapshot())
}

func TestLegacyExpvarTotalsAndGaugeRelease(t *testing.T) {
	// Legacy counters keep aggregating across instances under the same
	// historical names so existing dashboards keep working.
	before := expvar.Get(MetricGetNumUser).(*expvar.Int).Value()
	// The process-wide expvar maps accumulate across earlier tests, so read
	// the existing "l6" value and assert against a delta.
	var beforeLSM int64
	if v := expvar.Get(MetricGetNumLSM).(*expvar.Map).Get("l6"); v != nil {
		beforeLSM = v.(*expvar.Int).Value()
	}

	dir := t.Name()
	m := NewMetrics(dir, dir)
	m.NumGetsAdd(7)
	require.Equal(t, before+7, expvar.Get(MetricGetNumUser).(*expvar.Int).Value())
	m.NumLSMGetsAdd("l6", 2)
	require.Equal(t, beforeLSM+2,
		expvar.Get(MetricGetNumLSM).(*expvar.Map).Get("l6").(*expvar.Int).Value())

	// The owning instance publishes its directory-keyed size gauges.
	m.LSMSizeSet(1234)
	require.NotNil(t, expvar.Get(MetricSizeBytesLSM).(*expvar.Map).Get(dir))
	require.Equal(t, int64(1234),
		expvar.Get(MetricSizeBytesLSM).(*expvar.Map).Get(dir).(*expvar.Int).Value())

	// Close releases the gauge entry (cumulative counters are retained).
	m.Close()
	require.Nil(t, expvar.Get(MetricSizeBytesLSM).(*expvar.Map).Get(dir))
	require.Equal(t, before+7, expvar.Get(MetricGetNumUser).(*expvar.Int).Value())
}

func TestMetricsConcurrentObserversAndSnapshots(t *testing.T) {
	m := NewMetrics(t.Name(), t.Name())
	defer m.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				m.NumGetsAdd(1)
				m.NumLSMGetsAdd("l6", 1)
				m.NumBytesCompactionWrittenAdd("l6", 1)
				m.PendingWritesSet(int64(i))
				m.ObserveGet(time.Microsecond)
				m.ObserveDiagnostic(DiagnosticFlatten, time.Millisecond)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			_ = m.Snapshot()
		}
	}()
	wg.Wait()

	s := m.Snapshot()
	require.Equal(t, int64(8*2000), s.Counters[MetricGetNumUser])
	require.Equal(t, int64(8*2000), s.Maps[MetricGetNumLSM]["l6"])
	require.Equal(t, int64(8*2000), s.Histograms[MetricLatencyGetUser].Count)
	require.Equal(t, int64(8*2000), s.DiagnosticHistograms[DiagnosticFlatten].Count)
}

func TestAllSnapshotsSortedByInstanceID(t *testing.T) {
	const n = 3
	created := make([]*Metrics, 0, n)
	for i := 0; i < n; i++ {
		created = append(created, NewMetrics(t.Name(), t.Name()))
	}
	defer func() {
		for _, m := range created {
			m.Close()
		}
	}()
	snaps := AllMetricsSnapshots()
	require.GreaterOrEqual(t, len(snaps), n)
	for i := 1; i < len(snaps); i++ {
		require.Less(t, snaps[i-1].InstanceID, snaps[i].InstanceID)
	}
}
