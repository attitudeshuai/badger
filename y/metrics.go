/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"expvar"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	BADGER_METRIC_PREFIX = "badger_"
)

// Canonical metric names. The counter/map/gauge names are identical to the
// legacy process-wide expvar variable names so that a per-instance snapshot
// reconciles 1:1 with what the global collectors have always scraped.
const (
	// Cumulative counters.
	MetricReadNumVlog          = BADGER_METRIC_PREFIX + "read_num_vlog"
	MetricReadBytesVlog        = BADGER_METRIC_PREFIX + "read_bytes_vlog"
	MetricWriteNumVlog         = BADGER_METRIC_PREFIX + "write_num_vlog"
	MetricWriteBytesVlog       = BADGER_METRIC_PREFIX + "write_bytes_vlog"
	MetricReadBytesLSM         = BADGER_METRIC_PREFIX + "read_bytes_lsm"
	MetricWriteBytesL0         = BADGER_METRIC_PREFIX + "write_bytes_l0"
	MetricGetNumLSM            = BADGER_METRIC_PREFIX + "get_num_lsm"
	MetricWriteBytesCompaction = BADGER_METRIC_PREFIX + "write_bytes_compaction"
	MetricLSMBloomHits         = BADGER_METRIC_PREFIX + "hit_num_lsm_bloom_filter"
	MetricGetNumMemtable       = BADGER_METRIC_PREFIX + "get_num_memtable"
	MetricGetNumUser           = BADGER_METRIC_PREFIX + "get_num_user"
	MetricGetWithResultNumUser = BADGER_METRIC_PREFIX + "get_with_result_num_user"
	MetricIteratorNumUser      = BADGER_METRIC_PREFIX + "iterator_num_user"
	MetricPutNumUser           = BADGER_METRIC_PREFIX + "put_num_user"
	MetricCompactionCurrentNum = BADGER_METRIC_PREFIX + "compaction_current_num_lsm"
	MetricWriteBytesUser       = BADGER_METRIC_PREFIX + "write_bytes_user"

	// Point-in-time gauges.
	MetricSizeBytesLSM         = BADGER_METRIC_PREFIX + "size_bytes_lsm"
	MetricSizeBytesVlog        = BADGER_METRIC_PREFIX + "size_bytes_vlog"
	MetricPendingWriteNumMemtb = BADGER_METRIC_PREFIX + "write_pending_num_memtable"
)

// Latency distribution names carried by MetricsSnapshot.Histograms.
const (
	MetricLatencyGetUser       = BADGER_METRIC_PREFIX + "get_latency_user"
	MetricLatencyPutUser       = BADGER_METRIC_PREFIX + "put_latency_user"
	MetricLatencyReadVlog      = BADGER_METRIC_PREFIX + "read_latency_vlog"
	MetricLatencyCompactionLSM = BADGER_METRIC_PREFIX + "compaction_latency_lsm"
	MetricLatencyGCVlog        = BADGER_METRIC_PREFIX + "gc_latency_vlog"
)

// Diagnostic command names carried by MetricsSnapshot.DiagnosticHistograms.
const (
	DiagnosticFlatten        = "flatten"
	DiagnosticVerifyChecksum = "verify_checksum"
)

// Fixed-slot indices for lock-free counters/gauges/maps/histograms. Keeping
// these as arrays (rather than name-keyed maps) lets observers update them
// with plain atomics while a snapshot quiesces everything via Metrics.snap.
const (
	cReadNumVlog = iota
	cReadBytesVlog
	cWriteNumVlog
	cWriteBytesVlog
	cReadBytesLSM
	cWriteBytesL0
	cGetNumMemtable
	cGetNumUser
	cGetWithResultNumUser
	cIteratorNumUser
	cPutNumUser
	cCompactionCurrentNum
	cWriteBytesUser
	numCounters
)

var counterNames = [numCounters]string{
	cReadNumVlog:          MetricReadNumVlog,
	cReadBytesVlog:        MetricReadBytesVlog,
	cWriteNumVlog:         MetricWriteNumVlog,
	cWriteBytesVlog:       MetricWriteBytesVlog,
	cReadBytesLSM:         MetricReadBytesLSM,
	cWriteBytesL0:         MetricWriteBytesL0,
	cGetNumMemtable:       MetricGetNumMemtable,
	cGetNumUser:           MetricGetNumUser,
	cGetWithResultNumUser: MetricGetWithResultNumUser,
	cIteratorNumUser:      MetricIteratorNumUser,
	cPutNumUser:           MetricPutNumUser,
	cCompactionCurrentNum: MetricCompactionCurrentNum,
	cWriteBytesUser:       MetricWriteBytesUser,
}

const (
	gSizeBytesLSM = iota
	gSizeBytesVlog
	gPendingWrites
	numGauges
)

var gaugeNames = [numGauges]string{
	gSizeBytesLSM:  MetricSizeBytesLSM,
	gSizeBytesVlog: MetricSizeBytesVlog,
	gPendingWrites: MetricPendingWriteNumMemtb,
}

const (
	mBytesCompaction = iota
	mGetNumLSM
	mBloomHits
	numMaps
)

var mapNames = [numMaps]string{
	mBytesCompaction: MetricWriteBytesCompaction,
	mGetNumLSM:       MetricGetNumLSM,
	mBloomHits:       MetricLSMBloomHits,
}

const (
	hGet = iota
	hPut
	hVlogRead
	hCompaction
	hGC
	numHists
)

var histNames = [numHists]string{
	hGet:        MetricLatencyGetUser,
	hPut:        MetricLatencyPutUser,
	hVlogRead:   MetricLatencyReadVlog,
	hCompaction: MetricLatencyCompactionLSM,
	hGC:         MetricLatencyGCVlog,
}

var diagnosticNames = []string{DiagnosticFlatten, DiagnosticVerifyChecksum}

// Legacy process-wide expvar variables. They aggregate across every open DB
// in the process and keep their historical names/types/meanings, so existing
// expvar-based dashboards keep collecting the same totals without changes.
// Per-instance accounting lives in Metrics; each Metrics update also feeds
// these globals when metrics are enabled.
var (
	legacyLSM     *expvar.Map
	legacyVlog    *expvar.Map
	legacyPending *expvar.Map

	legacyIntVars [numCounters]*expvar.Int
	legacyMapVars [numMaps]*expvar.Map
)

// Naming convention of metrics: {badger_version}_{singular operation}_{granularity}_{component}
func init() {
	for i := 0; i < numCounters; i++ {
		legacyIntVars[i] = expvar.NewInt(counterNames[i])
	}
	for i := 0; i < numMaps; i++ {
		legacyMapVars[i] = expvar.NewMap(mapNames[i])
	}
	legacyLSM = expvar.NewMap(MetricSizeBytesLSM)
	legacyVlog = expvar.NewMap(MetricSizeBytesVlog)
	legacyPending = expvar.NewMap(MetricPendingWriteNumMemtb)
}

// ---------------------------------------------------------------------------
// Latency distribution
// ---------------------------------------------------------------------------

// latencyBoundsNanos are power-of-two bucket upper bounds in nanoseconds,
// spanning 64ns..~34s. Nanosecond granularity keeps sub-microsecond
// operations (e.g. an mmaped vlog read) measurable. Values above the last
// bound fall into an overflow bucket.
var latencyBoundsNanos = func() []int64 {
	const minExp, maxExp = 6, 35
	bounds := make([]int64, 0, maxExp-minExp+1)
	for i := minExp; i <= maxExp; i++ {
		bounds = append(bounds, int64(1)<<uint(i))
	}
	return bounds
}()

// latencyHistogram is a fixed-bucket latency distribution. Every mutation is
// serialized by mu; a Metrics snapshot additionally guarantees no observer
// is active while it copies any histogram.
type latencyHistogram struct {
	mu     sync.Mutex
	bounds []int64 // ascending upper bounds in nanoseconds
	// buckets[i] counts observations in [bounds[i-1], bounds[i]); the last
	// bucket is [bounds[last], +inf).
	buckets []int64
	count   int64
	sum     int64 // nanoseconds
	min     int64 // nanoseconds
	max     int64 // nanoseconds
}

func newLatencyHistogram() *latencyHistogram {
	return &latencyHistogram{
		bounds:  latencyBoundsNanos,
		buckets: make([]int64, len(latencyBoundsNanos)+1),
		min:     math.MaxInt64,
	}
}

// observe records a sample. Callers must hold the Metrics snapshot RLock.
func (h *latencyHistogram) observe(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v := d.Nanoseconds()
	if v < 0 {
		v = 0
	}
	idx := sort.Search(len(h.bounds), func(i int) bool { return v < h.bounds[i] })
	h.buckets[idx]++
	h.count++
	h.sum += v
	if v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
}

// HistogramSnapshot is an immutable point-in-time copy of a latency
// distribution. Bounds and Buckets describe the full distribution while
// Percentiles holds the commonly queried quantiles.
type HistogramSnapshot struct {
	Count int64
	Sum   time.Duration
	Min   time.Duration
	Max   time.Duration
	Mean  time.Duration

	// Percentiles holds the standard quantiles (p50, p90, p95, p99, p999).
	Percentiles map[string]time.Duration
	// Bounds are the bucket upper bounds; Buckets[i] is the count of
	// observations below Bounds[i]. The last bucket is the overflow.
	Bounds  []time.Duration
	Buckets []int64
}

// Percentile returns the nearest-rank value for quantile q (0..1),
// interpolated geometrically within its bucket. It can be queried for any
// quantile (not just the precomputed ones) from a snapshot.
func (s HistogramSnapshot) Percentile(q float64) time.Duration {
	if s.Count == 0 || q <= 0 {
		return 0
	}
	if q >= 1 {
		return s.Max
	}
	rank := int64(math.Ceil(float64(s.Count) * q)) // 1-based nearest rank.
	if rank < 1 {
		rank = 1
	}

	minNs := s.Min.Nanoseconds()
	maxNs := s.Max.Nanoseconds()
	clamp := func(v float64) time.Duration {
		// Interpolation can reach the bucket bound even when no observation
		// did, so never report outside the observed [Min, Max] range.
		if v < float64(minNs) {
			v = float64(minNs)
		}
		if v > float64(maxNs) {
			v = float64(maxNs)
		}
		return time.Duration(v+0.5) * time.Nanosecond
	}

	var cum, prevCum int64
	for i, c := range s.Buckets {
		cum += c
		if cum < rank {
			prevCum = cum
			continue
		}
		var lo, hi int64 // nanoseconds
		if i > 0 {
			lo = s.Bounds[i-1].Nanoseconds()
		}
		if i < len(s.Bounds) {
			hi = s.Bounds[i].Nanoseconds()
		} else {
			// Overflow bucket has no known upper bound; never exceed the
			// observed maximum.
			hi = maxNs
		}
		if hi <= lo {
			return clamp(float64(hi))
		}
		if lo <= 0 {
			// First bucket starts at zero; interpolate linearly.
			ratio := float64(rank-prevCum) / float64(c)
			return clamp(ratio * float64(hi))
		}
		ratio := float64(rank-prevCum) / float64(c)
		est := float64(lo) * math.Pow(float64(hi)/float64(lo), ratio)
		return clamp(est)
	}
	return s.Max
}

// snapshotLocked copies the histogram. Callers guarantee exclusive access
// (the histogram mutex plus the Metrics snapshot write lock).
func (h *latencyHistogram) snapshotLocked() HistogramSnapshot {
	s := HistogramSnapshot{
		Count:       h.count,
		Sum:         time.Duration(h.sum) * time.Nanosecond,
		Bounds:      make([]time.Duration, len(h.bounds)),
		Buckets:     append([]int64(nil), h.buckets...),
		Percentiles: map[string]time.Duration{},
	}
	for i, b := range h.bounds {
		s.Bounds[i] = time.Duration(b) * time.Nanosecond
	}
	if h.count > 0 {
		s.Min = time.Duration(h.min) * time.Nanosecond
		s.Max = time.Duration(h.max) * time.Nanosecond
		s.Mean = time.Duration(float64(h.sum)/float64(h.count)+0.5) * time.Nanosecond
	}
	for q, name := range map[float64]string{
		0.50:  "p50",
		0.90:  "p90",
		0.95:  "p95",
		0.99:  "p99",
		0.999: "p999",
	} {
		s.Percentiles[name] = s.Percentile(q)
	}
	return s
}

// lockedMap is a name-partitioned counter map (e.g. per-level). Its own mutex
// serializes the actual map writes, which still happen while the observer
// holds the Metrics snapshot RLock.
type lockedMap struct {
	mu sync.Mutex
	m  map[string]int64
}

// ---------------------------------------------------------------------------
// Per-instance metrics
// ---------------------------------------------------------------------------

// Metrics is the metric plane owned by a single open DB instance. All methods
// are safe for concurrent use and are nil-safe: when metrics are disabled
// the DB holds a nil *Metrics and every call is a no-op with no allocation,
// no synchronization and no expvar traffic, i.e. no extra overhead.
type Metrics struct {
	id       uint64
	dir      string
	valueDir string
	openedAt time.Time

	closeOnce sync.Once

	// ownerLegacyGauges is true only for the first live opener of each
	// (directory, value directory) slot in the process. Directory-keyed
	// legacy expvar gauges are published solely by that owner, so opening
	// another instance with the same directory name (e.g. an in-memory DB or
	// BypassLockGuard) can never overwrite an existing entry.
	ownerLegacyGauges bool
	legacyLSMSize     *expvar.Int
	legacyVlogSize    *expvar.Int
	legacyPending     *expvar.Int

	// snap implements a consistent point-in-time snapshot: observers take
	// RLock and run concurrently, Snapshot takes Lock and copies a quiescent
	// state. Plain counters/gauges are atomics (no further locking); the
	// key-partitioned maps and histograms carry their own mutexes, which are
	// only ever taken while holding snap, so a snapshot holder excludes all
	// observers while the reverse never happens (no lock-order cycle).
	snap      sync.RWMutex
	counters  [numCounters]atomic.Int64
	gauges    [numGauges]atomic.Int64
	parts     [numMaps]lockedMap
	hists     [numHists]*latencyHistogram
	diagHists map[string]*latencyHistogram
}

// MetricsSnapshot is a consistent, point-in-time reading of one DB instance.
type MetricsSnapshot struct {
	// InstanceID uniquely identifies an open (or, for a captured snapshot,
	// formerly open) DB instance. It is never reused within a process, so
	// reopening the same directory yields a new ID and cannot overwrite a
	// still-live instance.
	InstanceID uint64
	Dir        string
	ValueDir   string
	OpenedAt   time.Time
	TakenAt    time.Time

	// Counters are cumulative since the DB opened, keyed by the canonical
	// metric names (e.g. MetricGetNumUser == "badger_get_num_user").
	Counters map[string]int64
	// Maps holds level/key-partitioned cumulative counters keyed first by
	// metric name and then by the sub-key (level like "l6", bloom result,
	// ...).
	Maps map[string]map[string]int64
	// Gauges hold point-in-time values keyed by metric name.
	Gauges map[string]int64

	// Histograms holds request/background-operation latency distributions
	// keyed by the MetricLatency* constants.
	Histograms map[string]HistogramSnapshot
	// DiagnosticHistograms holds operational/diagnostic command latency
	// distributions keyed by the Diagnostic* constants.
	DiagnosticHistograms map[string]HistogramSnapshot
}

func newMetricsLocked(id uint64, dir, valueDir string, owner bool) *Metrics {
	m := &Metrics{
		id:                id,
		dir:               dir,
		valueDir:          valueDir,
		openedAt:          time.Now(),
		ownerLegacyGauges: owner,
		diagHists:         make(map[string]*latencyHistogram, len(diagnosticNames)),
	}
	for i := range m.parts {
		m.parts[i].m = make(map[string]int64)
	}
	for i := 0; i < numHists; i++ {
		m.hists[i] = newLatencyHistogram()
	}
	for _, name := range diagnosticNames {
		m.diagHists[name] = newLatencyHistogram()
	}
	return m
}

func (m *Metrics) addCounter(idx int32, delta int64) {
	if m == nil {
		return
	}
	m.snap.RLock()
	m.counters[idx].Add(delta)
	m.snap.RUnlock()
	legacyIntVars[idx].Add(delta)
}

func (m *Metrics) addMap(idx int32, key string, delta int64) {
	if m == nil {
		return
	}
	m.snap.RLock()
	p := &m.parts[idx]
	p.mu.Lock()
	p.m[key] += delta
	p.mu.Unlock()
	m.snap.RUnlock()
	legacyMapVars[idx].Add(key, delta)
}

func (m *Metrics) setGauge(idx int32, val int64) {
	if m == nil {
		return
	}
	m.snap.RLock()
	m.gauges[idx].Store(val)
	m.snap.RUnlock()
}

func (m *Metrics) observeHist(idx int32, d time.Duration) {
	if m == nil {
		return
	}
	m.snap.RLock()
	m.hists[idx].observe(d)
	m.snap.RUnlock()
}

func (m *Metrics) observeDiagnostic(name string, d time.Duration) {
	if m == nil {
		return
	}
	m.snap.RLock()
	if h := m.diagHists[name]; h != nil {
		h.observe(d)
	}
	m.snap.RUnlock()
}

// Snapshot returns a consistent point-in-time reading of this instance.
func (m *Metrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}
	m.snap.Lock()
	defer m.snap.Unlock()

	snap := MetricsSnapshot{
		InstanceID:           m.id,
		Dir:                  m.dir,
		ValueDir:             m.valueDir,
		OpenedAt:             m.openedAt,
		TakenAt:              time.Now(),
		Counters:             make(map[string]int64, numCounters),
		Maps:                 make(map[string]map[string]int64, numMaps),
		Gauges:               make(map[string]int64, numGauges),
		Histograms:           make(map[string]HistogramSnapshot, numHists),
		DiagnosticHistograms: make(map[string]HistogramSnapshot, len(m.diagHists)),
	}
	for i := 0; i < numCounters; i++ {
		snap.Counters[counterNames[i]] = m.counters[i].Load()
	}
	for i := 0; i < numGauges; i++ {
		snap.Gauges[gaugeNames[i]] = m.gauges[i].Load()
	}
	for i := 0; i < numMaps; i++ {
		p := &m.parts[i]
		p.mu.Lock()
		cp := make(map[string]int64, len(p.m))
		for k, v := range p.m {
			cp[k] = v
		}
		p.mu.Unlock()
		snap.Maps[mapNames[i]] = cp
	}
	for i := 0; i < numHists; i++ {
		h := m.hists[i]
		h.mu.Lock()
		snap.Histograms[histNames[i]] = h.snapshotLocked()
		h.mu.Unlock()
	}
	for name, h := range m.diagHists {
		h.mu.Lock()
		snap.DiagnosticHistograms[name] = h.snapshotLocked()
		h.mu.Unlock()
	}
	return snap
}

// ---------------------------------------------------------------------------
// Instance registry
// ---------------------------------------------------------------------------

type metricsRegistry struct {
	// nextID is outside the mutex: IDs must be unique even across
	// register/unregister churn.
	nextID atomic.Uint64

	mu        sync.RWMutex
	instances map[uint64]*Metrics
	// gaugeOwners records the instance ID currently owning the directory-keyed
	// legacy gauge entry for a given gauge key.
	gaugeOwners map[string]uint64
}

var globalRegistry = metricsRegistry{
	instances:   make(map[uint64]*Metrics),
	gaugeOwners: make(map[string]uint64),
}

// NewMetrics creates the per-DB metric plane and registers it with the
// process-wide registry. Every call returns a distinct instance whose
// identity never collides with another, even when the same directory is
// opened repeatedly. Call Metrics.Close when the DB is closed to release the
// registration.
func NewMetrics(dir, valueDir string) *Metrics {
	id := globalRegistry.nextID.Add(1)

	globalRegistry.mu.Lock()
	owner := false
	// Ownership is keyed by all three gauge slots; the common case is a
	// single opener of a directory, which owns all of them.
	lsmKey := gaugeOwnerLSM + dir
	vlogKey := gaugeOwnerVlog + valueDir
	pendKey := gaugeOwnerPending + dir
	if _, ok := globalRegistry.gaugeOwners[lsmKey]; !ok {
		globalRegistry.gaugeOwners[lsmKey] = id
		owner = true
	}
	if _, ok := globalRegistry.gaugeOwners[vlogKey]; !ok {
		globalRegistry.gaugeOwners[vlogKey] = id
	}
	if _, ok := globalRegistry.gaugeOwners[pendKey]; !ok {
		globalRegistry.gaugeOwners[pendKey] = id
	}
	m := newMetricsLocked(id, dir, valueDir, owner)
	globalRegistry.instances[id] = m
	globalRegistry.mu.Unlock()

	if owner {
		m.legacyLSMSize = new(expvar.Int)
		m.legacyVlogSize = new(expvar.Int)
		m.legacyPending = new(expvar.Int)
	}
	return m
}

const (
	gaugeOwnerLSM     = "lsm:"
	gaugeOwnerVlog    = "vlog:"
	gaugeOwnerPending = "pending:"
)

// Close unregisters the instance and releases its legacy directory-keyed
// gauge entries. It is idempotent. After Close, no metric from this instance
// remains visible either in the registry or in legacy gauge maps; cumulative
// legacy totals (counters) are intentionally retained, matching their
// historical process-cumulative meaning.
func (m *Metrics) Close() {
	if m == nil {
		return
	}
	m.closeOnce.Do(func() {
		globalRegistry.mu.Lock()
		delete(globalRegistry.instances, m.id)
		if m.ownerLegacyGauges {
			if globalRegistry.gaugeOwners[gaugeOwnerLSM+m.dir] == m.id {
				delete(globalRegistry.gaugeOwners, gaugeOwnerLSM+m.dir)
			}
			if globalRegistry.gaugeOwners[gaugeOwnerVlog+m.valueDir] == m.id {
				delete(globalRegistry.gaugeOwners, gaugeOwnerVlog+m.valueDir)
			}
			if globalRegistry.gaugeOwners[gaugeOwnerPending+m.dir] == m.id {
				delete(globalRegistry.gaugeOwners, gaugeOwnerPending+m.dir)
			}
		}
		globalRegistry.mu.Unlock()

		// Remove directory-keyed gauge entries so a stale entry for a closed
		// DB can never be mistaken for a live one or collide with a reopen.
		if m.ownerLegacyGauges {
			legacyLSM.Delete(m.dir)
			legacyVlog.Delete(m.valueDir)
			legacyPending.Delete(m.dir)
		}
	})
}

// AllMetricsSnapshots returns consistent snapshots of all currently open
// metric-enabled DB instances, ordered by instance ID. The registry cannot
// change while snapshots are taken, so the batch is a single point-in-time
// view across every open DB rather than a stitched-together per-variable
// reading.
func AllMetricsSnapshots() []MetricsSnapshot {
	globalRegistry.mu.RLock()
	ids := make([]uint64, 0, len(globalRegistry.instances))
	for id := range globalRegistry.instances {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	snaps := make([]MetricsSnapshot, 0, len(ids))
	for _, id := range ids {
		snaps = append(snaps, globalRegistry.instances[id].Snapshot())
	}
	globalRegistry.mu.RUnlock()
	return snaps
}

// ---------------------------------------------------------------------------
// Counter updates (nil-safe; no-op on nil receiver when metrics are off)
// ---------------------------------------------------------------------------

func (m *Metrics) NumIteratorsCreatedAdd(val int64) { m.addCounter(cIteratorNumUser, val) }

func (m *Metrics) NumGetsWithResultsAdd(val int64) { m.addCounter(cGetWithResultNumUser, val) }

func (m *Metrics) NumReadsVlogAdd(val int64) { m.addCounter(cReadNumVlog, val) }

func (m *Metrics) NumBytesWrittenUserAdd(val int64) { m.addCounter(cWriteBytesUser, val) }

func (m *Metrics) NumWritesVlogAdd(val int64) { m.addCounter(cWriteNumVlog, val) }

func (m *Metrics) NumBytesReadsVlogAdd(val int64) { m.addCounter(cReadBytesVlog, val) }

func (m *Metrics) NumBytesReadsLSMAdd(val int64) { m.addCounter(cReadBytesLSM, val) }

func (m *Metrics) NumBytesWrittenVlogAdd(val int64) { m.addCounter(cWriteBytesVlog, val) }

func (m *Metrics) NumBytesWrittenToL0Add(val int64) { m.addCounter(cWriteBytesL0, val) }

func (m *Metrics) NumBytesCompactionWrittenAdd(key string, val int64) {
	m.addMap(mBytesCompaction, key, val)
}

func (m *Metrics) NumGetsAdd(val int64) { m.addCounter(cGetNumUser, val) }

func (m *Metrics) NumPutsAdd(val int64) { m.addCounter(cPutNumUser, val) }

func (m *Metrics) NumMemtableGetsAdd(val int64) { m.addCounter(cGetNumMemtable, val) }

func (m *Metrics) NumCompactionTablesAdd(val int64) { m.addCounter(cCompactionCurrentNum, val) }

// LSMSizeSet records the on-disk LSM size gauge for this instance and, for
// the directory's owning instance, keeps the legacy expvar gauge in sync.
func (m *Metrics) LSMSizeSet(val int64) {
	if m == nil {
		return
	}
	m.setGauge(gSizeBytesLSM, val)
	if m.ownerLegacyGauges && m.legacyLSMSize != nil {
		m.legacyLSMSize.Set(val)
		legacyLSM.Set(m.dir, m.legacyLSMSize)
	}
}

// VlogSizeSet records the on-disk value log size gauge for this instance.
func (m *Metrics) VlogSizeSet(val int64) {
	if m == nil {
		return
	}
	m.setGauge(gSizeBytesVlog, val)
	if m.ownerLegacyGauges && m.legacyVlogSize != nil {
		m.legacyVlogSize.Set(val)
		legacyVlog.Set(m.valueDir, m.legacyVlogSize)
	}
}

// PendingWritesSet records the queued write-request gauge for this instance.
func (m *Metrics) PendingWritesSet(val int64) {
	if m == nil {
		return
	}
	m.setGauge(gPendingWrites, val)
	if m.ownerLegacyGauges && m.legacyPending != nil {
		m.legacyPending.Set(val)
		legacyPending.Set(m.dir, m.legacyPending)
	}
}

func (m *Metrics) NumLSMBloomHitsAdd(key string, val int64) {
	m.addMap(mBloomHits, key, val)
}

func (m *Metrics) NumLSMGetsAdd(key string, val int64) {
	m.addMap(mGetNumLSM, key, val)
}

// LSMSize returns the per-instance LSM size gauge. The boolean reports
// whether the instance has metrics enabled.
func (m *Metrics) LSMSize() (int64, bool) {
	if m == nil {
		return 0, false
	}
	return m.gauges[gSizeBytesLSM].Load(), true
}

// VlogSize returns the per-instance value log size gauge.
func (m *Metrics) VlogSize() (int64, bool) {
	if m == nil {
		return 0, false
	}
	return m.gauges[gSizeBytesVlog].Load(), true
}

// ---------------------------------------------------------------------------
// Latency observations (nil-safe)
// ---------------------------------------------------------------------------

// ObserveGet records a user point-read (Get) latency.
func (m *Metrics) ObserveGet(d time.Duration) { m.observeHist(hGet, d) }

// ObservePut records a user write-request latency.
func (m *Metrics) ObservePut(d time.Duration) { m.observeHist(hPut, d) }

// ObserveVlogRead records a value-log read latency.
func (m *Metrics) ObserveVlogRead(d time.Duration) { m.observeHist(hVlogRead, d) }

// ObserveCompaction records one LSM compaction latency.
func (m *Metrics) ObserveCompaction(d time.Duration) { m.observeHist(hCompaction, d) }

// ObserveVlogGC records one value-log garbage collection (reclamation)
// latency.
func (m *Metrics) ObserveVlogGC(d time.Duration) { m.observeHist(hGC, d) }

// ObserveDiagnostic records the latency of an operational/diagnostic command
// (see the Diagnostic* constants).
func (m *Metrics) ObserveDiagnostic(name string, d time.Duration) {
	m.observeDiagnostic(name, d)
}
