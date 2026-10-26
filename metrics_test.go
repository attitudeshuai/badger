/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"expvar"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/y"
)

func clearAllMetrics() {
	expvar.Do(func(kv expvar.KeyValue) {
		// Reset the value of each expvar variable based on its type
		switch v := kv.Value.(type) {
		case *expvar.Int:
			v.Set(0)
		case *expvar.Float:
			v.Set(0)
		case *expvar.Map:
			v.Init()
		case *expvar.String:
			v.Set("")
		}
	})
}

func TestWriteMetrics(t *testing.T) {
	opt := getTestOptions("")
	opt.managedTxns = true
	opt.CompactL0OnClose = true
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		clearAllMetrics()
		num := 10
		val := make([]byte, 1<<12)
		key := make([]byte, 40)
		for i := 0; i < num; i++ {
			_, err := rand.Read(key)
			require.NoError(t, err)
			_, err = rand.Read(val)
			require.NoError(t, err)

			writer := db.NewManagedWriteBatch()
			require.NoError(t, writer.SetEntryAt(NewEntry(key, val), 1))
			writer.Flush()
		}

		expectedSize := int64(len(val)) + 48 + 2 // 48 := size of key (40 + 8(ts)), 2 := meta
		write_metric := expvar.Get("badger_write_bytes_user")
		require.Equal(t, expectedSize*int64(num), write_metric.(*expvar.Int).Value())

		put_metric := expvar.Get("badger_put_num_user")
		require.Equal(t, int64(num), put_metric.(*expvar.Int).Value())

		lsm_metric := expvar.Get("badger_write_bytes_l0")
		require.Equal(t, expectedSize*int64(num), lsm_metric.(*expvar.Int).Value())

		compactionMetric := expvar.Get("badger_write_bytes_compaction").(*expvar.Map)
		require.Equal(t, nil, compactionMetric.Get("l6"))

		// Force compaction
		db.Close()

		_, err := OpenManaged(opt)
		require.NoError(t, err)

		compactionMetric = expvar.Get("badger_write_bytes_compaction").(*expvar.Map)
		require.GreaterOrEqual(t, expectedSize*int64(num)+int64(num*200), compactionMetric.Get("l6").(*expvar.Int).Value())
		// Because we have random values, compression is not able to do much, so we incur a cost on total size
	})
}

func TestVlogMetrics(t *testing.T) {
	opt := getTestOptions("")
	opt.managedTxns = true
	opt.CompactL0OnClose = true
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		clearAllMetrics()
		num := 10
		val := make([]byte, 1<<20) // Large Value
		key := make([]byte, 40)
		for i := 0; i < num; i++ {
			_, err := rand.Read(key)
			require.NoError(t, err)
			_, err = rand.Read(val)
			require.NoError(t, err)

			writer := db.NewManagedWriteBatch()
			require.NoError(t, writer.SetEntryAt(NewEntry(key, val), 1))
			writer.Flush()
		}

		expectedSize := int64(len(val)) + 200 // vlog expected size

		totalWrites := expvar.Get("badger_write_num_vlog")
		require.Equal(t, int64(num), totalWrites.(*expvar.Int).Value())

		bytesWritten := expvar.Get("badger_write_bytes_vlog")
		require.GreaterOrEqual(t, expectedSize*int64(num), bytesWritten.(*expvar.Int).Value())

		txn := db.NewTransactionAt(2, false)
		item, err := txn.Get(key)
		require.NoError(t, err)
		require.Equal(t, uint64(1), item.Version())

		err = item.Value(func(val []byte) error {
			totalReads := expvar.Get("badger_read_num_vlog")
			bytesRead := expvar.Get("badger_read_bytes_vlog")
			require.Equal(t, int64(1), totalReads.(*expvar.Int).Value())
			require.GreaterOrEqual(t, expectedSize, bytesRead.(*expvar.Int).Value())
			return nil
		})

		require.NoError(t, err)
	})
}

func TestReadMetrics(t *testing.T) {
	opt := getTestOptions("")
	opt.managedTxns = true
	opt.CompactL0OnClose = true
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		clearAllMetrics()
		num := 10
		val := make([]byte, 1<<15)
		keys := [][]byte{}
		writer := db.NewManagedWriteBatch()
		for i := 0; i < num; i++ {
			keyB := key("byte", 1)
			keys = append(keys, []byte(keyB))

			_, err := rand.Read(val)
			require.NoError(t, err)

			require.NoError(t, writer.SetEntryAt(NewEntry([]byte(keyB), val), 1))
		}
		writer.Flush()

		txn := db.NewTransactionAt(2, false)
		item, err := txn.Get(keys[0])
		require.NoError(t, err)
		require.Equal(t, uint64(1), item.Version())

		totalGets := expvar.Get("badger_get_num_user")
		require.Equal(t, int64(1), totalGets.(*expvar.Int).Value())

		totalMemtableReads := expvar.Get("badger_get_num_memtable")
		require.Equal(t, int64(1), totalMemtableReads.(*expvar.Int).Value())

		totalLSMGets := expvar.Get("badger_get_num_lsm")
		require.Nil(t, totalLSMGets.(*expvar.Map).Get("l6"))

		// Force compaction
		db.Close()

		db, err = OpenManaged(opt)
		require.NoError(t, err)

		txn = db.NewTransactionAt(2, false)
		item, err = txn.Get(keys[0])
		require.NoError(t, err)
		require.Equal(t, uint64(1), item.Version())

		_, err = txn.Get([]byte(key("abdbyte", 1000))) // val should be far enough that bloom filter doesn't hit
		require.Error(t, err)

		totalLSMGets = expvar.Get("badger_get_num_lsm")
		require.Equal(t, int64(0x1), totalLSMGets.(*expvar.Map).Get("l6").(*expvar.Int).Value())

		totalBloom := expvar.Get("badger_hit_num_lsm_bloom_filter")
		require.Equal(t, int64(0x1), totalBloom.(*expvar.Map).Get("l6").(*expvar.Int).Value())
		require.Equal(t, int64(0x1), totalBloom.(*expvar.Map).Get("DoesNotHave_HIT").(*expvar.Int).Value())
		require.Equal(t, int64(0x2), totalBloom.(*expvar.Map).Get("DoesNotHave_ALL").(*expvar.Int).Value())

		bytesLSM := expvar.Get("badger_read_bytes_lsm")
		require.Equal(t, int64(len(val)), bytesLSM.(*expvar.Int).Value())

		getWithResult := expvar.Get("badger_get_with_result_num_user")
		require.Equal(t, int64(2), getWithResult.(*expvar.Int).Value())

		iterOpts := DefaultIteratorOptions
		iter := txn.NewKeyIterator(keys[0], iterOpts)
		iter.Seek(keys[0])

		rangeQueries := expvar.Get("badger_iterator_num_user")
		require.Equal(t, int64(1), rangeQueries.(*expvar.Int).Value())
	})
}

// findInstanceSnapshot returns the registered snapshot for the given
// instance ID from a process-wide snapshot batch.
func findInstanceSnapshot(snaps []y.MetricsSnapshot, id uint64) *y.MetricsSnapshot {
	for i := range snaps {
		if snaps[i].InstanceID == id {
			return &snaps[i]
		}
	}
	return nil
}

// TestPerInstanceMetricsIsolation verifies that two concurrently open DBs
// keep fully separate counters/histograms, that the registry exposes both,
// and that the legacy expvar totals equal the sum of the two planes.
func TestPerInstanceMetricsIsolation(t *testing.T) {
	clearAllMetrics()

	dirA, err := os.MkdirTemp("", "badger-metrics-a")
	require.NoError(t, err)
	defer removeDir(dirA)
	dirB, err := os.MkdirTemp("", "badger-metrics-b")
	require.NoError(t, err)
	defer removeDir(dirB)

	openManaged := func(dir string) *DB {
		opt := getTestOptions(dir)
		opt.managedTxns = true
		db, err := OpenManaged(opt)
		require.NoError(t, err)
		return db
	}
	dbA, dbB := openManaged(dirA), openManaged(dirB)
	defer func() { _ = dbA.Close() }()
	defer func() { _ = dbB.Close() }()

	require.NotNil(t, dbA.Metrics())
	require.NotNil(t, dbB.Metrics())
	idA, idB := dbA.Metrics().Snapshot().InstanceID, dbB.Metrics().Snapshot().InstanceID
	require.NotEqual(t, idA, idB)

	// Managed writes attribute exactly one put per user entry (no internal
	// transaction-marker entry), which keeps counts deterministic.
	writeN := func(db *DB, prefix string, n int) {
		wb := db.NewManagedWriteBatch()
		for i := 0; i < n; i++ {
			require.NoError(t, wb.SetEntryAt(
				NewEntry([]byte(fmt.Sprintf("%s-%d", prefix, i)), []byte("v")), 1))
		}
		require.NoError(t, wb.Flush())
	}
	writeN(dbA, "a", 3)
	writeN(dbB, "b", 5)

	get := func(db *DB, k string) {
		txn := db.NewTransactionAt(1, false)
		defer txn.Discard()
		_, err := txn.Get([]byte(k))
		require.NoError(t, err)
	}
	get(dbA, "a-0")
	get(dbB, "b-0")

	snapA := dbA.Metrics().Snapshot()
	snapB := dbB.Metrics().Snapshot()
	require.Equal(t, dirA, snapA.Dir)
	require.Equal(t, idA, snapA.InstanceID)
	require.Equal(t, int64(3), snapA.Counters[y.MetricPutNumUser])
	require.Equal(t, int64(5), snapB.Counters[y.MetricPutNumUser])
	require.Equal(t, int64(1), snapA.Counters[y.MetricGetNumUser])
	require.Equal(t, int64(1), snapB.Counters[y.MetricGetNumUser])
	require.Equal(t, int64(1), snapA.Histograms[y.MetricLatencyGetUser].Count)
	require.Equal(t, int64(1), snapB.Histograms[y.MetricLatencyGetUser].Count)

	// A single registry-wide batch contains both independent instances.
	all := y.AllMetricsSnapshots()
	require.NotNil(t, findInstanceSnapshot(all, idA))
	require.NotNil(t, findInstanceSnapshot(all, idB))

	// Legacy totals keep aggregating across both instances under the same
	// historical names, so existing dashboards read the same grand total.
	totalPuts := expvar.Get(y.MetricPutNumUser).(*expvar.Int).Value()
	require.Equal(t, int64(8), totalPuts)
	require.Equal(t, totalPuts,
		snapA.Counters[y.MetricPutNumUser]+snapB.Counters[y.MetricPutNumUser])

	// Closing one instance releases only its own registry entry.
	require.NoError(t, dbA.Close())
	for _, s := range y.AllMetricsSnapshots() {
		require.NotEqual(t, idA, s.InstanceID)
	}
	require.NotNil(t, findInstanceSnapshot(y.AllMetricsSnapshots(), idB))
	// Its directory-keyed legacy gauge entry is released as well.
	require.Nil(t, expvar.Get(y.MetricSizeBytesLSM).(*expvar.Map).Get(dirA))
}

// TestDuplicateDirNameNoOverwrite opens two in-memory DBs whose directory
// names are both empty; their registrations and counters must not collide or
// overwrite each other.
func TestDuplicateDirNameNoOverwrite(t *testing.T) {
	clearAllMetrics()

	open := func() *DB {
		opt := getTestOptions("")
		opt.InMemory = true
		opt.managedTxns = true
		db, err := OpenManaged(opt)
		require.NoError(t, err)
		return db
	}
	dbA, dbB := open(), open()
	defer func() { _ = dbA.Close() }()
	defer func() { _ = dbB.Close() }()

	idA := dbA.Metrics().Snapshot().InstanceID
	idB := dbB.Metrics().Snapshot().InstanceID
	require.NotEqual(t, idA, idB)
	require.Equal(t, "", dbA.Metrics().Snapshot().Dir)

	w := dbA.NewManagedWriteBatch()
	require.NoError(t, w.SetEntryAt(NewEntry([]byte("a1"), []byte("v")), 1))
	require.NoError(t, w.SetEntryAt(NewEntry([]byte("a2"), []byte("v")), 1))
	require.NoError(t, w.Flush())
	w = dbB.NewManagedWriteBatch()
	require.NoError(t, w.SetEntryAt(NewEntry([]byte("b1"), []byte("v")), 1))
	require.NoError(t, w.SetEntryAt(NewEntry([]byte("b2"), []byte("v")), 1))
	require.NoError(t, w.SetEntryAt(NewEntry([]byte("b3"), []byte("v")), 1))
	require.NoError(t, w.Flush())

	require.Equal(t, int64(2), dbA.Metrics().Snapshot().Counters[y.MetricPutNumUser])
	require.Equal(t, int64(3), dbB.Metrics().Snapshot().Counters[y.MetricPutNumUser])
	require.Equal(t, int64(5), expvar.Get(y.MetricPutNumUser).(*expvar.Int).Value())

	require.NoError(t, dbA.Close())
	// B survives A's release with its own values intact.
	snapB := dbB.Metrics().Snapshot()
	require.Equal(t, idB, snapB.InstanceID)
	require.Equal(t, int64(3), snapB.Counters[y.MetricPutNumUser])
	require.Nil(t, findInstanceSnapshot(y.AllMetricsSnapshots(), idA))
}

// TestMetricsReleasedOnCloseAndReopen verifies that closing releases the
// registry/legacy entries and reopening the same directory registers a fresh
// identity without inheriting or overwriting anything.
func TestMetricsReleasedOnCloseAndReopen(t *testing.T) {
	clearAllMetrics()
	dir, err := os.MkdirTemp("", "badger-metrics-reopen")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	id1 := db.Metrics().Snapshot().InstanceID
	require.NotNil(t, expvar.Get(y.MetricSizeBytesLSM).(*expvar.Map).Get(dir))
	require.NoError(t, db.Close())

	require.Nil(t, findInstanceSnapshot(y.AllMetricsSnapshots(), id1))
	require.Nil(t, expvar.Get(y.MetricSizeBytesLSM).(*expvar.Map).Get(dir))

	db2, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	id2 := db2.Metrics().Snapshot().InstanceID
	require.NotEqual(t, id1, id2)
	require.NotNil(t, findInstanceSnapshot(y.AllMetricsSnapshots(), id2))
	require.NotNil(t, expvar.Get(y.MetricSizeBytesLSM).(*expvar.Map).Get(dir))
}

// TestMetricsDisabledNoOverhead checks that with MetricsEnabled=false the DB
// holds no metric plane, all operations work, and neither the per-instance
// registry nor legacy expvar globals receive any traffic.
func TestMetricsDisabledNoOverhead(t *testing.T) {
	clearAllMetrics()

	dir, err := os.MkdirTemp("", "badger-metrics-off")
	require.NoError(t, err)
	defer removeDir(dir)
	opt := getTestOptions(dir).WithMetricsEnabled(false)
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.Nil(t, db.Metrics())
	for _, s := range y.AllMetricsSnapshots() {
		require.NotEqual(t, dir, s.Dir, "disabled DB must not register")
	}

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	}))
	require.NoError(t, db.View(func(txn *Txn) error {
		_, e := txn.Get([]byte("k"))
		return e
	}))
	it := db.NewTransaction(false).NewIterator(DefaultIteratorOptions)
	it.Rewind()
	it.Close()
	lsm, vlog := db.Size()
	require.Equal(t, int64(0), lsm)
	require.Equal(t, int64(0), vlog)
	require.NoError(t, db.Flatten(1))
	require.NoError(t, db.VerifyChecksum())

	// Every legacy expvar variable must still be at its zero value.
	expvar.Do(func(kv expvar.KeyValue) {
		switch v := kv.Value.(type) {
		case *expvar.Int:
			require.Equal(t, int64(0), v.Value(), "legacy int %s should be zero", kv.Key)
		case *expvar.Map:
			require.Equal(t, "{}", v.String(), "legacy map %s should be empty", kv.Key)
		}
	})
}

// TestMetricsSnapshotConsumersReconcile drives all three classes of metric
// consumers (user read/write requests, background compaction and operational
// diagnostic commands) and verifies that ONE snapshot of the instance
// reconciles every reported value, and that the legacy expvar totals match
// that same snapshot.
func TestMetricsSnapshotConsumersReconcile(t *testing.T) {
	clearAllMetrics()

	dir, err := os.MkdirTemp("", "badger-metrics-reconcile")
	require.NoError(t, err)
	defer removeDir(dir)
	opt := getTestOptions(dir)
	opt.managedTxns = true
	// Small memtables so a handful of MB produces multiple L0 tables and
	// forces background compactions quickly. The value threshold must stay
	// below 15% of MemTableSize (the max write-batch size).
	opt.MemTableSize = 1 << 20   // 1 MiB
	opt.ValueThreshold = 1 << 13 // 8 KiB: keep 4 KiB values inline in the LSM
	// so memtables actually flush (pointers-only data would never fill one)

	db, err := OpenManaged(opt)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Class 1: user write requests (managed writes count exactly one put
	// per user entry).
	const numEntries = 60 * 30
	wb := db.NewManagedWriteBatch()
	for i := 0; i < numEntries; i++ {
		k := fmt.Sprintf("conskey-%05d", i)
		require.NoError(t, wb.SetEntryAt(NewEntry([]byte(k), make([]byte, 4<<10)), 1))
	}
	require.NoError(t, wb.Flush())

	// Class 2: wait until background compactions have rewritten bytes into
	// the last level.
	require.Eventually(t, func() bool {
		return db.Metrics().Snapshot().Maps[y.MetricWriteBytesCompaction]["l6"] > 0
	}, 60*time.Second, 100*time.Millisecond, "waiting for background compaction")

	// Class 1: user read requests (+ an iterator).
	txn := db.NewTransactionAt(2, false)
	for i := 0; i < 5; i++ {
		_, err := txn.Get([]byte(fmt.Sprintf("conskey-%05d", i*100)))
		require.NoError(t, err)
	}
	itr := txn.NewIterator(DefaultIteratorOptions)
	itr.Seek([]byte("conskey-"))
	itr.Close()
	txn.Discard()

	// Class 3: operational/diagnostic commands.
	require.NoError(t, db.VerifyChecksum())
	require.NoError(t, db.Flatten(2))

	// ONE consistent snapshot for the whole reconciliation: read/write
	// requests, background compaction and diagnostic commands all reconcile
	// against this same object.
	snap := db.Metrics().Snapshot()
	require.Equal(t, dir, snap.Dir)

	// Read/write request counters and latency distributions.
	require.Equal(t, int64(numEntries), snap.Counters[y.MetricPutNumUser])
	// write_bytes_user includes keys and per-entry metadata in addition to
	// the 4 KiB values.
	require.GreaterOrEqual(t, snap.Counters[y.MetricWriteBytesUser], int64(numEntries*4<<10))
	require.Equal(t, int64(5), snap.Counters[y.MetricGetNumUser])
	require.Equal(t, int64(1), snap.Counters[y.MetricIteratorNumUser])
	require.GreaterOrEqual(t, snap.Histograms[y.MetricLatencyPutUser].Count, int64(1))
	require.Equal(t, int64(5), snap.Histograms[y.MetricLatencyGetUser].Count)

	// Background compaction counters and latency distribution.
	require.Greater(t, snap.Maps[y.MetricWriteBytesCompaction]["l6"], int64(0))
	require.GreaterOrEqual(t, snap.Histograms[y.MetricLatencyCompactionLSM].Count, int64(1))
	comp := snap.Histograms[y.MetricLatencyCompactionLSM]
	require.GreaterOrEqual(t, comp.Percentiles["p99"], comp.Percentiles["p50"])

	// Operational/diagnostic command latency distributions carry the same
	// queryable buckets/percentiles as the request distributions.
	for _, name := range []string{y.DiagnosticVerifyChecksum, y.DiagnosticFlatten} {
		h := snap.DiagnosticHistograms[name]
		require.Equal(t, int64(1), h.Count)
		require.NotEmpty(t, h.Bounds)
		require.GreaterOrEqual(t, h.Percentiles["p99"], h.Percentiles["p50"])
	}

	// The values reconcile with the legacy expvar totals, which remain
	// scraped under the original names with no dashboard changes.
	require.Equal(t, snap.Counters[y.MetricPutNumUser],
		expvar.Get(y.MetricPutNumUser).(*expvar.Int).Value())
	require.Equal(t, snap.Maps[y.MetricWriteBytesCompaction]["l6"],
		expvar.Get(y.MetricWriteBytesCompaction).(*expvar.Map).Get("l6").(*expvar.Int).Value())
}

// TestVlogReadLatencyMetrics covers the value-log read consumer: large values
// served from the vlog record both the cumulative counters and a latency
// distribution on the owning instance.
func TestVlogReadLatencyMetrics(t *testing.T) {
	clearAllMetrics()

	dir, err := os.MkdirTemp("", "badger-metrics-vread")
	require.NoError(t, err)
	defer removeDir(dir)
	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	val := make([]byte, 1<<20)
	_, err = rand.Read(val)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("big"), val)
	}))
	require.NoError(t, db.View(func(txn *Txn) error {
		item, e := txn.Get([]byte("big"))
		require.NoError(t, e)
		return item.Value(func(got []byte) error {
			require.Equal(t, val, got)
			return nil
		})
	}))

	snap := db.Metrics().Snapshot()
	require.Equal(t, int64(1), snap.Counters[y.MetricReadNumVlog])
	// A vlog read produced one latency sample. Its measured duration can be
	// zero on a hot zero-copy mmap path, so distribution math (sum,
	// percentiles) is verified with controlled samples in the y package.
	require.Equal(t, int64(1), snap.Histograms[y.MetricLatencyReadVlog].Count)
	require.Equal(t, int64(1), expvar.Get(y.MetricReadNumVlog).(*expvar.Int).Value())
}

// TestValueLogGCMetrics covers the reclamation (value-log GC) consumer.
func TestValueLogGCMetrics(t *testing.T) {
	clearAllMetrics()

	dir, err := os.MkdirTemp("", "badger-metrics-gc")
	require.NoError(t, err)
	defer removeDir(dir)
	opt := getTestOptions(dir)
	opt.managedTxns = true
	opt.ValueLogFileSize = 1 << 20
	opt.ValueLogMaxEntries = 1000
	opt.BaseTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10
	opt.MemTableSize = 1 << 15

	db, err := OpenManaged(opt)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	const n = 10000
	sz := 64 << 10
	writer := db.NewManagedWriteBatch()
	for i := 0; i < n; i++ {
		v := make([]byte, sz)
		_, _ = rand.Read(v[:rand.Intn(sz)])
		require.NoError(t, writer.SetEntryAt(
			NewEntry([]byte(fmt.Sprintf("gckey-%05d", i)), v), uint64(2*i+1)))
	}
	require.NoError(t, writer.Flush())

	// Delete half of the keys.
	deleter := db.NewManagedWriteBatch()
	for i := 0; i < n/2; i++ {
		require.NoError(t, deleter.DeleteAt(
			[]byte(fmt.Sprintf("gckey-%05d", i)), uint64(2*n+2*i+1)))
	}
	require.NoError(t, deleter.Flush())

	db.SetDiscardTs(math.MaxUint32)
	require.NoError(t, db.Flatten(3))

	rewrote := false
	for i := 0; i < 100; i++ {
		// Try at most 100 times to GC even a single value log file (same
		// recipe as TestValueGCManaged).
		if err := db.RunValueLogGC(0.0001); err == nil {
			rewrote = true
			break
		}
	}
	require.True(t, rewrote, "expected at least one value log GC rewrite")

	snap := db.Metrics().Snapshot()
	gc := snap.Histograms[y.MetricLatencyGCVlog]
	require.GreaterOrEqual(t, gc.Count, int64(1))
	require.GreaterOrEqual(t, gc.Percentiles["p99"], gc.Percentiles["p50"])
	// GC rewrites go back through the user write path, so they are visible in
	// the same snapshot's write counters/distributions.
	require.Greater(t, snap.Counters[y.MetricPutNumUser], int64(0))
}
