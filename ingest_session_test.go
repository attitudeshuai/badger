/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// ---- feed helpers ---------------------------------------------------------

func ingestKV(key, val string, version uint64, meta ...byte) *pb.KV {
	kv := &pb.KV{Key: []byte(key), Value: []byte(val), Version: version}
	if len(meta) > 0 {
		kv.Meta = []byte{meta[0]}
	}
	return kv
}

func ingestKVsToBuffer(t *testing.T, kvs []*pb.KV) *z.Buffer {
	t.Helper()
	buf := z.NewBuffer(16<<20, "test")
	t.Cleanup(func() { require.NoError(t, buf.Release()) })
	for _, kv := range kvs {
		KVToBuffer(kv, buf)
	}
	return buf
}

func streamFeed(t *testing.T, sw *StreamWriter, kvs []*pb.KV) {
	t.Helper()
	require.NoError(t, sw.Write(ingestKVsToBuffer(t, kvs)))
}

func beginAndFeed(t *testing.T, db *DB, mode IngestMode, id string, kvs []*pb.KV) *StreamWriter {
	t.Helper()
	sw := db.NewStreamWriter()
	require.NoError(t, sw.Begin(mode, IngestOptions{SessionID: id}))
	streamFeed(t, sw, kvs)
	return sw
}

func ingestFull(t *testing.T, db *DB, id string, kvs []*pb.KV) {
	t.Helper()
	sw := beginAndFeed(t, db, IngestFull, id, kvs)
	require.NoError(t, sw.Flush())
}

func ingestIncremental(t *testing.T, db *DB, id string, kvs []*pb.KV) {
	t.Helper()
	sw := beginAndFeed(t, db, IngestIncremental, id, kvs)
	require.NoError(t, sw.Flush())
}

// keyState is the converged latest state of a user key.
type keyState struct {
	value   string
	deleted bool
}

// dumpLatest walks an AllVersions+InternalAccess iterator and keeps only the
// newest version per user key (the iterator yields versions newest first).
func dumpLatest(t *testing.T, db *DB) map[string]keyState {
	t.Helper()
	out := make(map[string]keyState)
	err := db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		opts.InternalAccess = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			if _, seen := out[key]; seen {
				continue
			}
			if item.IsDeletedOrExpired() {
				out[key] = keyState{deleted: true}
				continue
			}
			val, err := item.ValueCopy(nil)
			require.NoError(t, err)
			out[key] = keyState{value: string(val)}
		}
		return nil
	})
	require.NoError(t, err)
	return out
}

func dumpVersionCounts(t *testing.T, db *DB) map[string]int {
	t.Helper()
	out := make(map[string]int)
	err := db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		opts.InternalAccess = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			out[string(it.Item().Key())]++
		}
		return nil
	})
	require.NoError(t, err)
	return out
}

func expectState(t *testing.T, db *DB, want map[string]keyState) {
	t.Helper()
	require.Equal(t, want, dumpLatest(t, db))
}

func tableFileIDsOnDisk(t *testing.T, dir string) map[uint64]bool {
	t.Helper()
	out := make(map[uint64]bool)
	for id := range getIDMap(dir) {
		out[id] = true
	}
	return out
}

func manifestIDs(db *DB) map[uint64]bool {
	out := make(map[uint64]bool)
	for id := range db.manifest.manifest.Tables {
		out[id] = true
	}
	return out
}

// ---- AC-1 / AC-2: journal contract + invisible staging --------------------

func TestIngestBeginContractAndStagingInvisible(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)

	ingestFull(t, db, "seed", []*pb.KV{
		ingestKV("old1", "o", 1),
		ingestKV("old2", "o", 1),
	})
	require.NoError(t, db.Close())

	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err)

	sw := db.NewStreamWriter()
	require.NoError(t, sw.Begin(IngestIncremental, IngestOptions{SessionID: "inc1"}))
	streamFeed(t, sw, []*pb.KV{ingestKV("new1", "n", 10)})

	pending, err := db.PendingIngestSessions()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "inc1", pending[0].ID)
	require.Equal(t, IngestIncremental, pending[0].Mode)
	require.Equal(t, IngestStaging, pending[0].Phase)
	require.Equal(t, db.opt.MaxLevels-2, pending[0].TargetLevel)

	manifestCountBefore := len(db.manifest.manifest.Tables)
	tablesBefore := len(db.Tables())

	// Readers see only the pre-session state mid-staging.
	require.NoError(t, db.View(func(txn *Txn) error {
		_, gerr := txn.Get([]byte("new1"))
		require.ErrorIs(t, gerr, ErrKeyNotFound)
		_, oerr := txn.Get([]byte("old1"))
		require.NoError(t, oerr)
		return nil
	}))

	require.NoError(t, sw.Flush())
	require.Equal(t, manifestCountBefore+1, len(db.manifest.manifest.Tables))
	require.Equal(t, tablesBefore+1, len(db.Tables()))

	require.NoFileExists(t, ingestJournalPath(dir, "inc1"))
	require.FileExists(t, ingestDonePath(dir, "inc1"))

	pending, err = db.PendingIngestSessions()
	require.NoError(t, err)
	require.Empty(t, pending)
	expectState(t, db, map[string]keyState{
		"old1": {value: "o"},
		"old2": {value: "o"},
		"new1": {value: "n"},
	})
	require.NoError(t, db.Close())
}

// ---- AC-4: overlap convergence --------------------------------------------

func TestIngestIncrementalConvergence(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)

	var seed []*pb.KV
	for _, k := range []string{"k0", "k1", "k2", "k3", "k4"} {
		seed = append(seed, ingestKV(k, "old@5", 5))
	}
	ingestFull(t, db, "base", seed)

	// Keys must arrive sorted within the (single) stream.
	feed := []*pb.KV{
		ingestKV("k1", "stale@2", 2),                         // older value loses
		ingestKV("k3", "new@10", 10),                         // newer value wins
		{Key: []byte("k4"), Version: 9, Meta: []byte{bitDelete}}, // newer tombstone
		ingestKV("k7", "brand@1", 1),                         // new key
	}
	ingestIncremental(t, db, "inc-conv", feed)

	want := map[string]keyState{
		"k0": {value: "old@5"},
		"k1": {value: "old@5"},
		"k2": {value: "old@5"},
		"k3": {value: "new@10"},
		"k4": {deleted: true},
		"k7": {value: "brand@1"},
	}
	expectState(t, db, want)

	// Exactly one version per key after convergence.
	for k, n := range dumpVersionCounts(t, db) {
		require.Equalf(t, 1, n, "key %s has %d versions after convergence", k, n)
	}

	// All data is on the deepest level: overlapping incrementals do not pile
	// the staircase upward.
	for _, ti := range db.Tables() {
		require.Equal(t, db.opt.MaxLevels-1, ti.Level, "table %d not at last level", ti.ID)
	}

	require.NoError(t, db.Close())

	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err)
	expectState(t, db, want)
	require.NoError(t, db.Close())
}

// A second overlapping incremental converges again onto Lmax, never climbs.
func TestIngestRepeatedOverlappingStaysFlat(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)

	ingestFull(t, db, "base", []*pb.KV{
		ingestKV("k1", "v1", 1),
		ingestKV("k2", "v1", 1),
	})
	ingestIncremental(t, db, "inc-1", []*pb.KV{ingestKV("k1", "v2", 2)})
	ingestIncremental(t, db, "inc-2", []*pb.KV{ingestKV("k1", "v3", 3)})

	expectState(t, db, map[string]keyState{
		"k1": {value: "v3"},
		"k2": {value: "v1"},
	})
	for _, ti := range db.Tables() {
		require.Equal(t, db.opt.MaxLevels-1, ti.Level)
	}
	require.Equal(t, 1, dumpVersionCounts(t, db)["k1"])
	require.NoError(t, db.Close())
}

// Disjoint incremental ranges are placed without being rewritten.
func TestIngestDisjointIncrementalNotRewritten(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)

	ingestFull(t, db, "base", []*pb.KV{ingestKV("a1", "v1", 1)})

	sw := db.NewStreamWriter()
	require.NoError(t, sw.Begin(IngestIncremental, IngestOptions{SessionID: "disjoint"}))
	streamFeed(t, sw, []*pb.KV{ingestKV("z9", "v2", 2)})
	require.Equal(t, db.opt.MaxLevels-2, sw.targetLevel)
	require.NoError(t, sw.Flush())

	// The disjoint table is published as-is at the level above while the old
	// table remains untouched at Lmax (no convergence rewrite).
	levelFor := func(userKey string) int {
		k := []byte(userKey)
		for _, ti := range db.Tables() {
			if bytes.Compare(k, y.ParseKey(ti.Left)) >= 0 &&
				bytes.Compare(k, y.ParseKey(ti.Right)) <= 0 {
				return ti.Level
			}
		}
		t.Fatalf("no table covers key %s", userKey)
		return -1
	}
	require.Equal(t, db.opt.MaxLevels-1, levelFor("a1"))
	require.Equal(t, db.opt.MaxLevels-2, levelFor("z9"))
	expectState(t, db, map[string]keyState{
		"a1": {value: "v1"},
		"z9": {value: "v2"},
	})
	require.NoError(t, db.Close())
}

// ---- AC-5: tombstones ------------------------------------------------------

func TestIngestTombstonesAcrossReplay(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)

	ingestFull(t, db, "base", []*pb.KV{
		ingestKV("grave", "alive", 1),
		ingestKV("mixed", "keep@10", 10),
	})
	ingestIncremental(t, db, "inc-del", []*pb.KV{
		{Key: []byte("grave"), Version: 11, Meta: []byte{bitDelete}}, // newer delete wins
		{Key: []byte("mixed"), Version: 5, Meta: []byte{bitDelete}},  // older delete loses
	})

	check := func() {
		require.NoError(t, db.View(func(txn *Txn) error {
			_, gerr := txn.Get([]byte("grave"))
			require.ErrorIs(t, gerr, ErrKeyNotFound)
			item, merr := txn.Get([]byte("mixed"))
			require.NoError(t, merr)
			val, verr := item.ValueCopy(nil)
			require.NoError(t, verr)
			require.Equal(t, "keep@10", string(val))
			return nil
		}))
	}
	check()
	require.NoError(t, db.Close())

	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err)
	defer db.Close()
	check()
}

// ---- AC-3: crash + replay idempotency (full mode) --------------------------

func expectedSeedState(n int) map[string]keyState {
	want := make(map[string]keyState, n)
	for i := 0; i < n; i++ {
		want[fmt.Sprintf("k%03d", i)] = keyState{value: "v"}
	}
	return want
}

func TestIngestCrashReplayFull(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)

	const n = 40
	feed := make([]*pb.KV, n)
	for i := 0; i < n; i++ {
		feed[i] = ingestKV(fmt.Sprintf("k%03d", i), "v", 1)
	}

	// First attempt: begin + feed, then "crash" by closing without Flush.
	beginAndFeed(t, db, IngestFull, "replay-full", feed)
	require.NoError(t, db.Close())

	// Reopen: pending session visible; staged SSTs already reverted.
	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	pending, err := db.PendingIngestSessions()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "replay-full", pending[0].ID)
	require.Equal(t, IngestStaging, pending[0].Phase)
	expectState(t, db, map[string]keyState{})
	require.Equal(t, manifestIDs(db), tableFileIDsOnDisk(t, dir))

	// Continue: same id, re-feed the complete data set.
	sw2 := db.NewStreamWriter()
	require.NoError(t, sw2.Resume("replay-full"))
	streamFeed(t, sw2, feed)
	require.NoError(t, sw2.Flush())
	expectState(t, db, expectedSeedState(n))
	require.Equal(t, manifestIDs(db), tableFileIDsOnDisk(t, dir))

	// Replaying a committed session errors without creating anything.
	infosBefore, err := db.IngestSessions()
	require.NoError(t, err)
	filesBefore := tableFileIDsOnDisk(t, dir)
	sw3 := db.NewStreamWriter()
	require.ErrorIs(t, sw3.Begin(IngestFull, IngestOptions{SessionID: "replay-full"}),
		ErrIngestAlreadyCommitted)
	infosAfter, err := db.IngestSessions()
	require.NoError(t, err)
	require.Equal(t, infosBefore, infosAfter)
	require.Equal(t, filesBefore, tableFileIDsOnDisk(t, dir))
	require.NoError(t, db.Close())

	// After another reopen the session is committed, not pending.
	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err)
	defer db.Close()
	pending, err = db.PendingIngestSessions()
	require.NoError(t, err)
	require.Empty(t, pending)
	expectState(t, db, expectedSeedState(n))
}

// ---- AC-6/AC-8: discard after crash (incremental keeps old data) -----------

func TestIngestDiscardAfterCrash(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)
	ingestFull(t, db, "seed", []*pb.KV{
		ingestKV("seed1", "s", 1),
		ingestKV("seed2", "s", 1),
	})

	beginAndFeed(t, db, IngestIncremental, "inc-abort",
		[]*pb.KV{ingestKV("new9", "n", 9)})
	require.NoError(t, db.Close())

	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err)
	pending, err := db.PendingIngestSessions()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "inc-abort", pending[0].ID)

	// Old data stays readable while the session is pending.
	expectState(t, db, map[string]keyState{
		"seed1": {value: "s"},
		"seed2": {value: "s"},
	})

	require.NoError(t, db.DiscardIngestSession("inc-abort"))
	pending, err = db.PendingIngestSessions()
	require.NoError(t, err)
	require.Empty(t, pending)
	require.NoFileExists(t, ingestJournalPath(dir, "inc-abort"))
	require.Equal(t, manifestIDs(db), tableFileIDsOnDisk(t, dir))

	// DB stays usable: a fresh session can proceed.
	ingestIncremental(t, db, "inc-after", []*pb.KV{ingestKV("new10", "n", 10)})
	expectState(t, db, map[string]keyState{
		"seed1": {value: "s"},
		"seed2": {value: "s"},
		"new10": {value: "n"},
	})

	require.ErrorIs(t, db.DiscardIngestSession("inc-abort"), ErrIngestSessionNotFound)
	require.ErrorIs(t, db.DiscardIngestSession("seed"), ErrIngestAlreadyCommitted)
	require.NoError(t, db.Close())
}

// ---- AC-6: committing reconciliation on reopen -----------------------------

func TestIngestCommittingReconcile(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)
	ingestFull(t, db, "commit1", []*pb.KV{
		ingestKV("a", "1", 1),
		ingestKV("b", "1", 1),
	})
	require.NoError(t, db.Close())

	// Crash after the MANIFEST changeset landed but before the journal was
	// renamed to .done.
	done := ingestDonePath(dir, "commit1")
	active := ingestJournalPath(dir, "commit1")
	require.NoError(t, os.Rename(done, active))

	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err)
	require.FileExists(t, done)
	require.NoFileExists(t, active)
	pending, err := db.PendingIngestSessions()
	require.NoError(t, err)
	require.Empty(t, pending)
	expectState(t, db, map[string]keyState{
		"a": {value: "1"},
		"b": {value: "1"},
	})
	require.NoError(t, db.Close())

	// A COMMIT frame whose final tables are absent from the MANIFEST stays
	// pending and can be discarded.
	writeSyntheticCommittingJournal(t, dir, "ghost", 9999)

	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err)
	pending, err = db.PendingIngestSessions()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "ghost", pending[0].ID)
	require.Equal(t, IngestCommitting, pending[0].Phase)
	require.NoError(t, db.DiscardIngestSession("ghost"))
	pending, err = db.PendingIngestSessions()
	require.NoError(t, err)
	require.Empty(t, pending)
	require.NoError(t, db.Close())
}

func writeSyntheticCommittingJournal(t *testing.T, dir, id string, finalID uint64) {
	t.Helper()
	registry, err := OpenKeyRegistry(KeyRegistryOptions{Dir: dir})
	require.NoError(t, err)
	defer func() { _ = registry.Close() }()
	j, err := createIngestJournal(DefaultOptions(dir), registry, id,
		IngestIncremental, 5, IngestKeyRange{})
	require.NoError(t, err)
	require.NoError(t, j.appendCommit([]uint64{finalID}, nil))
	require.NoError(t, j.close())
}

// ---- AC-10: read atomicity during staging and across commit ---------------

func TestIngestReadAtomicity(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)
	ingestFull(t, db, "base", []*pb.KV{
		ingestKV("k1", "old", 1),
		ingestKV("k2", "old", 1),
	})

	// (a) During staging every reader observes the pre-session state.
	sw := db.NewStreamWriter()
	require.NoError(t, sw.Begin(IngestIncremental, IngestOptions{SessionID: "atomic1"}))
	streamFeed(t, sw, []*pb.KV{ingestKV("k1", "new", 10)})

	for i := 0; i < 20; i++ {
		require.NoError(t, db.View(func(txn *Txn) error {
			item, gerr := txn.Get([]byte("k1"))
			require.NoError(t, gerr)
			val, _ := item.ValueCopy(nil)
			require.Equal(t, "old", string(val))
			_, nerr := txn.Get([]byte("k3"))
			require.ErrorIs(t, nerr, ErrKeyNotFound)
			return nil
		}))
	}

	// (b) With the publish barrier held, a reader cannot complete in the
	// middle of the cross-level swap; it unblocks only at the converged state.
	entered := make(chan struct{})
	release := make(chan struct{})
	readDone := make(chan string, 1)
	testingIngestPublishHook = func() {
		close(entered)
		<-release
	}
	defer func() { testingIngestPublishHook = nil }()

	flushErr := make(chan error, 1)
	go func() { flushErr <- sw.Flush() }()
	<-entered

	go func() {
		var v string
		_ = db.View(func(txn *Txn) error {
			item, err := txn.Get([]byte("k1"))
			if err != nil {
				return err
			}
			val, _ := item.ValueCopy(nil)
			v = string(val)
			return nil
		})
		readDone <- v
	}()

	select {
	case v := <-readDone:
		t.Fatalf("reader completed mid-publish with value %q", v)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-flushErr)
	select {
	case v := <-readDone:
		require.Equal(t, "new", v)
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not complete after ingest publish")
	}
	require.NoError(t, db.Close())
}

// ---- AC-2: declared range enforcement --------------------------------------

func TestIngestDeclaredRangeEnforced(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		sw := db.NewStreamWriter()
		require.NoError(t, sw.Begin(IngestIncremental, IngestOptions{
			SessionID:     "bounded",
			DeclaredRange: IngestKeyRange{Left: []byte("a"), Right: []byte("m")},
		}))
		defer sw.Cancel()
		buf := z.NewBuffer(1<<20, "test")
		defer func() { require.NoError(t, buf.Release()) }()
		KVToBuffer(ingestKV("z-out-of-range", "x", 1), buf)
		err := sw.Write(buf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "outside the declared session range")
		// The out-of-range feed never reached the levels: nothing staged is
		// visible.
		require.Empty(t, db.Tables())
	})
}

// ---- InMemory behaviour -----------------------------------------------------

func TestIngestInMemorySessions(t *testing.T) {
	opt := DefaultOptions("").WithInMemory(true)
	db, err := Open(opt)
	require.NoError(t, err)
	defer db.Close()

	ingestFull(t, db, "mem1", []*pb.KV{ingestKV("a", "1", 1)})
	infos, err := db.IngestSessions()
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.Equal(t, IngestCommitted, infos[0].Phase)

	sw := db.NewStreamWriter()
	require.ErrorIs(t, sw.Begin(IngestFull, IngestOptions{SessionID: "mem1"}),
		ErrIngestAlreadyCommitted)

	sw2 := db.NewStreamWriter()
	require.NoError(t, sw2.Begin(IngestIncremental, IngestOptions{SessionID: "mem2"}))
	streamFeed(t, sw2, []*pb.KV{ingestKV("b", "2", 2)})
	require.NoError(t, sw2.Flush())
	expectState(t, db, map[string]keyState{
		"a": {value: "1"},
		"b": {value: "2"},
	})
}

// ---- AC-6: read-only open reports but never mutates ------------------------

func TestIngestReadOnlyPending(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("read-only mode is not supported on Windows")
	}
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)
	ingestFull(t, db, "seed", []*pb.KV{ingestKV("a", "1", 1)})
	beginAndFeed(t, db, IngestIncremental, "ro-pending",
		[]*pb.KV{ingestKV("b", "2", 2)})
	require.NoError(t, db.Close())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	before := make(map[string]int64)
	for _, e := range entries {
		if info, ierr := e.Info(); ierr == nil {
			before[e.Name()] = info.Size()
		}
	}

	ro, err := Open(DefaultOptions(dir).WithReadOnly(true))
	require.NoError(t, err)
	pending, err := ro.PendingIngestSessions()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Error(t, ro.DiscardIngestSession("ro-pending"))
	require.NoError(t, ro.Close())

	entries, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, len(before))
	for _, e := range entries {
		info, ierr := e.Info()
		require.NoError(t, ierr)
		require.Equalf(t, before[e.Name()], info.Size(),
			"file %s mutated on read-only open", e.Name())
	}
}

// ---- AC-9: API and direct-directory inspection agree -----------------------

func TestIngestInspectionParity(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)
	ingestFull(t, db, "done1", []*pb.KV{ingestKV("a", "1", 1)})
	beginAndFeed(t, db, IngestIncremental, "pend1",
		[]*pb.KV{ingestKV("b", "2", 2)})
	require.NoError(t, db.Close())

	infos, err := ReadIngestSessions(DefaultOptions(dir))
	require.NoError(t, err)
	byID := map[string]IngestSessionInfo{}
	for _, info := range infos {
		byID[info.ID] = info
	}
	require.Equal(t, IngestCommitted, byID["done1"].Phase)
	require.Equal(t, IngestStaging, byID["pend1"].Phase)
	require.Equal(t, IngestFull, byID["done1"].Mode)
	require.Equal(t, IngestIncremental, byID["pend1"].Mode)
	require.Equal(t, []byte("a"), byID["done1"].Range.Left)
	// pend1 never flushed and its single key did not roll an SST, so its
	// observed range is not known yet; BEGIN metadata still is.
	require.Empty(t, byID["pend1"].Range.Left)

	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err)
	defer db.Close()
	dbInfos, err := db.IngestSessions()
	require.NoError(t, err)
	keyed := func(list []IngestSessionInfo) map[string]IngestSessionInfo {
		m := map[string]IngestSessionInfo{}
		for _, i := range list {
			m[i.ID] = i
		}
		return m
	}
	dbByID := keyed(dbInfos)
	for id, want := range byID {
		got := dbByID[id]
		require.Equal(t, want.Phase, got.Phase)
		require.Equal(t, want.Mode, got.Mode)
		require.Equal(t, want.TargetLevel, got.TargetLevel)
		require.Equal(t, want.Range, got.Range)
	}
}

// Unrelated files never appear in a session listing.
func TestIngestJournalFileLayout(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(table.NewFilename(7, dir), []byte("sst"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MANIFEST"), []byte("mf"), 0600))
	infos, err := ReadIngestSessions(DefaultOptions(dir))
	require.NoError(t, err)
	require.Empty(t, infos)
}
