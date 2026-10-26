/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rotationTestOptions builds options with small files so a modest amount of
// data produces multiple SSTables and value log files. compactors may be
// disabled to keep file ids stable across a rotation.
func rotationTestOptions(dir string, key []byte, compactors int) Options {
	opt := getTestOptions(dir)
	opt.EncryptionKey = key
	opt.BlockCacheSize = 1 << 20
	opt.IndexCacheSize = 1 << 20
	opt.MemTableSize = 1 << 20     // 1 MB
	opt.BaseTableSize = 128 << 10  // 128 KB SSTables
	opt.ValueThreshold = 128       // everything bigger lands in the vlog
	opt.ValueLogFileSize = 1 << 20 // 1 MB value log files
	opt.NumMemtables = 3
	opt.NumLevelZeroTables = 2
	opt.NumLevelZeroTablesStall = 20
	opt.NumCompactors = compactors
	return opt
}

func rotationKey(t *testing.T) []byte {
	k := make([]byte, 32)
	_, err := rand.Read(k)
	require.NoError(t, err)
	return k
}

func vlogKey(i int) []byte   { return []byte(fmt.Sprintf("v%08d", i)) }
func inlineKey(i int) []byte { return []byte(fmt.Sprintf("l%08d", i)) }
func churnKey(i int) []byte  { return []byte(fmt.Sprintf("c%08d", i)) }

// seedRotationData writes two key families: large values into the value log
// (several vlog files) and small inline values (several SSTables).
func seedRotationData(t *testing.T, db *DB) {
	const nv, ni, vsz = 1500, 8000, 3000
	val := make([]byte, vsz)
	for i := 0; i < nv; i++ {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(vlogKey(i), val)
		}))
	}
	for i := 0; i < ni; i++ {
		v := []byte(fmt.Sprintf("inline-value-%08d", i))
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(inlineKey(i), v)
		}))
	}
}

func verifyRotationData(t *testing.T, db *DB, nv, ni int) {
	for i := 0; i < nv; i++ {
		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get(vlogKey(i))
			require.NoError(t, err)
			return item.Value(func(v []byte) error {
				require.Len(t, v, 3000)
				return nil
			})
		}))
	}
	for i := 0; i < ni; i++ {
		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get(inlineKey(i))
			require.NoError(t, err)
			want := []byte(fmt.Sprintf("inline-value-%08d", i))
			return item.Value(func(v []byte) error {
				require.True(t, bytes.Equal(v, want), "mismatch for %s", inlineKey(i))
				return nil
			})
		}))
	}
}

// sstKeyIDsSnapshot returns the set of SSTable ids currently encrypted with
// keyID along with the total live table count.
func sstKeyIDsSnapshot(db *DB, keyID uint64) (map[uint64]bool, int) {
	out := make(map[uint64]bool)
	total := 0
	for _, l := range db.lc.levels {
		l.RLock()
		for _, tb := range l.tables {
			total++
			if tb.KeyID() == keyID {
				out[tb.ID()] = true
			}
		}
		l.RUnlock()
	}
	return out, total
}

func assertAllFilesOnKey(t *testing.T, db *DB, targetKeyID uint64) {
	t.Helper()
	// SSTables in memory and recorded in the MANIFEST must all be on target.
	for _, l := range db.lc.levels {
		l.RLock()
		for _, tb := range l.tables {
			require.Equalf(t, targetKeyID, tb.KeyID(),
				"table %d on level %d not on target key", tb.ID(), l.level)
		}
		l.RUnlock()
	}
	for id, tm := range db.manifest.manifest.Tables {
		require.Equalf(t, targetKeyID, tm.KeyID, "manifest entry for table %d", id)
	}
	// Value log files must all carry the target key id in their headers.
	db.vlog.filesLock.RLock()
	for _, lf := range db.vlog.filesMap {
		require.Equalf(t, targetKeyID, lf.keyID(), "vlog fid %d", lf.fid)
	}
	db.vlog.filesLock.RUnlock()
	// Memtable WALs must be on target as well.
	_, walRemaining := db.oldMemWALCount(targetKeyID)
	require.Equal(t, 0, walRemaining)
}

func assertRegistryOnlyHas(t *testing.T, db *DB, keyID uint64) {
	t.Helper()
	db.registry.RLock()
	defer db.registry.RUnlock()
	require.Len(t, db.registry.dataKeys, 1)
	_, ok := db.registry.dataKeys[keyID]
	require.True(t, ok, "registry does not contain target key %d", keyID)
}

func waitForRotation(t *testing.T, db *DB) {
	t.Helper()
	require.NoError(t, db.WaitKeyRotation(context.Background()))
}

// TestKeyRotationOnline runs the main scenario: an encrypted database with
// multiple SSTables and vlog files is rotated online while concurrent writes
// and reads keep happening; afterwards everything is on the new generation,
// the old generation is retired, and only the new master key reopens the DB.
func TestKeyRotationOnline(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-rotate")
	require.NoError(t, err)
	defer removeDir(dir)

	oldKey := rotationKey(t)
	newKey := rotationKey(t)

	// Phase 1: create data, close so memtables flush and vlogs seal.
	db, err := Open(rotationTestOptions(dir, oldKey, 4))
	require.NoError(t, err)
	seedRotationData(t, db)
	require.NoError(t, db.Close())

	// Phase 2: reopen, start rotation while churning writes/reads.
	db, err = Open(rotationTestOptions(dir, oldKey, 4))
	require.NoError(t, err)

	st0 := db.KeyRotationStatus()
	require.False(t, st0.Active)

	require.NoError(t, db.StartKeyRotation(newKey))
	require.Equal(t, ErrKeyRotationInProgress, db.StartKeyRotation(newKey))

	targetID := db.KeyRotationStatus().TargetKeyID
	require.NotZero(t, targetID)

	// Snapshot: rotation must have sealed the active vlog and memtable, so the
	// new files use the target key right away.
	_, vlogRemaining := db.vlog.rotationCounts(targetID)
	require.NotZero(t, vlogRemaining, "expected old-generation sealed vlogs")

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(2)
	go func() { // concurrent writer
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			val := make([]byte, 2048)
			if err := db.Update(func(txn *Txn) error {
				return txn.Set(churnKey(i), val)
			}); err != nil {
				return
			}
			i++
		}
	}()
	go func() { // concurrent reader of old data
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = db.View(func(txn *Txn) error {
				item, err := txn.Get(vlogKey(rand.Intn(1500)))
				if err != nil {
					return nil
				}
				return item.Value(func(v []byte) error {
					if len(v) != 3000 {
						t.Errorf("unexpected value len %d", len(v))
					}
					return nil
				})
			})
		}
	}()

	// Observe intermediate progress.
	sawProgress := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		st := db.KeyRotationStatus()
		if !st.Active {
			break
		}
		if st.SSTRemaining < st.SSTTotal || st.VlogRemaining < st.VlogTotal {
			sawProgress = true
		}
		require.NoError(t, st.LastError)
		time.Sleep(20 * time.Millisecond)
	}
	waitForRotation(t, db)
	close(stop)
	wg.Wait()
	require.True(t, sawProgress, "expected to observe partial rotation progress")

	st := db.KeyRotationStatus()
	require.False(t, st.Active)
	require.False(t, st.FinishedAt.IsZero())
	require.Equal(t, 0, st.Remaining())

	assertAllFilesOnKey(t, db, targetID)
	assertRegistryOnlyHas(t, db, targetID)
	verifyRotationData(t, db, 1500, 8000)

	// Churned keys written during rotation must be readable too.
	count := 0
	require.NoError(t, db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.Prefix = []byte("c")
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	}))
	require.NotZero(t, count)

	require.NoError(t, db.Close())

	// Checkpoint must be gone.
	_, err = os.Stat(filepath.Join(dir, KeyRotationFileName))
	require.True(t, os.IsNotExist(err))

	// Reopen with the new key: readable. With the old key: rejected.
	db, err = Open(rotationTestOptions(dir, newKey, 2))
	require.NoError(t, err)
	verifyRotationData(t, db, 1500, 8000)
	require.NoError(t, db.Close())

	_, err = Open(rotationTestOptions(dir, oldKey, 2))
	require.EqualError(t, err, ErrEncryptionKeyMismatch.Error())
}

// TestKeyRotationResume interrupts a rotation mid-flight, reopens with the new
// key and expects it to resume from the checkpoint without rewriting files
// already on the target generation.
func TestKeyRotationResume(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-rotate-resume")
	require.NoError(t, err)
	defer removeDir(dir)

	oldKey := rotationKey(t)
	newKey := rotationKey(t)
	opts := rotationTestOptions(dir, oldKey, 0) // no background compaction: stable ids
	opts.BaseTableSize = 64 << 10               // many small SSTables
	opts.MemTableSize = 256 << 10               // several memtables -> several L0 SSTs
	opts.keyRotationSSTBatch = 1                // one table per batch: wide interrupt window

	db, err := Open(opts)
	require.NoError(t, err)
	seedRotationData(t, db)
	require.NoError(t, db.Close())

	db, err = Open(opts)
	require.NoError(t, err)
	require.GreaterOrEqual(t, db.KeyRotationStatus().SSTTotal, 0)
	require.NoError(t, db.StartKeyRotation(newKey))
	targetID := db.KeyRotationStatus().TargetKeyID

	// Wait until at least one SSTable has been rewritten but some still
	// remain, then interrupt the process.
	var doneBefore map[uint64]bool
	interrupted := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		st := db.KeyRotationStatus()
		if !st.Active {
			t.Fatal("rotation finished before it could be interrupted")
		}
		if st.SSTRemaining > 0 && st.SSTTotal >= 3 {
			doneBefore, _ = sstKeyIDsSnapshot(db, targetID)
			if len(doneBefore) > 0 {
				interrupted = true
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.True(t, interrupted, "never observed mid-rotation state")

	// Tables already on target before the interruption must not be rewritten
	// after resume (their file ids must survive).
	require.NotEmpty(t, doneBefore)

	require.NoError(t, db.Close())
	_, err = os.Stat(filepath.Join(dir, KeyRotationFileName))
	require.NoError(t, err)

	// Reopen with the new key: rotation resumes automatically.
	opts.EncryptionKey = newKey
	db, err = Open(opts)
	require.NoError(t, err)
	st := db.KeyRotationStatus()
	require.True(t, st.Active)
	require.Equal(t, targetID, st.TargetKeyID)
	waitForRotation(t, db)

	assertAllFilesOnKey(t, db, targetID)
	assertRegistryOnlyHas(t, db, targetID)
	verifyRotationData(t, db, 1500, 8000)

	// Every table that had already been rewritten kept its file id.
	for id := range doneBefore {
		require.Contains(t, db.manifest.manifest.Tables, id,
			"already-rotated table %d was rewritten again after resume", id)
	}
	require.NoError(t, db.Close())
}

// TestKeyRotationWithCompactionAndGC forces flatten and value log GC while a
// rotation is running: all resulting files must land under the target
// generation.
func TestKeyRotationWithCompactionAndGC(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-rotate-gc")
	require.NoError(t, err)
	defer removeDir(dir)

	oldKey := rotationKey(t)
	newKey := rotationKey(t)
	opts := rotationTestOptions(dir, oldKey, 4)

	db, err := Open(opts)
	require.NoError(t, err)
	// Two generations of values so GC has discardable data after flatten.
	for round := 0; round < 2; round++ {
		val := make([]byte, 3000)
		for i := 0; i < 1200; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set(vlogKey(i), val)
			}))
		}
	}
	for i := 0; i < 6000; i++ {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(inlineKey(i), []byte(fmt.Sprintf("inline-%08d", i)))
		}))
	}
	require.NoError(t, db.Close())

	db, err = Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.StartKeyRotation(newKey))
	targetID := db.KeyRotationStatus().TargetKeyID

	// Flatten compacts everything while rotation rewrites; both coordinate via
	// compactStatus and every produced table uses the pinned target key.
	require.NoError(t, db.Flatten(2))

	// Drive value log GC until rotation completes.
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		for {
			if !db.KeyRotationStatus().Active {
				return
			}
			err := db.RunValueLogGC(0.2)
			if err != nil && err != ErrNoRewrite && err != ErrRejected {
				t.Errorf("unexpected GC error: %v", err)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	waitForRotation(t, db)
	<-gcDone

	assertAllFilesOnKey(t, db, targetID)
	assertRegistryOnlyHas(t, db, targetID)
	for i := 0; i < 1200; i++ {
		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get(vlogKey(i))
			require.NoError(t, err)
			return item.Value(func(v []byte) error {
				require.Len(t, v, 3000)
				return nil
			})
		}))
	}
	require.NoError(t, db.Close())
}

// TestNoRotationUnchanged verifies that without starting a rotation the
// encryption behavior is exactly as before and no checkpoint is created.
func TestNoRotationUnchanged(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-no-rotate")
	require.NoError(t, err)
	defer removeDir(dir)

	key := rotationKey(t)
	opts := rotationTestOptions(dir, key, 2)
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("foo"), make([]byte, 2048))
	}))
	require.NoError(t, db.Close())

	db, err = Open(opts)
	require.NoError(t, err)
	require.False(t, db.registry.IsPinned())
	require.False(t, db.KeyRotationStatus().Active)
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("foo"))
		require.NoError(t, err)
		return item.Value(func(v []byte) error { require.Len(t, v, 2048); return nil })
	}))
	require.NoError(t, db.Close())

	_, err = os.Stat(filepath.Join(dir, KeyRotationFileName))
	require.True(t, os.IsNotExist(err))
}

// TestRotateToPlainText rotates an encrypted database to plain text: all
// files end up unencrypted and the DB then opens without a key.
func TestRotateToPlainText(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-rotate-plain")
	require.NoError(t, err)
	defer removeDir(dir)

	key := rotationKey(t)
	opts := rotationTestOptions(dir, key, 0)
	db, err := Open(opts)
	require.NoError(t, err)
	seedRotationData(t, db)
	require.NoError(t, db.Close())

	db, err = Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.StartKeyRotation(nil))
	require.Equal(t, uint64(0), db.KeyRotationStatus().TargetKeyID)
	waitForRotation(t, db)
	assertAllFilesOnKey(t, db, 0)
	verifyRotationData(t, db, 1500, 8000)
	require.NoError(t, db.Close())

	plainOpts := rotationTestOptions(dir, nil, 0)
	db, err = Open(plainOpts)
	require.NoError(t, err)
	verifyRotationData(t, db, 1500, 8000)
	require.NoError(t, db.Close())
}

// TestStartKeyRotationGuard checks the mode guards.
func TestStartKeyRotationGuard(t *testing.T) {
	t.Run("in-memory", func(t *testing.T) {
		db, err := Open(DefaultOptions("").WithInMemory(true))
		require.NoError(t, err)
		defer db.Close()
		require.ErrorIs(t, db.StartKeyRotation(rotationKey(t)), ErrKeyRotationInMemory)
	})
	t.Run("read-only", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("read-only mode is not supported on Windows")
		}
		dir, err := os.MkdirTemp("", "badger-rotate-ro")
		require.NoError(t, err)
		defer removeDir(dir)
		opts := getTestOptions(dir)
		db, err := Open(opts)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		db, err = Open(opts.WithReadOnly(true))
		require.NoError(t, err)
		defer db.Close()
		require.ErrorIs(t, db.StartKeyRotation(rotationKey(t)), ErrKeyRotationReadOnly)
	})
	t.Run("bad key length", func(t *testing.T) {
		dir, err := os.MkdirTemp("", "badger-rotate-badkey")
		require.NoError(t, err)
		defer removeDir(dir)
		db, err := Open(getTestOptions(dir))
		require.NoError(t, err)
		defer db.Close()
		err = db.StartKeyRotation([]byte("short"))
		require.Error(t, err)
	})
}
