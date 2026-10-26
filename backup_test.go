/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
)

func TestBackupRestore1(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)

	// Write some stuff
	entries := []struct {
		key      []byte
		val      []byte
		userMeta byte
		version  uint64
	}{
		{key: []byte("answer1"), val: []byte("42"), version: 1},
		{key: []byte("answer2"), val: []byte("43"), userMeta: 1, version: 2},
	}

	err = db.Update(func(txn *Txn) error {
		e := entries[0]
		err := txn.SetEntry(NewEntry(e.key, e.val).WithMeta(e.userMeta))
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(txn *Txn) error {
		e := entries[1]
		err := txn.SetEntry(NewEntry(e.key, e.val).WithMeta(e.userMeta))
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	// Use different directory.
	dir, err = os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	bak, err := os.CreateTemp(dir, "badgerbak")
	require.NoError(t, err)
	_, err = db.Backup(bak, 0)
	require.NoError(t, err)
	require.NoError(t, bak.Close())
	require.NoError(t, db.Close())

	db, err = Open(getTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()
	bak, err = os.Open(bak.Name())
	require.NoError(t, err)
	defer bak.Close()

	require.NoError(t, db.Load(bak, 16))

	err = db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			t.Logf("Got entry: %v\n", item.Version())
			require.Equal(t, entries[count].key, item.Key())
			require.Equal(t, entries[count].val, val)
			require.Equal(t, entries[count].version, item.Version())
			require.Equal(t, entries[count].userMeta, item.UserMeta())
			count++
		}
		require.Equal(t, count, 2)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, int(db.orc.nextTs()))
}

func TestBackupRestore2(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)

	defer removeDir(tmpdir)

	s1Path := filepath.Join(tmpdir, "test1")
	s2Path := filepath.Join(tmpdir, "test2")
	s3Path := filepath.Join(tmpdir, "test3")

	db1, err := Open(getTestOptions(s1Path))
	require.NoError(t, err)

	defer db1.Close()
	key1 := []byte("key1")
	key2 := []byte("key2")
	rawValue := []byte("NotLongValue")
	N := byte(251)
	err = db1.Update(func(tx *Txn) error {
		if err := tx.SetEntry(NewEntry(key1, rawValue)); err != nil {
			return err
		}
		return tx.SetEntry(NewEntry(key2, rawValue))
	})
	require.NoError(t, err)

	for i := byte(1); i < N; i++ {
		err = db1.Update(func(tx *Txn) error {
			if err := tx.SetEntry(NewEntry(append(key1, i), rawValue)); err != nil {
				return err
			}
			return tx.SetEntry(NewEntry(append(key2, i), rawValue))
		})
		require.NoError(t, err)

	}
	var backup bytes.Buffer
	_, err = db1.Backup(&backup, 0)
	require.NoError(t, err)

	fmt.Println("backup1 length:", backup.Len())

	db2, err := Open(getTestOptions(s2Path))
	require.NoError(t, err)

	defer db2.Close()
	err = db2.Load(&backup, 16)
	require.NoError(t, err)

	// Check nextTs is correctly set.
	require.Equal(t, db1.orc.nextTs(), db2.orc.nextTs())

	for i := byte(1); i < N; i++ {
		err = db2.View(func(tx *Txn) error {
			k := append(key1, i)
			item, err := tx.Get(k)
			if err != nil {
				if err == ErrKeyNotFound {
					return fmt.Errorf("Key %q has been not found, but was set\n", k)
				}
				return err
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(v, rawValue) {
				return fmt.Errorf("Values not match, got %v, expected %v", v, rawValue)
			}
			return nil
		})
		require.NoError(t, err)

	}

	for i := byte(1); i < N; i++ {
		err = db2.Update(func(tx *Txn) error {
			if err := tx.SetEntry(NewEntry(append(key1, i), rawValue)); err != nil {
				return err
			}
			return tx.SetEntry(NewEntry(append(key2, i), rawValue))
		})
		require.NoError(t, err)

	}

	backup.Reset()
	_, err = db2.Backup(&backup, 0)
	require.NoError(t, err)

	fmt.Println("backup2 length:", backup.Len())
	db3, err := Open(getTestOptions(s3Path))
	require.NoError(t, err)

	defer db3.Close()

	err = db3.Load(&backup, 16)
	require.NoError(t, err)

	// Check nextTs is correctly set.
	require.Equal(t, db2.orc.nextTs(), db3.orc.nextTs())

	for i := byte(1); i < N; i++ {
		err = db3.View(func(tx *Txn) error {
			k := append(key1, i)
			item, err := tx.Get(k)
			if err != nil {
				if err == ErrKeyNotFound {
					return fmt.Errorf("Key %q has been not found, but was set\n", k)
				}
				return err
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(v, rawValue) {
				return fmt.Errorf("Values not match, got %v, expected %v", v, rawValue)
			}
			return nil
		})
		require.NoError(t, err)

	}

}

var randSrc = rand.NewSource(time.Now().UnixNano())

func createEntries(n int) []*pb.KV {
	entries := make([]*pb.KV, n)
	for i := 0; i < n; i++ {
		entries[i] = &pb.KV{
			Key:      []byte(fmt.Sprint("key", i)),
			Value:    []byte{1},
			UserMeta: []byte{0},
			Meta:     []byte{0},
		}
	}
	return entries
}

func populateEntries(db *DB, entries []*pb.KV) error {
	return db.Update(func(txn *Txn) error {
		var err error
		for i, e := range entries {
			if err = txn.SetEntry(NewEntry(e.Key, e.Value)); err != nil {
				return err
			}
			entries[i].Version = 1
		}
		return nil
	})
}

func TestBackup(t *testing.T) {
	test := func(t *testing.T, db *DB) {
		var bb bytes.Buffer
		N := 1000
		entries := createEntries(N)
		require.NoError(t, populateEntries(db, entries))

		_, err := db.Backup(&bb, 0)
		require.NoError(t, err)

		err = db.View(func(txn *Txn) error {
			opts := DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()
			var count int
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				idx, err := strconv.Atoi(string(item.Key())[3:])
				if err != nil {
					return err
				}
				if idx > N || !bytes.Equal(entries[idx].Key, item.Key()) {
					return fmt.Errorf("%s: %s", string(item.Key()), ErrKeyNotFound)
				}
				count++
			}
			if N != count {
				return fmt.Errorf("wrong number of items: %d expected, %d actual", N, count)
			}
			return nil
		})
		require.NoError(t, err)
	}
	t.Run("disk mode", func(t *testing.T) {
		tmpdir, err := os.MkdirTemp("", "badger-test")
		require.NoError(t, err)

		defer removeDir(tmpdir)
		opt := DefaultOptions(filepath.Join(tmpdir, "backup0"))
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			test(t, db)
		})
	})
	t.Run("InMemory mode", func(t *testing.T) {
		opt := DefaultOptions("")
		opt.InMemory = true
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			test(t, db)
		})
	})
}

func TestBackupRestore3(t *testing.T) {
	var bb bytes.Buffer
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)

	defer removeDir(tmpdir)

	N := 1000
	entries := createEntries(N)

	var db1NextTs uint64
	// backup
	{
		db1, err := Open(DefaultOptions(filepath.Join(tmpdir, "backup1")))
		require.NoError(t, err)

		defer db1.Close()
		require.NoError(t, populateEntries(db1, entries))

		_, err = db1.Backup(&bb, 0)
		require.NoError(t, err)

		db1NextTs = db1.orc.nextTs()
		require.NoError(t, db1.Close())
	}
	require.True(t, len(entries) == N)
	require.True(t, bb.Len() > 0)

	// restore
	db2, err := Open(DefaultOptions(filepath.Join(tmpdir, "restore1")))
	require.NoError(t, err)

	defer db2.Close()
	require.NotEqual(t, db1NextTs, db2.orc.nextTs())
	require.NoError(t, db2.Load(&bb, 16))
	require.Equal(t, db1NextTs, db2.orc.nextTs())

	// verify
	err = db2.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			idx, err := strconv.Atoi(string(item.Key())[3:])
			if err != nil {
				return err
			}
			if idx > N || !bytes.Equal(entries[idx].Key, item.Key()) {
				return fmt.Errorf("%s: %s", string(item.Key()), ErrKeyNotFound)
			}
			count++
		}
		if N != count {
			return fmt.Errorf("wrong number of items: %d expected, %d actual", N, count)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestBackupLoadIncremental(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)

	defer removeDir(tmpdir)

	N := 100
	entries := createEntries(N)
	updates := make(map[int]byte)
	var bb bytes.Buffer

	var db1NextTs uint64
	// backup
	{
		db1, err := Open(DefaultOptions(filepath.Join(tmpdir, "backup2")))
		require.NoError(t, err)

		defer db1.Close()

		require.NoError(t, populateEntries(db1, entries))
		since, err := db1.Backup(&bb, 0)
		require.NoError(t, err)

		ints := rand.New(randSrc).Perm(N)

		// pick 10 items to mark as deleted.
		err = db1.Update(func(txn *Txn) error {
			for _, i := range ints[:10] {
				if err := txn.Delete(entries[i].Key); err != nil {
					return err
				}
				updates[i] = bitDelete
			}
			return nil
		})
		require.NoError(t, err)
		since, err = db1.Backup(&bb, since)
		require.NoError(t, err)

		// pick 5 items to mark as expired.
		err = db1.Update(func(txn *Txn) error {
			for _, i := range (ints)[10:15] {
				entry := NewEntry(entries[i].Key, entries[i].Value).WithTTL(-time.Hour)
				if err := txn.SetEntry(entry); err != nil {
					return err
				}
				updates[i] = bitDelete // expired
			}
			return nil
		})
		require.NoError(t, err)
		since, err = db1.Backup(&bb, since)
		require.NoError(t, err)

		// pick 5 items to mark as discard.
		err = db1.Update(func(txn *Txn) error {
			for _, i := range ints[15:20] {
				entry := NewEntry(entries[i].Key, entries[i].Value).WithDiscard()
				if err := txn.SetEntry(entry); err != nil {
					return err
				}
				updates[i] = bitDiscardEarlierVersions
			}
			return nil
		})
		require.NoError(t, err)
		_, err = db1.Backup(&bb, since)
		require.NoError(t, err)

		db1NextTs = db1.orc.nextTs()

		require.NoError(t, db1.Close())
	}
	require.True(t, len(entries) == N)
	require.True(t, bb.Len() > 0)

	// restore
	db2, err := Open(getTestOptions(filepath.Join(tmpdir, "restore2")))
	require.NoError(t, err)

	defer db2.Close()

	require.NotEqual(t, db1NextTs, db2.orc.nextTs())
	require.NoError(t, db2.Load(&bb, 16))
	require.Equal(t, db1NextTs, db2.orc.nextTs())

	// verify
	actual := make(map[int]byte)
	err = db2.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			idx, err := strconv.Atoi(string(item.Key())[3:])
			if err != nil {
				return err
			}
			if item.IsDeletedOrExpired() {
				_, ok := updates[idx]
				if !ok {
					return fmt.Errorf("%s: not expected to be updated but it is",
						string(item.Key()))
				}
				actual[idx] = item.meta
				count++
				continue
			}
		}
		if len(updates) != count {
			return fmt.Errorf("mismatched updated items: %d expected, %d actual",
				len(updates), count)
		}
		return nil
	})
	require.NoError(t, err, "%v %v", updates, actual)
}

func TestBackupBitClear(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	opt.ValueThreshold = 10 // This is important
	db, err := Open(opt)
	require.NoError(t, err)

	key := []byte("foo")
	val := []byte(fmt.Sprintf("%0100d", 1))
	require.Greater(t, int64(len(val)), db.valueThreshold())

	err = db.Update(func(txn *Txn) error {
		e := NewEntry(key, val)
		// Value > valueTheshold so bitValuePointer will be set.
		return txn.SetEntry(e)
	})
	require.NoError(t, err)

	// Use different directory.
	dir, err = os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	bak, err := os.CreateTemp(dir, "badgerbak")
	require.NoError(t, err)
	_, err = db.Backup(bak, 0)
	require.NoError(t, err)
	require.NoError(t, bak.Close())

	oldValue := db.orc.nextTs()
	require.NoError(t, db.Close())

	opt = getTestOptions(dir)
	opt.ValueThreshold = 200 // This is important.
	db, err = Open(opt)
	require.NoError(t, err)
	defer db.Close()

	bak, err = os.Open(bak.Name())
	require.NoError(t, err)
	defer bak.Close()

	require.NoError(t, db.Load(bak, 16))
	// Ensure nextTs is still the same.
	require.Equal(t, oldValue, db.orc.nextTs())

	require.NoError(t, db.View(func(txn *Txn) error {
		e, err := txn.Get(key)
		require.NoError(t, err)
		v, err := e.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, val, v)
		return nil
	}))
}

// --- self-describing envelope tests -----------------------------------------

// backupTestOptions keeps all versions around so dumpDBVersions can compare
// the complete version history.
func backupTestOptions(dir string) Options {
	return DefaultOptions(dir).WithNumVersionsToKeep(math.MaxInt32)
}

// backupTestTempDir creates a temp directory and removes it on cleanup with a
// small retry loop: on Windows a just-closed value log can briefly retain its
// file handle after DB.Close returns.
func backupTestTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "badger-envtest-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if err := os.RemoveAll(dir); err == nil {
				return
			} else if time.Now().After(deadline) {
				return // best effort; handle lag must not fail the test
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
	return dir
}

// dumpDBVersions returns key -> version -> "<deleted>" or value string, as
// visible through an AllVersions iterator.
func dumpDBVersions(t *testing.T, db *DB) map[string]map[uint64]string {
	t.Helper()
	out := make(map[string]map[uint64]string)
	err := db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.Key())
			if out[k] == nil {
				out[k] = make(map[uint64]string)
			}
			if item.IsDeletedOrExpired() {
				out[k][item.Version()] = "<deleted>"
				continue
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			out[k][item.Version()] = string(v)
		}
		return nil
	})
	require.NoError(t, err)
	return out
}

func countDBVersions(t *testing.T, db *DB) int {
	t.Helper()
	dump := dumpDBVersions(t, db)
	var n int
	for _, versions := range dump {
		n += len(versions)
	}
	return n
}

// dumpEffective returns the visible state of the DB: key -> latest value, with
// deleted/expired keys omitted. This is what a backup is expected to preserve
// relative to its source (old versions shadowed by a tombstone are not part of
// the backup contract).
func dumpEffective(t *testing.T, db *DB) map[string]string {
	t.Helper()
	out := make(map[string]string)
	err := db.View(func(txn *Txn) error {
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			out[string(item.Key())] = string(v)
		}
		return nil
	})
	require.NoError(t, err)
	return out
}

// populateBackupDB writes a mix of multi-version keys, big values and a few
// tombstones so that backup/restore exercises every KV shape.
func populateBackupDB(t *testing.T, db *DB, n int) {
	t.Helper()
	big := bytes.Repeat([]byte("z"), 2<<10)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		val := []byte(fmt.Sprintf("val%d-v1", i))
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(key, val)
		}))
	}
	// Second and third versions for a subset of keys.
	for i := 0; i < n; i += 7 {
		key := []byte(fmt.Sprintf("key%05d", i))
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(key, []byte(fmt.Sprintf("val%d-v2", i)))
		}))
	}
	for i := 0; i < n; i += 29 {
		key := []byte(fmt.Sprintf("key%05d", i))
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(key, big)
		}))
	}
	// A handful of tombstones.
	for i := 3; i < n; i += 53 {
		key := []byte(fmt.Sprintf("key%05d", i))
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Delete(key)
		}))
	}
}

func mustBackupToFile(t *testing.T, db *DB, since uint64) *os.File {
	t.Helper()
	f, err := os.CreateTemp("", "badger-envelope-*.bak")
	require.NoError(t, err)
	_, err = db.Backup(f, since)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// TestEnvelopeBackupFormat verifies the self-description: header with format
// version and range, per-chunk checksums, and a footer index covering every
// chunk and the time range.
func TestEnvelopeBackupFormat(t *testing.T) {
	tmp := backupTestTempDir(t)
	db, err := Open(backupTestOptions(filepath.Join(tmp, "src")))
	require.NoError(t, err)
	populateBackupDB(t, db, 200)

	var buf bytes.Buffer
	maxVersion, err := db.Backup(&buf, 0)
	require.NoError(t, err)
	require.Positive(t, maxVersion)

	f, cleanup, err := spoolBackup(&buf)
	require.NoError(t, err)
	defer cleanup()
	size := spooledSize(f)
	require.Positive(t, size)

	hdr, hdrEnd, err := readEnvelopeHeader(f, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(0), hdr.sinceTs)
	require.Empty(t, hdr.prefix)
	require.Equal(t, backupAlgoCRC32C, hdr.algo)
	require.GreaterOrEqual(t, hdr.numSlots(), uint32(1))
	require.Positive(t, hdr.createdAt)

	segs, err := validateEnvelope(f, size, 0)
	require.NoError(t, err)
	require.Len(t, segs, 1)
	footer := segs[0].footer
	require.Equal(t, maxVersion, footer.maxVersion)
	require.Equal(t, hdr.sinceTs, footer.sinceTs)
	require.Equal(t, hdr.numSlots(), footer.numSlots)

	// Every slot ends in exactly one final chunk; no index entry is empty.
	finals := make(map[uint32]int)
	require.NotEmpty(t, footer.chunks)
	for _, ref := range footer.chunks {
		require.GreaterOrEqual(t, ref.offset, hdrEnd)
		require.GreaterOrEqual(t, ref.length, int64(backupChunkFrameMin))
		require.NotZero(t, ref.length)
		if ref.final() {
			finals[ref.slot]++
		}
	}
	require.Len(t, finals, int(hdr.numSlots()))
	for slot, n := range finals {
		require.Equal(t, 1, n, "slot %d should have exactly one final chunk", slot)
	}
	require.NoError(t, db.Close())
}

// TestEnvelopeMultipleSegments covers incremental exports appended into the
// same artifact: each invocation adds its own envelope segment.
func TestEnvelopeMultipleSegments(t *testing.T) {
	tmp := backupTestTempDir(t)
	db, err := Open(backupTestOptions(filepath.Join(tmp, "src")))
	require.NoError(t, err)

	var buf bytes.Buffer
	since, err := db.Backup(&buf, 0)
	require.NoError(t, err)
	require.NoError(t, populateAndBackupMore(t, db, &buf, &since))

	f, cleanup, err := spoolBackup(&buf)
	require.NoError(t, err)
	defer cleanup()
	segs, err := validateEnvelope(f, spooledSize(f), 0)
	require.NoError(t, err)
	require.Len(t, segs, 2)
	require.Equal(t, since, segs[1].footer.maxVersion)
	require.NoError(t, db.Close())
}

func populateAndBackupMore(t *testing.T, db *DB, buf *bytes.Buffer, since *uint64) error {
	t.Helper()
	if err := db.Update(func(txn *Txn) error {
		return txn.Set([]byte("incremental-key"), []byte("inc"))
	}); err != nil {
		return err
	}
	next, err := db.Backup(buf, *since)
	if err != nil {
		return err
	}
	*since = next
	return nil
}

// TestEnvelopeLoadCorruptionBeforeWrite proves that truncation and checksum
// failures are located and reported before a single key reaches the database.
func TestEnvelopeLoadCorruptionBeforeWrite(t *testing.T) {
	tmp := backupTestTempDir(t)
	src, err := Open(backupTestOptions(filepath.Join(tmp, "src")))
	require.NoError(t, err)
	populateBackupDB(t, src, 200)

	good := mustBackupToFile(t, src, 0)
	goodSize := spooledSize(good)
	segs, err := validateEnvelope(good, goodSize, 0)
	require.NoError(t, err)

	// Locate the first non-empty chunk so corruption lands inside payload.
	var (
		targetSeg int
		targetOrd int
		targetRef chunkRef
	)
	for i, seg := range segs {
		for j, ref := range seg.footer.chunks {
			if ref.payloadLn > 0 {
				targetSeg, targetOrd, targetRef = i, j, ref
				break
			}
		}
		if targetRef.length > 0 {
			break
		}
	}
	require.NotZero(t, targetRef.length)

	t.Run("bad chunk checksum points at chunk", func(t *testing.T) {
		var ce *BackupCorruptionError
		corruptAndLoadCapture(t, good, &ce, func(f *os.File) {
			pos := segs[targetSeg].start + targetRef.offset + backupChunkHeadSize + 2
			var b [1]byte
			_, err := f.ReadAt(b[:], pos)
			require.NoError(t, err)
			b[0] ^= 0xff
			_, err = f.WriteAt(b[:], pos)
			require.NoError(t, err)
		})
		require.Equal(t, targetSeg, ce.Segment)
		require.Equal(t, targetOrd, ce.Chunk)
		require.Equal(t, "chunk", ce.Part)
		require.Equal(t, targetRef.offset, ce.Offset)
		require.True(t, errors.Is(ce, y.ErrChecksumMismatch))
	})

	t.Run("truncated inside chunk", func(t *testing.T) {
		var ce *BackupCorruptionError
		corruptAndLoadCapture(t, good, &ce, func(f *os.File) {
			require.NoError(t, f.Truncate(segs[targetSeg].start+targetRef.offset+10))
		})
		require.Equal(t, targetSeg, ce.Segment)
		require.Equal(t, targetOrd, ce.Chunk)
		require.Equal(t, "chunk", ce.Part)
		require.True(t, errors.Is(ce, ErrBackupTruncated))
	})

	t.Run("truncated inside footer", func(t *testing.T) {
		var ce *BackupCorruptionError
		corruptAndLoadCapture(t, good, &ce, func(f *os.File) {
			require.NoError(t, f.Truncate(goodSize-5))
		})
		require.True(t, errors.Is(ce, ErrBackupTruncated), "got %v", ce)
		require.Equal(t, "footer", ce.Part)
	})

	t.Run("corrupt footer checksum", func(t *testing.T) {
		var ce *BackupCorruptionError
		corruptAndLoadCapture(t, good, &ce, func(f *os.File) {
			var b [1]byte
			pos := goodSize - 2
			_, err := f.ReadAt(b[:], pos)
			require.NoError(t, err)
			b[0] ^= 0xff
			_, err = f.WriteAt(b[:], pos)
			require.NoError(t, err)
		})
		require.Equal(t, "footer", ce.Part)
		require.True(t, errors.Is(ce, y.ErrChecksumMismatch))
	})

	require.NoError(t, src.Close())
}

func corruptAndLoadCapture(
	t *testing.T, src *os.File, ceOut **BackupCorruptionError, mutate func(f *os.File),
) {
	t.Helper()
	f, err := os.CreateTemp("", "badger-corrupt-cap-*.bak")
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = src.Seek(0, 0)
	require.NoError(t, err)
	_, err = io.Copy(f, src)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	mutate(f)

	db, err := Open(backupTestOptions(filepath.Join(backupTestTempDir(t), "dst")))
	require.NoError(t, err)
	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	loadErr := db.Load(f, 16)
	require.Error(t, loadErr)
	var ce *BackupCorruptionError
	require.True(t, errors.As(loadErr, &ce), "got %v", loadErr)
	require.Zero(t, countDBVersions(t, db))
	*ceOut = ce
	require.NoError(t, db.Close())
}

// TestLegacyBackupStillLoads proves old, envelope-less artifacts (plain
// length-prefixed KVList frames) keep importing without conversion.
func TestLegacyBackupStillLoads(t *testing.T) {
	var buf bytes.Buffer
	list := &pb.KVList{Kv: []*pb.KV{
		{Key: []byte("legacy1"), Value: []byte("v1"), Version: 1, UserMeta: []byte{0}, Meta: []byte{0}},
		{Key: []byte("legacy2"), Value: []byte("v2"), Version: 2, UserMeta: []byte{0}, Meta: []byte{0}},
	}}
	require.NoError(t, writeTo(list, &buf))

	db, err := Open(backupTestOptions(filepath.Join(backupTestTempDir(t), "dst")))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Load(&buf, 16))
	require.Equal(t, 2, countDBVersions(t, db))
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("legacy1"))
		require.NoError(t, err)
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), v)
		return nil
	}))
}

// TestLegacyFollowedByEnvelopeSegment loads a mixed artifact: legacy frames
// followed by an appended envelope segment.
func TestLegacyFollowedByEnvelopeSegment(t *testing.T) {
	tmp := backupTestTempDir(t)
	db, err := Open(backupTestOptions(filepath.Join(tmp, "src")))
	require.NoError(t, err)
	populateBackupDB(t, db, 50)

	var buf bytes.Buffer
	require.NoError(t, writeTo(&pb.KVList{Kv: []*pb.KV{{
		Key: []byte("old-key"), Value: []byte("old"), Version: 1,
		UserMeta: []byte{0}, Meta: []byte{0},
	}}}, &buf))
	_, err = db.Backup(&buf, 0)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	dst, err := Open(backupTestOptions(filepath.Join(backupTestTempDir(t), "dst")))
	require.NoError(t, err)
	defer dst.Close()
	require.NoError(t, dst.Load(&buf, 16))
	require.NoError(t, dst.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("old-key"))
		require.NoError(t, err)
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("old"), v)
		return nil
	}))
}

// TestRepeatedLoadNoDuplicateVersions imports the same artifact twice; no
// second copy of any version may appear.
func TestRepeatedLoadNoDuplicateVersions(t *testing.T) {
	tmp := backupTestTempDir(t)
	src, err := Open(backupTestOptions(filepath.Join(tmp, "src")))
	require.NoError(t, err)
	populateBackupDB(t, src, 150)
	f := mustBackupToFile(t, src, 0)
	require.NoError(t, src.Close())

	dst, err := Open(backupTestOptions(filepath.Join(tmp, "dst")))
	require.NoError(t, err)
	defer dst.Close()

	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	require.NoError(t, dst.Load(f, 16))
	firstCount := countDBVersions(t, dst)
	firstTs := dst.orc.nextTxnTs
	firstDump := dumpDBVersions(t, dst)

	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	require.NoError(t, dst.Load(f, 16))
	secondCount := countDBVersions(t, dst)
	secondTs := dst.orc.nextTxnTs
	secondDump := dumpDBVersions(t, dst)

	require.Equal(t, firstCount, secondCount)
	require.Equal(t, firstTs, secondTs)
	require.Equal(t, firstDump, secondDump)
}

// limitWriter stops accepting bytes after limit, simulating a crash or a full
// disk in the middle of the export.
type limitWriter struct {
	f       *os.File
	limit   int64
	written int64
}

var errSimulatedInterruption = errors.New("simulated backup interruption")

func (w *limitWriter) Write(p []byte) (int, error) {
	if w.written >= w.limit {
		return 0, errSimulatedInterruption
	}
	avail := w.limit - w.written
	if int64(len(p)) <= avail {
		n, err := w.f.Write(p)
		w.written += int64(n)
		return n, err
	}
	n, err := w.f.Write(p[:avail])
	w.written += int64(n)
	if err == nil {
		err = errSimulatedInterruption
	}
	return n, err
}

// TestBackupResumeChunkGranularity interrupts exports at many points and
// resumes them; the final database state must match a one-shot restore.
func TestBackupResumeChunkGranularity(t *testing.T) {
	tmp := backupTestTempDir(t)
	src, err := Open(backupTestOptions(filepath.Join(tmp, "src")))
	require.NoError(t, err)
	populateBackupDB(t, src, 300)
	wantEffective := dumpEffective(t, src)

	// Reference restore from a one-shot export.
	refDB, err := Open(backupTestOptions(filepath.Join(tmp, "ref")))
	require.NoError(t, err)
	refFile := mustBackupToFile(t, src, 0)
	_, err = refFile.Seek(0, 0)
	require.NoError(t, err)
	require.NoError(t, refDB.Load(refFile, 16))
	refDump := dumpDBVersions(t, refDB)
	require.Equal(t, wantEffective, dumpEffective(t, refDB))
	require.NoError(t, refDB.Close())

	// Measure a full export length to pick interruption points.
	var measure bytes.Buffer
	fullLen, err := src.Backup(&measure, 0)
	require.NoError(t, err)
	require.Positive(t, fullLen)
	total := int64(measure.Len())

	limits := []int64{
		1,
		total / 10,
		total / 4,
		total / 2,
		total * 3 / 4,
		total * 9 / 10,
		total - 4096,
		total - 128,
		total - 8,
		total - 1,
	}
	for _, limit := range limits {
		limit := limit
		t.Run(fmt.Sprintf("interrupt@%d", limit), func(t *testing.T) {
			part, err := os.CreateTemp("", "badger-partial-*.bak")
			require.NoError(t, err)
			defer func() { _ = part.Close() }()

			lw := &limitWriter{f: part, limit: limit}
			_, berr := src.Backup(lw, 0)
			require.Error(t, berr)
			// Orchestration may surface the canceled producer context instead
			// of the original write error when the writer fails early.
			require.True(t,
				errors.Is(berr, errSimulatedInterruption) || errors.Is(berr, context.Canceled),
				"unexpected backup error: %v", berr)
			require.NoError(t, part.Sync())

			// The raw partial artifact must not validate yet.
			fi, err := part.Stat()
			require.NoError(t, err)
			if fi.Size() > 0 {
				_, verr := validateEnvelope(part, fi.Size(), 0)
				require.Error(t, verr)
			}

			// Resume until completion.
			maxVersion, rerr := src.BackupResume(part, 0)
			require.NoError(t, rerr)
			require.Equal(t, fullLen, maxVersion)
			require.NoError(t, part.Sync())

			fi, err = part.Stat()
			require.NoError(t, err)
			segs, verr := validateEnvelope(part, fi.Size(), 0)
			require.NoError(t, verr)
			require.Len(t, segs, 1)
			require.Equal(t, fullLen, segs[0].footer.maxVersion)

			// Resuming a finished artifact is a no-op.
			noopVersion, nerr := src.BackupResume(part, 0)
			require.NoError(t, nerr)
			require.Equal(t, fullLen, noopVersion)

			dst, err := Open(backupTestOptions(filepath.Join(backupTestTempDir(t), "dst")))
			require.NoError(t, err)
			_, err = part.Seek(0, 0)
			require.NoError(t, err)
			require.NoError(t, dst.Load(part, 16))
			// Resumed restore must be identical to the one-shot restore, both
			// in full version history and in visible state.
			require.Equal(t, refDump, dumpDBVersions(t, dst))
			require.Equal(t, wantEffective, dumpEffective(t, dst))
			require.NoError(t, dst.Close())
		})
	}
	require.NoError(t, src.Close())
}

// TestBackupResumeRepeatedInterruptions resumes across several successive
// interruptions (including an interrupted continuation); completed chunks
// survive every restart.
func TestBackupResumeRepeatedInterruptions(t *testing.T) {
	tmp := backupTestTempDir(t)
	src, err := Open(backupTestOptions(filepath.Join(tmp, "src")))
	require.NoError(t, err)
	defer src.Close()
	populateBackupDB(t, src, 250)
	wantEffective := dumpEffective(t, src)
	fullLen, err := src.Backup(&bytes.Buffer{}, 0)
	require.NoError(t, err)

	part, err := os.CreateTemp("", "badger-multi-*.bak")
	require.NoError(t, err)
	defer func() { _ = part.Close() }()

	// First export dies halfway through.
	var measure bytes.Buffer
	_, err = src.Backup(&measure, 0)
	require.NoError(t, err)
	firstCut := int64(measure.Len()) / 3
	lw := &limitWriter{f: part, limit: firstCut}
	_, berr := src.Backup(lw, 0)
	require.Error(t, berr)
	require.True(t,
		errors.Is(berr, errSimulatedInterruption) || errors.Is(berr, context.Canceled),
		"unexpected backup error: %v", berr)
	require.NoError(t, part.Sync())

	// Two resume attempts are themselves interrupted, each allowing only a
	// small number of additional bytes through.
	for _, extra := range []int64{512, 2048} {
		cur := partSize(t, part)
		st := src.NewStream()
		st.resumeWriteWrap = func(base io.Writer) io.Writer {
			return &limitWriter{f: part, written: cur, limit: cur + extra}
		}
		_, rerr := st.BackupResume(part, 0)
		require.Error(t, rerr)
		require.True(t,
			errors.Is(rerr, errSimulatedInterruption) || errors.Is(rerr, context.Canceled),
			"unexpected resume error: %v", rerr)
		require.NoError(t, part.Sync())
		_, verr := validateEnvelope(part, partSize(t, part), 0)
		require.Error(t, verr, "partial artifact must not validate yet")
	}

	// Final resume with no interruption completes the segment.
	maxVersion, err := src.BackupResume(part, 0)
	require.NoError(t, err)
	require.Equal(t, fullLen, maxVersion)

	dst, err := Open(backupTestOptions(filepath.Join(backupTestTempDir(t), "dst")))
	require.NoError(t, err)
	defer dst.Close()
	_, err = part.Seek(0, 0)
	require.NoError(t, err)
	require.NoError(t, dst.Load(part, 16))
	require.Equal(t, wantEffective, dumpEffective(t, dst))
}

func partSize(t *testing.T, f *os.File) int64 {
	t.Helper()
	fi, err := f.Stat()
	require.NoError(t, err)
	return fi.Size()
}

// TestBackupResumeRejectsLegacyFile ensures resume never overwrites a file it
// cannot interpret as an envelope.
func TestBackupResumeRejectsLegacyFile(t *testing.T) {
	tmp := backupTestTempDir(t)
	src, err := Open(backupTestOptions(filepath.Join(tmp, "src")))
	require.NoError(t, err)
	defer src.Close()
	require.NoError(t, src.Update(func(txn *Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	}))

	f, err := os.CreateTemp("", "badger-legacy-resume-*.bak")
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	require.NoError(t, writeTo(&pb.KVList{Kv: []*pb.KV{{
		Key: []byte("k"), Value: []byte("v"), Version: 1,
		UserMeta: []byte{0}, Meta: []byte{0},
	}}}, f))
	fi, err := f.Stat()
	require.NoError(t, err)
	originalSize := fi.Size()

	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	_, err = src.BackupResume(f, 0)
	require.Error(t, err)

	// Legacy file content must be untouched.
	fi, err = f.Stat()
	require.NoError(t, err)
	require.Equal(t, originalSize, fi.Size())
}

// TestRangeBackupEnvelope covers the range-export consumer: the key prefix is
// recorded in the header and resume works for range backups too.
func TestRangeBackupEnvelope(t *testing.T) {
	tmp := backupTestTempDir(t)
	src, err := Open(backupTestOptions(filepath.Join(tmp, "src")))
	require.NoError(t, err)
	populateBackupDB(t, src, 200)
	require.NoError(t, src.Update(func(txn *Txn) error {
		return txn.Set([]byte("other-prefix-key"), []byte("x"))
	}))

	var buf bytes.Buffer
	stream := src.NewStream()
	stream.Prefix = []byte("key")
	maxVersion, err := stream.Backup(&buf, 0)
	require.NoError(t, err)
	require.Positive(t, maxVersion)

	f, cleanup, err := spoolBackup(&buf)
	require.NoError(t, err)
	defer cleanup()
	hdr, _, err := readEnvelopeHeader(f, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("key"), hdr.prefix)
	segs, err := validateEnvelope(f, spooledSize(f), 0)
	require.NoError(t, err)
	require.Len(t, segs, 1)

	dst, err := Open(backupTestOptions(filepath.Join(backupTestTempDir(t), "dst")))
	require.NoError(t, err)
	defer dst.Close()
	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	require.NoError(t, dst.Load(f, 16))
	got := dumpDBVersions(t, dst)
	_, hasOther := got["other-prefix-key"]
	require.False(t, hasOther)
	for k := range got {
		require.True(t, bytes.HasPrefix([]byte(k), []byte("key")))
	}
	require.NoError(t, src.Close())
}

// TestRangeBackupResume exercises resume on a prefix-scoped export.
func TestRangeBackupResume(t *testing.T) {
	tmp := backupTestTempDir(t)
	src, err := Open(backupTestOptions(filepath.Join(tmp, "src")))
	require.NoError(t, err)
	populateBackupDB(t, src, 200)

	var measure bytes.Buffer
	ms := src.NewStream()
	ms.Prefix = []byte("key")
	fullLen, err := ms.Backup(&measure, 0)
	require.NoError(t, err)
	total := int64(measure.Len())

	part, err := os.CreateTemp("", "badger-range-partial-*.bak")
	require.NoError(t, err)
	defer func() { _ = part.Close() }()

	s1 := src.NewStream()
	s1.Prefix = []byte("key")
	lw := &limitWriter{f: part, limit: total / 2}
	_, berr := s1.Backup(lw, 0)
	require.Error(t, berr)
	require.True(t,
		errors.Is(berr, errSimulatedInterruption) || errors.Is(berr, context.Canceled),
		"unexpected backup error: %v", berr)
	require.NoError(t, part.Sync())

	s2 := src.NewStream()
	s2.Prefix = []byte("key")
	resumed, err := s2.BackupResume(part, 0)
	require.NoError(t, err)
	require.Equal(t, fullLen, resumed)

	dst, err := Open(backupTestOptions(filepath.Join(backupTestTempDir(t), "dst")))
	require.NoError(t, err)
	defer dst.Close()
	_, err = part.Seek(0, 0)
	require.NoError(t, err)
	require.NoError(t, dst.Load(part, 16))

	// Compare against a one-shot range restore.
	ref, err := Open(backupTestOptions(filepath.Join(backupTestTempDir(t), "ref")))
	require.NoError(t, err)
	defer ref.Close()
	var refBuf bytes.Buffer
	s3 := src.NewStream()
	s3.Prefix = []byte("key")
	_, err = s3.Backup(&refBuf, 0)
	require.NoError(t, err)
	require.NoError(t, ref.Load(&refBuf, 16))
	require.Equal(t, dumpDBVersions(t, ref), dumpDBVersions(t, dst))
	require.NoError(t, src.Close())
}
