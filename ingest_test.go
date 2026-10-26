/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

func newTestJournal(t *testing.T, opt Options, mode IngestMode, level int,
	declared IngestKeyRange) (*ingestJournal, string) {
	t.Helper()
	registry, err := OpenKeyRegistry(KeyRegistryOptions{
		Dir:                           opt.Dir,
		EncryptionKey:                 opt.EncryptionKey,
		EncryptionKeyRotationDuration: opt.EncryptionKeyRotationDuration,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = registry.Close() })

	id := newIngestSessionID()
	j, err := createIngestJournal(opt, registry, id, mode, level, declared)
	require.NoError(t, err)
	return j, id
}

// TR-1.1: frames roundtrip and a torn tail is tolerated.
func TestIngestJournalRoundTrip(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions(dir)
	declared := IngestKeyRange{Left: []byte("ka"), Right: []byte("kz")}
	j, id := newTestJournal(t, opt, IngestIncremental, 5, declared)

	sm1 := y.KeyWithTs([]byte("ka"), 10)
	bg1 := y.KeyWithTs([]byte("km"), 9)
	require.NoError(t, j.appendStage(stagedTable{ID: 7, Smallest: sm1, Biggest: bg1}))
	sm2 := y.KeyWithTs([]byte("kn"), 8)
	bg2 := y.KeyWithTs([]byte("kz"), 1)
	require.NoError(t, j.appendStage(stagedTable{ID: 9, Smallest: sm2, Biggest: bg2}))
	require.NoError(t, j.appendCommit([]uint64{20, 21}, []uint64{7, 9}))
	require.NoError(t, j.close())

	jf, err := readIngestJournalFile(ingestJournalPath(dir, id),
		ingestFilePrefix+id, mustRegistry(t, opt))
	require.NoError(t, err)
	require.Equal(t, id, jf.id)
	require.False(t, jf.done)
	require.Equal(t, IngestCommitting, jf.info.Phase)
	require.Equal(t, IngestIncremental, jf.info.Mode)
	require.Equal(t, 5, jf.info.TargetLevel)
	require.Equal(t, declared, jf.info.Range)
	require.Equal(t, []uint64{7, 9}, jf.info.StagedTableIDs)
	require.Equal(t, []uint64{20, 21}, jf.info.FinalTableIDs)
	require.Equal(t, []uint64{7, 9}, jf.info.DroppedTableIDs)
	require.WithinDuration(t, time.Now(), jf.info.StartedAt, 5*time.Second)
}

func TestIngestJournalObservedRange(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions(dir)
	// No declared range: range is observed from staged tables.
	j, id := newTestJournal(t, opt, IngestFull, 6, IngestKeyRange{})
	require.NoError(t, j.appendStage(stagedTable{
		ID: 3, Smallest: y.KeyWithTs([]byte("k2"), 5), Biggest: y.KeyWithTs([]byte("k9"), 4)}))
	require.NoError(t, j.appendStage(stagedTable{
		ID: 4, Smallest: y.KeyWithTs([]byte("k1"), 9), Biggest: y.KeyWithTs([]byte("k5"), 1)}))
	require.NoError(t, j.close())

	jf, err := readIngestJournalFile(ingestJournalPath(dir, id),
		ingestFilePrefix+id, mustRegistry(t, opt))
	require.NoError(t, err)
	require.Equal(t, []byte("k1"), jf.info.Range.Left)
	require.Equal(t, []byte("k9"), jf.info.Range.Right)
}

// A crash mid-append leaves a torn final frame (short read or bad CRC at EOF).
// The journal must replay everything up to it without error.
func TestIngestJournalTornTail(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions(dir)
	j, id := newTestJournal(t, opt, IngestFull, 6, IngestKeyRange{})
	require.NoError(t, j.appendStage(stagedTable{
		ID: 1, Smallest: y.KeyWithTs([]byte("a"), 1), Biggest: y.KeyWithTs([]byte("b"), 1)}))
	require.NoError(t, j.close())

	path := ingestJournalPath(dir, id)
	// Append a half-written frame.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{0, 0, 0, 20, 0, 0, 0, 0, 'x'})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	jf, err := readIngestJournalFile(path, ingestFilePrefix+id, mustRegistry(t, opt))
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, jf.info.StagedTableIDs)
	require.Equal(t, IngestStaging, jf.info.Phase)

	// A CRC-corrupt frame NOT at tail is fatal.
	good, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, good, 0600))
	j2, err := createIngestJournal(opt, mustRegistry(t, opt), "sess2", IngestFull, 6,
		IngestKeyRange{})
	require.NoError(t, err)
	require.NoError(t, j2.appendStage(stagedTable{
		ID: 2, Smallest: y.KeyWithTs([]byte("c"), 1), Biggest: y.KeyWithTs([]byte("d"), 1)}))
	require.NoError(t, j2.appendStage(stagedTable{
		ID: 3, Smallest: y.KeyWithTs([]byte("e"), 1), Biggest: y.KeyWithTs([]byte("f"), 1)}))
	require.NoError(t, j2.close())
	p2 := ingestJournalPath(dir, "sess2")
	raw, err := os.ReadFile(p2)
	require.NoError(t, err)
	// Flip a byte in the first frame payload (after 6B header + 8B frame header)
	// while leaving the second valid frame after it, so it is not a torn tail.
	raw[6+8+10] ^= 0xff
	require.NoError(t, os.WriteFile(p2, raw, 0600))
	_, err = readIngestJournalFile(p2, ingestFilePrefix+"sess2", mustRegistry(t, opt))
	require.Error(t, err)
}

// TR-1.2: encrypted journals do not leak user keys in plaintext and still
// roundtrip; a nil registry cannot decrypt them.
func TestIngestJournalEncryption(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions(dir).WithEncryptionKey([]byte("badgerkey16bytes")).
		WithBlockCacheSize(1 << 20).WithIndexCacheSize(1 << 20)
	var err error
	registry, err := OpenKeyRegistry(KeyRegistryOptions{
		Dir:                           opt.Dir,
		EncryptionKey:                 opt.EncryptionKey,
		EncryptionKeyRotationDuration: opt.EncryptionKeyRotationDuration,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = registry.Close() })

	j, err := createIngestJournal(opt, registry, "enc1", IngestIncremental, 5,
		IngestKeyRange{Left: []byte("secret-left"), Right: []byte("secret-right")})
	require.NoError(t, err)
	require.NotZero(t, j.dataKeyID)
	require.NoError(t, j.appendStage(stagedTable{
		ID: 1,
		Smallest: y.KeyWithTs([]byte("secret-left"), 1),
		Biggest:  y.KeyWithTs([]byte("secret-mid"), 1),
	}))
	require.NoError(t, j.close())

	raw, err := os.ReadFile(ingestJournalPath(dir, "enc1"))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "secret-left")
	require.NotContains(t, string(raw), "secret-mid")

	// Without a registry the encrypted frames cannot be decoded.
	_, err = readIngestJournalFile(ingestJournalPath(dir, "enc1"),
		ingestFilePrefix+"enc1", nil)
	require.Error(t, err)

	// With the correct registry the content is fully recovered.
	jf, err := readIngestJournalFile(ingestJournalPath(dir, "enc1"),
		ingestFilePrefix+"enc1", registry)
	require.NoError(t, err)
	require.Equal(t, []byte("secret-left"), jf.info.Range.Left)
	require.Equal(t, []uint64{1}, jf.info.StagedTableIDs)
}

// TR-1.3: the single classifier reconciles committing journals against the
// MANIFEST table set.
func TestClassifyIngestSession(t *testing.T) {
	manifest := map[uint64]TableManifest{20: {}, 21: {}}
	require.Equal(t, IngestStaging,
		reconcileIngestPhase(IngestSessionInfo{Phase: IngestStaging}, manifest))
	// A COMMIT record with no final tables means an empty feed: nothing to
	// reconcile, so it is durably committed.
	require.Equal(t, IngestCommitted,
		reconcileIngestPhase(IngestSessionInfo{Phase: IngestCommitting}, manifest))
	require.Equal(t, IngestCommitting,
		reconcileIngestPhase(IngestSessionInfo{
			Phase: IngestCommitting, FinalTableIDs: []uint64{20, 22},
		}, manifest))
	require.Equal(t, IngestCommitted,
		reconcileIngestPhase(IngestSessionInfo{
			Phase: IngestCommitting, FinalTableIDs: []uint64{20, 21},
		}, manifest))
}

// TR-1.4: ReadIngestSessions works against a directory without opening the DB.
func TestReadIngestSessions(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions(dir)

	// Empty directory: no sessions, no error.
	infos, err := ReadIngestSessions(opt)
	require.NoError(t, err)
	require.Empty(t, infos)

	jActive, idActive := newTestJournal(t, opt, IngestFull, 6, IngestKeyRange{})
	require.NoError(t, jActive.appendStage(stagedTable{
		ID: 1, Smallest: y.KeyWithTs([]byte("a"), 1), Biggest: y.KeyWithTs([]byte("b"), 1)}))
	require.NoError(t, jActive.close())

	jDone, idDone := newTestJournal(t, opt, IngestIncremental, 5, IngestKeyRange{})
	require.NoError(t, jDone.appendStage(stagedTable{
		ID: 2, Smallest: y.KeyWithTs([]byte("c"), 1), Biggest: y.KeyWithTs([]byte("d"), 1)}))
	require.NoError(t, jDone.appendCommit([]uint64{2}, nil))
	require.NoError(t, jDone.markCommitted())

	infos, err = ReadIngestSessions(opt)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	byID := map[string]IngestSessionInfo{}
	for _, info := range infos {
		byID[info.ID] = info
	}
	active := byID[idActive]
	require.Equal(t, IngestStaging, active.Phase)
	require.True(t, active.Pending())
	done := byID[idDone]
	require.Equal(t, IngestCommitted, done.Phase)
	require.False(t, done.Pending())
}

func TestValidIngestID(t *testing.T) {
	require.True(t, validIngestID("abc-0123"))
	require.False(t, validIngestID(""))
	require.False(t, validIngestID("../escape"))
	require.False(t, validIngestID("x.done"))
}

func TestStagedFilesCleanup(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions(dir)
	require.NoError(t, os.WriteFile(table.NewFilename(42, dir), []byte("x"), 0600))
	require.NoError(t, removeStagedTables(opt, []uint64{42, 43}))
	_, err := os.Stat(table.NewFilename(42, dir))
	require.True(t, os.IsNotExist(err))
}

func TestIngestJournalPaths(t *testing.T) {
	dir := filepath.Clean("/tmp/db")
	require.Equal(t, filepath.Join(dir, "INGEST-abc"), ingestJournalPath(dir, "abc"))
	require.Equal(t, filepath.Join(dir, "INGEST-abc.done"), ingestDonePath(dir, "abc"))
}

func mustRegistry(t *testing.T, opt Options) *KeyRegistry {
	t.Helper()
	kr, err := OpenKeyRegistry(KeyRegistryOptions{
		Dir:                           opt.Dir,
		ReadOnly:                      true,
		EncryptionKey:                 opt.EncryptionKey,
		EncryptionKeyRotationDuration: opt.EncryptionKeyRotationDuration,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = kr.Close() })
	return kr
}
