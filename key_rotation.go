/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

const (
	// KeyRotationFileName stores the persistent state of an in-progress online
	// key rotation. Its presence means the rotation must resume on open and
	// all new files must keep using the pinned target key generation.
	KeyRotationFileName = "KEYROTATION"

	keyRotationRewriteFileName = "REWRITE-KEYROTATION"
	keyRotationVersion         = 1

	// rotationSSTBatch is how many SSTables one rotation batch rewrites before
	// yielding. Rotation deliberately paces itself in the background so normal
	// reads and writes are not impacted.
	rotationSSTBatch = 8
	// rotationBatchInterval is the pause between rotation batches.
	rotationBatchInterval = 200 * time.Millisecond
)

// persistedKeyRotation is the on-disk checkpoint of an in-progress key
// rotation. It is atomically rewritten (temp file, fsync, rename, dir fsync)
// whenever a rotation starts. Per-file progress is deliberately not stored
// here: the MANIFEST (KeyID per SSTable), the vlog file headers and the
// memtable WAL headers already record which generation every live file uses,
// so a resumed rotation derives its exact remaining work from those sources
// and never rewrites a file that is already on the target generation.
type persistedKeyRotation struct {
	Version       int    `json:"version"`
	TargetKeyID   uint64 `json:"target_key_id"`
	StartedAtUnix int64  `json:"started_at_unix"`
}

// KeyRotationStatus is a point-in-time view of an online key rotation.
//
// SST/Vlog/WAL "Total" counts enumerate the live files of each kind;
// "Remaining" is the number still encrypted under an older data key
// generation (Remaining == 0 means every file is on the target generation).
type KeyRotationStatus struct {
	// Active is true between StartKeyRotation and the moment the last file has
	// been rewritten and the old key generations have been retired.
	Active bool
	// TargetKeyID is the data key generation files are being rewritten to. 0
	// means plain text (rotation to an empty master key).
	TargetKeyID uint64
	// StartedAt is when StartKeyRotation committed the new registry.
	StartedAt time.Time
	// FinishedAt is set once the rotation completes. Zero while active.
	FinishedAt time.Time

	SSTTotal      int
	SSTRemaining  int
	VlogTotal     int
	VlogRemaining int
	WALTotal      int
	WALRemaining  int

	// LastError is the last transient error encountered by the background
	// worker, if any. Errors are retried indefinitely until they clear.
	LastError error
}

// Remaining reports the number of files (SST + vlog + WAL) still on an old
// key generation.
func (s *KeyRotationStatus) Remaining() int {
	return s.SSTRemaining + s.VlogRemaining + s.WALRemaining
}

// keyRotationRunner drives the online key rotation in the background and
// exposes its progress. One runner exists per DB while a rotation is active
// or after one has finished in this process.
type keyRotationRunner struct {
	db     *DB
	closer *z.Closer
	doneCh chan struct{}

	mu          sync.Mutex
	active      bool
	targetKeyID uint64
	startedAt   time.Time
	finishedAt  time.Time
	lastErr     error
}

func newKeyRotationRunner(db *DB) *keyRotationRunner {
	return &keyRotationRunner{
		db:     db,
		closer: z.NewCloser(1),
		doneCh: make(chan struct{}),
	}
}

// StartKeyRotation begins an online rotation of the encryption at rest keys.
//
// newEncryptionKey is the new master (storage) key; it must be nil/empty
// (rotate to plain text) or 16, 24 or 32 bytes. The call:
//
//  1. Generates a new data key generation and atomically re-wraps the key
//     registry with the new master key, fsynced before returning. All old
//     data keys are retained, so every existing file stays readable.
//  2. Seals the active value log and memtable WAL so that all subsequent
//     transaction writes land in files created under the new generation.
//  3. Persists a KEYROTATION checkpoint and starts a background, batched
//     re-encryption of the existing SSTables and value log files. Reads and
//     writes keep working while it runs; compaction and value log GC output
//     produced meanwhile is written under the new generation as well.
//
// Progress is observable through KeyRotationStatus and can be awaited with
// WaitKeyRotation. If the process exits mid-rotation, reopening the directory
// (with the new master key) automatically resumes from the checkpoint: files
// already on the new generation are not rewritten again. Old data key
// generations are removed from the registry only after the last old file has
// been rewritten.
//
// The directory lock still guarantees that a second instance cannot open the
// same directory for writes, so two processes can never rewrite the same
// file concurrently.
func (db *DB) StartKeyRotation(newEncryptionKey []byte) error {
	if db.opt.InMemory {
		return ErrKeyRotationInMemory
	}
	if db.opt.ReadOnly {
		return ErrKeyRotationReadOnly
	}
	if db.IsClosed() {
		return ErrDBClosed
	}
	if !validEncryptionKeyLen(newEncryptionKey) {
		return fmt.Errorf("%w: master key length must be 0 (plain text), 16, 24 or 32 bytes",
			ErrInvalidEncryptionKey)
	}
	if db.keyRotation == nil {
		db.keyRotation = newKeyRotationRunner(db)
	}
	r := db.keyRotation

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active {
		return ErrKeyRotationInProgress
	}

	// 1. New data key generation + re-wrap registry under the new master key.
	//    The registry rewrite is fsynced before any new file can use the key.
	target, err := db.registry.RotateMasterKey(newEncryptionKey)
	if err != nil {
		return y.Wrapf(err, "while rotating master key")
	}
	targetKeyID := uint64(0)
	if target != nil {
		targetKeyID = target.KeyId
	}

	// 2. Seal the active value log and memtable WAL on the (single) write path.
	//    Everything written afterwards is produced under the new generation.
	if err := db.vlog.requestRollover(); err != nil {
		return y.Wrapf(err, "while rolling over value log during key rotation; "+
			"the key registry was already re-wrapped, reopen with the new key to retry")
	}
	if err := db.rotateActiveMemTable(); err != nil {
		return y.Wrapf(err, "while sealing memtable during key rotation; "+
			"the key registry was already re-wrapped, reopen with the new key to retry")
	}
	db.opt.EncryptionKey = newEncryptionKey

	// 3. Persist the checkpoint, then pin the generation and start the worker.
	//    The state file is fsynced before the pin, so a crash can only leave a
	//    complete checkpoint that reopen pins deterministically.
	state := persistedKeyRotation{
		Version:       keyRotationVersion,
		TargetKeyID:   targetKeyID,
		StartedAtUnix: time.Now().Unix(),
	}
	if err := writeKeyRotationState(db.opt.Dir, state); err != nil {
		return y.Wrapf(err, "while persisting key rotation state; "+
			"the key registry was already re-wrapped, reopen with the new key to retry")
	}
	if err := db.registry.PinDataKey(targetKeyID); err != nil {
		return y.Wrapf(err, "while pinning target data key")
	}

	r.active = true
	r.targetKeyID = targetKeyID
	r.startedAt = time.Unix(state.StartedAtUnix, 0)
	r.finishedAt = time.Time{}
	r.lastErr = nil
	// A closer left over from a previous, finished rotation in this process
	// is replaced before starting the goroutine.
	r.closer = z.NewCloser(1)
	r.doneCh = make(chan struct{})
	db.closers.keyRotation = r.closer
	go r.worker()

	db.opt.Infof("Key rotation started. Target data key id: %d\n", targetKeyID)
	return nil
}

// KeyRotationStatus returns the current rotation progress. It never returns
// nil: when no rotation was ever started, Active is false and all counters
// are zero.
func (db *DB) KeyRotationStatus() *KeyRotationStatus {
	st := &KeyRotationStatus{}
	if db.keyRotation == nil {
		return st
	}
	r := db.keyRotation
	r.mu.Lock()
	st.Active = r.active
	st.TargetKeyID = r.targetKeyID
	st.StartedAt = r.startedAt
	st.FinishedAt = r.finishedAt
	st.LastError = r.lastErr
	target := r.targetKeyID
	active := r.active
	r.mu.Unlock()

	if !active && st.FinishedAt.IsZero() {
		// Never started in this process.
		return st
	}
	st.SSTTotal, st.SSTRemaining = db.lc.rotateEncryptionRemaining(target)
	st.VlogTotal, st.VlogRemaining = db.vlog.rotationCounts(target)
	st.WALTotal, st.WALRemaining = db.oldMemWALCount(target)
	return st
}

// WaitKeyRotation blocks until the rotation finishes, the context is done or
// the DB is closed. It returns nil if the rotation has completed (or never
// ran); a context cancellation/timeout returns the context error while the
// rotation keeps running in the background.
func (db *DB) WaitKeyRotation(ctx context.Context) error {
	if db.keyRotation == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-db.keyRotation.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// rotateActiveMemTable pushes the currently active memtable to the flush
// queue and installs a fresh one, whose WAL is bootstrapped with the pinned
// new data key. It mirrors the flush-push performed during Close but runs
// while writes continue.
func (db *DB) rotateActiveMemTable() error {
	for {
		db.lock.Lock()
		y.AssertTrue(db.mt != nil)
		select {
		case db.flushChan <- db.mt:
			db.imm = append(db.imm, db.mt)
			newmt, err := db.newMemTable()
			if err != nil {
				db.lock.Unlock()
				return y.Wrapf(err, "cannot create sealed memtable for key rotation")
			}
			db.mt = newmt
			db.lock.Unlock()
			db.opt.Debugf("Key rotation sealed the active memtable; new WAL uses the new key\n")
			return nil
		default:
			db.lock.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// rotationBatchSize returns the number of SSTables one worker batch should
// rewrite. Tests can shrink it through the test-only option to get
// fine-grained batches.
func (db *DB) rotationBatchSize() int {
	if db.opt.keyRotationSSTBatch > 0 {
		return db.opt.keyRotationSSTBatch
	}
	return rotationSSTBatch
}

// oldMemWALCount counts live memtable WALs and those whose header names a key
// generation other than targetKeyID. Every live WAL is tracked by db.mt or
// db.imm, so this in-memory view is authoritative and never races a WAL
// deletion by the flusher.
func (db *DB) oldMemWALCount(targetKeyID uint64) (total, remaining int) {
	db.lock.RLock()
	mts := make([]*memTable, 0, len(db.imm)+1)
	if db.mt != nil {
		mts = append(mts, db.mt)
	}
	mts = append(mts, db.imm...)
	db.lock.RUnlock()

	for _, mt := range mts {
		if mt.wal == nil {
			continue
		}
		total++
		if mt.wal.keyID() != targetKeyID {
			remaining++
		}
	}
	return total, remaining
}

// worker performs rotation batches until everything is on the target
// generation and the old keys are retired, or until the DB closes. Transient
// errors are logged and retried; a batch never leaves a partially switched
// file (each file switch is a single atomic MANIFEST changeset).
func (r *keyRotationRunner) worker() {
	defer r.closer.Done()

	pause := func() bool {
		select {
		case <-r.closer.HasBeenClosed():
			return true
		case <-time.After(rotationBatchInterval):
			return false
		}
	}

	for {
		select {
		case <-r.closer.HasBeenClosed():
			return
		default:
		}

		n, err := r.db.lc.rotateEncryptionBatch(r.db.rotationBatchSize(), r.targetKeyID)
		if err != nil {
			r.recordError(err)
			if pause() {
				return
			}
			continue
		}
		// Rewrite one old vlog file per tick as well. GC output produced
		// meanwhile already lands under the pinned generation.
		if _, err := r.db.vlog.rotateRewriteOne(r.targetKeyID); err != nil {
			r.recordError(err)
			if pause() {
				return
			}
			continue
		}

		done, err := r.tryFinalize()
		if err != nil {
			r.recordError(err)
			if pause() {
				return
			}
			continue
		}
		if done {
			r.db.opt.Infof("Key rotation to data key %d complete\n", r.targetKeyID)
			return
		}
		if n == 0 {
			// Nothing rewritable this tick (compaction owns a table or a GC is
			// running); pace the idle loop.
			if pause() {
				return
			}
		}
	}
}

func (r *keyRotationRunner) recordError(err error) {
	// A batch in flight when Close blocks writes reports ErrBlockedWrites; the
	// checkpoint persists and reopen resumes the rotation, so this is expected
	// rather than a rotation failure.
	if r.db.IsClosed() {
		return
	}
	r.db.opt.Errorf("key rotation worker error (will retry): %v", err)
	r.mu.Lock()
	r.lastErr = err
	r.mu.Unlock()
}

// tryFinalize retires old generations only once not a single live file
// references them: all SSTables, all value log files and all memtable WALs
// must be on the target generation. It then prunes the registry, releases the
// pin and removes the checkpoint.
func (r *keyRotationRunner) tryFinalize() (bool, error) {
	_, sstRemaining := r.db.lc.rotateEncryptionRemaining(r.targetKeyID)
	_, vlogRemaining := r.db.vlog.rotationCounts(r.targetKeyID)
	_, walRemaining := r.db.oldMemWALCount(r.targetKeyID)
	if sstRemaining != 0 || vlogRemaining != 0 || walRemaining != 0 {
		return false, nil
	}

	// Every file is on the target generation. Retire the old data keys. The
	// registry rewrite is atomic, and the target key (still pinned) is kept.
	if err := r.db.registry.PruneDataKeys(r.targetKeyID); err != nil {
		return false, y.Wrapf(err, "while pruning old data keys")
	}
	r.db.registry.UnpinDataKey()

	// Remove the checkpoint and sync the directory. A crash right here at most
	// leaves a stale KEYROTATION pointing at the sole remaining key: resume
	// finds zero old files, prunes again (a no-op) and finishes.
	if err := os.Remove(filepath.Join(r.db.opt.Dir, KeyRotationFileName)); err != nil &&
		!os.IsNotExist(err) {
		return false, y.Wrapf(err, "while removing key rotation checkpoint")
	}
	if err := syncDir(r.db.opt.Dir); err != nil {
		return false, y.Wrapf(err, "while syncing dir after key rotation")
	}

	r.mu.Lock()
	r.active = false
	r.finishedAt = time.Now()
	r.lastErr = nil
	r.mu.Unlock()
	close(r.doneCh)
	return true, nil
}

// stopAndWait signals the worker and waits for the current batch to finish.
// It is called during Close before the write/vlog-GC goroutines are stopped,
// so an in-flight rewrite can still drain its write-back requests.
func (r *keyRotationRunner) stopAndWait() {
	if r == nil {
		return
	}
	r.closer.SignalAndWait()
}

// pauseKeyRotationForDrop stops the rotation worker around a DropAll/DropPrefix
// operation, which stops the write goroutine: the worker's vlog rewrite path
// drains write-backs through that goroutine, so it must be idle first. It
// reports whether a rotation was active. The durable checkpoint is untouched,
// and the restarted worker re-scans and finishes whatever remains.
func (db *DB) pauseKeyRotationForDrop() bool {
	r := db.keyRotation
	if r == nil {
		return false
	}
	r.mu.Lock()
	active := r.active
	r.mu.Unlock()
	if active {
		r.closer.SignalAndWait()
	}
	return active
}

// resumeKeyRotationAfterDrop restarts the worker after a DropAll/DropPrefix
// operation, iff one was active when the drop started.
func (db *DB) resumeKeyRotationAfterDrop(wasActive bool) {
	if !wasActive || db.keyRotation == nil {
		return
	}
	r := db.keyRotation
	r.closer = z.NewCloser(1)
	r.doneCh = make(chan struct{})
	db.closers.keyRotation = r.closer
	go r.worker()
}

// resumeKeyRotationIfNeeded is called during Open right after the key registry
// is loaded. If a durable checkpoint is present it pins the target
// generation, so every file created during the rest of Open (memtable flushes,
// the fresh value log) already lands under the pinned generation. The
// background worker itself is started later via startResumedKeyRotation, once
// all subsystems are up.
func (db *DB) resumeKeyRotationIfNeeded() error {
	if db.opt.InMemory {
		return nil
	}
	state, err := readKeyRotationState(db.opt.Dir)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	if err := db.registry.PinDataKey(state.TargetKeyID); err != nil {
		return y.Wrapf(err, "while resuming key rotation")
	}
	r := newKeyRotationRunner(db)
	r.active = true
	r.targetKeyID = state.TargetKeyID
	r.startedAt = time.Unix(state.StartedAtUnix, 0)
	db.keyRotation = r
	db.closers.keyRotation = r.closer
	return nil
}

// startResumedKeyRotation launches the background worker for a rotation whose
// checkpoint was found during Open. It is called after the write, compaction
// and value log subsystems have been started.
func (db *DB) startResumedKeyRotation() {
	if db.keyRotation == nil {
		return
	}
	// A read-only reopen observes the checkpoint and pins the target key for
	// reads, but it must not rewrite anything. No goroutine owns the closer,
	// so detach it to keep Close's SignalAndWait from blocking.
	if db.opt.ReadOnly {
		db.closers.keyRotation = nil
		return
	}
	r := db.keyRotation
	db.opt.Infof("Resumed key rotation. Target data key id: %d\n", r.targetKeyID)
	go r.worker()
}

func keyRotationPath(dir string) string {
	return filepath.Join(dir, KeyRotationFileName)
}

// writeKeyRotationState atomically persists the rotation checkpoint.
func writeKeyRotationState(dir string, state persistedKeyRotation) error {
	payload, err := json.Marshal(&state)
	if err != nil {
		return err
	}
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.Checksum(payload, y.CastagnoliCrcTable))

	tmpPath := filepath.Join(dir, keyRotationRewriteFileName)
	fp, err := y.OpenTruncFile(tmpPath, true)
	if err != nil {
		return y.Wrapf(err, "while creating rotation checkpoint")
	}
	if _, err := fp.Write(buf[:]); err != nil {
		fp.Close()
		return y.Wrapf(err, "while writing rotation checkpoint header")
	}
	if _, err := fp.Write(payload); err != nil {
		fp.Close()
		return y.Wrapf(err, "while writing rotation checkpoint")
	}
	// Close (and flush) before renaming; required on Windows.
	if err := fp.Close(); err != nil {
		return y.Wrapf(err, "while closing rotation checkpoint")
	}
	if err := os.Rename(tmpPath, keyRotationPath(dir)); err != nil {
		return y.Wrapf(err, "while renaming rotation checkpoint")
	}
	return syncDir(dir)
}

// readKeyRotationState loads the checkpoint. A missing checkpoint returns
// (nil, nil).
func readKeyRotationState(dir string) (*persistedKeyRotation, error) {
	data, err := os.ReadFile(keyRotationPath(dir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, y.Wrapf(err, "while reading rotation checkpoint")
	}
	if len(data) < 8 {
		return nil, errors.New("key rotation checkpoint is too short")
	}
	l := binary.BigEndian.Uint32(data[0:4])
	wantCrc := binary.BigEndian.Uint32(data[4:8])
	if uint32(len(data)) < 8+l {
		return nil, errors.New("key rotation checkpoint is truncated")
	}
	payload := data[8 : 8+l]
	if crc32.Checksum(payload, y.CastagnoliCrcTable) != wantCrc {
		return nil, y.Wrapf(y.ErrChecksumMismatch, "key rotation checkpoint")
	}
	state := &persistedKeyRotation{}
	if err := json.Unmarshal(payload, state); err != nil {
		return nil, y.Wrapf(err, "while parsing key rotation checkpoint")
	}
	return state, nil
}
