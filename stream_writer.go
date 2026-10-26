/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// StreamWriter is used to write data coming from multiple streams. The streams must not have any
// overlapping key ranges. Within each stream, the keys must be sorted. Badger Stream framework is
// capable of generating such an output. So, this StreamWriter can be used at the other end to build
// BadgerDB at a much faster pace by writing SSTables (and value logs) directly to LSM tree levels
// without causing any compactions at all. This is way faster than using batched writer or using
// transactions, but only applicable in situations where the keys are pre-sorted and the DB is being
// bootstrapped. Existing data would get deleted when using this writer in full mode. So, this is
// only useful when restoring from backup or replicating DB across servers.
//
// StreamWriter should not be called on in-use DB instances. It is designed only to bootstrap new
// DBs.
//
// Every run of the writer is a replayable ingest session (see Begin/Resume): the session journal
// records the session identity, key range and target level before any data becomes visible, tables
// stay staged (invisible to readers and absent from the MANIFEST) until Flush atomically publishes
// the converged result, and an interrupted session can be continued or explicitly discarded after
// reopening the database.
type StreamWriter struct {
	writeLock  sync.Mutex
	db         *DB
	done       func()
	throttle   *y.Throttle
	maxVersion uint64
	writers    map[uint32]*sortedWriter

	// Session contract state. Guarded by writeLock for the flags/slices; the
	// staged slice itself has stageMu because createTable runs in throttle
	// goroutines while Write may hold writeLock.
	journal     *ingestJournal
	iopts       IngestOptions
	mode        IngestMode
	targetLevel int
	stageMu sync.Mutex
	staged  []*stagedTable
	// refsReleased is set once the staged table refs have been consumed by a
	// (possibly partially failed) commit, so Cancel never double-releases them.
	refsReleased bool
	committed     bool
}

// NewStreamWriter creates a StreamWriter. Right after creating StreamWriter, Prepare (full ingest)
// or PrepareIncremental/Begin must be called. The memory usage of a StreamWriter is directly
// proportional to the number of streams possible. So, efforts must be made to keep the number of
// streams low. Stream framework would typically use 16 goroutines and hence create 16 streams.
func (db *DB) NewStreamWriter() *StreamWriter {
	return &StreamWriter{
		db: db,
		// throttle shouldn't make much difference. Memory consumption is based on the number of
		// concurrent streams being processed.
		throttle: y.NewThrottle(16),
		writers:  make(map[uint32]*sortedWriter),
	}
}

// Prepare should be called before writing any entry to StreamWriter. It deletes all data present in
// existing DB, stops compactions and any writes being done by other means. Be very careful when
// calling Prepare, because it could result in permanent data loss. Not calling Prepare would result
// in a corrupt Badger instance. Use PrepareIncremental to do incremental stream write.
//
// Prepare starts a full ingest session with a generated ID; pass an explicit
// session ID via Begin to make the run replayable across interruptions.
func (sw *StreamWriter) Prepare() error {
	return sw.Begin(IngestFull, IngestOptions{})
}

// PrepareIncremental should be called before writing any entry to StreamWriter incrementally.
// In incremental stream write, the tables are staged one level above the current base level and,
// at Flush, overlapping older levels are converged onto the newest version of every key.
func (sw *StreamWriter) PrepareIncremental() error {
	return sw.Begin(IngestIncremental, IngestOptions{})
}

// Begin starts a replayable ingest session. In IngestFull mode it has the
// destructive semantics of Prepare (existing data is dropped); in
// IngestIncremental mode it has the semantics of PrepareIncremental.
//
// When opt.SessionID is empty a random ID is generated. Starting another
// session while one is unfinished returns ErrIngestSessionActive; reusing the
// ID of an unfinished session attaches/resumes it (same as Resume); reusing
// the ID of an already committed session returns ErrIngestAlreadyCommitted
// without writing anything.
func (sw *StreamWriter) Begin(mode IngestMode, opt IngestOptions) error {
	// mode may be zero when resuming via an explicit session ID; the mode is
	// then taken from the interrupted session's journal.
	if mode != 0 && mode != IngestFull && mode != IngestIncremental {
		return fmt.Errorf("invalid ingest mode: %d", mode)
	}
	sw.writeLock.Lock()
	defer sw.writeLock.Unlock()
	if sw.journal != nil {
		return fmt.Errorf("StreamWriter session already started")
	}
	if sw.committed {
		return fmt.Errorf("StreamWriter already flushed")
	}

	id := opt.SessionID
	if id == "" {
		id = newIngestSessionID()
	}
	if !validIngestID(id) {
		return fmt.Errorf("invalid ingest session ID %q", id)
	}
	if err := validateDeclaredRange(opt.DeclaredRange); err != nil {
		return err
	}

	// A journal with this ID may already exist (a previous, interrupted run).
	// Same ID + unfinished => resume/reattach; same ID + .done => idempotency
	// error; any other unfinished session => rejected.
	resumeMode, stagedIDs, isResume, err := sw.lookupSession(id)
	if err != nil {
		return err
	}
	if isResume {
		// The resumed run adopts the mode recorded durably by the interrupted
		// session's journal; the target level is re-derived against the current
		// tree by prepareMode below (compactions may have run after reopen).
		mode = resumeMode
	}
	if mode != IngestFull && mode != IngestIncremental {
		// A mode-less Begin is only valid when it attaches to an existing
		// session; anything else is a missing session.
		if mode == 0 {
			return ErrIngestSessionNotFound
		}
		return fmt.Errorf("invalid ingest mode: %d", mode)
	}

	resume, target, err := sw.prepareMode(mode)
	if err != nil {
		return err
	}
	if isResume {
		// Stale staged files from the interrupted attempt are removed before we
		// re-stage the replayed feed. On a normal reopen they were already
		// removed by revertToManifest, so missing files are tolerated.
		if err := removeStagedTables(sw.db.opt, stagedIDs); err != nil {
			resume()
			return err
		}
	}

	j, err := sw.startJournal(id, mode, target, opt, isResume)
	if err != nil {
		resume()
		return err
	}

	sw.wrapDone(resume)
	sw.journal = j
	sw.mode = mode
	sw.iopts = opt
	sw.targetLevel = target
	sw.db.registerIngestWriter(id, sw)
	return nil
}

// Resume attaches the StreamWriter to an unfinished ingest session previously
// created with id. The caller must then re-send the complete data feed and
// call Flush; the converged outcome is identical to one uninterrupted run, and
// replaying the feed creates no duplicate versions because staged tables only
// enter the MANIFEST once, at commit.
//
// It returns ErrIngestSessionNotFound when no such session exists and
// ErrIngestAlreadyCommitted when id already committed successfully.
func (sw *StreamWriter) Resume(id string) error {
	sw.writeLock.Lock()
	already := sw.db.liveIngestSession(id)
	sw.writeLock.Unlock()
	if already {
		return ErrIngestSessionActive
	}
	return sw.Begin(0, IngestOptions{SessionID: id})
}

// validateDeclaredRange ensures left <= right so every incoming key has a
// well-formed containment test.
func validateDeclaredRange(r IngestKeyRange) error {
	if len(r.Left) > 0 && len(r.Right) > 0 && bytes.Compare(r.Left, r.Right) > 0 {
		return fmt.Errorf("declared ingest range is invalid: left %x > right %x", r.Left, r.Right)
	}
	return nil
}

func (sw *StreamWriter) withinDeclaredRange(key []byte) bool {
	r := sw.iopts.DeclaredRange
	if len(r.Left) == 0 && len(r.Right) == 0 {
		return true
	}
	if len(r.Left) > 0 && bytes.Compare(key, r.Left) < 0 {
		return false
	}
	if len(r.Right) > 0 && bytes.Compare(key, r.Right) > 0 {
		return false
	}
	return true
}

// wrapDone ensures done() is invoked exactly once even if Begin is retried.
func (sw *StreamWriter) wrapDone(resume func()) {
	var once sync.Once
	sw.done = func() { once.Do(resume) }
}

// lookupSession checks disk (or the in-memory session table) for an existing
// session. Return values: mode/level/stagedIDs are zero unless isResume is
// true.
func (sw *StreamWriter) lookupSession(id string) (
	mode IngestMode, stagedIDs []uint64, isResume bool, err error) {
	db := sw.db

	db.ingestMu.Lock()
	defer db.ingestMu.Unlock()

	// Reject when any *other* unfinished session is attached in this process.
	if live, ok := db.ingestLive[id]; ok {
		// Same ID attached live: treat as resume of the in-process session.
		info := live.snapshot()
		return info.Mode, append([]uint64(nil), info.StagedTableIDs...), true, nil
	}
	for otherID := range db.ingestLive {
		if otherID != id {
			return 0, nil, false, ErrIngestSessionActive
		}
	}

	if db.opt.InMemory {
		if info, ok := db.ingestHistory[id]; ok && info.Phase == IngestCommitted {
			return 0, nil, false, ErrIngestAlreadyCommitted
		}
		return 0, nil, false, nil
	}

	dirEntries, readErr := os.ReadDir(db.opt.Dir)
	if readErr != nil {
		return 0, nil, false, readErr
	}
	var active, done bool
	for _, entry := range dirEntries {
		name := entry.Name()
		switch {
		case name == ingestFilePrefix+id:
			active = true
		case name == ingestFilePrefix+id+ingestDoneSuffix:
			done = true
		case strings.HasPrefix(name, ingestFilePrefix):
			// Another, unrelated session journal (active or committed): only an
			// *unfinished* active journal blocks a new session.
			if !strings.HasSuffix(name, ingestDoneSuffix) {
				return 0, nil, false, ErrIngestSessionActive
			}
		}
	}
	if done && !active {
		return 0, nil, false, ErrIngestAlreadyCommitted
	}
	if !active {
		return 0, nil, false, nil
	}

	// Re-read the interrupted journal to recover mode and staged ids.
	registry := db.registry
	jf, readErr := readIngestJournalFile(ingestJournalPath(db.opt.Dir, id),
		ingestFilePrefix+id, registry)
	if readErr != nil {
		return 0, nil, false, readErr
	}
	return jf.info.Mode, append([]uint64(nil), jf.info.StagedTableIDs...), true, nil
}

// prepareMode runs the mode-specific quiescing work and returns the resume
// function and the level staged tables will be built at.
func (sw *StreamWriter) prepareMode(mode IngestMode) (func(), int, error) {
	db := sw.db
	switch mode {
	case IngestFull:
		done, err := db.dropAll()
		if err != nil {
			return done, 0, err
		}
		return done, db.opt.MaxLevels - 1, nil
	case IngestIncremental:
		return sw.prepareIncremental()
	}
	return nil, 0, fmt.Errorf("invalid ingest mode: %d", mode)
}

// prepareIncremental contains the historical PrepareIncremental preparation:
// block writes, stop compactions, require empty memtables, flatten if data
// reached L0, and derive the level one above the current top of the tree.
func (sw *StreamWriter) prepareIncremental() (func(), int, error) {
	// prepareToDrop will stop all the incoming writes and process any pending flush tasks.
	// Before we start writing, we'll stop the compactions because no one else should be writing to
	// the same level as the stream writer is writing to.
	f, err := sw.db.prepareToDrop()
	if err != nil {
		return f, 0, err
	}
	sw.db.stopCompactions()
	resume := func() {
		sw.db.startCompactions()
		f()
	}

	mts, decr := sw.db.getMemTables()
	defer decr()
	for _, m := range mts {
		if !m.sl.Empty() {
			resume()
			return nil, 0, fmt.Errorf("Unable to do incremental writes because MemTable has data")
		}
	}

	topLevel := -1
	for _, level := range sw.db.Levels() {
		if level.NumTables > 0 {
			topLevel = level.Level
			break
		}
	}
	if topLevel == -1 {
		// Empty DB: stage directly at the last level, mirroring the historical
		// empty-DB branch.
		return resume, sw.db.opt.MaxLevels - 1, nil
	}
	if topLevel == 0 {
		// Data reaches all the way up to L0: flatten it to the last level
		// first, otherwise writing above L0 would break level placement.
		if err := sw.db.Flatten(3); err != nil {
			resume()
			return nil, 0, fmt.Errorf("error during flatten in StreamWriter: %w", err)
		}
		topLevel = sw.db.opt.MaxLevels - 1
	}
	return resume, topLevel - 1, nil
}

// startJournal creates (or recreates, when resuming) the active session
// journal and registers it on the DB.
func (sw *StreamWriter) startJournal(id string, mode IngestMode, targetLevel int,
	opt IngestOptions, isResume bool) (*ingestJournal, error) {
	db := sw.db
	if isResume && !db.opt.InMemory {
		// Drop the stale journal; createIngestJournal writes a fresh BEGIN.
		if err := os.Remove(ingestJournalPath(db.opt.Dir, id)); err != nil &&
			!os.IsNotExist(err) {
			return nil, err
		}
		if j := db.ingestLive[id]; j != nil {
			_ = j.close()
			delete(db.ingestLive, id)
		}
	}
	j, err := createIngestJournal(db.opt, db.registry, id, mode, targetLevel, opt.DeclaredRange)
	if err != nil {
		return nil, err
	}
	db.ingestMu.Lock()
	db.ingestLive[id] = j
	db.ingestMu.Unlock()
	return j, nil
}

// Write writes KVList to DB. Each KV within the list contains the stream id which StreamWriter
// would use to demux the writes. Write is thread safe and can be called concurrently by multiple
// goroutines.
func (sw *StreamWriter) Write(buf *z.Buffer) error {
	if buf.LenNoPadding() == 0 {
		return nil
	}
	if sw.journal == nil {
		return fmt.Errorf("StreamWriter.Begin must be called before Write")
	}

	// closedStreams keeps track of all streams which are going to be marked as done. We are
	// keeping track of all streams so that we can close them at the end, after inserting all
	// the valid kvs.
	closedStreams := make(map[uint32]struct{})
	streamReqs := make(map[uint32]*request)

	err := buf.SliceIterate(func(s []byte) error {
		var kv pb.KV
		if err := proto.Unmarshal(s, &kv); err != nil {
			return err
		}
		if kv.StreamDone {
			closedStreams[kv.StreamId] = struct{}{}
			return nil
		}

		// Panic if some kv comes after stream has been marked as closed.
		if _, ok := closedStreams[kv.StreamId]; ok {
			panic(fmt.Sprintf("write performed on closed stream: %d", kv.StreamId))
		}

		sw.writeLock.Lock()
		if sw.maxVersion < kv.Version {
			sw.maxVersion = kv.Version
		}
		sw.writeLock.Unlock()

		// The session contract confines every feed key to its declared range.
		if !sw.withinDeclaredRange(kv.Key) {
			return fmt.Errorf("ingest key %x falls outside the declared session range", kv.Key)
		}

		var meta, userMeta byte
		if len(kv.Meta) > 0 {
			meta = kv.Meta[0]
		}
		if len(kv.UserMeta) > 0 {
			userMeta = kv.UserMeta[0]
		}
		e := &Entry{
			Key:       y.KeyWithTs(kv.Key, kv.Version),
			Value:     y.Copy(kv.Value),
			UserMeta:  userMeta,
			ExpiresAt: kv.ExpiresAt,
			meta:      meta,
		}
		// If the value can be collocated with the key in LSM tree, we can skip
		// writing the value to value log.
		req := streamReqs[kv.StreamId]
		if req == nil {
			req = &request{}
			streamReqs[kv.StreamId] = req
		}
		req.Entries = append(req.Entries, e)
		return nil
	})
	if err != nil {
		return err
	}

	all := make([]*request, 0, len(streamReqs))
	for _, req := range streamReqs {
		all = append(all, req)
	}

	sw.writeLock.Lock()
	defer sw.writeLock.Unlock()

	// We are writing all requests to vlog even if some request belongs to already closed stream.
	// It is safe to do because we are panicking while writing to sorted writer, which will be nil
	// for closed stream. The staged SSTs stay invisible until Flush, so a failed/aborted run can
	// never expose them.
	if err := sw.db.vlog.write(all); err != nil {
		return err
	}

	for streamID, req := range streamReqs {
		writer, ok := sw.writers[streamID]
		if !ok {
			var err error
			writer, err = sw.newWriter(streamID)
			if err != nil {
				return y.Wrapf(err, "failed to create writer with ID %d", streamID)
			}
			sw.writers[streamID] = writer
		}

		if writer == nil {
			panic(fmt.Sprintf("write performed on closed stream: %d", streamID))
		}

		writer.reqCh <- req
	}

	// Now we can close any streams if required. We will make writer for
	// the closed streams as nil.
	for streamId := range closedStreams {
		writer, ok := sw.writers[streamId]
		if !ok {
			sw.db.opt.Warningf("Trying to close stream: %d, but no sorted "+
				"writer found for it", streamId)
			continue
		}

		writer.closer.SignalAndWait()
		if err := writer.Done(); err != nil {
			return err
		}

		sw.writers[streamId] = nil
	}
	return nil
}

// Flush is called once we are done writing all the entries. It waits for all staged tables, runs
// the converging commit for incremental sessions, publishes the result atomically, syncs DB
// directories and updates the Oracle with maxVersion found in all entries (if DB is not managed).
// Flushing a session makes its committed ID idempotent: replaying the same session afterwards is
// rejected with ErrIngestAlreadyCommitted and produces no new versions.
func (sw *StreamWriter) Flush() error {
	sw.writeLock.Lock()
	defer sw.writeLock.Unlock()

	if sw.committed {
		return nil
	}
	if sw.journal == nil {
		return fmt.Errorf("StreamWriter.Begin must be called before Flush")
	}
	// Resume writes/compactions exactly once under all exit paths.
	defer sw.done()

	for _, writer := range sw.writers {
		if writer != nil {
			writer.closer.SignalAndWait()
		}
	}

	for _, writer := range sw.writers {
		if writer == nil {
			continue
		}
		if err := writer.Done(); err != nil {
			return err
		}
	}

	if !sw.db.opt.managedTxns {
		if sw.db.orc != nil {
			sw.db.orc.Stop()
		}

		if curMax := sw.db.orc.readTs(); curMax >= sw.maxVersion {
			sw.maxVersion = curMax
		}

		sw.db.orc = newOracle(sw.db.opt)
		sw.db.orc.nextTxnTs = sw.maxVersion
		sw.db.orc.txnMark.Done(sw.maxVersion)
		sw.db.orc.readMark.Done(sw.maxVersion)
		sw.db.orc.incrementNextTs()
	}

	// Wait for all staged table files to be written and recorded.
	if err := sw.throttle.Finish(); err != nil {
		return err
	}

	// Stage -> converge -> durably commit -> atomically publish.
	if err := sw.commitSession(); err != nil {
		return err
	}

	// Sort tables at the end.
	for _, l := range sw.db.lc.levels {
		l.sortTables()
	}

	// Now sync the directories, so all the files are registered.
	if sw.db.opt.ValueDir != sw.db.opt.Dir {
		if err := sw.db.syncDir(sw.db.opt.ValueDir); err != nil {
			return err
		}
	}
	if err := sw.db.syncDir(sw.db.opt.Dir); err != nil {
		return err
	}
	if err := sw.db.lc.validate(); err != nil {
		return err
	}

	sw.committed = true
	return nil
}

// commitSession implements the atomic ingest commit. Staged tables are absent
// from both the MANIFEST and the in-memory levels until this function runs.
//
// Full mode publishes the staged tables at the last level (the tree was
// dropped at begin). Incremental mode either publishes staged tables directly
// at their target level when no older table overlaps the session range, or
// merges staged and overlapping older tables so that every key converges to
// its newest version (newest tombstones preserved), placing the result on the
// deepest level that held old data.
func (sw *StreamWriter) commitSession() error {
	sw.stageMu.Lock()
	staged := append([]*stagedTable(nil), sw.staged...)
	sw.stageMu.Unlock()

	// Stable order for concat iterators and deterministic manifests.
	sort.Slice(staged, func(i, j int) bool {
		return y.CompareKeys(staged[i].Smallest, staged[j].Smallest) < 0
	})

	var (
		changes    []*pb.ManifestChange
		adds       = make(map[int][]*table.Table)
		dels       = make(map[int][]*table.Table)
		finalIDs   []uint64
		droppedIDs []uint64
		plan       *incrementalPlan
	)

	if sw.mode == IngestFull || len(staged) == 0 {
		level := sw.targetLevel
		tbls := make([]*table.Table, 0, len(staged))
		for _, st := range staged {
			changes = append(changes, newCreateChange(
				st.ID, level, st.Table.KeyID(), st.Table.CompressionType()))
			tbls = append(tbls, st.Table)
			finalIDs = append(finalIDs, st.ID)
		}
		adds[level] = tbls
	} else {
		var err error
		plan, err = sw.planIncrementalCommit(staged)
		if err != nil {
			return err
		}
		if plan.rewrite {
			for _, t := range plan.finalTables {
				changes = append(changes, newCreateChange(
					t.ID(), plan.finalLevel, t.KeyID(), t.CompressionType()))
				finalIDs = append(finalIDs, t.ID())
			}
			for _, old := range plan.dropped {
				if !old.IsInmemory {
					changes = append(changes, newDeleteChange(old.ID()))
				}
				droppedIDs = append(droppedIDs, old.ID())
			}
			adds[plan.finalLevel] = plan.finalTables
			for level, tbls := range plan.droppedByLevel {
				dels[level] = tbls
			}
		} else {
			level := sw.targetLevel
			tbls := make([]*table.Table, 0, len(staged))
			for _, st := range staged {
				changes = append(changes, newCreateChange(
					st.ID, level, st.Table.KeyID(), st.Table.CompressionType()))
				tbls = append(tbls, st.Table)
				finalIDs = append(finalIDs, st.ID)
			}
			adds[level] = tbls
		}
	}

	// Phase 1 of the two-phase commit: durably record the intent, including the
	// exact final/dropped table ids, BEFORE touching the MANIFEST. A crash after
	// this point is reconciled against the MANIFEST on reopen.
	if err := sw.journal.appendCommit(finalIDs, droppedIDs); err != nil {
		plan.abortBuilt()
		return err
	}

	// Phase 2: one atomic MANIFEST changeset (CREATEs and DELETEs share a
	// single CRC record, so replay is all-or-nothing).
	if len(changes) > 0 {
		if err := sw.db.manifest.addChanges(changes, sw.db.opt); err != nil {
			plan.abortBuilt()
			return err
		}
	}

	// Phase 3: publish to in-memory levels under the global read barrier. No
	// reader can observe a cross-level intermediate state. publishIngest drops
	// the extra refs gathered on deleted tables after swapping them out.
	if err := sw.db.lc.publishIngest(adds, dels); err != nil {
		return err
	}

	// Phase 4: balance references.
	//
	// Directly published staged tables: the handler IncrRef'd them; our builder
	// ref goes away and the file survives. Superseded staged tables (a rewrite
	// happened): the handler never saw them, so DecrRef unlinks the file (they
	// were never in the MANIFEST).
	for _, st := range staged {
		_ = st.Table.DecrRef()
	}
	sw.refsReleased = true
	// Converged tables: the handler IncrRef'd them; drop the builder-side ref.
	if plan != nil && plan.rewrite {
		for _, t := range plan.finalTables {
			_ = t.DecrRef()
		}
	}

	// Phase 5: mark the session durably committed (rename journal to .done).
	if err := sw.journal.markCommitted(); err != nil {
		return err
	}
	sessionID := sw.journal.id()
	sw.db.noteIngestCommitted(sw.journal.snapshot())
	sw.db.dropLiveIngest(sessionID)
	return nil
}

// incrementalPlan carries the decision for an incremental commit.
type incrementalPlan struct {
	rewrite       bool
	finalLevel    int
	finalTables   []*table.Table
	dropped       []*table.Table
	droppedByLevel map[int][]*table.Table
}

// planIncrementalCommit gathers every old table overlapping the session range
// at levels [target, Lmax]. With no overlap the staged tables publish
// unmodified. With overlap the newest version of every key is computed across
// staged + old tables, keeping winning tombstones.
func (sw *StreamWriter) planIncrementalCommit(staged []*stagedTable) (*incrementalPlan, error) {
	db := sw.db
	target := sw.targetLevel

	stagedTbls := make([]*table.Table, len(staged))
	for i, st := range staged {
		stagedTbls[i] = st.Table
	}
	kr := getKeyRange(stagedTbls...)

	droppedByLevel := make(map[int][]*table.Table)
	var dropped []*table.Table
	deepest := -1
	for level := target; level < db.opt.MaxLevels; level++ {
		h := db.lc.levels[level]
		h.RLock()
		left, right := h.overlappingTables(levelHandlerRLocked{}, kr)
		picked := make([]*table.Table, right-left)
		copy(picked, h.tables[left:right])
		h.RUnlock()
		if len(picked) == 0 {
			continue
		}
		// No extra IncrRef here: compactions are stopped for the whole session,
		// so these tables can neither move nor vanish before publishIngest runs.
		// publishIngest releases exactly the handler's ownership once.
		deepest = level
		droppedByLevel[level] = picked
		dropped = append(dropped, picked...)
	}

	if deepest < 0 {
		// Nothing old overlaps the feed range: staged tables can land as-is.
		return &incrementalPlan{rewrite: false, finalLevel: target}, nil
	}

	finalTables, err := sw.buildConvergedTables(stagedTbls, droppedByLevel, target, deepest, kr)
	if err != nil {
		return nil, err
	}
	return &incrementalPlan{
		rewrite:        true,
		finalLevel:     deepest,
		finalTables:    finalTables,
		dropped:        dropped,
		droppedByLevel: droppedByLevel,
	}, nil
}

// buildConvergedTables merges staged tables (highest precedence) with the
// overlapping old tables level by level (upper levels before lower ones).
//
// Merge iteration yields versions from newest to oldest per user key. For keys
// inside the session range only the newest version is emitted; keys outside the
// range belong exclusively to old tables and all their versions are preserved.
// A newest-version tombstone is emitted like any other value and therefore can
// never be covered by an older value or resurrected by replay.
func (sw *StreamWriter) buildConvergedTables(stagedTbls []*table.Table,
	droppedByLevel map[int][]*table.Table, target, deepest int, kr keyRange) ([]*table.Table, error) {
	db := sw.db
	left := y.ParseKey(kr.left)
	right := y.ParseKey(kr.right)
	inRange := func(userKey []byte) bool {
		if len(left) > 0 && bytes.Compare(userKey, left) < 0 {
			return false
		}
		if len(right) > 0 && bytes.Compare(userKey, right) > 0 {
			return false
		}
		return true
	}

	var iters []y.Iterator
	iters = append(iters, table.NewConcatIterator(stagedTbls, table.NOCACHE))
	for level := target; level <= deepest; level++ {
		if tbls := droppedByLevel[level]; len(tbls) > 0 {
			iters = append(iters, table.NewConcatIterator(tbls, table.NOCACHE))
		}
	}
	mi := table.NewMergeIterator(iters, false)
	defer mi.Close()

	bopts := buildTableOptions(db)
	bopts.TableSize = uint64(db.lc.levelTargets().fileSz[deepest])

	var (
		out       []*table.Table
		builder   = table.NewTableBuilder(bopts)
		lastUser  []byte
		within    bool
		versionsN int
	)
	flushBuilder := func() error {
		if builder.Empty() {
			builder.Finish()
			builder.Close()
			builder = table.NewTableBuilder(bopts)
			return nil
		}
		tbl, err := sw.createConvergedTable(builder)
		if err != nil {
			builder.Close()
			return err
		}
		out = append(out, tbl)
		builder = table.NewTableBuilder(bopts)
		return nil
	}

	mi.Rewind()
	for mi.Valid() {
		key := mi.Key()
		userKey := y.ParseKey(key)
		if !bytes.Equal(userKey, lastUser) {
			// New user-key group: rotate table on capacity only at group
			// boundaries, then decide whether the group is in the session
			// range. The first version seen is the newest.
			if builder.ReachedCapacity() {
				if err := flushBuilder(); err != nil {
					releaseTables(out)
					return nil, err
				}
			}
			within = inRange(userKey)
			versionsN = 0
			lastUser = y.SafeCopy(lastUser[:0], userKey)
		}
		versionsN++
		if within && versionsN > 1 {
			mi.Next()
			continue // Converge to the newest version inside the range.
		}

		vs := mi.Value()
		var vpLen uint32
		if vs.Meta&bitValuePointer > 0 {
			var vp valuePointer
			vp.Decode(vs.Value)
			vpLen = vp.Len
		}
		builder.Add(key, vs, vpLen)
		mi.Next()
	}
	if err := flushBuilder(); err != nil {
		releaseTables(out)
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("ingest convergence produced no tables for a non-empty feed")
	}
	return out, nil
}

func (sw *StreamWriter) createConvergedTable(builder *table.Builder) (*table.Table, error) {
	fileID := sw.db.lc.reserveFileID()
	if sw.db.opt.InMemory {
		data := builder.Finish()
		tbl, err := table.OpenInMemoryTable(data, fileID, builder.Opts())
		if err != nil {
			return nil, err
		}
		return tbl, nil
	}
	fname := table.NewFilename(fileID, sw.db.opt.Dir)
	tbl, err := table.CreateTable(fname, builder)
	if err != nil {
		return nil, err
	}
	sw.db.opt.Infof("Converged ingest table created: %d. Size: %s\n",
		fileID, humanize.IBytes(uint64(tbl.Size())))
	return tbl, nil
}

// abortBuilt releases every table gathered/built while planning a rewrite,
// after a failure before publication. Staged tables are untouched: they remain
// owned by the session until Cancel/Resume/Discard.
func (p *incrementalPlan) abortBuilt() {
	if p == nil {
		return
	}
	// Only converged tables built by us carry a ref to release; gathered old
	// tables remain owned by their level handlers (compactions are stopped).
	for _, t := range p.finalTables {
		_ = t.DecrRef()
	}
}

func releaseTables(tbls []*table.Table) {
	for _, t := range tbls {
		_ = t.DecrRef()
	}
}

// Cancel signals all goroutines to exit and abandons the writer's session.
// Unlike the historical behaviour, Cancel also removes every staged table and
// the active session journal, so no half-finished table is left behind and no
// unfinished session is reported on reopen. Calling Cancel after a successful
// Flush is a harmless no-op.
func (sw *StreamWriter) Cancel() {
	sw.writeLock.Lock()
	for _, writer := range sw.writers {
		if writer != nil {
			writer.closer.Signal()
		}
	}
	sw.writeLock.Unlock()

	sw.writeLock.Lock()
	for _, writer := range sw.writers {
		if writer != nil {
			writer.closer.Wait()
		}
	}

	if err := sw.throttle.Finish(); err != nil {
		sw.db.opt.Errorf("error in throttle.Finish: %+v", err)
	}

	if sw.journal != nil && !sw.committed {
		sw.discardStagedLocked()
	}
	if sw.done != nil {
		sw.done()
	}
	sw.writeLock.Unlock()
}

// abortForClose releases resources of an unfinished session while the DB is
// closing. It first balances the quiescing Begin performed (resuming blocked
// writes/flushes/compactions) so the normal shutdown path signals live
// closers exactly once. Staged SST files and the journal are preserved on
// purpose: the next open reverts unreferenced tables and reports the session
// pending. Unlike Cancel, it must not delete files.
func (sw *StreamWriter) abortForClose() {
	sw.writeLock.Lock()
	for _, writer := range sw.writers {
		if writer != nil {
			writer.closer.Signal()
		}
	}
	for _, writer := range sw.writers {
		if writer != nil {
			writer.closer.Wait()
		}
	}
	_ = sw.throttle.Finish()

	sw.stageMu.Lock()
	staged := append([]*stagedTable(nil), sw.staged...)
	sw.stageMu.Unlock()
	for _, st := range staged {
		// Close releases mmap/OS handles without deleting the file.
		_ = st.Table.Close(-1)
	}
	if sw.journal != nil {
		_ = sw.journal.close()
	}

	// Resume writes/flushes/compactions (once); Close stops them next. Guarded
	// by sw.done's sync.Once in the normal lifecycle.
	done := sw.done
	sw.writeLock.Unlock()
	if done != nil {
		done()
	}
}

// discardStagedLocked removes all staged tables (files and in-memory handles)
// and the active journal. Staged tables are never in the MANIFEST, so no
// manifest changes are needed. Caller must hold sw.writeLock.
func (sw *StreamWriter) discardStagedLocked() {
	sw.stageMu.Lock()
	staged := append([]*stagedTable(nil), sw.staged...)
	sw.staged = nil
	sw.stageMu.Unlock()

	// Staged refs are only ours to release while the commit has not consumed
	// them. After a publish (even if Flush later failed at the journal-rename
	// step) the tables are MANIFEST-owned and must not be discarded here; the
	// reopen reconciles the committing journal instead.
	if !sw.refsReleased {
		for _, st := range staged {
			if err := st.Table.DecrRef(); err != nil {
				sw.db.opt.Errorf("while discarding staged table %d: %v", st.ID, err)
			}
		}
	}
	if sw.journal != nil {
		id := sw.journal.id()
		if err := sw.journal.remove(); err != nil {
			sw.db.opt.Errorf("while removing ingest journal %s: %v", id, err)
		}
		sw.db.dropLiveIngest(id)
	}
}

type sortedWriter struct {
	db       *DB
	sw       *StreamWriter
	throttle *y.Throttle
	opts     table.Options

	builder  *table.Builder
	lastKey  []byte
	level    int
	streamID uint32
	reqCh    chan *request
	// Have separate closer for each writer, as it can be closed at any time.
	closer *z.Closer
}

func (sw *StreamWriter) newWriter(streamID uint32) (*sortedWriter, error) {
	bopts := buildTableOptions(sw.db)
	for i := 2; i < sw.db.opt.MaxLevels; i++ {
		bopts.TableSize *= uint64(sw.db.opt.TableSizeMultiplier)
	}
	w := &sortedWriter{
		db:       sw.db,
		sw:       sw,
		opts:     bopts,
		streamID: streamID,
		throttle: sw.throttle,
		builder:  table.NewTableBuilder(bopts),
		reqCh:    make(chan *request, 3),
		closer:   z.NewCloser(1),
		level:    sw.targetLevel,
	}

	go w.handleRequests()
	return w, nil
}

func (w *sortedWriter) handleRequests() {
	defer w.closer.Done()

	process := func(req *request) {
		for i, e := range req.Entries {
			// If badger is running in InMemory mode, len(req.Ptrs) == 0.
			var vs y.ValueStruct
			if e.skipVlogAndSetThreshold(w.db.valueThreshold()) {
				vs = y.ValueStruct{
					Value:     e.Value,
					Meta:      e.meta,
					UserMeta:  e.UserMeta,
					ExpiresAt: e.ExpiresAt,
				}
			} else {
				vptr := req.Ptrs[i]
				vs = y.ValueStruct{
					Value:     vptr.Encode(),
					Meta:      e.meta | bitValuePointer,
					UserMeta:  e.UserMeta,
					ExpiresAt: e.ExpiresAt,
				}
			}
			if err := w.Add(e.Key, vs); err != nil {
				panic(err)
			}
		}
	}

	for {
		select {
		case req := <-w.reqCh:
			process(req)
		case <-w.closer.HasBeenClosed():
			close(w.reqCh)
			for req := range w.reqCh {
				process(req)
			}
			return
		}
	}
}

// Add adds key and vs to sortedWriter.
func (w *sortedWriter) Add(key []byte, vs y.ValueStruct) error {
	if len(w.lastKey) > 0 && y.CompareKeys(key, w.lastKey) <= 0 {
		return fmt.Errorf("keys not in sorted order (last key: %s, key: %s)",
			hex.Dump(w.lastKey), hex.Dump(key))
	}

	sameKey := y.SameKey(key, w.lastKey)

	// Same keys should go into the same SSTable.
	if !sameKey && w.builder.ReachedCapacity() {
		if err := w.send(false); err != nil {
			return err
		}
	}

	w.lastKey = y.SafeCopy(w.lastKey, key)
	var vp valuePointer
	if vs.Meta&bitValuePointer > 0 {
		vp.Decode(vs.Value)
	}

	w.builder.Add(key, vs, vp.Len)
	return nil
}

func (w *sortedWriter) send(done bool) error {
	if err := w.throttle.Do(); err != nil {
		return err
	}
	go func(builder *table.Builder) {
		err := w.createStagedTable(builder)
		w.throttle.Done(err)
	}(w.builder)
	// If done is true, this indicates we can close the writer.
	// No need to allocate underlying TableBuilder now.
	if done {
		w.builder = nil
		return nil
	}

	w.builder = table.NewTableBuilder(w.opts)
	return nil
}

// Done is called once we are done writing all keys and valueStructs
// to sortedWriter. It completes writing current SST to disk.
func (w *sortedWriter) Done() error {
	if w.builder.Empty() {
		w.builder.Close()
		// Assign builder as nil, so that underlying memory can be garbage collected.
		w.builder = nil
		return nil
	}

	return w.send(true)
}

// createStagedTable builds the SST and records it in the session journal, but
// deliberately does NOT touch the MANIFEST or the in-memory levels: the table
// stays invisible until Flush commits the session.
func (w *sortedWriter) createStagedTable(builder *table.Builder) error {
	defer builder.Close()
	if builder.Empty() {
		builder.Finish()
		return nil
	}

	fileID := w.db.lc.reserveFileID()
	var tbl *table.Table
	if w.db.opt.InMemory {
		data := builder.Finish()
		var err error
		if tbl, err = table.OpenInMemoryTable(data, fileID, builder.Opts()); err != nil {
			return err
		}
	} else {
		var err error
		fname := table.NewFilename(fileID, w.db.opt.Dir)
		if tbl, err = table.CreateTable(fname, builder); err != nil {
			return err
		}
	}

	st := &stagedTable{
		ID:       tbl.ID(),
		Table:    tbl,
		Smallest: y.Copy(tbl.Smallest()),
		Biggest:  y.Copy(tbl.Biggest()),
	}
	if w.sw.journal == nil {
		// Defensive: without a journal there is no session to stage into.
		_ = tbl.DecrRef()
		return fmt.Errorf("no active ingest session")
	}
	if err := w.sw.journal.appendStage(*st); err != nil {
		_ = tbl.DecrRef()
		return err
	}

	w.sw.stageMu.Lock()
	w.sw.staged = append(w.sw.staged, st)
	w.sw.stageMu.Unlock()

	w.db.opt.Infof("Table staged: %d for stream: %d at target level: %d. Size: %s\n",
		fileID, w.streamID, w.level, humanize.IBytes(uint64(tbl.Size())))
	return nil
}
