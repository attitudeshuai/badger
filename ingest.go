/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

// stagedTable is an SST produced during ingest staging. It exists on disk and
// is tracked by the session journal, but until commit it is neither in the
// MANIFEST nor in any in-memory level handler, so no reader can see it.
type stagedTable struct {
	ID       uint64
	Table    *table.Table
	Smallest []byte // Internal key (user key + timestamp).
	Biggest  []byte // Internal key (user key + timestamp).
}

// IngestMode identifies the kind of bulk-ingest session owned by a StreamWriter.
type IngestMode uint8

const (
	// IngestFull is the bootstrap/full-restore mode. All previously committed
	// data is dropped when the session begins; staged tables land on the last
	// level. It mirrors the historical StreamWriter.Prepare() behaviour.
	IngestFull IngestMode = 1
	// IngestIncremental writes one level above the current base level and, at
	// commit, converges any overlapping older levels onto the newest version of
	// every key. It mirrors StreamWriter.PrepareIncremental().
	IngestIncremental IngestMode = 2
)

func (m IngestMode) String() string {
	switch m {
	case IngestFull:
		return "full"
	case IngestIncremental:
		return "incremental"
	}
	return "unknown"
}

// IngestPhase is the durable state of an ingest session, derived solely from
// its on-disk journal. It is the single classification shared by the full and
// incremental ingest paths, database open/recovery and all operational
// tooling.
type IngestPhase uint8

const (
	// IngestStaging means the session has begun (tables may be staged) but its
	// commit record has not been written. It is unfinished.
	IngestStaging IngestPhase = 1
	// IngestCommitting means the commit record exists but reconciliation
	// against the MANIFEST has not observed all final tables yet. It is still
	// unfinished and may be continued or discarded.
	IngestCommitting IngestPhase = 2
	// IngestCommitted means the session finished durably (the journal was
	// renamed to its .done form, or a committing journal was reconciled
	// against a MANIFEST that already contains every final table).
	IngestCommitted IngestPhase = 3
)

func (p IngestPhase) String() string {
	switch p {
	case IngestStaging:
		return "staging"
	case IngestCommitting:
		return "committing"
	case IngestCommitted:
		return "committed"
	}
	return "unknown"
}

// Pending reports whether the session still needs an explicit Continue
// (StreamWriter.Resume) or Discard (DB.DiscardIngestSession) decision.
func (p IngestPhase) Pending() bool {
	return p == IngestStaging || p == IngestCommitting
}

// IngestKeyRange is a user-key interval (timestamps stripped). Empty bounds
// mean "unbounded" on that side.
type IngestKeyRange struct {
	Left  []byte
	Right []byte
}

// IngestOptions configures a replayable bulk-ingest session.
type IngestOptions struct {
	// SessionID is the idempotency key of the session. It must consist of
	// filename-safe characters (hexadecimal is recommended). When empty, a
	// random ID is generated.
	//
	// Resuming an unfinished session with its ID re-runs the feed against the
	// same contract and converges to a single successful run. Reusing the ID of
	// an already committed session returns ErrIngestAlreadyCommitted and
	// creates no new tables.
	SessionID string

	// DeclaredRange optionally states the user-key interval the feed is
	// contractually confined to. When set, every staged key must fall inside
	// it; the bounds are recorded in the journal at session begin. When empty,
	// the session range is observed from the staged tables instead.
	DeclaredRange IngestKeyRange
}

// IngestSessionInfo describes a bulk-ingest session as recorded on disk.
type IngestSessionInfo struct {
	ID            string
	Mode          IngestMode
	Phase         IngestPhase
	TargetLevel   int
	Range         IngestKeyRange // Effective range: declared bounds if given, else observed.
	DeclaredRange IngestKeyRange
	// StagedTableIDs lists tables produced during staging. They never appear
	// in the MANIFEST until the session commits.
	StagedTableIDs []uint64
	// FinalTableIDs are the tables the converged state consists of (present in
	// the COMMIT record; empty until committing/committed).
	FinalTableIDs []uint64
	// DroppedTableIDs are older tables removed by the converging commit.
	DroppedTableIDs []uint64
	NumTables       int
	StartedAt       time.Time
	FinishedAt      time.Time
}

// Pending reports whether the session is unfinished.
func (s IngestSessionInfo) Pending() bool { return s.Phase.Pending() }

const (
	ingestFilePrefix = "INGEST-"
	ingestDoneSuffix = ".done"

	// ingestMagicText can never change.
	ingestMagicText = "BdIr"
	ingestVersion   = uint16(1)

	ingestFrameBegin  = byte(1)
	ingestFrameStage  = byte(2)
	ingestFrameCommit = byte(3)

	ingestIVLen = 16
)

// ingestJournalPath returns the path of the active (unfinished) journal.
func ingestJournalPath(dir, id string) string {
	return filepath.Join(dir, ingestFilePrefix+id)
}

// ingestDonePath returns the path of the committed-session marker journal.
func ingestDonePath(dir, id string) string {
	return ingestJournalPath(dir, id) + ingestDoneSuffix
}

// validIngestID enforces a filename-safe charset so a session ID can never
// escape the data directory or collide with the .done suffix.
func validIngestID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return !strings.HasSuffix(id, ingestDoneSuffix)
}

func newIngestSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		y.Check(err)
	}
	return fmt.Sprintf("%x", b[:])
}

// ----------------------------------------------------------------------------
// Frame codec
// ----------------------------------------------------------------------------

type ingestEncoder struct {
	buf []byte
}

func (e *ingestEncoder) u8(v uint8) {
	e.buf = append(e.buf, v)
}

func (e *ingestEncoder) u16(v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	e.buf = append(e.buf, b[:]...)
}

func (e *ingestEncoder) u32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	e.buf = append(e.buf, b[:]...)
}

func (e *ingestEncoder) u64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	e.buf = append(e.buf, b[:]...)
}

func (e *ingestEncoder) bytes(v []byte) {
	e.u32(uint32(len(v)))
	e.buf = append(e.buf, v...)
}

func (e *ingestEncoder) str(v string) {
	e.bytes([]byte(v))
}

type ingestDecoder struct {
	buf []byte
}

func (d *ingestDecoder) u8() (uint8, error) {
	if len(d.buf) < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	v := d.buf[0]
	d.buf = d.buf[1:]
	return v, nil
}

func (d *ingestDecoder) u32() (uint32, error) {
	if len(d.buf) < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint32(d.buf)
	d.buf = d.buf[4:]
	return v, nil
}

func (d *ingestDecoder) u64() (uint64, error) {
	if len(d.buf) < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint64(d.buf)
	d.buf = d.buf[8:]
	return v, nil
}

func (d *ingestDecoder) bytesCopy() ([]byte, error) {
	n, err := d.u32()
	if err != nil {
		return nil, err
	}
	if uint32(len(d.buf)) < n {
		return nil, io.ErrUnexpectedEOF
	}
	v := y.Copy(d.buf[:n])
	d.buf = d.buf[n:]
	return v, nil
}

func (d *ingestDecoder) str() (string, error) {
	v, err := d.bytesCopy()
	return string(v), err
}

// encodeBegin builds the plaintext body of a BEGIN frame.
func encodeBegin(id string, mode IngestMode, targetLevel int, startedAt time.Time,
	declared IngestKeyRange) []byte {
	var e ingestEncoder
	e.str(id)
	e.u8(uint8(mode))
	e.u32(uint32(targetLevel))
	e.u64(uint64(startedAt.UnixNano()))
	hasDeclared := len(declared.Left) > 0 || len(declared.Right) > 0
	e.u8(boolToByte(hasDeclared))
	if hasDeclared {
		e.bytes(declared.Left)
		e.bytes(declared.Right)
	}
	return e.buf
}

func decodeBegin(body []byte) (string, IngestMode, int, time.Time, IngestKeyRange, error) {
	d := ingestDecoder{buf: body}
	id, err := d.str()
	if err != nil {
		return "", 0, 0, time.Time{}, IngestKeyRange{}, err
	}
	mode, err := d.u8()
	if err != nil {
		return "", 0, 0, time.Time{}, IngestKeyRange{}, err
	}
	lvl, err := d.u32()
	if err != nil {
		return "", 0, 0, time.Time{}, IngestKeyRange{}, err
	}
	ts, err := d.u64()
	if err != nil {
		return "", 0, 0, time.Time{}, IngestKeyRange{}, err
	}
	var declared IngestKeyRange
	hasDeclared, err := d.u8()
	if err != nil {
		return "", 0, 0, time.Time{}, IngestKeyRange{}, err
	}
	if hasDeclared != 0 {
		if declared.Left, err = d.bytesCopy(); err != nil {
			return "", 0, 0, time.Time{}, IngestKeyRange{}, err
		}
		if declared.Right, err = d.bytesCopy(); err != nil {
			return "", 0, 0, time.Time{}, IngestKeyRange{}, err
		}
	}
	return id, IngestMode(mode), int(lvl), time.Unix(0, int64(ts)), declared, nil
}

func encodeStage(tableID uint64, smallest, biggest []byte, seqno uint32) []byte {
	var e ingestEncoder
	e.u64(tableID)
	e.bytes(smallest)
	e.bytes(biggest)
	e.u32(seqno)
	return e.buf
}

func decodeStage(body []byte) (tableID uint64, smallest, biggest []byte, seqno uint32, err error) {
	d := ingestDecoder{buf: body}
	if tableID, err = d.u64(); err != nil {
		return
	}
	if smallest, err = d.bytesCopy(); err != nil {
		return
	}
	if biggest, err = d.bytesCopy(); err != nil {
		return
	}
	if seqno, err = d.u32(); err != nil {
		return
	}
	return
}

func encodeCommit(committedAt time.Time, finalIDs, droppedIDs []uint64) []byte {
	var e ingestEncoder
	e.u64(uint64(committedAt.UnixNano()))
	e.u32(uint32(len(finalIDs)))
	for _, id := range finalIDs {
		e.u64(id)
	}
	e.u32(uint32(len(droppedIDs)))
	for _, id := range droppedIDs {
		e.u64(id)
	}
	return e.buf
}

func decodeCommit(body []byte) (time.Time, []uint64, []uint64, error) {
	d := ingestDecoder{buf: body}
	ts, err := d.u64()
	if err != nil {
		return time.Time{}, nil, nil, err
	}
	readIDs := func() ([]uint64, error) {
		n, err := d.u32()
		if err != nil {
			return nil, err
		}
		out := make([]uint64, n)
		for i := range out {
			if out[i], err = d.u64(); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	finalIDs, err := readIDs()
	if err != nil {
		return time.Time{}, nil, nil, err
	}
	droppedIDs, err := readIDs()
	if err != nil {
		return time.Time{}, nil, nil, err
	}
	return time.Unix(0, int64(ts)), finalIDs, droppedIDs, nil
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// ----------------------------------------------------------------------------
// Journal writer
// ----------------------------------------------------------------------------

// ingestJournal is an append-only session journal. The zero value with a nil fp
// represents an InMemory session whose metadata lives only in memory.
type ingestJournal struct {
	dir       string
	opt       Options
	registry  *KeyRegistry
	fp        *os.File
	mu        sync.Mutex
	info      IngestSessionInfo
	observed  IngestKeyRange // User-key bounds observed across staged tables.
	dataKeyID uint64
	dataKey   []byte
	seqno     uint32
}

// createIngestJournal writes a new active journal with a BEGIN frame. The
// caller must have validated the session ID and ensured no other active
// session exists.
func createIngestJournal(opt Options, registry *KeyRegistry, id string, mode IngestMode,
	targetLevel int, declared IngestKeyRange) (*ingestJournal, error) {
	if !validIngestID(id) {
		return nil, fmt.Errorf("invalid ingest session ID %q", id)
	}
	info := IngestSessionInfo{
		ID:            id,
		Mode:          mode,
		Phase:         IngestStaging,
		TargetLevel:   targetLevel,
		DeclaredRange: declared,
		Range:         declared,
		StartedAt:     time.Now(),
	}
	j := &ingestJournal{opt: opt, registry: registry, info: info}

	if opt.InMemory {
		return j, nil
	}
	j.dir = opt.Dir
	active := ingestJournalPath(j.dir, id)
	if _, err := os.Stat(active); err == nil {
		return nil, fmt.Errorf("ingest session %q already exists on disk", id)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if _, err := os.Stat(ingestDonePath(j.dir, id)); err == nil {
		return nil, ErrIngestAlreadyCommitted
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	// Pick the encryption context (if any) before writing the header.
	if err := j.selectDataKey(); err != nil {
		return nil, err
	}

	fp, err := y.OpenTruncFile(active, true)
	if err != nil {
		return nil, y.Wrapf(err, "while creating ingest journal %s", active)
	}
	j.fp = fp
	if _, err := fp.Write(ingestHeader()); err != nil {
		_ = fp.Close()
		return nil, err
	}
	if err := j.appendFrameLocked(ingestFrameBegin,
		encodeBegin(id, mode, targetLevel, info.StartedAt, declared)); err != nil {
		_ = fp.Close()
		_ = os.Remove(active)
		return nil, err
	}
	return j, nil
}

func ingestHeader() []byte {
	buf := make([]byte, 6)
	copy(buf[0:4], ingestMagicText)
	binary.BigEndian.PutUint16(buf[4:6], ingestVersion)
	return buf
}

// selectDataKey chooses the datakey used to encrypt frame bodies. It is a
// no-op without encryption or in InMemory mode.
func (j *ingestJournal) selectDataKey() error {
	if j.opt.InMemory || j.registry == nil || len(j.opt.EncryptionKey) == 0 {
		return nil
	}
	dk, err := j.registry.LatestDataKey()
	if err != nil {
		return err
	}
	if dk == nil {
		return nil
	}
	j.dataKeyID = dk.KeyId
	j.dataKey = dk.Data
	return nil
}

// appendFrame writes one length+CRC framed payload. The payload layout is:
// kind byte, dataKeyId u64, then either an IV followed by the AES-CTR
// ciphertext or the plaintext body.
//
// Caller need not hold j.mu.
func (j *ingestJournal) appendFrame(kind byte, plainBody []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.appendFrameLocked(kind, plainBody)
}

func (j *ingestJournal) appendFrameLocked(kind byte, plainBody []byte) error {
	payload := []byte{kind}
	var u64b [8]byte
	binary.BigEndian.PutUint64(u64b[:], j.dataKeyID)
	payload = append(payload, u64b[:]...)
	if j.dataKeyID != 0 {
		iv := make([]byte, ingestIVLen)
		if _, err := rand.Read(iv); err != nil {
			return err
		}
		ct, err := y.XORBlockAllocate(plainBody, j.dataKey, iv)
		if err != nil {
			return err
		}
		payload = append(payload, iv...)
		payload = append(payload, ct...)
	} else {
		payload = append(payload, plainBody...)
	}

	if j.fp == nil {
		// InMemory journal: keep no frames, only the decoded info is tracked.
		return nil
	}
	var frame [8]byte
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(payload, y.CastagnoliCrcTable))
	if _, err := j.fp.Write(frame[:]); err != nil {
		return err
	}
	if _, err := j.fp.Write(payload); err != nil {
		return err
	}
	return j.fp.Sync()
}

// appendStage records one durably staged table and refreshes the observed
// range. When the session declared a range, the effective Range stays the
// declared one (callers validate every incoming key against it); otherwise
// the effective range is the union of observed table bounds.
func (j *ingestJournal) appendStage(tbl stagedTable) error {
	j.mu.Lock()
	j.seqno++
	seqno := j.seqno
	left := y.ParseKey(tbl.Smallest)
	right := y.ParseKey(tbl.Biggest)
	if len(j.observed.Left) == 0 || bytes.Compare(left, j.observed.Left) < 0 {
		j.observed.Left = y.Copy(left)
	}
	if len(j.observed.Right) == 0 || bytes.Compare(right, j.observed.Right) > 0 {
		j.observed.Right = y.Copy(right)
	}
	if len(j.info.DeclaredRange.Left) == 0 && len(j.info.DeclaredRange.Right) == 0 {
		j.info.Range.Left = y.Copy(j.observed.Left)
		j.info.Range.Right = y.Copy(j.observed.Right)
	}
	j.info.StagedTableIDs = append(j.info.StagedTableIDs, tbl.ID)
	j.info.NumTables = len(j.info.StagedTableIDs)
	j.mu.Unlock()

	return j.appendFrame(ingestFrameStage,
		encodeStage(tbl.ID, tbl.Smallest, tbl.Biggest, seqno))
}

// appendCommit records the converged outcome and flips the durable phase to
// committing. It happens BEFORE the MANIFEST changeset is written (two-phase
// commit).
func (j *ingestJournal) appendCommit(finalIDs, droppedIDs []uint64) error {
	j.mu.Lock()
	j.info.Phase = IngestCommitting
	j.info.FinalTableIDs = append(j.info.FinalTableIDs[:0], finalIDs...)
	j.info.DroppedTableIDs = append(j.info.DroppedTableIDs[:0], droppedIDs...)
	j.info.FinishedAt = time.Now()
	j.mu.Unlock()

	return j.appendFrame(ingestFrameCommit,
		encodeCommit(j.info.FinishedAt, finalIDs, droppedIDs))
}

// id returns the immutable session identifier.
func (j *ingestJournal) id() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.info.ID
}

// snapshot returns a copy of the current session info.
func (j *ingestJournal) snapshot() IngestSessionInfo {
	j.mu.Lock()
	defer j.mu.Unlock()
	info := j.info
	info.StagedTableIDs = append([]uint64(nil), j.info.StagedTableIDs...)
	info.FinalTableIDs = append([]uint64(nil), j.info.FinalTableIDs...)
	info.DroppedTableIDs = append([]uint64(nil), j.info.DroppedTableIDs...)
	info.Range.Left = y.Copy(j.info.Range.Left)
	info.Range.Right = y.Copy(j.info.Range.Right)
	info.DeclaredRange.Left = y.Copy(j.info.DeclaredRange.Left)
	info.DeclaredRange.Right = y.Copy(j.info.DeclaredRange.Right)
	return info
}

// markCommitted flips the in-memory phase and renames the active journal to the
// durable .done marker. It must be called only after the converged MANIFEST and
// directory entries are durable.
func (j *ingestJournal) markCommitted() error {
	j.mu.Lock()
	j.info.Phase = IngestCommitted
	j.mu.Unlock()

	if j.fp == nil {
		return nil
	}
	if err := j.fp.Sync(); err != nil {
		return err
	}
	// Windows requires the file to be closed before it can be renamed.
	if err := j.fp.Close(); err != nil {
		return err
	}
	j.fp = nil
	active := ingestJournalPath(j.dir, j.info.ID)
	done := ingestDonePath(j.dir, j.info.ID)
	if err := os.Rename(active, done); err != nil {
		return err
	}
	return syncDir(j.dir)
}

func (j *ingestJournal) close() error {
	if j.fp == nil {
		return nil
	}
	err := j.fp.Close()
	j.fp = nil
	return err
}

// remove deletes the active journal file if it still exists. A .done marker is
// never removed by this path.
func (j *ingestJournal) remove() error {
	if j.fp != nil {
		if err := j.fp.Close(); err != nil {
			return err
		}
		j.fp = nil
	}
	if j.opt.InMemory || j.dir == "" {
		return nil
	}
	active := ingestJournalPath(j.dir, j.info.ID)
	if err := os.Remove(active); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(j.dir)
}

// ----------------------------------------------------------------------------
// Journal reading and classification
// ----------------------------------------------------------------------------

type ingestJournalFile struct {
	name     string
	id       string
	done     bool
	info     IngestSessionInfo
	observed IngestKeyRange
	complete bool // a valid BEGIN frame was observed
}

// readIngestJournalFile parses one journal file. A torn trailing frame (short
// read or CRC mismatch on the final frame, as left by a crash mid-append) is
// truncated/ignored just like a trailing partial MANIFEST record. CRC errors on
// any other frame are fatal.
func readIngestJournalFile(path, name string, registry *KeyRegistry) (*ingestJournalFile, error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	r := bufio.NewReader(fp)
	var header [6]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("ingest journal %s: bad header: %w", name, err)
	}
	if !bytes.Equal(header[0:4], []byte(ingestMagicText)) {
		return nil, fmt.Errorf("ingest journal %s: bad magic", name)
	}
	version := binary.BigEndian.Uint16(header[4:6])
	if version != ingestVersion {
		return nil, fmt.Errorf("ingest journal %s: unsupported version %d", name, version)
	}

	jf := &ingestJournalFile{name: name}
	if strings.HasSuffix(name, ingestDoneSuffix) {
		jf.done = true
		jf.id = strings.TrimSuffix(strings.TrimPrefix(name, ingestFilePrefix), ingestDoneSuffix)
	} else {
		jf.id = strings.TrimPrefix(name, ingestFilePrefix)
	}

	frameIdx := 0
	for {
		var lenCrc [8]byte
		if _, err := io.ReadFull(r, lenCrc[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		length := binary.BigEndian.Uint32(lenCrc[0:4])
		if length > 64<<20 {
			return nil, fmt.Errorf("ingest journal %s: implausible frame size %d", name, length)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			// Torn frame at tail: stop without applying it.
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		if crc32.Checksum(payload, y.CastagnoliCrcTable) !=
			binary.BigEndian.Uint32(lenCrc[4:8]) {
			// A CRC failure is tolerated only for the final (torn) frame.
			if peekEOF(r) {
				break
			}
			return nil, fmt.Errorf("ingest journal %s: checksum mismatch", name)
		}
		if err := jf.applyFrame(payload, registry); err != nil {
			return nil, fmt.Errorf("ingest journal %s: %w", name, err)
		}
		frameIdx++
	}
	if !jf.complete {
		return nil, fmt.Errorf("ingest journal %s: missing BEGIN frame", name)
	}
	if len(jf.info.DeclaredRange.Left) == 0 && len(jf.info.DeclaredRange.Right) == 0 {
		jf.info.Range = jf.observed
	}
	if jf.done {
		jf.info.Phase = IngestCommitted
	}
	return jf, nil
}

func peekEOF(r *bufio.Reader) bool {
	_, err := r.ReadByte()
	return err != nil
}

func (j *ingestJournalFile) applyFrame(payload []byte, registry *KeyRegistry) error {
	if len(payload) < 9 {
		return io.ErrUnexpectedEOF
	}
	kind := payload[0]
	dataKeyID := binary.BigEndian.Uint64(payload[1:9])
	body := payload[9:]
	if dataKeyID != 0 {
		if registry == nil {
			return fmt.Errorf("encrypted frame but no key registry available")
		}
		dk, err := registry.DataKey(dataKeyID)
		if err != nil {
			return err
		}
		if dk == nil {
			return fmt.Errorf("data key %d not found for ingest journal", dataKeyID)
		}
		if len(body) < ingestIVLen {
			return io.ErrUnexpectedEOF
		}
		iv := body[:ingestIVLen]
		body, err = y.XORBlockAllocate(body[ingestIVLen:], dk.Data, iv)
		if err != nil {
			return err
		}
	}

	switch kind {
	case ingestFrameBegin:
		id, mode, level, startedAt, declared, err := decodeBegin(body)
		if err != nil {
			return err
		}
		if id != j.id {
			return fmt.Errorf("session id mismatch: file=%s frame=%s", j.id, id)
		}
		j.info = IngestSessionInfo{
			ID:            id,
			Mode:          mode,
			Phase:         IngestStaging,
			TargetLevel:   level,
			DeclaredRange: declared,
			Range:         declared,
			StartedAt:     startedAt,
		}
		j.complete = true
	case ingestFrameStage:
		tableID, smallest, biggest, _, err := decodeStage(body)
		if err != nil {
			return err
		}
		j.info.StagedTableIDs = append(j.info.StagedTableIDs, tableID)
		j.extendObserved(y.ParseKey(smallest), y.ParseKey(biggest))
	case ingestFrameCommit:
		finishedAt, finalIDs, droppedIDs, err := decodeCommit(body)
		if err != nil {
			return err
		}
		j.info.Phase = IngestCommitting
		j.info.FinishedAt = finishedAt
		j.info.FinalTableIDs = finalIDs
		j.info.DroppedTableIDs = droppedIDs
	default:
		return fmt.Errorf("unknown frame kind %d", kind)
	}
	j.info.NumTables = len(j.info.StagedTableIDs)
	return nil
}

func (j *ingestJournalFile) extendObserved(left, right []byte) {
	if len(j.observed.Left) == 0 || bytes.Compare(left, j.observed.Left) < 0 {
		j.observed.Left = y.Copy(left)
	}
	if len(j.observed.Right) == 0 || bytes.Compare(right, j.observed.Right) > 0 {
		j.observed.Right = y.Copy(right)
	}
}

// reconcileIngestPhase applies the single reconciliation rule against the
// committed MANIFEST: a journal carrying a COMMIT record is only considered
// committed once every final table is present in the MANIFEST. Because a
// MANIFEST changeset is one atomic CRC record, an absent changeset means none
// of the final tables exist and the session stays unfinished (its staged files
// are unreferenced and get reverted on open).
func reconcileIngestPhase(info IngestSessionInfo, manifestTables map[uint64]TableManifest) IngestPhase {
	if info.Phase != IngestCommitting {
		return info.Phase
	}
	// An empty feed commits zero tables and zero manifest changes, so a commit
	// record without final tables is trivially fully committed.
	if len(info.FinalTableIDs) == 0 {
		return IngestCommitted
	}
	for _, id := range info.FinalTableIDs {
		if _, ok := manifestTables[id]; !ok {
			return IngestCommitting
		}
	}
	return IngestCommitted
}

// listIngestJournalFiles scans a directory for active and .done ingest
// journals. Order is by ID, with active journals before their .done marker.
func listIngestJournalFiles(dir string, registry *KeyRegistry) ([]*ingestJournalFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []*ingestJournalFile
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ingestFilePrefix) {
			continue
		}
		jf, err := readIngestJournalFile(filepath.Join(dir, entry.Name()), entry.Name(), registry)
		if err != nil {
			return nil, err
		}
		out = append(out, jf)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].id != out[j].id {
			return out[i].id < out[j].id
		}
		// Active entry before its .done marker (should never coexist).
		return !out[i].done && out[j].done
	})
	return out, nil
}

// ReadIngestSessions inspects a database directory without opening the DB and
// returns all bulk-ingest sessions recorded there, including committed (.done)
// sessions. Committing journals are reconciled against the MANIFEST so that
// operational tooling reports the same phase as a freshly opened database.
//
// This is the read-only entry point used by operational tooling; the in-DB
// equivalents are DB.IngestSessions and DB.PendingIngestSessions.
func ReadIngestSessions(opt Options) ([]IngestSessionInfo, error) {
	if opt.InMemory {
		return nil, nil
	}
	registry, err := OpenKeyRegistry(KeyRegistryOptions{
		Dir:                           opt.Dir,
		ReadOnly:                      true,
		EncryptionKey:                 opt.EncryptionKey,
		EncryptionKeyRotationDuration: opt.EncryptionKeyRotationDuration,
	})
	if err != nil {
		return nil, y.Wrap(err, "while opening key registry for ingest inspection")
	}
	defer func() { _ = registry.Close() }()

	files, err := listIngestJournalFiles(opt.Dir, registry)
	if err != nil {
		return nil, err
	}

	// Reconcile committing journals against the MANIFEST. A missing or
	// unreadable MANIFEST must not hide the session listing: phases are then
	// reported unreconciled (a committing journal stays conservatively
	// pending), which is the safe direction for operational inspection.
	var manifestTables map[uint64]TableManifest
	if fp, err := y.OpenExistingFile(filepath.Join(opt.Dir, ManifestFilename), y.ReadOnly); err == nil {
		manifest, _, replayErr := ReplayManifestFile(fp, opt.ExternalMagicVersion, opt)
		_ = fp.Close()
		if replayErr == nil {
			manifestTables = manifest.Tables
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	infos := make([]IngestSessionInfo, 0, len(files))
	for _, jf := range files {
		info := jf.info
		if !jf.done {
			info.Phase = reconcileIngestPhase(info, manifestTables)
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// liveIngestSession reports whether an unfinished session is currently
// attached to a StreamWriter in this process.
func (db *DB) liveIngestSession(id string) bool {
	db.ingestMu.Lock()
	defer db.ingestMu.Unlock()
	_, ok := db.ingestLive[id]
	return ok
}

// dropLiveIngest detaches a session journal from the live set.
func (db *DB) dropLiveIngest(id string) {
	db.ingestMu.Lock()
	defer db.ingestMu.Unlock()
	delete(db.ingestLive, id)
	delete(db.ingestWriters, id)
}

// registerIngestWriter attaches a StreamWriter to its session id.
func (db *DB) registerIngestWriter(id string, sw *StreamWriter) {
	db.ingestMu.Lock()
	defer db.ingestMu.Unlock()
	db.ingestWriters[id] = sw
}

// noteIngestCommitted records the final session state. On disk the .done marker
// is the durable record; this map mainly serves InMemory mode and within-process
// idempotency checks.
func (db *DB) noteIngestCommitted(info IngestSessionInfo) {
	info.Phase = IngestCommitted
	db.ingestMu.Lock()
	defer db.ingestMu.Unlock()
	delete(db.ingestLive, info.ID)
	delete(db.ingestWriters, info.ID)
	db.ingestHistory[info.ID] = info
}

// recoverIngestSessions reconciles ingest journals found on disk during Open.
//
// Staged tables are never in the MANIFEST, so newLevelsController's
// revertToManifest has already deleted their files. What remains here is pure
// metadata reconciliation: a committing journal whose final tables all made it
// into the MANIFEST is finalized (journal renamed to .done); everything else is
// left visibly pending for an explicit Resume or Discard. The function never
// starts or abandons a session on its own.
func (db *DB) recoverIngestSessions() error {
	if db.opt.InMemory {
		return nil
	}
	files, err := listIngestJournalFiles(db.opt.Dir, db.registry)
	if err != nil {
		return err
	}
	for _, jf := range files {
		if jf.done {
			info := jf.info
			info.Phase = IngestCommitted
			db.ingestHistory[jf.id] = info
			continue
		}

		phase := reconcileIngestPhase(jf.info, db.manifest.manifest.Tables)
		if phase != IngestCommitted {
			// Unfinished: leave both files and journal untouched. Read-only
			// opens land here too; they never mutate the directory.
			db.opt.Infof("Found unfinished ingest session %s in phase %s; "+
				"use StreamWriter.Resume or DB.DiscardIngestSession\n", jf.id, jf.info.Phase)
			continue
		}

		if db.opt.ReadOnly {
			// Report the reconciled truth without rewriting anything.
			continue
		}

		// Finalize: remove any stray staged files not referenced by the
		// MANIFEST, then rename the journal to its .done marker.
		if err := removeStagedTables(db.opt,
			unreferencedIDs(jf.info.StagedTableIDs, db.manifest.manifest.Tables)); err != nil {
			return err
		}
		active := ingestJournalPath(db.opt.Dir, jf.id)
		if err := os.Rename(active, ingestDonePath(db.opt.Dir, jf.id)); err != nil {
			return err
		}
		info := jf.info
		info.Phase = IngestCommitted
		db.ingestHistory[jf.id] = info
		db.opt.Infof("Recovered committed ingest session %s\n", jf.id)
	}
	return syncDir(db.opt.Dir)
}

func unreferencedIDs(ids []uint64, tables map[uint64]TableManifest) []uint64 {
	var out []uint64
	for _, id := range ids {
		if _, ok := tables[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// IngestSessions returns all bulk-ingest sessions visible to this DB,
// including committed (.done) sessions. Phases are reconciled against the
// current MANIFEST, so the result is identical to what operational tooling
// reports via ReadIngestSessions.
func (db *DB) IngestSessions() ([]IngestSessionInfo, error) {
	if db.opt.InMemory {
		db.ingestMu.Lock()
		defer db.ingestMu.Unlock()
		infos := make([]IngestSessionInfo, 0, len(db.ingestHistory)+len(db.ingestLive))
		for _, info := range db.ingestHistory {
			infos = append(infos, info)
		}
		for _, j := range db.ingestLive {
			infos = append(infos, j.snapshot())
		}
		sort.Slice(infos, func(i, j int) bool { return infos[i].ID < infos[j].ID })
		return infos, nil
	}

	files, err := listIngestJournalFiles(db.opt.Dir, db.registry)
	if err != nil {
		return nil, err
	}
	infos := make([]IngestSessionInfo, 0, len(files))
	for _, jf := range files {
		info := jf.info
		if !jf.done {
			info.Phase = reconcileIngestPhase(info, db.manifest.manifest.Tables)
		}
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].ID < infos[j].ID })
	return infos, nil
}

// PendingIngestSessions returns ingest sessions that are neither committed nor
// discarded and therefore require an explicit Resume or Discard decision.
func (db *DB) PendingIngestSessions() ([]IngestSessionInfo, error) {
	all, err := db.IngestSessions()
	if err != nil {
		return nil, err
	}
	var pending []IngestSessionInfo
	for _, info := range all {
		if info.Phase.Pending() {
			pending = append(pending, info)
		}
	}
	return pending, nil
}

// DiscardIngestSession explicitly abandons an unfinished ingest session.
//
// Every table the session staged (and every converged-but-uncommitted table
// recorded in its COMMIT frame) is deleted: none of them are in the MANIFEST
// for an unfinished session, so no half-finished table survives either in the
// MANIFEST or on disk. The active journal is removed and, when a StreamWriter
// for the session is attached in this process, that writer is cancelled.
//
// Already committed sessions and read-only opens return an error. Previously
// committed data is never touched.
func (db *DB) DiscardIngestSession(id string) error {
	if db.opt.ReadOnly {
		return fmt.Errorf("cannot discard ingest session in read-only mode")
	}
	if !validIngestID(id) {
		return fmt.Errorf("invalid ingest session ID %q", id)
	}

	db.ingestMu.Lock()
	writer := db.ingestWriters[id]
	_, live := db.ingestLive[id]
	db.ingestMu.Unlock()
	if live {
		// Cancel performs the full staged-table + journal teardown under the
		// writer lock.
		if writer != nil {
			writer.Cancel()
			return nil
		}
	}

	if db.opt.InMemory {
		db.ingestMu.Lock()
		defer db.ingestMu.Unlock()
		if _, ok := db.ingestHistory[id]; ok {
			return ErrIngestAlreadyCommitted
		}
		return ErrIngestSessionNotFound
	}

	active := ingestJournalPath(db.opt.Dir, id)
	if _, err := os.Stat(active); err != nil {
		if os.IsNotExist(err) {
			if _, statErr := os.Stat(ingestDonePath(db.opt.Dir, id)); statErr == nil {
				return ErrIngestAlreadyCommitted
			}
			return ErrIngestSessionNotFound
		}
		return err
	}
	jf, err := readIngestJournalFile(active, ingestFilePrefix+id, db.registry)
	if err != nil {
		return err
	}
	phase := reconcileIngestPhase(jf.info, db.manifest.manifest.Tables)
	if phase == IngestCommitted {
		return ErrIngestAlreadyCommitted
	}

	// Remove every file the session produced but never committed: staged ids
	// plus the converged ids from a COMMIT record whose MANIFEST changeset did
	// not land.
	ids := append(append([]uint64(nil), jf.info.StagedTableIDs...), jf.info.FinalTableIDs...)
	if err := removeStagedTables(db.opt, ids); err != nil {
		return err
	}
	if err := os.Remove(active); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(db.opt.Dir)
}

// quiesceIngestSessionsOnClose releases OS handles held by unfinished sessions
// during DB close. Journals and staged files are intentionally preserved on
// disk (staged SSTs are deleted by revertToManifest on the next open; the
// journals keep the session pending for inspection/decision).
func (db *DB) quiesceIngestSessionsOnClose() {
	db.ingestMu.Lock()
	ids := make([]string, 0, len(db.ingestLive))
	for id := range db.ingestLive {
		ids = append(ids, id)
	}
	db.ingestMu.Unlock()

	for _, id := range ids {
		db.ingestMu.Lock()
		sw := db.ingestWriters[id]
		db.ingestMu.Unlock()
		if sw != nil {
			sw.abortForClose()
		}
	}
}

// ingestSessionExists reports whether any active (non-.done) journal is
// present for the given session ID.
func ingestSessionActiveOnDisk(dir, id string) (exists, done bool, err error) {
	if _, statErr := os.Stat(ingestJournalPath(dir, id)); statErr == nil {
		return true, false, nil
	} else if !os.IsNotExist(statErr) {
		return false, false, statErr
	}
	if _, statErr := os.Stat(ingestDonePath(dir, id)); statErr == nil {
		return false, true, nil
	} else if !os.IsNotExist(statErr) {
		return false, false, statErr
	}
	return false, false, nil
}

// removeStagedTables deletes the given SST files from disk. Missing files are
// tolerated: on-disk open already removes MANIFEST-unreferenced tables via
// revertToManifest.
func removeStagedTables(opt Options, ids []uint64) error {
	if opt.InMemory {
		return nil
	}
	var firstErr error
	for _, id := range ids {
		fname := table.NewFilename(id, opt.Dir)
		if err := os.Remove(fname); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		firstErr = syncDir(opt.Dir)
	}
	return firstErr
}
