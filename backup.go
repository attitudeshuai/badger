/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// flushThreshold determines when a buffer will be flushed. When performing a
// backup/restore, the entries will be batched up until the total size of batch
// is more than flushThreshold or entry size (without the value size) is more
// than the maxBatchSize.
const flushThreshold = 100 << 20

// -------------------------------------------------------------------------------
// Self-describing backup envelope (format version backupFormatVersion).
//
// A backup file is a concatenation of one or more segments. Each segment is
// produced by one invocation of Stream.Backup (incremental backups append a new
// segment to the same writer). Every segment is self-describing:
//
//	header frame: magic | version | sinceTs | createdAt | key prefix | the key
//	              range split plan (one slot per planned range)
//	chunk frames: magic | slotId | flags | payload length | protobuf KVList
//	              payload | CRC32C of the frame
//	footer frame: magic | version | time range (sinceTs, maxVersion) | a full
//	              index of every chunk (slot, flags, offset, length, checksum) |
//	              CRC32C of the frame
//
// The footer turns a sequence of chunks into an all-or-nothing artifact: a
// backup that was interrupted mid-write has no valid footer, and every chunk
// not followed by the slot's final chunk is provably incomplete. That property
// is what makes chunk-granular resume possible (see Stream.BackupResume).
//
// Files produced by older versions start directly with an 8-byte little-endian
// length (they have no magic); they are recognized automatically and can still
// be loaded without conversion.
// -------------------------------------------------------------------------------

const (
	backupHeaderMagic = "BDGBKHDR" // 8 bytes
	backupChunkMagic  = "BDGBKBLK" // 8 bytes
	backupFooterMagic = "BDGBKFTR" // 8 bytes

	backupFormatVersion uint32 = 1

	backupAlgoCRC32C uint32 = 0

	// backupFlagFinal marks the last chunk of a slot. A slot is only considered
	// fully backed up once its final chunk has been verified.
	backupFlagFinal uint32 = 1

	backupFixedFrameHead = 8 + 4 + 4 // magic | version | payload length
	backupChunkHeadSize  = 8 + 4 + 4 + 8
	backupChunkTailSize  = 4
	backupChunkFrameMin  = backupChunkHeadSize + backupChunkTailSize

	// backupMaxFramePayload is a sanity bound used while decoding frames. Real
	// payloads are bounded by the stream soft limit; this only guards against
	// framing corruption being interpreted as a huge allocation.
	backupMaxFramePayload = 1 << 40
)

// Sentinel errors describing why an envelope backup could not be loaded. They
// are wrapped by BackupCorruptionError, so callers can use errors.Is.
var (
	// ErrBackupTruncated is returned when the backup ends in the middle of a
	// header, chunk or footer frame, or when a valid footer is missing.
	ErrBackupTruncated = errors.New("backup is truncated")
	// ErrBackupCorrupted is returned when a frame's magic, version or checksum
	// is invalid, or when the chunk index does not match the payload.
	ErrBackupCorrupted = errors.New("backup is corrupted")
)

// BackupCorruptionError locates the frame that made a backup unloadable. All
// offsets are relative to the start of the segment named by Segment. The error
// is always returned before any key from the failing backup is written.
type BackupCorruptionError struct {
	// Segment is the zero-based ordinal of the segment within the backup file.
	Segment int
	// Chunk is the zero-based ordinal of the chunk within its segment, or -1
	// when the header or footer is the failing frame.
	Chunk int
	// Offset is the byte offset of the failing frame relative to the segment.
	Offset int64
	// Part names the failing frame: "header", "chunk", "footer" or "index".
	Part string
	// Err wraps the underlying cause (ErrBackupTruncated,
	// ErrBackupCorrupted or y.ErrChecksumMismatch).
	Err error
}

func (e *BackupCorruptionError) Error() string {
	loc := fmt.Sprintf("segment %d", e.Segment)
	if e.Chunk >= 0 {
		loc = fmt.Sprintf("%s, chunk %d", loc, e.Chunk)
	}
	return fmt.Sprintf("backup envelope error at %s (offset %d, %s): %v",
		loc, e.Offset, e.Part, e.Err)
}

func (e *BackupCorruptionError) Unwrap() error { return e.Err }

func backupCorruption(seg, chunk int, offset int64, part string, err error) error {
	return &BackupCorruptionError{
		Segment: seg,
		Chunk:   chunk,
		Offset:  offset,
		Part:    part,
		Err:     err,
	}
}

func backupCRC(parts ...[]byte) uint32 {
	h := crc32.New(y.CastagnoliCrcTable)
	for _, p := range parts {
		_, _ = h.Write(p)
	}
	return h.Sum32()
}

// --- small endian helpers ---------------------------------------------------

func encPutU32(b []byte, v uint32) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	return append(b, tmp[:]...)
}

func encPutU64(b []byte, v uint64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	return append(b, tmp[:]...)
}

func encPutBytes(b, p []byte) []byte {
	b = encPutU32(b, uint32(len(p)))
	return append(b, p...)
}

type decBuffer struct {
	b   []byte
	err error
}

func (d *decBuffer) u32() uint32 {
	if d.err != nil || len(d.b) < 4 {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	v := binary.LittleEndian.Uint32(d.b)
	d.b = d.b[4:]
	return v
}

func (d *decBuffer) u64() uint64 {
	if d.err != nil || len(d.b) < 8 {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	v := binary.LittleEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

func (d *decBuffer) bytes() []byte {
	n := d.u32()
	if d.err != nil || uint64(len(d.b)) < uint64(n) {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}

// --- header -----------------------------------------------------------------

type backupHeader struct {
	sinceTs   uint64
	createdAt uint64 // UnixNano of when the segment export started.
	prefix    []byte // Empty means the whole keyspace.
	algo      uint32 // Checksum algorithm used by every chunk.
	// bounds are the interior right edges of the planned key ranges. A plan of
	// N slots has N-1 interior bounds; slot i is [bounds[i-1], bounds[i]) with
	// nil edges meaning -inf/+inf.
	bounds [][]byte
}

func (h backupHeader) numSlots() uint32 { return uint32(len(h.bounds) + 1) }

func (h backupHeader) encodePayload() []byte {
	var b []byte
	b = encPutU64(b, h.sinceTs)
	b = encPutU64(b, h.createdAt)
	b = encPutU32(b, h.algo)
	b = encPutBytes(b, h.prefix)
	b = encPutU32(b, uint32(len(h.bounds)))
	for _, bound := range h.bounds {
		b = encPutBytes(b, bound)
	}
	return b
}

func decodeHeaderPayload(p []byte) (backupHeader, error) {
	d := &decBuffer{b: p}
	h := backupHeader{
		sinceTs:   d.u64(),
		createdAt: d.u64(),
		algo:      d.u32(),
		prefix:    d.bytes(),
	}
	n := int(d.u32())
	h.bounds = make([][]byte, n)
	for i := 0; i < n; i++ {
		h.bounds[i] = d.bytes()
	}
	if d.err != nil {
		return h, d.err
	}
	if len(d.b) != 0 {
		return h, ErrBackupCorrupted
	}
	return h, nil
}

// writeEnvelopeHeader writes a header frame and returns its total length.
func writeEnvelopeHeader(w io.Writer, h backupHeader) (int64, error) {
	payload := h.encodePayload()
	var head [backupFixedFrameHead]byte
	copy(head[:8], backupHeaderMagic)
	binary.LittleEndian.PutUint32(head[8:12], backupFormatVersion)
	binary.LittleEndian.PutUint32(head[12:16], uint32(len(payload)))
	sum := backupCRC(head[8:], payload)
	var tail [4]byte
	binary.LittleEndian.PutUint32(tail[:], sum)
	if _, err := w.Write(head[:]); err != nil {
		return 0, err
	}
	if _, err := w.Write(payload); err != nil {
		return 0, err
	}
	if _, err := w.Write(tail[:]); err != nil {
		return 0, err
	}
	return int64(len(head) + len(payload) + len(tail)), nil
}

// readEnvelopeHeader reads the header frame starting at off. The returned
// offset is the absolute position just past the frame.
func readEnvelopeHeader(ra io.ReaderAt, off int64) (backupHeader, int64, error) {
	var h backupHeader
	var head [backupFixedFrameHead]byte
	if err := readAtFull(ra, off, head[:]); err != nil {
		return h, off, err
	}
	if !bytes.Equal(head[:8], []byte(backupHeaderMagic)) {
		return h, off, ErrBackupCorrupted
	}
	version := binary.LittleEndian.Uint32(head[8:12])
	if version != backupFormatVersion {
		return h, off, fmt.Errorf("%w: unknown format version %d",
			ErrBackupCorrupted, version)
	}
	hlen := int64(binary.LittleEndian.Uint32(head[12:16]))
	if hlen > backupMaxFramePayload {
		return h, off, ErrBackupCorrupted
	}
	payload := make([]byte, hlen)
	if err := readAtFull(ra, off+int64(len(head)), payload); err != nil {
		return h, off, err
	}
	var tail [4]byte
	if err := readAtFull(ra, off+int64(len(head))+hlen, tail[:]); err != nil {
		return h, off, err
	}
	if backupCRC(head[8:], payload) != binary.LittleEndian.Uint32(tail[:]) {
		return h, off, y.ErrChecksumMismatch
	}
	h, err := decodeHeaderPayload(payload)
	if err != nil {
		return h, off, fmt.Errorf("%w: %v", ErrBackupCorrupted, err)
	}
	return h, off + int64(backupFixedFrameHead) + hlen + 4, nil
}

// --- chunks -----------------------------------------------------------------

// chunkRef is one entry of a segment's chunk index.
type chunkRef struct {
	slot      uint32
	flags     uint32
	offset    int64 // segment-relative offset of the frame magic.
	length    int64 // full frame length, including head and checksum tail.
	payloadLn int64
	crc       uint32
}

func (r chunkRef) final() bool { return r.flags&backupFlagFinal != 0 }

// writeChunkFrame writes one chunk frame.
func writeChunkFrame(w io.Writer, slot, flags uint32, payload []byte) (chunkRef, error) {
	var head [backupChunkHeadSize]byte
	copy(head[:8], backupChunkMagic)
	binary.LittleEndian.PutUint32(head[8:12], slot)
	binary.LittleEndian.PutUint32(head[12:16], flags)
	binary.LittleEndian.PutUint64(head[16:24], uint64(len(payload)))
	sum := backupCRC(head[8:], payload)
	var tail [4]byte
	binary.LittleEndian.PutUint32(tail[:], sum)
	if _, err := w.Write(head[:]); err != nil {
		return chunkRef{}, err
	}
	if _, err := w.Write(payload); err != nil {
		return chunkRef{}, err
	}
	if _, err := w.Write(tail[:]); err != nil {
		return chunkRef{}, err
	}
	return chunkRef{
		slot:      slot,
		flags:     flags,
		length:    int64(backupChunkHeadSize + len(payload) + backupChunkTailSize),
		payloadLn: int64(len(payload)),
		crc:       sum,
	}, nil
}

// readEnvelopeChunk reads the chunk frame starting at absOff. segStart is the
// absolute start of the segment the chunk belongs to, so ref.offset is reported
// segment-relative. nextOff is the absolute position past the frame.
func readEnvelopeChunk(ra io.ReaderAt, segStart, absOff int64) (chunkRef, []byte, int64, error) {
	var head [backupChunkHeadSize]byte
	if err := readAtFull(ra, absOff, head[:]); err != nil {
		return chunkRef{}, nil, absOff, err
	}
	if !bytes.Equal(head[:8], []byte(backupChunkMagic)) {
		return chunkRef{}, nil, absOff, ErrBackupCorrupted
	}
	ref := chunkRef{
		slot:      binary.LittleEndian.Uint32(head[8:12]),
		flags:     binary.LittleEndian.Uint32(head[12:16]),
		offset:    absOff - segStart,
		payloadLn: int64(binary.LittleEndian.Uint64(head[16:24])),
	}
	if ref.payloadLn > backupMaxFramePayload {
		return ref, nil, absOff, ErrBackupCorrupted
	}
	payload := make([]byte, ref.payloadLn)
	if err := readAtFull(ra, absOff+backupChunkHeadSize, payload); err != nil {
		return ref, nil, absOff, err
	}
	var tail [4]byte
	if err := readAtFull(ra, absOff+backupChunkHeadSize+ref.payloadLn, tail[:]); err != nil {
		return ref, nil, absOff, err
	}
	ref.crc = binary.LittleEndian.Uint32(tail[:])
	if backupCRC(head[8:], payload) != ref.crc {
		return ref, nil, absOff, y.ErrChecksumMismatch
	}
	ref.length = backupChunkHeadSize + ref.payloadLn + backupChunkTailSize
	return ref, payload, absOff + ref.length, nil
}

// --- footer -----------------------------------------------------------------

type backupFooter struct {
	sinceTs    uint64
	maxVersion uint64
	numEntries uint64
	numSlots   uint32
	algo       uint32
	chunks     []chunkRef
}

func (f backupFooter) encodePayload() []byte {
	var b []byte
	b = encPutU64(b, f.sinceTs)
	b = encPutU64(b, f.maxVersion)
	b = encPutU64(b, f.numEntries)
	b = encPutU32(b, f.numSlots)
	b = encPutU32(b, f.algo)
	b = encPutU64(b, uint64(len(f.chunks)))
	for _, c := range f.chunks {
		b = encPutU32(b, c.slot)
		b = encPutU32(b, c.flags)
		b = encPutU64(b, uint64(c.offset))
		b = encPutU64(b, uint64(c.length))
		b = encPutU32(b, c.crc)
	}
	return b
}

func decodeFooterPayload(p []byte) (backupFooter, error) {
	d := &decBuffer{b: p}
	f := backupFooter{
		sinceTs:    d.u64(),
		maxVersion: d.u64(),
		numEntries: d.u64(),
		numSlots:   d.u32(),
		algo:       d.u32(),
	}
	n := int(d.u64())
	if d.err != nil {
		return f, d.err
	}
	if int64(n) > backupMaxFramePayload/int64(backupChunkFrameMin) {
		return f, ErrBackupCorrupted
	}
	f.chunks = make([]chunkRef, n)
	for i := 0; i < n; i++ {
		c := chunkRef{
			slot:   d.u32(),
			flags:  d.u32(),
			offset: int64(d.u64()),
			length: int64(d.u64()),
			crc:    d.u32(),
		}
		if c.length < backupChunkFrameMin {
			d.err = ErrBackupCorrupted
			return f, d.err
		}
		c.payloadLn = c.length - backupChunkFrameMin
		f.chunks[i] = c
	}
	if d.err != nil {
		return f, d.err
	}
	if len(d.b) != 0 {
		return f, ErrBackupCorrupted
	}
	return f, nil
}

// writeEnvelopeFooter writes a footer frame.
func writeEnvelopeFooter(w io.Writer, f backupFooter) error {
	payload := f.encodePayload()
	var head [backupFixedFrameHead]byte
	copy(head[:8], backupFooterMagic)
	binary.LittleEndian.PutUint32(head[8:12], backupFormatVersion)
	binary.LittleEndian.PutUint32(head[12:16], uint32(len(payload)))
	sum := backupCRC(head[8:], payload)
	var tail [4]byte
	binary.LittleEndian.PutUint32(tail[:], sum)
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	_, err := w.Write(tail[:])
	return err
}

// readEnvelopeFooter reads the footer frame starting at absOff and returns the
// absolute position past the frame.
func readEnvelopeFooter(ra io.ReaderAt, absOff int64) (backupFooter, int64, error) {
	var f backupFooter
	var head [backupFixedFrameHead]byte
	if err := readAtFull(ra, absOff, head[:]); err != nil {
		return f, absOff, err
	}
	if !bytes.Equal(head[:8], []byte(backupFooterMagic)) {
		return f, absOff, ErrBackupCorrupted
	}
	version := binary.LittleEndian.Uint32(head[8:12])
	if version != backupFormatVersion {
		return f, absOff, fmt.Errorf("%w: unknown format version %d",
			ErrBackupCorrupted, version)
	}
	flen := int64(binary.LittleEndian.Uint32(head[12:16]))
	if flen > backupMaxFramePayload {
		return f, absOff, ErrBackupCorrupted
	}
	payload := make([]byte, flen)
	if err := readAtFull(ra, absOff+int64(len(head)), payload); err != nil {
		return f, absOff, err
	}
	var tail [4]byte
	if err := readAtFull(ra, absOff+int64(len(head))+flen, tail[:]); err != nil {
		return f, absOff, err
	}
	if backupCRC(head[8:], payload) != binary.LittleEndian.Uint32(tail[:]) {
		return f, absOff, y.ErrChecksumMismatch
	}
	f, err := decodeFooterPayload(payload)
	if err != nil {
		return f, absOff, fmt.Errorf("%w: %v", ErrBackupCorrupted, err)
	}
	return f, absOff + int64(backupFixedFrameHead) + flen + 4, nil
}

func readAtFull(ra io.ReaderAt, off int64, p []byte) error {
	_, err := io.ReadFull(io.NewSectionReader(ra, off, int64(len(p))), p)
	return err
}

// peekFrameMagic reads the 8-byte magic at off without consuming anything.
func peekFrameMagic(ra io.ReaderAt, off int64) ([]byte, error) {
	var m [8]byte
	if err := readAtFull(ra, off, m[:]); err != nil {
		return nil, err
	}
	return m[:], nil
}

// --- planned key ranges (slots) ---------------------------------------------

func planInteriorBounds(plan []*keyRange) [][]byte {
	if len(plan) <= 1 {
		return nil
	}
	bounds := make([][]byte, 0, len(plan)-1)
	for i := 0; i < len(plan)-1; i++ {
		bounds = append(bounds, y.SafeCopy(nil, plan[i].right))
	}
	return bounds
}

func rangesFromBounds(bounds [][]byte) []*keyRange {
	ranges := make([]*keyRange, len(bounds)+1)
	var left []byte
	for i := range ranges {
		var right []byte
		if i < len(bounds) {
			right = y.SafeCopy(nil, bounds[i])
		}
		ranges[i] = &keyRange{left: left, right: right, slot: uint32(i)}
		if i < len(bounds) {
			left = y.SafeCopy(nil, bounds[i])
		}
	}
	return ranges
}

// segWriter serializes one envelope segment.
type segWriter struct {
	w         io.Writer
	pos       int64 // segment-relative position of the next byte.
	index     []chunkRef
	entries   uint64
	maxVer    uint64
	numSlots  int
	finalized []bool
}

func (sw *segWriter) writeChunk(slot, flags uint32, payload []byte) error {
	ref, err := writeChunkFrame(sw.w, slot, flags, payload)
	if err != nil {
		return err
	}
	ref.offset = sw.pos
	sw.pos += ref.length
	sw.index = append(sw.index, ref)
	return nil
}

// resumeState carries the already durable part of a segment being resumed.
type resumeState struct {
	pos     int64 // segment-relative append position after compaction.
	index   []chunkRef
	entries uint64
	maxVer  uint64
	skip    map[uint32]bool // slots already covered by verified final chunks.
}

// --- export -----------------------------------------------------------------

// Backup dumps a protobuf-encoded list of all entries in the database into the
// given writer, that are newer than or equal to the specified version. It
// returns a timestamp (version) indicating the version of last entry that is
// dumped, which after incrementing by 1 can be passed into later invocation to
// generate incremental backup of entries that have been added/modified since
// the last invocation of DB.Backup().
// DB.Backup is a wrapper function over Stream.Backup to generate full and
// incremental backups of the DB. For more control over how many goroutines are
// used to generate the backup, or if you wish to backup only a certain range
// of keys, use Stream.Backup directly.
//
// The output is a self-describing envelope: every call writes one segment
// holding a format header, checksummed chunks and a chunk-index footer.
// Incremental backups can append further segments to the same writer.
func (db *DB) Backup(w io.Writer, since uint64) (uint64, error) {
	stream := db.NewStream()
	stream.LogPrefix = "DB.Backup"
	stream.SinceTs = since
	return stream.Backup(w, since)
}

// Backup dumps a protobuf-encoded list of all entries in the database into the
// given writer, that are newer than or equal to the specified version. It returns a
// timestamp(version) indicating the version of last entry that was dumped, which
// after incrementing by 1 can be passed into a later invocation to generate an
// incremental dump of entries that have been added/modified since the last
// invocation of Stream.Backup().
//
// Setting stream.Prefix before the call produces a range backup; both full and
// range backups use the exact same envelope format.
//
// This can be used to backup the data in a database at a given point in time.
func (stream *Stream) Backup(w io.Writer, since uint64) (uint64, error) {
	stream.setBackupKeyToList(since)
	plan := stream.backupPlan()
	hdr := backupHeader{
		sinceTs:   since,
		createdAt: uint64(time.Now().UnixNano()),
		prefix:    y.SafeCopy(nil, stream.Prefix),
		algo:      backupAlgoCRC32C,
		bounds:    planInteriorBounds(plan),
	}
	return stream.runBackupSegment(w, hdr, plan, nil)
}

func (stream *Stream) setBackupKeyToList(since uint64) {
	stream.KeyToList = func(key []byte, itr *Iterator) (*pb.KVList, error) {
		list := &pb.KVList{}
		a := itr.Alloc
		for ; itr.Valid(); itr.Next() {
			item := itr.Item()
			if !bytes.Equal(item.Key(), key) {
				return list, nil
			}
			if item.Version() < since {
				return nil, fmt.Errorf("Backup: Item Version: %d less than sinceTs: %d",
					item.Version(), since)
			}

			var valCopy []byte
			if !item.IsDeletedOrExpired() {
				// No need to copy value, if item is deleted or expired.
				err := item.Value(func(val []byte) error {
					valCopy = a.Copy(val)
					return nil
				})
				if err != nil {
					stream.db.opt.Errorf("Key [%x, %d]. Error while fetching value [%v]\n",
						item.Key(), item.Version(), err)
					return nil, err
				}
			}

			// clear txn bits
			meta := item.meta &^ (bitTxn | bitFinTxn)
			kv := y.NewKV(a)
			*kv = pb.KV{
				Key:       a.Copy(item.Key()),
				Value:     valCopy,
				UserMeta:  a.Copy([]byte{item.UserMeta()}),
				Version:   item.Version(),
				ExpiresAt: item.ExpiresAt(),
				Meta:      a.Copy([]byte{meta}),
			}
			list.Kv = append(list.Kv, kv)

			switch {
			case item.DiscardEarlierVersions():
				// If we need to discard earlier versions of this item, add a delete
				// marker just below the current version.
				list.Kv = append(list.Kv, &pb.KV{
					Key:     item.KeyCopy(nil),
					Version: item.Version() - 1,
					Meta:    []byte{bitDelete},
				})
				return list, nil

			case item.IsDeletedOrExpired():
				return list, nil
			}
		}
		return list, nil
	}
}

// backupPlan returns the deterministic key-range plan for this export. The
// stream iterates exactly these ranges and every KV is tagged with its slot
// ordinal, which makes chunk-granular resume independent of scheduling order.
func (stream *Stream) backupPlan() []*keyRange {
	numGo := stream.NumGo
	if numGo <= 0 {
		numGo = stream.db.opt.NumGoroutines
	}
	plan := stream.db.Ranges(stream.Prefix, numGo)
	y.AssertTrue(len(plan) > 0)
	for i := range plan {
		plan[i].slot = uint32(i)
	}
	return plan
}

func (stream *Stream) runBackupSegment(
	w io.Writer, hdr backupHeader, plan []*keyRange, resume *resumeState,
) (uint64, error) {
	numSlots := len(plan)
	sw := &segWriter{
		w:         w,
		numSlots:  numSlots,
		finalized: make([]bool, numSlots),
	}
	if resume == nil {
		n, err := writeEnvelopeHeader(w, hdr)
		if err != nil {
			return 0, err
		}
		sw.pos = n
	} else {
		sw.pos = resume.pos
		sw.index = append(sw.index, resume.index...)
		sw.entries = resume.entries
		sw.maxVer = resume.maxVer
		for slot := range resume.skip {
			if int(slot) >= numSlots {
				return 0, fmt.Errorf("backup resume: slot %d outside plan of %d slots",
					slot, numSlots)
			}
			sw.finalized[slot] = true
		}
	}

	stream.backupPlanOverride = plan
	stream.SinceTs = hdr.sinceTs
	stream.SendDoneMarkers(true)
	stream.Send = func(buf *z.Buffer) error {
		return stream.backupSend(sw, resume, buf)
	}

	if err := stream.Orchestrate(context.Background()); err != nil {
		return 0, err
	}
	for slot, done := range sw.finalized {
		if !done {
			return 0, fmt.Errorf("backup: slot %d finished without a final chunk", slot)
		}
	}

	footer := backupFooter{
		sinceTs:    hdr.sinceTs,
		maxVersion: sw.maxVer,
		numEntries: sw.entries,
		numSlots:   uint32(numSlots),
		algo:       hdr.algo,
		chunks:     sw.index,
	}
	if err := writeEnvelopeFooter(w, footer); err != nil {
		return 0, err
	}
	return sw.maxVer, nil
}

func (stream *Stream) backupSend(sw *segWriter, resume *resumeState, buf *z.Buffer) error {
	list, err := BufferToKVList(buf)
	if err != nil {
		return err
	}
	groups := make([]*pb.KVList, sw.numSlots)
	var finals []uint32
	for _, kv := range list.Kv {
		if kv.StreamDone {
			if kv.StreamId == 0 || int(kv.StreamId-1) >= sw.numSlots {
				return fmt.Errorf("backup: done marker with invalid slot id %d", kv.StreamId)
			}
			finals = append(finals, kv.StreamId-1)
			continue
		}
		if kv.StreamId == 0 || int(kv.StreamId-1) >= sw.numSlots {
			return fmt.Errorf("backup: entry with invalid slot id %d", kv.StreamId)
		}
		slot := kv.StreamId - 1
		if resume != nil && resume.skip[slot] {
			// Slot already covered by a verified chunk from before the interruption.
			continue
		}
		if sw.maxVer < kv.Version {
			sw.maxVer = kv.Version
		}
		if groups[slot] == nil {
			groups[slot] = &pb.KVList{}
		}
		groups[slot].Kv = append(groups[slot].Kv, kv)
	}

	for slot, group := range groups {
		if group == nil {
			continue
		}
		payload, err := proto.Marshal(group)
		if err != nil {
			return err
		}
		if err := sw.writeChunk(uint32(slot), 0, payload); err != nil {
			return err
		}
		sw.entries += uint64(len(group.Kv))
	}
	for _, slot := range finals {
		if resume != nil && resume.skip[slot] {
			continue
		}
		if sw.finalized[slot] {
			continue
		}
		if err := sw.writeChunk(slot, backupFlagFinal, nil); err != nil {
			return err
		}
		sw.finalized[slot] = true
	}
	return nil
}

// writeTo writes a legacy (non-envelope) length-prefixed KVList frame. It is
// kept for producing backward-compatibility test artifacts.
func writeTo(list *pb.KVList, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(proto.Size(list))); err != nil {
		return err
	}
	buf, err := proto.Marshal(list)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// --- resume -----------------------------------------------------------------

// BackupResume continues a backup into f that was interrupted by a write
// failure. f must contain a partial envelope produced by an earlier call to
// Backup against a database with the same contents (same key prefix and since
// version).
//
// Resume validates every byte already on disk, keeps the verified chunks of
// every fully completed slot, drops the incomplete trailing slot chunks, and
// then re-runs only the missing slots. The finished artifact imports to the
// same database state as an uninterrupted backup. If f already ends in a
// complete segment, BackupResume is a no-op and returns that segment's max
// version.
func (db *DB) BackupResume(f *os.File, since uint64) (uint64, error) {
	stream := db.NewStream()
	stream.LogPrefix = "DB.BackupResume"
	stream.SinceTs = since
	return stream.BackupResume(f, since)
}

// BackupResume is Stream.Backup's resumable counterpart. See DB.BackupResume
// for the semantics.
func (stream *Stream) BackupResume(f *os.File, since uint64) (uint64, error) {
	stream.setBackupKeyToList(since)
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := fi.Size()
	if size == 0 {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return 0, err
		}
		return stream.Backup(f, since)
	}

	// The very first bytes decide whether this is a resumable artifact. A
	// legacy backup (or any other file) must never be silently overwritten.
	var magic [8]byte
	switch merr := readAtFull(f, 0, magic[:]); {
	case merr == nil:
		if !bytes.Equal(magic[:], []byte(backupHeaderMagic)) {
			return 0, errors.New("backup resume: file is not an envelope backup")
		}
	case isIncompleteRead(merr):
		// Fewer than 8 bytes on disk: the header write was interrupted.
		if err := f.Truncate(0); err != nil {
			return 0, err
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return 0, err
		}
		return stream.Backup(f, since)
	default:
		return 0, merr
	}

	// Walk complete segments; stop at the partial trailing one.
	var (
		pos            int64
		segOrdinal     int
		lastMaxVersion uint64
		partial        *partialSegment
	)
	for pos < size {
		hdr, hdrEnd, err := readEnvelopeHeader(f, pos)
		if err != nil {
			if pos == 0 && (isIncompleteRead(err) || errors.Is(err, y.ErrChecksumMismatch)) {
				// Header frame interrupted or torn mid-write: restart it.
				if err := f.Truncate(0); err != nil {
					return 0, err
				}
				if _, err := f.Seek(0, io.SeekStart); err != nil {
					return 0, err
				}
				return stream.Backup(f, since)
			}
			return 0, envelopeReadError(err, segOrdinal, -1, 0, "header")
		}

		refs, footer, next, complete, err := scanSegment(f, pos, hdrEnd, size)
		if err != nil {
			return 0, err
		}
		if complete {
			pos = next
			segOrdinal++
			lastMaxVersion = footer.maxVersion
			continue
		}
		partial = &partialSegment{start: pos, hdr: hdr, hdrEnd: hdrEnd, refs: refs}
		break
	}

	if partial == nil {
		// The file already ends in a complete segment; nothing to do.
		return lastMaxVersion, nil
	}
	return stream.resumePartialSegment(f, since, partial)
}

func isIncompleteRead(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)
}

type partialSegment struct {
	start  int64 // absolute segment start.
	hdrEnd int64 // absolute position past the header.
	hdr    backupHeader
	refs   []chunkRef // every fully verifiable chunk, in file order.
}

// scanSegment reads chunks until the footer or the first unreadable frame.
// The boolean reports whether a complete, checksum-valid footer was found.
func scanSegment(ra io.ReaderAt, segStart, hdrEnd, size int64) (
	[]chunkRef, *backupFooter, int64, bool, error,
) {
	abs := hdrEnd
	var refs []chunkRef
	for abs < size {
		magic, err := peekFrameMagic(ra, abs)
		if err != nil {
			if isIncompleteRead(err) {
				return refs, nil, abs, false, nil
			}
			return nil, nil, abs, false, err
		}
		switch string(magic) {
		case backupFooterMagic:
			footer, footerEnd, ferr := readEnvelopeFooter(ra, abs)
			if ferr != nil {
				if isIncompleteRead(ferr) {
					return refs, nil, abs, false, nil
				}
				return nil, nil, abs, false, ferr
			}
			return refs, &footer, footerEnd, true, nil
		case backupChunkMagic:
			ref, _, next, cerr := readEnvelopeChunk(ra, segStart, abs)
			if cerr != nil {
				if isIncompleteRead(cerr) {
					return refs, nil, abs, false, nil
				}
				// A fully present frame with a bad checksum is the first
				// unverifiable frame; everything after it is unusable.
				return refs, nil, abs, false, nil
			}
			refs = append(refs, ref)
			abs = next
		default:
			// Lost framing: stop at this boundary.
			return refs, nil, abs, false, nil
		}
	}
	return refs, nil, abs, false, nil
}

func (stream *Stream) resumePartialSegment(
	f *os.File, since uint64, p *partialSegment,
) (uint64, error) {
	hdr := p.hdr
	if hdr.sinceTs != since {
		return 0, fmt.Errorf("backup resume: since version mismatch: file has %d, requested %d",
			hdr.sinceTs, since)
	}
	if !bytes.Equal(hdr.prefix, stream.Prefix) {
		return 0, errors.New("backup resume: key prefix does not match the partial backup")
	}
	numSlots := int(hdr.numSlots())

	completed := make(map[uint32]bool)
	for _, ref := range p.refs {
		if ref.final() {
			if int(ref.slot) >= numSlots {
				return 0, backupCorruption(0, -1, ref.offset, "chunk",
					fmt.Errorf("%w: final chunk slot %d out of range",
						ErrBackupCorrupted, ref.slot))
			}
			completed[ref.slot] = true
		}
	}

	// Keep only chunks belonging to fully completed slots; they are copied
	// verbatim so their checksums stay valid.
	kept := make([]chunkRef, 0, len(p.refs))
	for _, ref := range p.refs {
		if completed[ref.slot] {
			kept = append(kept, ref)
		}
	}

	// Aggregate stats of retained chunks and compact them towards the header.
	// Each retained frame is read in full and rewritten at its new position:
	// source and destination regions overlap when the dropped gap is small, so
	// streaming in-place copies could overwrite bytes not yet read.
	var (
		keptEntries uint64
		keptMaxVer  uint64
		headBuf     [backupChunkHeadSize]byte
		tailBuf     [backupChunkTailSize]byte
	)
	writeCursor := p.hdrEnd - p.start
	newRefs := make([]chunkRef, 0, len(kept))
	for _, ref := range kept {
		frameSrc := p.start + ref.offset
		if err := readAtFull(f, frameSrc, headBuf[:]); err != nil {
			return 0, err
		}
		payload := make([]byte, ref.payloadLn)
		if err := readAtFull(f, frameSrc+backupChunkHeadSize, payload); err != nil {
			return 0, err
		}
		if err := readAtFull(f, frameSrc+backupChunkHeadSize+ref.payloadLn, tailBuf[:]); err != nil {
			return 0, err
		}
		list := &pb.KVList{}
		if err := proto.Unmarshal(payload, list); err != nil {
			return 0, backupCorruption(0, -1, ref.offset, "chunk",
				fmt.Errorf("%w: %v", y.ErrChecksumMismatch, err))
		}
		for _, kv := range list.Kv {
			if keptMaxVer < kv.Version {
				keptMaxVer = kv.Version
			}
		}
		keptEntries += uint64(len(list.Kv))

		dst := p.start + writeCursor
		if dst != frameSrc {
			if _, err := f.WriteAt(headBuf[:], dst); err != nil {
				return 0, err
			}
			if _, err := f.WriteAt(payload, dst+backupChunkHeadSize); err != nil {
				return 0, err
			}
			if _, err := f.WriteAt(tailBuf[:], dst+backupChunkHeadSize+ref.payloadLn); err != nil {
				return 0, err
			}
		}
		ref.offset = writeCursor
		newRefs = append(newRefs, ref)
		writeCursor += ref.length
	}

	end := p.start + writeCursor
	if err := f.Truncate(end); err != nil {
		return 0, err
	}
	if _, err := f.Seek(end, io.SeekStart); err != nil {
		return 0, err
	}

	// Every slot was complete; the interruption happened while writing the
	// footer. Just rewrite it.
	if len(completed) == numSlots {
		footer := backupFooter{
			sinceTs:    hdr.sinceTs,
			maxVersion: keptMaxVer,
			numEntries: keptEntries,
			numSlots:   uint32(numSlots),
			algo:       hdr.algo,
			chunks:     newRefs,
		}
		if err := writeEnvelopeFooter(f, footer); err != nil {
			return 0, err
		}
		return keptMaxVer, nil
	}

	plan := rangesFromBounds(hdr.bounds)
	state := &resumeState{
		pos:     writeCursor,
		index:   newRefs,
		entries: keptEntries,
		maxVer:  keptMaxVer,
		skip:    completed,
	}
	// resumeWriteWrap is a test-only seam used to interrupt the continuation
	// itself; nil in production.
	var w io.Writer = f
	if stream.resumeWriteWrap != nil {
		w = stream.resumeWriteWrap(f)
	}
	return stream.runBackupSegment(w, hdr, plan, state)
}

// --- KVLoader (unchanged write pacing semantics) -----------------------------

// KVLoader is used to write KVList objects in to badger. It can be used to restore a backup.
type KVLoader struct {
	db          *DB
	throttle    *y.Throttle
	entries     []*Entry
	entriesSize int64
	totalSize   int64
}

// NewKVLoader returns a new instance of KVLoader.
func (db *DB) NewKVLoader(maxPendingWrites int) *KVLoader {
	return &KVLoader{
		db:       db,
		throttle: y.NewThrottle(maxPendingWrites),
		entries:  make([]*Entry, 0, db.opt.maxBatchCount),
	}
}

// Set writes the key-value pair to the database.
func (l *KVLoader) Set(kv *pb.KV) error {
	var userMeta, meta byte
	if len(kv.UserMeta) > 0 {
		userMeta = kv.UserMeta[0]
	}
	if len(kv.Meta) > 0 {
		meta = kv.Meta[0]
	}
	e := &Entry{
		Key:       y.KeyWithTs(kv.Key, kv.Version),
		Value:     kv.Value,
		UserMeta:  userMeta,
		ExpiresAt: kv.ExpiresAt,
		meta:      meta,
	}
	estimatedSize := e.estimateSizeAndSetThreshold(l.db.valueThreshold())
	// Flush entries if inserting the next entry would overflow the transactional limits.
	if int64(len(l.entries))+1 >= l.db.opt.maxBatchCount ||
		l.entriesSize+estimatedSize >= l.db.opt.maxBatchSize ||
		l.totalSize >= flushThreshold {
		if err := l.send(); err != nil {
			return err
		}
	}
	l.entries = append(l.entries, e)
	l.entriesSize += estimatedSize
	l.totalSize += estimatedSize + int64(len(e.Value))
	return nil
}

func (l *KVLoader) send() error {
	if err := l.throttle.Do(); err != nil {
		return err
	}
	if err := l.db.batchSetAsync(l.entries, func(err error) {
		l.throttle.Done(err)
	}); err != nil {
		return err
	}

	l.entries = make([]*Entry, 0, l.db.opt.maxBatchCount)
	l.entriesSize = 0
	l.totalSize = 0
	return nil
}

// Finish is meant to be called after all the key-value pairs have been loaded.
func (l *KVLoader) Finish() error {
	if len(l.entries) > 0 {
		if err := l.send(); err != nil {
			return err
		}
	}
	return l.throttle.Finish()
}

// finishLoad waits for pending writes and publishes the watermark. It is the
// single tail of both legacy and envelope restore paths.
func (db *DB) finishLoad(ldr *KVLoader) error {
	if err := ldr.Finish(); err != nil {
		return err
	}
	db.orc.txnMark.Done(db.orc.nextTxnTs - 1)
	return nil
}

// --- import -----------------------------------------------------------------

// Load reads a protobuf-encoded list of all entries from a reader and writes
// them to the database. This can be used to restore the database from a backup
// made by calling DB.Backup(). If more complex logic is needed to restore a badger
// backup, the KVLoader interface should be used instead.
//
// Envelope backups (the current format) are fully verified before any key is
// written: every chunk checksum and the chunk index must validate and the
// backup must end in a valid footer. Truncation or corruption fails the call
// with a *BackupCorruptionError naming the bad block and leaves the database
// untouched. Legacy backups produced by older Badger versions (plain
// length-prefixed frames, no envelope) are loaded directly without conversion.
//
// Loading the same backup more than once does not create extra versions:
// entries carry their commit version and overwrite the identical (key,
// version) slot.
//
// DB.Load() should be called on a database that is not running any other
// concurrent transactions while it is running.
func (db *DB) Load(r io.Reader, maxPendingWrites int) error {
	// Fast path for *os.File: validate in place (no spool copy), replay with
	// ReadAt so the two passes do not interfere with the file offset.
	var (
		fileAt   io.ReaderAt
		fileSize int64
		start    int64
	)
	if f, ok := r.(*os.File); ok {
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		fileAt, fileSize, start = f, fi.Size(), pos
	}

	var head [8]byte
	n, err := io.ReadFull(r, head[:])
	if err == io.EOF && n == 0 {
		// Empty input: behave like the legacy implementation.
		return db.finishLoad(db.NewKVLoader(maxPendingWrites))
	}
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return err
	}

	magic := []byte(backupHeaderMagic)
	if n >= len(magic) && bytes.Equal(head[:len(magic)], magic) {
		if fileAt != nil {
			return db.loadEnvelope(fileAt, fileSize, start, maxPendingWrites)
		}
		spooled, cleanup, serr := spoolBackup(io.MultiReader(bytes.NewReader(head[:n]), r))
		if serr != nil {
			return serr
		}
		defer cleanup()
		return db.loadEnvelope(spooled, spooledSize(spooled), 0, maxPendingWrites)
	}
	// A partial magic prefix can only be a truncated envelope header.
	if n < len(magic) && bytes.HasPrefix(magic, head[:n]) {
		if fileAt != nil {
			return db.loadEnvelope(fileAt, fileSize, start, maxPendingWrites)
		}
		spooled, cleanup, serr := spoolBackup(io.MultiReader(bytes.NewReader(head[:n]), r))
		if serr != nil {
			return serr
		}
		defer cleanup()
		return db.loadEnvelope(spooled, spooledSize(spooled), 0, maxPendingWrites)
	}

	// Legacy path. The remaining input may itself switch to the envelope
	// format (incremental segments appended by a newer Badger); handle that at
	// every frame boundary.
	var remaining io.Reader
	if fileAt != nil {
		// head[:n] was consumed from the file; let the section start after it.
		remaining = io.MultiReader(
			bytes.NewReader(head[:n]),
			io.NewSectionReader(fileAt.(*os.File), start+int64(n), fileSize-start-int64(n)))
	} else {
		remaining = io.MultiReader(bytes.NewReader(head[:n]), r)
	}
	return db.loadLegacy(remaining, maxPendingWrites)
}

func spooledSize(f *os.File) int64 {
	fi, err := f.Stat()
	if err != nil {
		return -1
	}
	return fi.Size()
}

// spoolBackup copies r into a temporary file and returns it rewound. The
// cleanup function closes and removes the file.
func spoolBackup(r io.Reader) (*os.File, func(), error) {
	f, err := os.CreateTemp("", "badger-backup-load-*")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}
	if _, err := io.Copy(f, r); err != nil {
		cleanup()
		return nil, nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, nil, err
	}
	return f, cleanup, nil
}

// validatedSegment is the result of verifying one segment end to end.
type validatedSegment struct {
	start  int64 // absolute start of the segment within the file.
	hdr    backupHeader
	footer backupFooter
}

// loadEnvelope runs the validate-then-write restore. No KVLoader exists until
// the whole backup has been verified, so a failing check can never have written
// a key.
func (db *DB) loadEnvelope(ra io.ReaderAt, size, start int64, maxPendingWrites int) error {
	segs, err := validateEnvelope(ra, size, start)
	if err != nil {
		return err
	}

	ldr := db.NewKVLoader(maxPendingWrites)
	for segIdx, seg := range segs {
		for _, ref := range seg.footer.chunks {
			if ref.payloadLn == 0 {
				continue
			}
			payload := make([]byte, ref.payloadLn)
			if err := readAtFull(ra, seg.start+ref.offset+backupChunkHeadSize, payload); err != nil {
				return backupCorruption(segIdx, -1, ref.offset, "chunk",
					fmt.Errorf("%w: %v", ErrBackupTruncated, err))
			}
			list := &pb.KVList{}
			if err := proto.Unmarshal(payload, list); err != nil {
				return backupCorruption(segIdx, -1, ref.offset, "chunk",
					fmt.Errorf("%w: %v", ErrBackupCorrupted, err))
			}
			for _, kv := range list.Kv {
				if err := ldr.Set(kv); err != nil {
					return err
				}
				// Update nextTxnTs, memtable stores this
				// timestamp in badger head when flushed.
				if kv.Version >= db.orc.nextTxnTs {
					db.orc.nextTxnTs = kv.Version + 1
				}
			}
		}
	}
	return db.finishLoad(ldr)
}

// validateEnvelope verifies every segment from segStart up to size. It is a
// pure read: it never touches the database.
func validateEnvelope(ra io.ReaderAt, size, segStart int64) ([]validatedSegment, error) {
	var segs []validatedSegment
	abs := segStart
	for abs < size {
		segIdx := len(segs)
		hdr, hdrEnd, err := readEnvelopeHeader(ra, abs)
		if err != nil {
			return nil, envelopeReadError(err, segIdx, -1, abs-segStart, "header")
		}
		numSlots := int(hdr.numSlots())

		var refs []chunkRef
		cursor := hdrEnd
		for {
			magic, merr := peekFrameMagic(ra, cursor)
			if merr != nil {
				return nil, envelopeReadError(
					merr, segIdx, len(refs), cursor-segStart, "chunk")
			}
			if string(magic) == backupFooterMagic {
				break
			}
			if string(magic) != backupChunkMagic {
				return nil, backupCorruption(segIdx, len(refs), cursor-segStart, "chunk",
					fmt.Errorf("%w: bad chunk magic %q", ErrBackupCorrupted, string(magic)))
			}
			ref, _, next, cerr := readEnvelopeChunk(ra, abs, cursor)
			if cerr != nil {
				return nil, envelopeReadError(
					cerr, segIdx, len(refs), cursor-segStart, "chunk")
			}
			if int(ref.slot) >= numSlots {
				return nil, backupCorruption(segIdx, len(refs), ref.offset, "chunk",
					fmt.Errorf("%w: chunk slot %d out of range (slots=%d)",
						ErrBackupCorrupted, ref.slot, numSlots))
			}
			refs = append(refs, ref)
			cursor = next
		}

		footer, footerEnd, ferr := readEnvelopeFooter(ra, cursor)
		if ferr != nil {
			return nil, envelopeReadError(
				ferr, segIdx, len(refs), cursor-segStart, "footer")
		}

		if err := crossCheckSegment(hdr, footer, refs, hdrEnd-abs); err != nil {
			return nil, backupCorruption(segIdx, -1, cursor-segStart, "index", err)
		}

		segs = append(segs, validatedSegment{start: abs, hdr: hdr, footer: footer})
		abs = footerEnd
	}
	return segs, nil
}

// crossCheckSegment compares the frames found on disk with the footer index
// and enforces slot completion and frame contiguity invariants.
func crossCheckSegment(hdr backupHeader, footer backupFooter, refs []chunkRef, dataStart int64) error {
	numSlots := int(hdr.numSlots())
	if int(footer.numSlots) != numSlots {
		return fmt.Errorf("%w: footer slots=%d header slots=%d",
			ErrBackupCorrupted, footer.numSlots, numSlots)
	}
	if footer.sinceTs != hdr.sinceTs {
		return fmt.Errorf("%w: footer sinceTs=%d header sinceTs=%d",
			ErrBackupCorrupted, footer.sinceTs, hdr.sinceTs)
	}
	if footer.algo != hdr.algo {
		return fmt.Errorf("%w: footer checksum algo=%d header algo=%d",
			ErrBackupCorrupted, footer.algo, hdr.algo)
	}
	if len(footer.chunks) != len(refs) {
		return fmt.Errorf("%w: index lists %d chunks but %d are present",
			ErrBackupTruncated, len(footer.chunks), len(refs))
	}

	// Frames must sit contiguously right after the header and every indexed
	// field must match the bytes on disk.
	expected := dataStart
	finalSeen := make([]int, numSlots)
	for i := range finalSeen {
		finalSeen[i] = -1
	}
	for i, onDisk := range refs {
		idx := footer.chunks[i]
		if onDisk.offset != expected {
			return fmt.Errorf("%w: chunk %d has a gap or overlap (offset %d, expected %d)",
				ErrBackupCorrupted, i, onDisk.offset, expected)
		}
		if idx.slot != onDisk.slot || idx.flags != onDisk.flags ||
			idx.offset != onDisk.offset || idx.length != onDisk.length ||
			idx.crc != onDisk.crc {
			return fmt.Errorf("%w: chunk %d does not match its index entry",
				ErrBackupCorrupted, i)
		}
		if onDisk.final() {
			if finalSeen[onDisk.slot] != -1 {
				return fmt.Errorf("%w: slot %d has more than one final chunk",
					ErrBackupCorrupted, onDisk.slot)
			}
			finalSeen[onDisk.slot] = i
		}
		expected += onDisk.length
	}

	// Every slot must end with a final chunk, and the final chunk has to be the
	// last on-disk chunk of that slot.
	lastForSlot := make([]int, numSlots)
	for i := range lastForSlot {
		lastForSlot[i] = -1
	}
	for i, ref := range refs {
		lastForSlot[ref.slot] = i
	}
	for slot := 0; slot < numSlots; slot++ {
		if finalSeen[slot] < 0 {
			return fmt.Errorf("%w: slot %d has no final chunk",
				ErrBackupTruncated, slot)
		}
		if finalSeen[slot] != lastForSlot[slot] {
			return fmt.Errorf("%w: data chunks of slot %d follow its final chunk",
				ErrBackupCorrupted, slot)
		}
	}
	return nil
}

func envelopeReadError(err error, seg, chunk int, off int64, part string) error {
	switch {
	case errors.Is(err, y.ErrChecksumMismatch):
		return backupCorruption(seg, chunk, off, part,
			fmt.Errorf("%w: %v", y.ErrChecksumMismatch, err))
	case isIncompleteRead(err):
		return backupCorruption(seg, chunk, off, part,
			fmt.Errorf("%w: %v", ErrBackupTruncated, err))
	default:
		return backupCorruption(seg, chunk, off, part,
			fmt.Errorf("%w: %v", ErrBackupCorrupted, err))
	}
}

// loadLegacy restores the pre-envelope format: a stream of
// [uint64 LE length][protobuf KVList] frames until EOF. If an envelope header
// magic is encountered at a frame boundary, the remainder is restored through
// the envelope path, so a legacy file with newer incremental segments appended
// still loads.
func (db *DB) loadLegacy(r io.Reader, maxPendingWrites int) error {
	br := bufio.NewReaderSize(r, 16<<10)
	unmarshalBuf := make([]byte, 1<<10)

	ldr := db.NewKVLoader(maxPendingWrites)
	for {
		// Detect an envelope segment appended after legacy frames. First flush
		// everything the legacy frames already produced, then loadEnvelope
		// validates the whole remainder before any of its keys are written. The
		// legacy frames preceding it are applied as they are read, matching the
		// historical incremental-restore behavior (they have no checksums to
		// validate ahead of time).
		if peek, perr := br.Peek(8); perr == nil && bytes.Equal(peek, []byte(backupHeaderMagic)) {
			if err := db.finishLoad(ldr); err != nil {
				return err
			}
			spooled, cleanup, serr := spoolBackup(br)
			if serr != nil {
				return serr
			}
			defer cleanup()
			return db.loadEnvelope(spooled, spooledSize(spooled), 0, maxPendingWrites)
		}

		var sz uint64
		err := binary.Read(br, binary.LittleEndian, &sz)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if cap(unmarshalBuf) < int(sz) {
			unmarshalBuf = make([]byte, sz)
		}

		if _, err = io.ReadFull(br, unmarshalBuf[:sz]); err != nil {
			return err
		}

		list := &pb.KVList{}
		if err := proto.Unmarshal(unmarshalBuf[:sz], list); err != nil {
			return err
		}

		for _, kv := range list.Kv {
			if err := ldr.Set(kv); err != nil {
				return err
			}

			// Update nextTxnTs, memtable stores this
			// timestamp in badger head when flushed.
			if kv.Version >= db.orc.nextTxnTs {
				db.orc.nextTxnTs = kv.Version + 1
			}
		}
	}

	return db.finishLoad(ldr)
}
