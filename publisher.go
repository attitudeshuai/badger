/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/trie"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// ChangeOrigin identifies how a delivered change entered the write pipeline.
type ChangeOrigin uint8

const (
	// OriginTxnCommit is the origin of changes applied through a regular
	// Txn.Commit / Txn.CommitWith (including Merge and backup restore).
	OriginTxnCommit ChangeOrigin = iota
	// OriginBatchCommit is the origin of changes applied through WriteBatch.
	OriginBatchCommit
	// OriginRewrite is the origin of changes written back by the background
	// value-log GC rewrite. Such changes carry the SAME key Version as the
	// original committed entry; subscribers may filter them by Origin.
	OriginRewrite
)

// OverflowPolicy declares how a subscription behaves when it cannot keep up
// with the matched change stream.
type OverflowPolicy uint8

const (
	// OverflowQueue keeps buffering undelivered changes without skipping
	// anything. The buffer is unbounded (bounded only by process memory).
	// This is the policy used internally by the legacy Subscribe API.
	OverflowQueue OverflowPolicy = iota
	// OverflowCoalesce keeps only the latest undelivered version of a key.
	// Superseded versions are reported one-by-one as SkipCoalesced; when the
	// Capacity is reached by distinct keys, the oldest pending change is
	// reported as SkipOverflow.
	OverflowCoalesce
	// OverflowDiscard skips the oldest pending changes once Capacity is
	// reached. Every skipped change is reported one-by-one as SkipOverflow.
	OverflowDiscard
)

// SkipReason explains why a change was not delivered as a regular Change.
type SkipReason uint8

const (
	// SkipCoalesced means a newer version of the same key superseded this one
	// under OverflowCoalesce.
	SkipCoalesced SkipReason = iota
	// SkipOverflow means the change was skipped to respect Capacity.
	SkipOverflow
	// SkipUndelivered means the change could not be completed before the DB
	// was closed (either never handed to the handler or not acknowledged
	// within DrainTimeout).
	SkipUndelivered
)

// Change is a single matched key-version delivered to a subscription.
type Change struct {
	// Key is the user key without the commit-timestamp suffix.
	Key []byte
	// Value is the value committed for this key version.
	Value []byte
	// UserMeta is the user metadata byte set on the entry.
	UserMeta byte
	// ExpiresAt is the expiration timestamp of the entry, if any.
	ExpiresAt uint64
	// Version is the key's commit version (commit timestamp). Background
	// rewrites re-deliver changes with the same Version.
	Version uint64
	// Watermark is the per-item delivery watermark; within a batch it is at
	// most ChangeBatch.Watermark and is strictly increasing across items.
	Watermark uint64
	// Origin tells how this change entered the write pipeline.
	Origin ChangeOrigin
}

// SkippedChange reports a single key-version that was not delivered as a
// regular Change. Skipped entries are always reported, never dropped silently.
type SkippedChange struct {
	// Key is the user key (without the commit-timestamp suffix).
	Key []byte
	// Version is the skipped key version.
	Version uint64
	// Watermark is the delivery watermark that was assigned to this change.
	Watermark uint64
	// Reason explains why the change was skipped.
	Reason SkipReason
}

// ChangeBatch is a unit of delivery to a subscription handler. Changes and
// Skipped are each ordered by watermark. Ack with Watermark confirms every
// item in the batch, including the skip reports.
type ChangeBatch struct {
	// Watermark is the highest delivery watermark contained in this batch.
	Watermark uint64
	// Changes are the matched key versions delivered in this batch.
	Changes []*Change
	// Skipped are the key versions that were skipped, reported one-by-one
	// in watermark order.
	Skipped []SkippedChange
}

// SubscriptionOptions declares a subscription's delivery contract.
type SubscriptionOptions struct {
	// Matches specifies the key ranges to subscribe to. Matching semantics
	// are identical to the legacy Subscribe API (prefix + IgnoreBytes).
	Matches []pb.Match
	// Policy selects the overflow behavior when the subscriber cannot keep
	// up. Defaults to OverflowQueue.
	Policy OverflowPolicy
	// Capacity is the maximum number of pending (not yet delivered) data
	// changes before OverflowCoalesce/OverflowDiscard starts skipping. It
	// must be positive for those two policies and is ignored for
	// OverflowQueue. Skip reports are never counted against Capacity.
	Capacity int
	// Context, when non-nil, cancels the subscription when done, equivalent
	// to calling Subscription.Unsubscribe.
	Context context.Context
	// DrainTimeout bounds how long DB.Close waits for pending changes to be
	// delivered and acknowledged. Zero selects the default (5 seconds).
	DrainTimeout time.Duration
}

// changeOrigin is the internal alias threaded through write requests.
type changeOrigin = ChangeOrigin

// defaultDrainTimeout is used when SubscriptionOptions.DrainTimeout is zero.
const defaultDrainTimeout = 5 * time.Second

// publisherInputCap bounds how many accepted write batches wait for the
// dispatch goroutine. The dispatcher only performs trie matching (short
// critical section) and non-blocking per-subscriber enqueues, so it always
// drains fast; the buffer only absorbs scheduling bursts.
const publisherInputCap = 1000

type publisher struct {
	// mu guards subscribers, indexer, seq and closed. It is never held
	// while waiting on a subscriber: fan-out hands copies to per-subscriber
	// queues outside the lock, so a slow consumer cannot stall writers.
	mu          sync.RWMutex
	seq         atomic.Uint64
	subscribers map[uint64]*subscriber
	indexer     *trie.Trie
	nextID      uint64
	closed      bool

	input  chan requests
	closer *z.Closer
}

func newPublisher() *publisher {
	return &publisher{
		input:       make(chan requests, publisherInputCap),
		subscribers: make(map[uint64]*subscriber),
		indexer:     trie.NewTrie(),
	}
}

// run is the single dispatch goroutine. It serializes watermark assignment
// and trie matching, but every per-subscriber enqueue is non-blocking and
// happens without the publisher lock held.
func (p *publisher) run(c *z.Closer) {
	defer c.Done()
	for {
		select {
		case reqs := <-p.input:
			p.dispatch(reqs)
		case <-c.HasBeenClosed():
			// DB close stops writers and closes writeCh before signaling us,
			// but drain anything already accepted into the input queue.
			for {
				select {
				case reqs := <-p.input:
					p.dispatch(reqs)
				default:
					p.shutdown()
					return
				}
			}
		}
	}
}

func (p *publisher) dispatch(first requests) {
	batch := first
	// Coalesce everything already queued, matching the old slurp behavior.
	for {
		select {
		case reqs := <-p.input:
			batch = append(batch, reqs...)
		default:
			p.fanout(batch)
			batch.DecrRef()
			return
		}
	}
}

type matchedChange struct {
	id uint64
	wm uint64
	c  *Change
}

// fanout matches entries against the index and hands copies to subscribers.
func (p *publisher) fanout(batch requests) {
	p.mu.RLock()
	if len(p.subscribers) == 0 {
		p.mu.RUnlock()
		return
	}
	hits := make([]matchedChange, 0)
	targets := make(map[uint64]*subscriber)
	for _, req := range batch {
		for _, e := range req.Entries {
			ids := p.indexer.Get(e.Key)
			if len(ids) == 0 {
				continue
			}
			c := entryToChange(e, req.origin)
			wm := p.seq.Add(1)
			c.Watermark = wm
			for id := range ids {
				hits = append(hits, matchedChange{id: id, wm: wm, c: c})
				targets[id] = p.subscribers[id]
			}
		}
	}
	p.mu.RUnlock()

	// Enqueue outside the lock. Subscribers that unsubscribe concurrently
	// reject the hand-off; their trie matches were removed atomically.
	for _, h := range hits {
		if s := targets[h.id]; s != nil {
			s.enqueue(h.c, h.wm)
		}
	}
}

func entryToChange(e *Entry, origin changeOrigin) *Change {
	k := y.SafeCopy(nil, e.Key)
	return &Change{
		Key:       y.ParseKey(k),
		Value:     y.SafeCopy(nil, e.Value),
		UserMeta:  e.UserMeta,
		ExpiresAt: e.ExpiresAt,
		Version:   y.ParseTs(k),
		Origin:    origin,
	}
}

// sendUpdates is the single delivery entry point, called only from
// writeRequests after the write has landed in the memtable.
func (p *publisher) sendUpdates(reqs requests) {
	p.mu.RLock()
	n := len(p.subscribers)
	p.mu.RUnlock()
	if n == 0 {
		return
	}
	reqs.IncrRef()
	// The closer guard only matters during shutdown: writers are stopped
	// first, but never block here even if dispatch has exited.
	select {
	case p.input <- reqs:
	case <-p.closer.HasBeenClosed():
		reqs.DecrRef()
	}
}

func (p *publisher) newSubscription(opts SubscriptionOptions, handler ChangeHandler) (*subscriber, error) {
	if handler == nil {
		return nil, ErrNilCallback
	}
	switch opts.Policy {
	case OverflowQueue, OverflowCoalesce, OverflowDiscard:
	default:
		return nil, fmt.Errorf("%w: unknown OverflowPolicy %d", ErrInvalidSubscription, opts.Policy)
	}
	if opts.Policy != OverflowQueue && opts.Capacity <= 0 {
		return nil, fmt.Errorf("%w: Capacity must be positive for policy %d",
			ErrInvalidSubscription, opts.Policy)
	}
	drainTimeout := opts.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = defaultDrainTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	s := &subscriber{
		p:             p,
		matches:       opts.Matches,
		policy:        opts.Policy,
		capacity:      opts.Capacity,
		drainTimeout:  drainTimeout,
		handler:       handler,
		ctx:           ctx,
		notify:        make(chan struct{}, 1),
		done:          make(chan struct{}),
		finished:      make(chan struct{}),
	}

	p.mu.Lock()
	s.id = p.nextID
	p.nextID++
	added := 0
	for _, m := range opts.Matches {
		if err := p.indexer.AddMatch(m, s.id); err != nil {
			// Roll back anything added so a failed subscribe leaves no
			// residual index entries.
			for i := 0; i < added; i++ {
				_ = p.indexer.DeleteMatch(opts.Matches[i], s.id)
			}
			p.mu.Unlock()
			return nil, y.Wrapf(err, "while creating a new subscriber")
		}
		added++
	}
	p.subscribers[s.id] = s
	p.mu.Unlock()

	// Build the public handle before starting goroutines so handlers can
	// safely receive and use it without racing NewSubscription's return.
	s.handle = &Subscription{s: s}
	go s.deliver()
	if opts.Context != nil {
		go s.watchContext()
	}
	return s, nil
}

// removeSubscriber removes the trie matches and the registry entry. It must
// not be called with the subscriber mutex held (lock order is publisher ->
// subscriber).
func (p *publisher) removeSubscriber(id uint64) *subscriber {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.subscribers[id]
	if !ok {
		return nil
	}
	for _, m := range s.matches {
		_ = p.indexer.DeleteMatch(m, id)
	}
	delete(p.subscribers, id)
	return s
}

func (p *publisher) unsubscribe(id uint64, err error) {
	if s := p.removeSubscriber(id); s != nil {
		s.finish(err, true)
	}
}

// shutdown drains every subscriber after all accepted writes have been
// dispatched. Remaining changes are either completed within DrainTimeout or
// reported one-by-one as SkipUndelivered.
func (p *publisher) shutdown() {
	p.mu.Lock()
	p.closed = true
	subs := make([]*subscriber, 0, len(p.subscribers))
	for _, s := range p.subscribers {
		subs = append(subs, s)
	}
	p.mu.Unlock()

	for _, s := range subs {
		s.beginDrain()
	}

	var wg sync.WaitGroup
	for _, s := range subs {
		wg.Add(1)
		go func(s *subscriber) {
			defer wg.Done()
			s.waitUntilQuiet(s.drainTimeout)
		}(s)
	}
	wg.Wait()

	// Finalize in registration order so shutdown reporting is deterministic.
	for _, s := range subs {
		s.finalizeShutdown()
	}
}

// queuedItem is one retained watermark slot. It is either a data change or a
// skip report. Eviction converts a data slot in place, never growing the
// queue.
type queuedItem struct {
	wm uint64
	c  *Change
	sk SkippedChange
}

type subscriber struct {
	p       *publisher
	id      uint64
	matches []pb.Match

	policy       OverflowPolicy
	capacity     int
	drainTimeout time.Duration
	handler      ChangeHandler
	handle       *Subscription
	ctx          context.Context

	// mu guards all state below.
	mu               sync.Mutex
	closed           bool // unsubscribed / handler error: drop and stop
	draining         bool // DB close in progress
	finalized        bool // shutdown finalization done
	finishedFlag     bool
	terminalAttempt  bool
	ackWm            uint64
	pending          []*queuedItem
	inflight         []*queuedItem
	pendingDataCount int
	keyIdx           map[string]int
	finalBatch       []SkippedChange
	err              error
	undelivered      []SkippedChange

	notify   chan struct{}
	done     chan struct{} // closed when the delivery goroutine exits
	finished chan struct{} // closed when the subscription lifecycle ends
}

// enqueue hands one matched change to the subscriber. It must never block on
// the consumer: bounded policies convert overflow into skip reports.
func (s *subscriber) enqueue(c *Change, wm uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.finalized {
		return
	}
	item := &queuedItem{wm: wm, c: c}
	switch s.policy {
	case OverflowQueue:
		s.pending = append(s.pending, item)
		s.pendingDataCount++

	case OverflowCoalesce:
		if s.keyIdx == nil {
			s.keyIdx = make(map[string]int)
		}
		key := string(c.Key)
		if idx, ok := s.keyIdx[key]; ok {
			// An undelivered older version of this key is still pending:
			// convert that slot into a coalesced skip report.
			s.convertToSkip(idx, SkipCoalesced)
			delete(s.keyIdx, key)
		} else if s.pendingDataCount >= s.capacity {
			s.evictOldestData(SkipOverflow)
		}
		s.pending = append(s.pending, item)
		s.pendingDataCount++
		s.keyIdx[key] = len(s.pending) - 1

	case OverflowDiscard:
		if s.pendingDataCount >= s.capacity {
			s.evictOldestData(SkipOverflow)
		}
		s.pending = append(s.pending, item)
		s.pendingDataCount++
	}
	s.signalLocked()
}

// convertToSkip turns a pending data slot into a skip report in place.
// Caller holds s.mu.
func (s *subscriber) convertToSkip(idx int, reason SkipReason) {
	old := s.pending[idx]
	y.AssertTrue(old.c != nil)
	old.sk = SkippedChange{
		Key:       old.c.Key,
		Version:   old.c.Version,
		Watermark: old.wm,
		Reason:    reason,
	}
	old.c = nil
	s.pendingDataCount--
}

// evictOldestData converts the oldest pending data slot into a skip report.
// Skip-report slots themselves are never evicted. Caller holds s.mu.
func (s *subscriber) evictOldestData(reason SkipReason) {
	for idx, it := range s.pending {
		if it.c == nil {
			continue
		}
		delete(s.keyIdx, string(it.c.Key))
		s.convertToSkip(idx, reason)
		return
	}
}

func (s *subscriber) signal() {
	s.mu.Lock()
	s.signalLocked()
	s.mu.Unlock()
}

func (s *subscriber) signalLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// deliver is the per-subscriber delivery goroutine. A blocked handler only
// blocks this goroutine; the dispatcher and other subscribers are unaffected.
func (s *subscriber) deliver() {
	defer close(s.done)
	for {
		items, finalBatch, stop := s.takeNext()
		if len(items) > 0 {
			batch := buildBatch(items)
			if err := s.safeInvoke(batch); err != nil {
				s.p.unsubscribe(s.id, err)
				return
			}
			continue
		}
		if stop {
			if !s.terminalAttempted() && len(finalBatch) > 0 {
				// Best-effort in-band delivery of the undelivered report.
				wm := uint64(0)
				for _, sk := range finalBatch {
					if sk.Watermark > wm {
						wm = sk.Watermark
					}
				}
				s.markTerminalAttempted()
				_ = s.safeInvoke(&ChangeBatch{Watermark: wm, Skipped: finalBatch})
			}
			return
		}
		select {
		case <-s.notify:
		case <-s.finished:
		}
	}
}

// takeNext pops all pending items into the inflight ledger, or reports the
// terminal state. Items are handed over under the lock; the handler runs
// without any lock.
func (s *subscriber) takeNext() (items []*queuedItem, finalBatch []SkippedChange, stop bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed && !s.finalized && len(s.pending) > 0 {
		items = s.pending
		s.pending = nil
		s.keyIdx = nil
		s.pendingDataCount = 0
		s.inflight = append(s.inflight, items...)
		return items, nil, false
	}
	return nil, s.finalBatch, s.closed || s.finalized
}

func (s *subscriber) terminalAttempted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.terminalAttempt
}

func (s *subscriber) markTerminalAttempted() {
	s.mu.Lock()
	s.terminalAttempt = true
	s.mu.Unlock()
}

func buildBatch(items []*queuedItem) *ChangeBatch {
	b := &ChangeBatch{
		Watermark: items[len(items)-1].wm,
		Changes:   make([]*Change, 0, len(items)),
		Skipped:   make([]SkippedChange, 0),
	}
	for _, it := range items {
		if it.c != nil {
			b.Changes = append(b.Changes, it.c)
		} else {
			b.Skipped = append(b.Skipped, it.sk)
		}
	}
	return b
}

func (s *subscriber) safeInvoke(batch *ChangeBatch) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("badger: subscription handler panic: %v", r)
		}
	}()
	return s.handler(s.handle, batch)
}

func (s *subscriber) watchContext() {
	select {
	case <-s.ctx.Done():
		s.p.unsubscribe(s.id, s.ctx.Err())
	case <-s.finished:
	}
}

// ack advances the confirmation watermark. Items at or below the watermark
// are released immediately. Acks are monotone: smaller/duplicate values are
// ignored.
func (s *subscriber) ack(wm uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if wm <= s.ackWm {
		return
	}
	s.ackWm = wm
	trimmed := 0
	for _, it := range s.inflight {
		if it.wm > wm {
			break
		}
		trimmed++
	}
	if trimmed > 0 {
		remaining := s.inflight[trimmed:]
		s.inflight = s.inflight[:0]
		s.inflight = append(s.inflight, remaining...)
	}
	s.signalLocked()
}

func (s *subscriber) beginDrain() {
	s.mu.Lock()
	s.draining = true
	s.signalLocked()
	s.mu.Unlock()
}

// waitUntilQuiet waits until every item has been delivered and acknowledged,
// the subscription ended otherwise, or the timeout elapsed.
func (s *subscriber) waitUntilQuiet(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		s.mu.Lock()
		quiet := len(s.pending) == 0 && len(s.inflight) == 0
		done := s.finishedFlag
		s.mu.Unlock()
		if quiet || done {
			return true
		}
		select {
		case <-s.notify:
		case <-s.finished:
			return true
		case <-timer.C:
			return false
		}
	}
}

// finalizeShutdown marks any remaining change as SkipUndelivered and ends the
// subscription with ErrSubscriptionClosed.
func (s *subscriber) finalizeShutdown() {
	s.mu.Lock()
	if s.finishedFlag {
		s.mu.Unlock()
		return
	}
	remaining := make([]*queuedItem, 0, len(s.pending)+len(s.inflight))
	remaining = append(remaining, s.pending...)
	remaining = append(remaining, s.inflight...)
	und := make([]SkippedChange, 0, len(remaining))
	for _, it := range remaining {
		if it.c != nil {
			und = append(und, SkippedChange{
				Key:       it.c.Key,
				Version:   it.c.Version,
				Watermark: it.wm,
				Reason:    SkipUndelivered,
			})
		}
	}
	s.finishedFlag = true
	s.finalized = true
	s.terminalAttempt = false
	s.err = ErrSubscriptionClosed
	s.undelivered = und
	s.finalBatch = und
	s.pending = nil
	s.inflight = nil
	s.keyIdx = nil
	s.pendingDataCount = 0
	s.signalLocked()
	s.mu.Unlock()
	close(s.finished)
}

// finish ends the subscription for non-shutdown reasons (explicit
// unsubscribe, context cancellation or handler error), dropping all retained
// buffers immediately.
func (s *subscriber) finish(err error, drop bool) {
	s.mu.Lock()
	if s.finishedFlag {
		s.mu.Unlock()
		return
	}
	s.finishedFlag = true
	s.closed = true
	s.err = err
	if drop {
		s.pending = nil
		s.inflight = nil
		s.keyIdx = nil
		s.pendingDataCount = 0
	}
	s.signalLocked()
	s.mu.Unlock()
	close(s.finished)
}

func (s *subscriber) errResult() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *subscriber) undeliveredList() []SkippedChange {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.undelivered) == 0 {
		return nil
	}
	out := make([]SkippedChange, len(s.undelivered))
	copy(out, s.undelivered)
	return out
}

// retainedCount is the number of items kept since the ack watermark
// (pending + inflight). Used by tests.
func (s *subscriber) retainedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending) + len(s.inflight)
}

// ChangeHandler is invoked serially, in watermark order, with every
// ChangeBatch delivered to a subscription. The supplied *Subscription is the
// handle returned by NewSubscription; use it to Ack batches. It must not be
// closed over from before NewSubscription returns.
type ChangeHandler func(*Subscription, *ChangeBatch) error

// Subscription is a handle returned by DB.NewSubscription. The handler runs
// on an internal goroutine and ChangeBatch items are delivered strictly in
// watermark order, one batch at a time.
type Subscription struct {
	s *subscriber
}

// Ack confirms that every item up to and including watermark has been
// processed. Retained items at or below the watermark are released. Delivery
// always continues from the highest confirmed watermark.
func (sub *Subscription) Ack(watermark uint64) {
	sub.s.ack(watermark)
}

// Unsubscribe immediately removes the subscription's key-range matches from
// the matching index and drops all its private buffers. An in-flight handler
// invocation cannot be interrupted, but no further batches are delivered and
// the delivery goroutine exits as soon as it returns. Safe to call multiple
// times.
func (sub *Subscription) Unsubscribe() {
	sub.s.p.unsubscribe(sub.s.id, nil)
}

// Wait blocks until the subscription ends (Unsubscribe, context done,
// handler error or DB close).
func (sub *Subscription) Wait() error {
	<-sub.s.finished
	return sub.s.errResult()
}

// Done returns a channel closed when the subscription lifecycle ends.
func (sub *Subscription) Done() <-chan struct{} {
	return sub.s.finished
}

// Err returns the terminal error (nil after Unsubscribe, ctx error after
// cancellation, ErrSubscriptionClosed after DB close, or the handler error).
func (sub *Subscription) Err() error {
	select {
	case <-sub.s.finished:
		return sub.s.errResult()
	default:
		return nil
	}
}

// Undelivered returns the changes that could not be completed before DB
// close, one entry per key-version with reason SkipUndelivered. The slice is
// empty when all pending changes were delivered and acknowledged.
func (sub *Subscription) Undelivered() []SkippedChange {
	return sub.s.undeliveredList()
}

// NewSubscription starts a change subscription with explicit delivery
// semantics: monotonic watermarks, per-item skip reporting, explicit Ack and
// a declared OverflowPolicy. It returns immediately; batches are delivered to
// handler on an internal goroutine, along with the subscription handle (use
// it to Ack). Call Subscription.Wait to block until the subscription ends.
//
// The key-range matching semantics are identical to DB.Subscribe.
func (db *DB) NewSubscription(opts SubscriptionOptions, handler ChangeHandler) (*Subscription, error) {
	s, err := db.pub.newSubscription(opts, handler)
	if err != nil {
		return nil, err
	}
	return s.handle, nil
}
