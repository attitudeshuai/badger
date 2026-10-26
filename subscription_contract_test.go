/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/pb"
)

func inMemOpts() *Options {
	opt := DefaultOptions("").WithInMemory(true)
	return &opt
}

func matchPrefix(p string) []pb.Match {
	return []pb.Match{{Prefix: []byte(p), IgnoreBytes: ""}}
}

func setKey(t *testing.T, db *DB, key, val string) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.SetEntry(NewEntry([]byte(key), []byte(val)))
	}))
}

// triple identifies a single key-version at a delivery watermark.
type triple struct {
	key string
	ver uint64
	wm  uint64
}

type skipTriple struct {
	triple
	reason SkipReason
}

// collectTriples flattens delivered changes and skip reports across batches.
func collectTriples(batches []*ChangeBatch) (changes []triple, skipped []skipTriple) {
	for _, b := range batches {
		for _, c := range b.Changes {
			changes = append(changes, triple{string(c.Key), c.Version, c.Watermark})
		}
		for _, sk := range b.Skipped {
			skipped = append(skipped, skipTriple{
				triple: triple{string(sk.Key), sk.Version, sk.Watermark},
				reason: sk.Reason,
			})
		}
	}
	return changes, skipped
}

// AC-1: a subscriber that never consumes must not block writes nor other
// subscribers.
func TestSubscriptionSlowConsumerIsolation(t *testing.T) {
	runBadgerTest(t, inMemOpts(), func(t *testing.T, db *DB) {
		const N = 20000

		// A: Queue policy, handler blocks on the first batch until cleanup.
		firstA := make(chan struct{})
		releaseA := make(chan struct{})
		var aOnce sync.Once
		subA, err := db.NewSubscription(SubscriptionOptions{
			Matches: matchPrefix("key"),
			Policy:  OverflowQueue,
		}, func(_ *Subscription, batch *ChangeBatch) error {
			aOnce.Do(func() { close(firstA) })
			<-releaseA
			return nil
		})
		require.NoError(t, err)

		// B: Queue policy, fast consumer with explicit Ack; receives all.
		var bmu sync.Mutex
		gotB := make(map[string]uint64, N)
		var subB *Subscription
		subB, err = db.NewSubscription(SubscriptionOptions{
			Matches: matchPrefix("key"),
			Policy:  OverflowQueue,
		}, func(batch *ChangeBatch) error {
			bmu.Lock()
			for _, c := range batch.Changes {
				gotB[string(c.Key)] = c.Version
			}
			bmu.Unlock()
			subB.Ack(batch.Watermark)
			return nil
		})
		require.NoError(t, err)

		// Make sure A is parked in its handler before the write storm.
		setKey(t, db, "keywarmup", "v")
		select {
		case <-firstA:
		case <-time.After(10 * time.Second):
			t.Fatal("subscriber A handler never parked")
		}

		// Bounded worker pool hammering writes; all must finish.
		type res struct{ err error }
		work := make(chan int, N)
		for i := 0; i < N; i++ {
			work <- i
		}
		close(work)
		var rmu sync.Mutex
		var errs []error
		var wg sync.WaitGroup
		for w := 0; w < 64; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range work {
					err := db.Update(func(txn *Txn) error {
						return txn.SetEntry(NewEntry(
							[]byte(fmt.Sprintf("key%05d", i)), []byte("v")))
					})
					if err != nil {
						rmu.Lock()
						errs = append(errs, err)
						rmu.Unlock()
					}
				}
			}()
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(90 * time.Second):
			t.Fatal("writes stalled while subscriber A was blocked")
		}
		require.Empty(t, errs)

		// B receives the exact set of storm keys (warmup excluded).
		require.Eventually(t, func() bool {
			bmu.Lock()
			defer bmu.Unlock()
			return len(gotB) == N+1
		}, 15*time.Second, 10*time.Millisecond, "subscriber B got %d", len(gotB))

		close(releaseA)
		subA.Unsubscribe()
		subB.Unsubscribe()
		select {
		case <-subA.s.done:
		case <-time.After(5 * time.Second):
			t.Fatal("subscriber A goroutine did not exit")
		}
		select {
		case <-subB.s.done:
		case <-time.After(5 * time.Second):
			t.Fatal("subscriber B goroutine did not exit")
		}
	})
}

// AC-2: Discard reports every skipped key-version one by one; the delivered
// changes plus skip reports reconstruct the full watermark-dense change set.
func TestSubscriptionDiscardPolicy(t *testing.T) {
	runBadgerTest(t, inMemOpts(), func(t *testing.T, db *DB) {
		const Cap = 10
		const N = 100

		var mu sync.Mutex
		var batches []*ChangeBatch
		first := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		var sub *Subscription
		handler := func(batch *ChangeBatch) error {
			once.Do(func() { close(first) })
			<-release
			mu.Lock()
			batches = append(batches, batch)
			mu.Unlock()
			sub.Ack(batch.Watermark)
			return nil
		}
		var err error
		sub, err = db.NewSubscription(SubscriptionOptions{
			Matches:  matchPrefix("key"),
			Policy:   OverflowDiscard,
			Capacity: Cap,
		}, handler)
		require.NoError(t, err)

		// Item 1 goes inflight and stays there while the handler parks.
		setKey(t, db, "key001", "v1")
		<-first

		// Items 2..N commit despite the blocked consumer.
		for i := 2; i <= N; i++ {
			setKey(t, db, fmt.Sprintf("key%03d", i), fmt.Sprintf("v%d", i))
		}
		close(release)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			ch, sk := collectTriples(batches)
			return len(ch)+len(sk) == N
		}, 10*time.Second, 10*time.Millisecond)

		mu.Lock()
		saved := make([]*ChangeBatch, len(batches))
		copy(saved, batches)
		mu.Unlock()
		changes, skipped := collectTriples(saved)

		// key1 inflight + Cap data pending; the rest are skip reports.
		require.Len(t, changes, Cap+1)
		require.Len(t, skipped, N-Cap-1)

		byKey := make(map[string]skipOrChange, N)
		var wms []uint64
		for _, c := range changes {
			byKey[c.key] = skipOrChange{wm: c.wm, skipped: false}
			wms = append(wms, c.wm)
		}
		for _, s := range skipped {
			require.Equal(t, SkipOverflow, s.reason)
			byKey[s.triple.key] = skipOrChange{wm: s.triple.wm, skipped: true}
			wms = append(wms, s.triple.wm)
		}
		require.Len(t, byKey, N)
		for i := 1; i <= N; i++ {
			_, ok := byKey[fmt.Sprintf("key%03d", i)]
			require.True(t, ok, "missing key%03d", i)
		}
		sort.Slice(wms, func(i, j int) bool { return wms[i] < wms[j] })
		require.Len(t, wms, N)
		for i, wm := range wms {
			require.Equal(t, uint64(i+1), wm, "watermark not dense at %d", i)
		}
		sub.Unsubscribe()
	})
}

type skipOrChange struct {
	wm      uint64
	skipped bool
}

// AC-3: Coalesce delivers only the newest undelivered version of a key and
// reports every superseded version individually.
func TestSubscriptionCoalescePolicy(t *testing.T) {
	runBadgerTest(t, inMemOpts(), func(t *testing.T, db *DB) {
		var mu sync.Mutex
		var batches []*ChangeBatch
		first := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		var sub *Subscription
		handler := func(batch *ChangeBatch) error {
			once.Do(func() { close(first) })
			<-release
			mu.Lock()
			batches = append(batches, batch)
			mu.Unlock()
			sub.Ack(batch.Watermark)
			return nil
		}
		var err error
		sub, err = db.NewSubscription(SubscriptionOptions{
			Matches:  matchPrefix("k"),
			Policy:   OverflowCoalesce,
			Capacity: 8,
		}, handler)
		require.NoError(t, err)

		// Anchor item goes inflight while handler parks.
		setKey(t, db, "k-anchor", "anchor")
		<-first

		// Five versions of the same key while pending: only the last stays
		// a data change; the four earlier versions become skip reports.
		for v := 2; v <= 6; v++ {
			setKey(t, db, "k-hot", fmt.Sprintf("hot%d", v))
		}
		// Distinct filler keys are not coalesced.
		for _, name := range []string{"k-b", "k-c", "k-d"} {
			setKey(t, db, name, name+"-v")
		}
		close(release)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			ch, sk := collectTriples(batches)
			return len(sk) == 4 && len(ch) == 5
		}, 10*time.Second, 10*time.Millisecond)

		mu.Lock()
		saved := make([]*ChangeBatch, len(batches))
		copy(saved, batches)
		mu.Unlock()

		var hotDeliveredValues []string
		var hotSkippedVersions []uint64
		hotVersions := map[uint64]bool{}
		deliveredKeys := map[string]int{}
		for _, b := range saved {
			for _, c := range b.Changes {
				deliveredKeys[string(c.Key)]++
				if string(c.Key) == "k-hot" {
					hotDeliveredValues = append(hotDeliveredValues, string(c.Value))
					hotVersions[c.Version] = true
				}
			}
			for _, sk := range b.Skipped {
				require.Equal(t, "k-hot", string(sk.Key))
				require.Equal(t, SkipCoalesced, sk.Reason)
				hotSkippedVersions = append(hotSkippedVersions, sk.Version)
				hotVersions[sk.Version] = true
			}
		}

		require.ElementsMatch(t, []string{"hot6"}, hotDeliveredValues)
		require.Len(t, hotSkippedVersions, 4)
		sort.Slice(hotSkippedVersions, func(i, j int) bool {
			return hotSkippedVersions[i] < hotSkippedVersions[j]
		})
		// The five hot versions are all accounted for exactly once.
		require.Len(t, hotVersions, 5)
		require.Equal(t, 1, deliveredKeys["k-anchor"])
		require.Equal(t, 1, deliveredKeys["k-b"])
		require.Equal(t, 1, deliveredKeys["k-c"])
		require.Equal(t, 1, deliveredKeys["k-d"])
		sub.Unsubscribe()
	})
}

// AC-4: Ack releases retained items up to the watermark and delivery resumes
// exactly past the acked watermark.
func TestSubscriptionAckAndResume(t *testing.T) {
	runBadgerTest(t, inMemOpts(), func(t *testing.T, db *DB) {
		started := make(chan struct{}, 16)
		proceed := make(chan struct{})
		var mu sync.Mutex
		var batches []*ChangeBatch
		var sub *Subscription
		handler := func(batch *ChangeBatch) error {
			mu.Lock()
			batches = append(batches, batch)
			mu.Unlock()
			started <- struct{}{}
			<-proceed // parks on every invocation until the test releases it
			return nil
		}
		var err error
		sub, err = db.NewSubscription(SubscriptionOptions{
			Matches: matchPrefix("key"),
			Policy:  OverflowQueue,
		}, handler)
		require.NoError(t, err)

		// Batch 1: exactly key1 (handler parks, rest stays pending).
		setKey(t, db, "key1", "v")
		<-started
		for i := 2; i <= 6; i++ {
			setKey(t, db, fmt.Sprintf("key%d", i), "v")
		}
		// Dispatch is asynchronous, wait until all five reached the queue.
		require.Eventually(t, func() bool { return sub.s.retainedCount() == 6 },
			5*time.Second, 5*time.Millisecond)

		// Release batch 1; the delivery goroutine pulls key2..key6 into
		// batch 2 and parks again.
		proceed <- struct{}{}
		<-started
		require.Len(t, batches, 2)

		// Inflight now spans watermarks 1..6; acking 3 retains exactly 4..6.
		sub.Ack(3)
		require.Eventually(t, func() bool { return sub.s.retainedCount() == 3 },
			2*time.Second, 5*time.Millisecond)
		// Duplicate / older acks are no-ops.
		sub.Ack(3)
		sub.Ack(1)
		require.Equal(t, 3, sub.s.retainedCount())

		// Write 7..9 while batch 2 is parked; they stay pending.
		for i := 7; i <= 9; i++ {
			setKey(t, db, fmt.Sprintf("key%d", i), "v")
		}
		require.Eventually(t, func() bool { return sub.s.retainedCount() == 6 },
			5*time.Second, 5*time.Millisecond)
		proceed <- struct{}{}
		<-started
		mu.Lock()
		require.Len(t, batches, 3)
		mu.Unlock()

		// Batch 3 resumes strictly past the acked watermark: first item is
		// key7; nothing <= 6 is re-delivered.
		mu.Lock()
		b3 := batches[2]
		mu.Unlock()
		require.Len(t, b3.Changes, 3)
		require.Equal(t, "key7", string(b3.Changes[0].Key))
		require.Equal(t, "key9", string(b3.Changes[2].Key))
		require.Empty(t, b3.Skipped)

		sub.Ack(6)
		require.Eventually(t, func() bool { return sub.s.retainedCount() == 3 },
			2*time.Second, 5*time.Millisecond)
		sub.Ack(9)
		require.Eventually(t, func() bool { return sub.s.retainedCount() == 0 },
			2*time.Second, 5*time.Millisecond)

		proceed <- struct{}{}
		sub.Unsubscribe()
	})
}

// AC-5: Unsubscribe releases the trie index and buffers immediately; the
// same Matches can be re-subscribed right away.
func TestSubscriptionUnsubscribeReleases(t *testing.T) {
	runBadgerTest(t, inMemOpts(), func(t *testing.T, db *DB) {
		var sub *Subscription
		got := make(chan *ChangeBatch, 16)
		sub, err := db.NewSubscription(SubscriptionOptions{
			Matches: matchPrefix("foo"),
			Policy:  OverflowQueue,
		}, func(batch *ChangeBatch) error {
			cp := *batch
			got <- &cp
			sub.Ack(batch.Watermark)
			return nil
		})
		require.NoError(t, err)
		setKey(t, db, "foo1", "v")
		select {
		case b := <-got:
			require.Len(t, b.Changes, 1)
			require.Equal(t, "foo1", string(b.Changes[0].Key))
		case <-time.After(10 * time.Second):
			t.Fatal("first delivery missing")
		}

		sub.Unsubscribe()
		select {
		case <-sub.s.done:
		case <-time.After(5 * time.Second):
			t.Fatal("delivery goroutine did not exit")
		}
		require.Equal(t, 0, sub.s.retainedCount())
		sub.Unsubscribe() // idempotent

		db.pub.mu.RLock()
		_, present := db.pub.subscribers[sub.s.id]
		db.pub.mu.RUnlock()
		require.False(t, present)

		// Writes after unsubscribe are not delivered and never block.
		setKey(t, db, "foo2", "v")

		// Re-subscribe with the same range: receives only new writes, which
		// also proves the trie has no stale id entry.
		var sub2 *Subscription
		got2 := make(chan *ChangeBatch, 16)
		sub2, err = db.NewSubscription(SubscriptionOptions{
			Matches: matchPrefix("foo"),
			Policy:  OverflowQueue,
		}, func(batch *ChangeBatch) error {
			cp := *batch
			got2 <- &cp
			sub2.Ack(batch.Watermark)
			return nil
		})
		require.NoError(t, err)
		setKey(t, db, "foo3", "v")
		select {
		case b := <-got2:
			var keys []string
			for _, c := range b.Changes {
				keys = append(keys, string(c.Key))
			}
			require.Equal(t, []string{"foo3"}, keys)
		case <-time.After(10 * time.Second):
			t.Fatal("re-subscribed delivery missing")
		}
		sub2.Unsubscribe()
	})
}

// AC-5 (equivalence): context cancellation unsubscribes and releases.
func TestSubscriptionContextCancel(t *testing.T) {
	runBadgerTest(t, inMemOpts(), func(t *testing.T, db *DB) {
		ctx, cancel := context.WithCancel(context.Background())
		var sub *Subscription
		got := make(chan struct{}, 16)
		sub, err := db.NewSubscription(SubscriptionOptions{
			Matches: matchPrefix("bar"),
			Policy:  OverflowQueue,
			Context: ctx,
		}, func(batch *ChangeBatch) error {
			got <- struct{}{}
			sub.Ack(batch.Watermark)
			return nil
		})
		require.NoError(t, err)
		setKey(t, db, "bar1", "v")
		select {
		case <-got:
		case <-time.After(10 * time.Second):
			t.Fatal("delivery missing")
		}

		cancel()
		select {
		case <-sub.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("subscription did not end on ctx cancel")
		}
		require.ErrorIs(t, sub.Wait(), context.Canceled)
		require.Equal(t, 0, sub.s.retainedCount())
	})
}

// Invalid subscription options are rejected without partial state.
func TestSubscriptionInvalidOptions(t *testing.T) {
	runBadgerTest(t, inMemOpts(), func(t *testing.T, db *DB) {
		_, err := db.NewSubscription(SubscriptionOptions{
			Matches: matchPrefix("k"),
			Policy:  OverflowQueue,
		}, nil)
		require.ErrorIs(t, err, ErrNilCallback)

		h := func(*ChangeBatch) error { return nil }
		_, err = db.NewSubscription(SubscriptionOptions{
			Matches: matchPrefix("k"), Policy: OverflowDiscard, Capacity: 0,
		}, h)
		require.ErrorIs(t, err, ErrInvalidSubscription)

		_, err = db.NewSubscription(SubscriptionOptions{
			Matches: matchPrefix("k"), Policy: OverflowCoalesce, Capacity: -1,
		}, h)
		require.ErrorIs(t, err, ErrInvalidSubscription)

		_, err = db.NewSubscription(SubscriptionOptions{
			Matches: matchPrefix("k"), Policy: OverflowPolicy(99), Capacity: 4,
		}, h)
		require.ErrorIs(t, err, ErrInvalidSubscription)

		// Invalid IgnoreBytes must not leave a registered subscriber.
		_, err = db.NewSubscription(SubscriptionOptions{
			Matches: []pb.Match{{Prefix: []byte("k"), IgnoreBytes: "a-b"}},
			Policy:  OverflowQueue,
		}, h)
		require.Error(t, err)
		db.pub.mu.RLock()
		n := len(db.pub.subscribers)
		db.pub.mu.RUnlock()
		require.Equal(t, 0, n)
	})
}
