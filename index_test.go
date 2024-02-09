package vfs

import (
	"crypto/rand"
	"errors"
	"fmt"
	mrand "math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jimsnab/afero"
)

const kTestKeyGroup = "test"

func benchmarkInitialize(b *testing.B) (ts *testState) {
	ts = &testState{}
	ts.originalFs = AppFs
	AppFs = afero.NewMemMapFs()

	ts.testDir = "/afero/data"
	err := AppFs.MkdirAll(ts.testDir, 0744)
	if err != nil {
		b.Fatal(err)
	}

	ts.shard = mrand.Uint64()
	ts.position = mrand.Uint64()

	b.Cleanup(func() {
		AppFs = ts.originalFs
	})
	return
}

func TestIndexWrites(t *testing.T) {
	ts := testInitialize(t, false)

	index, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	ids := make([][20]byte, 0, 10000)

	for i := 0; i < 10000; i++ {
		buf := [20]byte{}
		_, err := rand.Read(buf[:])
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, buf)

		txn, err := index.BeginTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
		if err != nil {
			t.Fatal(err)
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 10000; i++ {
		buf := ids[i]
		txn, err := index.BeginTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		found, shard, position, err := txn.Get(kTestKeyGroup, buf)
		if err != nil {
			t.Fatal(err)
		}

		if !found {
			t.Fatal("not found")
		}

		if position != uint64(i) {
			t.Fatal("not i")
		}

		if shard != ts.shard {
			t.Fatal("not shard")
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestIndexWrites2(t *testing.T) {
	ts := testInitialize(t, false)

	AppFs = afero.NewOsFs()
	ts.testDir = "/tmp/data"
	AppFs.RemoveAll(ts.testDir)
	AppFs.MkdirAll(ts.testDir, 0744)

	index1, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}

	ids := make([][20]byte, 0, 10000)

	for i := 0; i < 10000; i++ {
		buf := [20]byte{}
		_, err := rand.Read(buf[:])
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, buf)

		txn, err := index1.BeginTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
		if err != nil {
			t.Fatal(err)
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}
	}

	index1.Close()

	index2, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10000; i++ {
		buf := ids[i]
		txn, err := index2.BeginTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		found, shard, position, err := txn.Get(kTestKeyGroup, buf)
		if err != nil {
			t.Fatal(err)
		}

		if !found {
			t.Fatal("not found")
		}

		if position != uint64(i) {
			t.Fatal("not i")
		}

		if shard != ts.shard {
			t.Fatal("not shard")
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}
	}

	index2.Close()
}

func BenchmarkIndex(b *testing.B) {
	ts := benchmarkInitialize(b)

	index1, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 20000; i++ {
		buf := [20]byte{}
		_, err := rand.Read(buf[:])
		if err != nil {
			b.Fatal(err)
		}

		txn, err := index1.BeginTransaction(nil)
		if err != nil {
			b.Fatal(err)
		}

		err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
		if err != nil {
			b.Fatal(err)
		}

		err = txn.EndTransaction()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStore(b *testing.B) {
	originalFs := AppFs
	//AppFs = afero.NewMemMapFs()
	AppFs = afero.NewOsFs()
	b.Cleanup(func() {
		AppFs = originalFs
	})

	testDir := "/tmp/data"
	err := AppFs.MkdirAll(testDir, 0744)
	if err != nil {
		b.Fatal(err)
	}

	cfg := VfsConfig{
		IndexDir:        testDir,
		DataDir:         testDir,
		BaseName:        "the.test",
		Sync:            true,
		RecoveryEnabled: true,
		ReferenceTables: []string{"A", "B"},
	}

	st, err := NewStore(&cfg)
	if err != nil {
		b.Fatal(err)
	}

	var wg sync.WaitGroup
	count := 200
	batchSize := 128

	start := time.Now()
	last := start
	for i := 0; i < count; i++ {
		if time.Since(last).Seconds() >= 1 {
			last = time.Now()
			fmt.Printf("%d\n", i)
		}
		records := make([]StoreRecord, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			key := [20]byte{}
			_, err := rand.Read(key[:])
			if err != nil {
				b.Fatal(err)
			}

			doc := make([]byte, mrand.Intn(2048)+2048)
			if _, err = rand.Read(doc); err != nil {
				b.Fatal(err)
			}

			ref1 := [20]byte{}
			if _, err = rand.Read(ref1[:]); err != nil {
				b.Fatal(err)
			}

			ref2 := [20]byte{}
			if _, err = rand.Read(ref2[:]); err != nil {
				b.Fatal(err)
			}

			record := StoreRecord{
				KeyGroup: keyGroupFromKey(key),
				Key:      key,
				Content:  doc,
				RefKeys: map[string]StoreReference{
					"A": {keyGroupFromKey(ref1), ref1},
					"B": {keyGroupFromKey(ref2), ref2},
				},
			}

			records = append(records, record)
		}

		wg.Add(1)
		err = st.StoreContent(records, func(err error) {
			if err != nil {
				panic(err)
			}
			wg.Done()
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	wg.Wait()

	delta := time.Since(start)
	fmt.Printf("%d per second\n", (count*batchSize)/int(delta.Seconds()))
}

func TestIndexDiscardSome(t *testing.T) {
	ts := testInitialize(t, false)

	index1, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}

	count := 1500

	ids := make([][20]byte, 0, count)
	var start time.Time

	for i := 0; i < count; i++ {
		if i == count/2 {
			start = time.Now().UTC()
		}

		buf := [20]byte{}
		_, err := rand.Read(buf[:])
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, buf)

		txn, err := index1.BeginTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
		if err != nil {
			t.Fatal(err)
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}
	}

	index1.Check()
	index1.Close()

	index2, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}

	err = index2.RemoveBefore(start, nil)
	if err != nil {
		t.Fatal(err)
	}

	index2.Close()

	index3, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}

	started := false
	deletes := 0
	for i := 0; i < count; i++ {
		buf := ids[i]
		txn, err := index3.BeginTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		found, shard, position, err := txn.Get(kTestKeyGroup, buf)
		if err != nil {
			t.Fatal(err)
		}

		if !found {
			if started {
				t.Fatal("expected to find", i)
			}
		} else {
			if i != int(position) {
				t.Fatal("expected position match")
			}
			if shard != ts.shard {
				t.Fatal("not shard")
			}
			if started == false {
				started = true
				deletes = count - i
			}
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}
	}

	stats := index3.Stats()
	if stats.Deletes != uint64(deletes) {
		t.Errorf("wrong delete count %d vs expected %d", stats.Deletes, deletes)
	}
	if stats.Sets != uint64(count) {
		t.Errorf("wrong set count %d vs expected %d", stats.Sets, count)
	}

	index3.Close()
}

func TestIndexDiscardShortLru(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir: ts.testDir,
		BaseName: "index",
	}

	index1, err := newIndex(&cfg, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}

	count := 15000

	ids := make([][20]byte, 0, count)

	for i := 0; i < count; i++ {
		buf := [20]byte{}
		_, err := rand.Read(buf[:])
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, buf)

		txn, err := index1.BeginTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
		if err != nil {
			t.Fatal(err)
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}

		index1.RemoveBefore(time.Now().UTC().Add(-time.Millisecond*10), nil)
	}

	index1.Check()
	index1.Close()

	index2, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}

	expected := -1
	for i := 0; i < count; i++ {
		buf := ids[i]
		txn, err := index2.BeginTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		found, shard, position, err := txn.Get(kTestKeyGroup, buf)
		if err != nil {
			t.Fatal(err)
		}

		if !found {
			if expected >= 0 {
				t.Error("expected to find", expected)
			}
		} else {
			if expected >= 0 {
				expected++
				if expected != int(position) {
					t.Fatal("expected position match")
				}
			} else {
				expected = int(position)
			}
			if shard != ts.shard {
				t.Fatal("not shard")
			}
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}
	}

	index2.Close()
}

func TestIndexDiscardShortLruManySets(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:  ts.testDir,
		BaseName:  "index",
		CacheSize: 256,
	}

	index1, err := newIndex(&cfg, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}

	count := 5000

	ids := make([][20]byte, 0, count)

	for i := 0; i < count; i++ {
		txn, err := index1.BeginTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		for j := 0; j < 128; j++ {
			buf := [20]byte{}
			if _, err = rand.Read(buf[:]); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, buf)

			err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
			if err != nil {
				t.Fatal(err)
			}
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}

		index1.RemoveBefore(time.Now().UTC().Add(-time.Millisecond*10), nil)
	}

	index1.Close()

	index2, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}

	expected := -1
	for i := 0; i < count; i++ {
		buf := ids[i]
		txn, err := index2.BeginTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		found, shard, position, err := txn.Get(kTestKeyGroup, buf)
		if err != nil {
			t.Fatal(err)
		}

		if !found {
			if expected >= 0 {
				t.Error("expected to find", expected)
			}
		} else {
			if expected >= 0 {
				expected++
				if expected != int(position) {
					t.Fatal("expected position match")
				}
			} else {
				expected = int(position)
			}
			if shard != ts.shard {
				t.Fatal("not shard")
			}
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}
	}

	index2.Close()
}

func TestRecover(t *testing.T) {
	ts := testInitialize(t, false)

	var counter atomic.Int32
	for pass := 0; pass < 100; pass++ {
		// create or open the test index
		fmt.Printf("starting pass %d\n", pass)
		index, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index", RecoveryEnabled: true}, kMainIndexExt)
		if err != nil {
			t.Fatal(err)
		}

		// launch a go routine to insert randomly
		var wg sync.WaitGroup
		wg.Add(2)
		var setError error
		go func() {
			defer wg.Done()

			for {
				buf := [20]byte{}
				_, setError = rand.Read(buf[:])
				if setError != nil {
					return
				}

				var wg2 sync.WaitGroup
				wg2.Add(1)
				var txn *avlTransaction

				tm := newTransactionManager(func(err error) {
					if err == nil {
						counter.Add(1)
					} else {
						setError = err
					}
					wg2.Done()
				})

				txn, setError = index.BeginTransaction(tm)
				if setError != nil {
					return
				}

				setError = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(counter.Load()))
				if setError != nil {
					return
				}

				// we made tm, so we call tm.Resolve() instead of txn.EndTransaction()
				setError = tm.Resolve(nil)
				if setError != nil {
					return
				}

				wg2.Wait()
				if setError != nil {
					return
				}
			}
		}()

		// after a delay, force the index closed anywhere in the middle of processing
		go func() {
			defer wg.Done()

			time.Sleep(time.Millisecond * 50)
			for _, tree := range index.trees {
				tree.f.Close()
			}
		}()

		wg.Wait()
		index.Close()

		if setError != nil && !errors.Is(setError, os.ErrClosed) && setError.Error() != "File is closed" {
			t.Fatalf("setError: %v", setError)
		}
	}

	// verify linear insertion
	index, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	tree, err := index.getTree(kTestKeyGroup)
	if err != nil {
		t.Fatal(err)
	}

	seq := 0
	err = tree.IterateByTimestamp(func(node *avlNode) error {
		if node.position != uint64(seq) {
			fmt.Printf("expected position=%d, got %d", seq, node.position)
			return ErrIteratorAbort
		}
		seq++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	stats := index.Stats()
	if uint64(seq) != stats.NodeCount {
		t.Fatalf("seq=%d, expected %d", seq, stats.NodeCount)
	}
}

func TestRecoverWithPurge(t *testing.T) {
	ts := testInitialize(t, false)

	var counter atomic.Int32
	for pass := 0; pass < 100; pass++ {

		// create or open the test index
		fmt.Printf("starting pass %d at %d\n", pass, counter.Load())
		index, err := newIndex(
			&VfsConfig{
				IndexDir:           ts.testDir,
				BaseName:           "index",
				RecoveryEnabled:    true,
				ShardDurationDays:  20 / float64(86400000),
				ShardRetentionDays: 40 / float64(86400000),
			},
			kMainIndexExt,
		)
		if err != nil {
			t.Fatal(err)
		}

		// launch a go routine to insert randomly
		var wg sync.WaitGroup
		wg.Add(4)
		var setError error
		var mu sync.Mutex
		var flushError error
		go func() {
			defer wg.Done()

			for {
				buf := [20]byte{}
				_, setError = rand.Read(buf[:])
				if setError != nil {
					return
				}

				mu.Lock()
				if flushError != nil {
					mu.Unlock()
					return
				}

				var wg2 sync.WaitGroup
				wg2.Add(1)
				var txn *avlTransaction

				tm := newTransactionManager(func(err error) {
					index.Stats() // get stats in completion function to ensure no mutex deadlock
					if err == nil {
						counter.Add(1)
					} else {
						setError = err
					}
					wg2.Done()
					mu.Unlock()
				})

				txn, setError = index.BeginTransaction(tm)
				if setError != nil {
					mu.Unlock()
					return
				}

				setError = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(counter.Load()))
				if setError != nil {
					mu.Unlock()
					return
				}

				// we made tm, so we call tm.Resolve() instead of txn.EndTransaction()
				setError = tm.Resolve(nil)
				if setError != nil {
					return
				}

				wg2.Wait() // completion function must complete before this pass is done

				if setError != nil {
					return
				}
			}
		}()

		// after a delay, force the index closed anywhere in the middle of processing
		readyForMore := false
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * 25)
			mu.Lock()
			if flushError != nil || setError != nil {
				mu.Unlock()
				return
			}

			wg.Add(1)
			index.RemoveBefore(time.Now().UTC().Add(-time.Millisecond*10), func(err error) {
				flushError = err
				readyForMore = true
				mu.Unlock()
				wg.Done()
			})
		}()

		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * 75)
			mu.Lock()
			if !readyForMore || flushError != nil || setError != nil {
				mu.Unlock()
				return
			}

			wg.Add(1)
			index.RemoveBefore(time.Now().UTC().Add(-time.Millisecond*10), func(err error) {
				flushError = err
				mu.Unlock()
				wg.Done()
			})
		}()

		// after a delay, force the index closed anywhere in the middle of processing
		go func() {
			defer wg.Done()

			time.Sleep(time.Millisecond * time.Duration(mrand.Intn(100)+20))
			for _, tree := range index.trees {
				tree.f.Close()
			}
		}()

		wg.Wait()

		index.Close()

		if setError != nil && !errors.Is(setError, os.ErrClosed) && setError.Error() != "File is closed" {
			t.Fatalf("setError: %v", setError)
		}
	}

	// verify linear insertion
	index, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"}, kMainIndexExt)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	tree, err := index.getTree(kTestKeyGroup)
	if err != nil {
		t.Fatal(err)
	}

	seq := 0
	err = tree.IterateByTimestamp(func(node *avlNode) error {
		if seq == 0 {
			seq = int(node.position)
		} else if node.position != uint64(seq) {
			fmt.Printf("expected position=%d, got %d", seq, node.position)
			return ErrIteratorAbort
		}
		seq++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	stats := index.Stats()
	if uint64(seq) != stats.Sets {
		t.Fatalf("seq=%d, expected %d", seq, stats.Sets)
	}

	if stats.Sets-100 <= stats.NodeCount {
		t.Fatal("expected some deleted nodes")
	}
}
