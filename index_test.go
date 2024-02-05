package vfs

import (
	"crypto/rand"
	"errors"
	"fmt"
	mrand "math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
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

	index, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	ids := make([][]byte, 0, 10000)

	for i := 0; i < 10000; i++ {
		buf := make([]byte, 20)
		_, err := rand.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, buf)

		txn, err := index.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}

		err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
		if err != nil {
			t.Fatal(err)
		}

		err = txn.EndTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 10000; i++ {
		buf := ids[i]
		txn, err := index.BeginTransaction()
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

		err = txn.EndTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestIndexWrites2(t *testing.T) {
	ts := testInitialize(t, false)

	index1, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	ids := make([][]byte, 0, 10000)

	for i := 0; i < 10000; i++ {
		buf := make([]byte, 20)
		_, err := rand.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, buf)

		txn, err := index1.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}

		err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
		if err != nil {
			t.Fatal(err)
		}

		err = txn.EndTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	index1.Close()

	index2, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10000; i++ {
		buf := ids[i]
		txn, err := index2.BeginTransaction()
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

		err = txn.EndTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	index2.Close()
}

func BenchmarkIndex(b *testing.B) {
	ts := benchmarkInitialize(b)

	index1, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 20000; i++ {
		buf := make([]byte, 20)
		_, err := rand.Read(buf)
		if err != nil {
			b.Fatal(err)
		}

		txn, err := index1.BeginTransaction()
		if err != nil {
			b.Fatal(err)
		}

		err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
		if err != nil {
			b.Fatal(err)
		}

		err = txn.EndTransaction(nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestIndexDiscardSome(t *testing.T) {
	ts := testInitialize(t, false)

	index1, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	count := 1500

	ids := make([][]byte, 0, count)
	var start time.Time

	for i := 0; i < count; i++ {
		if i == count/2 {
			start = time.Now().UTC()
		}

		buf := make([]byte, 20)
		_, err := rand.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, buf)

		txn, err := index1.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}

		err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
		if err != nil {
			t.Fatal(err)
		}

		err = txn.EndTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	index1.Check()
	index1.Close()

	index2, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	err = index2.RemoveBefore(start, nil)
	if err != nil {
		t.Fatal(err)
	}

	index2.Close()

	index3, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	started := false
	deletes := 0
	for i := 0; i < count; i++ {
		buf := ids[i]
		txn, err := index3.BeginTransaction()
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

		err = txn.EndTransaction(nil)
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

	index1, err := newIndex(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	count := 15000

	ids := make([][]byte, 0, count)

	for i := 0; i < count; i++ {
		buf := make([]byte, 20)
		_, err := rand.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, buf)

		txn, err := index1.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}

		err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
		if err != nil {
			t.Fatal(err)
		}

		err = txn.EndTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		index1.RemoveBefore(time.Now().UTC().Add(-time.Millisecond*10), nil)
	}

	index1.Check()
	index1.Close()

	index2, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	expected := -1
	for i := 0; i < count; i++ {
		buf := ids[i]
		txn, err := index2.BeginTransaction()
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

		err = txn.EndTransaction(nil)
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

	index1, err := newIndex(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	count := 5000

	ids := make([][]byte, 0, count)

	for i := 0; i < count; i++ {
		txn, err := index1.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}

		for j := 0; j < 128; j++ {
			buf := make([]byte, 20)
			if _, err = rand.Read(buf); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, buf)

			err = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(i))
			if err != nil {
				t.Fatal(err)
			}
		}

		err = txn.EndTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}

		index1.RemoveBefore(time.Now().UTC().Add(-time.Millisecond*10), nil)
	}

	index1.Close()

	index2, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	expected := -1
	for i := 0; i < count; i++ {
		buf := ids[i]
		txn, err := index2.BeginTransaction()
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

		err = txn.EndTransaction(nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	index2.Close()
}

func TestRecover(t *testing.T) {
	ts := testInitialize(t, false)

	counter := 0
	for pass := 0; pass < 100; pass++ {
		// create or open the test index
		fmt.Printf("starting pass %d\n", pass)
		index, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index", RecoveryEnabled: true})
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
				buf := make([]byte, 20)
				_, setError = rand.Read(buf)
				if setError != nil {
					return
				}

				var txn *avlTransaction
				txn, setError = index.BeginTransaction()
				if setError != nil {
					return
				}

				setError = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(counter))
				if setError != nil {
					return
				}

				var wg sync.WaitGroup
				wg.Add(1)
				setError = txn.EndTransaction(func(err error) {
					if err == nil {
						counter++
					}
					wg.Done()
				})
				if setError != nil {
					return
				}

				wg.Wait()
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
	index, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
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

	counter := 0
	for pass := 0; pass < 100; pass++ {

		// create or open the test index
		fmt.Printf("starting pass %d at %d\n", pass, counter)
		index, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index", RecoveryEnabled: true, ShardDurationDays: 20 / float64(86400000), ShardRetentionDays: 40 / float64(86400000)})
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
				buf := make([]byte, 20)
				_, setError = rand.Read(buf)
				if setError != nil {
					return
				}

				mu.Lock()
				if flushError != nil {
					mu.Unlock()
					return
				}

				var txn *avlTransaction
				txn, setError = index.BeginTransaction()
				if setError != nil {
					mu.Unlock()
					return
				}

				setError = txn.Set(kTestKeyGroup, buf, ts.shard, uint64(counter))
				if setError != nil {
					mu.Unlock()
					return
				}

				var wg2 sync.WaitGroup
				wg2.Add(1)
				setError = txn.EndTransaction(func(err error) {
					index.Stats() // get stats in completion function to ensure no mutex deadlock
					if err == nil {
						counter++
					}
					wg2.Done()
					mu.Unlock()
				})

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
	index, err := newIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
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
