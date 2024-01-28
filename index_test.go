package vfs

import (
	"crypto/rand"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/spf13/afero"
)

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

	index, err := NewIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
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

		err = txn.Set(buf, ts.shard, uint64(i))
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
		txn, err := index.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}

		found, shard, position, err := txn.Get(buf)
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

	index1, err := NewIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
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

		err = txn.Set(buf, ts.shard, uint64(i))
		if err != nil {
			t.Fatal(err)
		}

		err = txn.EndTransaction()
		if err != nil {
			t.Fatal(err)
		}
	}

	index1.Close()

	index2, err := NewIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10000; i++ {
		buf := ids[i]
		txn, err := index2.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}

		found, shard, position, err := txn.Get(buf)
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

	index1, err := NewIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
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

		err = txn.Set(buf, ts.shard, uint64(i))
		if err != nil {
			b.Fatal(err)
		}

		err = txn.EndTransaction()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestIndexDiscardSome(t *testing.T) {
	ts := testInitialize(t, false)

	index1, err := NewIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	count := 15000

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

		err = txn.Set(buf, ts.shard, uint64(i))
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

	index2, err := NewIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	err = index2.PurgeOlder(start)
	if err != nil {
		t.Fatal(err)
	}

	index2.Close()

	index3, err := NewIndex(&VfsConfig{IndexDir: ts.testDir, BaseName: "index"})
	if err != nil {
		t.Fatal(err)
	}

	expected := -1
	for i := 0; i < count; i++ {
		buf := ids[i]
		txn, err := index3.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}

		found, shard, position, err := txn.Get(buf)
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

	index3.Close()
}
