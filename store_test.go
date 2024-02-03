package vfs

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

func TestStoreAndGetOne(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		SyncTask:           true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
	}

	st, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 20)
	rand.Read(key)
	datalen := mrand.Intn(16384) + 1
	data := make([]byte, datalen)
	rand.Read(data)

	records := []StoreRecord{{key, data}}

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	content, err := st.RetrieveContent(key)
	if err != nil {
		t.Fatal(err)
	}

	if content == nil {
		t.Fatal("content not found")
	}

	if !bytes.Equal(data, content) {
		t.Fatal("content not equal")
	}
}

func TestStoreAndGetOneSet(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		SyncTask:           true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
	}

	st, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	setSize := 200

	records := make([]StoreRecord, 0, setSize)

	for i := 0; i < setSize; i++ {
		key := make([]byte, 20)
		rand.Read(key)
		datalen := mrand.Intn(16384) + 1
		data := make([]byte, datalen)
		rand.Read(data)

		records = append(records, StoreRecord{key, data})
	}

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	for _, record := range records {
		content, err := st.RetrieveContent(record.Key)
		if err != nil {
			t.Fatal(err)
		}

		if content == nil {
			t.Fatal("content not found")
		}

		if !bytes.Equal(record.Content, content) {
			t.Fatal("content not equal")
		}
	}
}

func TestStoreAndGet1000(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		SyncTask:           true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
	}

	st, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	for i := 0; i < 1000; i++ {
		key := make([]byte, 20)
		rand.Read(key)
		datalen := mrand.Intn(16384) + 1
		data := make([]byte, datalen)
		rand.Read(data)

		records := []StoreRecord{{key, data}}

		if err = st.StoreContent(records, nil); err != nil {
			t.Fatal(err)
		}

		content, err := st.RetrieveContent(key)
		if err != nil {
			t.Fatal(err)
		}

		if content == nil {
			t.Fatal("content not found")
		}

		if !bytes.Equal(data, content) {
			t.Fatal("content not equal")
		}
	}
}

func TestStoreAndGetMany(t *testing.T) {
	ts := testInitialize(t, false)
	count := 10000

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		SyncTask:           true,
		ShardDurationDays:  0.000002315,
		ShardRetentionDays: 0.00000463,
		RecoveryEnabled:    true,
	}

	fmt.Printf("shard life: %d ms\n", uint64(24*60*60*1000*cfg.ShardDurationDays))
	fmt.Printf("shard retention: %d ms\n", uint64(24*60*60*1000*cfg.ShardRetentionDays))

	st, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	allKeys := map[int]string{}
	allData := map[int][]byte{}

	var fatal atomic.Pointer[error]
	var mu sync.Mutex
	var wg sync.WaitGroup
	recordNumber := 0
	retrievals := 0
	purges := 0
	var pending atomic.Int32
	completions := map[int]struct{}{}
	var completionsMu sync.Mutex

	for i := 0; i < count; i++ {
		if fatal.Load() != nil {
			break
		}

		if i%250 == 0 {
			fmt.Printf("records: %d\n", recordNumber)
		}

		// pick an operation at random, with 40% lookup, 60% set, and purge every 500
		//
		// N.B. the afero ram disk uses a memmove on a single allocation to expand a file,
		//      and if the index gets too large, the test will slow to a crawl
		op := 2
		if i%500 != 0 {
			if mrand.Intn(100) < 40 {
				op = 0
			} else {
				op = 1
			}
		}

		// thrashing in go when there are too many go routines waiting on the same mutex
		if pending.Load() == 25 {
			wg.Wait()
		}

		if op == 0 {
			pending.Add(1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer pending.Add(-1)

				mu.Lock()
				if recordNumber > 0 {
					idx := mrand.Intn(recordNumber)
					keyStr := allKeys[idx]
					key, err := hex.DecodeString(keyStr)
					if err != nil {
						mu.Unlock()
						fatal.Store(&err)
						return
					}

					data := allData[idx]
					mu.Unlock()

					content, err := st.RetrieveContent(key)
					if err != nil {
						fatal.Store(&err)
						return
					}

					// old content is removed
					if content != nil {
						if !bytes.Equal(data, content) {
							err := errors.New("content not equal")
							fatal.Store(&err)
							return
						}
						retrievals++
					}
				} else {
					mu.Unlock()
				}
			}()
		} else if op == 1 {
			wg.Add(1)
			pending.Add(1)
			go func() {
				defer wg.Done()
				defer pending.Add(-1)

				mu.Lock()
				records := make([]StoreRecord, 0, 48)
				round := recordNumber

				setSize := mrand.Intn(8) + 8
				for i := 0; i < setSize; i++ {
					key := make([]byte, 20)
					rand.Read(key)
					datalen := mrand.Intn(256) + 1
					data := make([]byte, datalen)
					rand.Read(data)

					records = append(records, StoreRecord{key, data})

					keyStr := hex.EncodeToString(key)
					allKeys[recordNumber] = keyStr
					allData[recordNumber] = data
					recordNumber++
				}
				mu.Unlock()

				if err = st.StoreContent(records, func(err error) {
					// ensure completion routine is called only once
					completionsMu.Lock()
					defer completionsMu.Unlock()

					if _, found := completions[round]; found {
						fail := errors.New("already completed")
						fatal.Store(&fail)
					} else {
						completions[round] = struct{}{}
					}
				}); err != nil {
					fatal.Store(&err)
					return
				}
			}()
		} else {
			// block to limit the go routine growth
			if err = st.PurgeOld(nil); err != nil {
				t.Fatal(err)
				return
			}
			purges++
		}
	}

	wg.Wait()
	perr := fatal.Load()
	if perr != nil {
		t.Fatal(*perr)
	}

	fmt.Printf("records set: %d, records retrieved: %d\n", recordNumber, retrievals)
	s := st.(*store)
	fmt.Printf("purges: %d, files removed: %d, keys removed: %d\n", purges, s.shardsRemoved, s.keysRemoved)
}
