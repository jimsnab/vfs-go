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

	if err = st.StoreContent(records); err != nil {
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

	if err = st.StoreContent(records); err != nil {
		t.Fatal(err)
	}

	for _, record := range records {
		content, err := st.RetrieveContent(record.key)
		if err != nil {
			t.Fatal(err)
		}

		if content == nil {
			t.Fatal("content not found")
		}

		if !bytes.Equal(record.content, content) {
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

		if err = st.StoreContent(records); err != nil {
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
	count := 4000

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
	for i := 0; i < count; i++ {
		if fatal.Load() != nil {
			break
		}

		// pick an operation at random, with 29% lookup, 70% set and 1% purge
		op := mrand.Intn(100)
		if op < 29 {
			op = 0
		} else if op < 99 {
			op = 1
		} else {
			op = 2
		}

		if op == 0 {
			wg.Add(1)
			go func() {
				defer wg.Done()

				mu.Lock()
				if recordNumber > 0 {
					idx := mrand.Intn(recordNumber)
					keyStr := allKeys[idx]
					key, err := hex.DecodeString(keyStr)
					if err != nil {
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
			go func() {
				mu.Lock()
				records := make([]StoreRecord, 0, 48)

				setSize := mrand.Intn(32) + 16
				for i := 0; i < setSize; i++ {
					key := make([]byte, 20)
					rand.Read(key)
					datalen := mrand.Intn(2048) + 1
					data := make([]byte, datalen)
					rand.Read(data)

					records = append(records, StoreRecord{key, data})

					keyStr := hex.EncodeToString(key)
					allKeys[recordNumber] = keyStr
					allData[recordNumber] = data
					recordNumber++
				}
				mu.Unlock()

				if err = st.StoreContent(records); err != nil {
					fatal.Store(&err)
					return
				}

				defer wg.Done()
			}()
		} else {
			// block to limit the go routine growth
			if err = st.PurgeOld(); err != nil {
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
	fmt.Printf("purges: %d, files removed: %d, keys removed: %d\n", purges, s.filesRemoved, s.keysRemoved)
}
