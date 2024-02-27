package vfs

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	mrand "math/rand"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jimsnab/afero"
)

type (
	testStoreKey struct {
		keyGroup  string
		key       [20]byte
		timestamp time.Time
	}
)

func TestStoreAndGetOne(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
	}

	st, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key := [20]byte{}
	rand.Read(key[:])
	datalen := mrand.Intn(16384) + 1
	data := make([]byte, datalen)
	rand.Read(data)

	records := []StoreRecord{{KeyGroup: kTestKeyGroup, Key: key, Content: data}}

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	content, err := st.RetrieveContent(kTestKeyGroup, key)
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

func TestStoreAndGetOneReopen(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
	}

	st, err := newStoreInternal(&cfg, func(st *store) {
		st.cleanupInterval = time.Millisecond * 20
		st.idleFileHandle = time.Millisecond * 40
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key := [20]byte{}
	rand.Read(key[:])
	datalen := mrand.Intn(16384) + 1
	data := make([]byte, datalen)
	rand.Read(data)

	records := []StoreRecord{{KeyGroup: kTestKeyGroup, Key: key, Content: data}}

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	// make sure file handle gets closed
	for {
		stats := st.Stats()
		if stats.ShardsClosed == 1 {
			break
		}
		time.Sleep(time.Millisecond * 20)
	}

	content, err := st.RetrieveContent(kTestKeyGroup, key)
	if err != nil {
		t.Fatal(err)
	}

	if content == nil {
		t.Fatal("content not found")
	}

	if !bytes.Equal(data, content) {
		t.Fatal("content not equal")
	}

	stats := st.Stats()
	if stats.ShardsOpened != 2 || stats.ShardsClosed != 1 {
		t.Fatal("unexpected open/close counts")
	}
}

func TestStoreAndGetOneReloaded(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
		ReferenceTables:    []string{"x"},
	}

	st1, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	key := [20]byte{}
	rand.Read(key[:])
	datalen := mrand.Intn(16384) + 1
	data := make([]byte, datalen)
	rand.Read(data)
	valueKey := [20]byte{}
	rand.Read(valueKey[:])

	records := []StoreRecord{{KeyGroup: kTestKeyGroup, Key: key, Content: data, RefLists: map[string][]StoreReference{"x": {{keyGroupFromKey(valueKey), valueKey}}}}}

	if err = st1.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	st1.Close()

	st2, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	content, err := st2.RetrieveContent(kTestKeyGroup, key)
	if err != nil {
		t.Fatal(err)
	}

	if content == nil {
		t.Fatal("content not found")
	}

	if !bytes.Equal(data, content) {
		t.Fatal("content not equal")
	}

	st2.Close()

	st3, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	refs, err := st3.RetrieveReferences("x", keyGroupFromKey(valueKey), valueKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatal("didn't get reference")
	}

	if !keysEqual(refs[0], key) {
		t.Fatal("reference is not to the key")
	}

	st3.Close()
}

func TestStoreAndGetTwoReloaded(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
		ReferenceTables:    []string{"x"},
	}

	st1, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	key1 := [20]byte{}
	rand.Read(key1[:])
	datalen := mrand.Intn(16384) + 1
	data1 := make([]byte, datalen)
	rand.Read(data1)
	valueKey1 := [20]byte{}
	rand.Read(valueKey1[:])

	records := []StoreRecord{{KeyGroup: kTestKeyGroup, Key: key1, Content: data1, RefLists: map[string][]StoreReference{"x": {{kTestKeyGroup, valueKey1}}}}}

	if err = st1.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	st1.Close()

	st2, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	key2 := [20]byte{}
	rand.Read(key2[:])
	datalen = mrand.Intn(16384) + 1
	data2 := make([]byte, datalen)
	rand.Read(data2)
	valueKey2 := [20]byte{}
	rand.Read(valueKey2[:])

	records = []StoreRecord{{KeyGroup: kTestKeyGroup, Key: key2, Content: data2, RefLists: map[string][]StoreReference{"x": {{kTestKeyGroup, valueKey2}}}}}

	before := time.Now().UTC()
	if err = st2.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC()
	window := after.Sub(before)

	content, err := st2.RetrieveContent(kTestKeyGroup, key1)
	if err != nil {
		t.Fatal(err)
	}

	if content == nil {
		t.Fatal("content not found")
	}

	timestamp, err := st2.GetKeyTimestamp(kTestKeyGroup, key1)
	if err != nil {
		t.Fatal(err)
	}

	tsDelta := timestamp.Sub(before)
	if tsDelta.Milliseconds() > window.Milliseconds() {
		t.Error("timestamp did not fall in store window")
	}

	if !bytes.Equal(data1, content) {
		t.Fatal("content not equal")
	}

	st2.Close()

	st3, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	refs, err := st3.RetrieveReferences("x", kTestKeyGroup, valueKey1)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatal("didn't get reference")
	}

	if !keysEqual(refs[0], key1) {
		t.Fatal("reference is not to the key 1")
	}

	refs, err = st3.RetrieveReferences("x", kTestKeyGroup, valueKey2)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatal("didn't get reference")
	}

	if !keysEqual(refs[0], key2) {
		t.Fatal("reference is not to the key 2")
	}

	st3.Close()

	err = afero.Walk(AppFs, cfg.IndexDir, func(path string, info fs.FileInfo, err error) error {
		name := info.Name()
		if name == "data" {
			return nil
		}

		// main index
		if name == "the.test.test.dt1" || name == "the.test.test.dt2" {
			return nil
		}

		// ref table files
		if name == "the.test.test.x.dt5" || name == "the.test.test.x.dt6" {
			return nil
		}

		parts := strings.Split(name, ".")

		// main data
		if len(parts) == 4 && parts[0] == "the" && parts[1] == "test" && parts[3] == "dt3" {
			return nil
		}

		// reference arrays
		if len(parts) == 5 && parts[0] == "the" && parts[1] == "test" && parts[2] == "x" && parts[4] == "dt4" {
			return nil
		}

		// unexpected file
		return fmt.Errorf("unexpected file: %s", name)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStoreAndGetOneRealDisk(t *testing.T) {
	testInitialize(t, false)
	AppFs = afero.NewOsFs()

	testDir := "/tmp/vfs-test"
	AppFs.RemoveAll(testDir)
	AppFs.MkdirAll(testDir, 0744)
	defer AppFs.RemoveAll(testDir)

	cfg := VfsConfig{
		IndexDir:           testDir,
		DataDir:            testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
		ReferenceTables:    []string{"x"},
	}

	st1, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	key := [20]byte{}
	rand.Read(key[:])
	datalen := mrand.Intn(16384) + 1
	data := make([]byte, datalen)
	rand.Read(data)
	valueKey := [20]byte{}
	rand.Read(valueKey[:])

	records := []StoreRecord{{KeyGroup: kTestKeyGroup, Key: key, Content: data, RefLists: map[string][]StoreReference{"x": {{keyGroupFromKey(valueKey), valueKey}}}}}

	if err = st1.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	st1.Close()

	st2, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	content, err := st2.RetrieveContent(kTestKeyGroup, key)
	if err != nil {
		t.Fatal(err)
	}

	if content == nil {
		t.Fatal("content not found")
	}

	if !bytes.Equal(data, content) {
		t.Fatal("content not equal")
	}

	st2.Close()

	st3, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	refs, err := st3.RetrieveReferences("x", keyGroupFromKey(valueKey), valueKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatal("didn't get reference")
	}

	if !keysEqual(refs[0], key) {
		t.Fatal("reference is not to the key")
	}

	st3.Close()
}

func TestStoreAndGetOneSet(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
	}

	st, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	setSize := 200

	records := make([]StoreRecord, 0, setSize)

	for i := 0; i < setSize; i++ {
		key := [20]byte{}
		rand.Read(key[:])
		datalen := mrand.Intn(16384) + 1
		data := make([]byte, datalen)
		rand.Read(data)

		records = append(records, StoreRecord{KeyGroup: kTestKeyGroup, Key: key, Content: data})
	}

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	for _, record := range records {
		content, err := st.RetrieveContent(record.KeyGroup, record.Key)
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
		Sync:               true,
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
		key := [20]byte{}
		rand.Read(key[:])
		datalen := mrand.Intn(16384) + 1
		data := make([]byte, datalen)
		rand.Read(data)

		records := []StoreRecord{{KeyGroup: kTestKeyGroup, Key: key, Content: data}}

		if err = st.StoreContent(records, nil); err != nil {
			t.Fatal(err)
		}

		content, err := st.RetrieveContent(kTestKeyGroup, key)
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
		Sync:               true,
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
					keySlice, err := hex.DecodeString(keyStr)
					if err != nil {
						mu.Unlock()
						fatal.Store(&err)
						return
					}
					key := [20]byte{}
					copy(key[:], keySlice)

					data := allData[idx]
					mu.Unlock()

					content, err := st.RetrieveContent(kTestKeyGroup, key)
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
					key := [20]byte{}
					rand.Read(key[:])
					datalen := mrand.Intn(256) + 1
					data := make([]byte, datalen)
					rand.Read(data)

					records = append(records, StoreRecord{KeyGroup: kTestKeyGroup, Key: key, Content: data})

					keyStr := hex.EncodeToString(key[:])
					allKeys[recordNumber] = keyStr
					allData[recordNumber] = data
					recordNumber++
				}
				mu.Unlock()

				var swg sync.WaitGroup
				swg.Add(1)
				err = st.StoreContent(records, func(err error) {
					defer swg.Done()

					// ensure completion routine is called only once
					completionsMu.Lock()
					defer completionsMu.Unlock()

					if _, found := completions[round]; found {
						fail := errors.New("already completed")
						fatal.Store(&fail)
					} else {
						completions[round] = struct{}{}
					}
				})

				if err != nil {
					fatal.Store(&err)
					return
				}

				swg.Wait()
			}()
		} else {
			// block to limit the go routine growth
			if _, err = st.PurgeOld(nil); err != nil {
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

func keyGroupFromKey(key [20]byte) string {
	return fmt.Sprintf("%X", key[0]>>4)
}

func TestStoreAndGetManyMultiGroup(t *testing.T) {
	ts := testInitialize(t, false)
	count := 10000

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.000002315,
		ShardRetentionDays: 0.00000463,
		RecoveryEnabled:    true,
		ReferenceTables:    []string{"A", "B"},
	}

	fmt.Printf("shard life: %d ms\n", uint64(24*60*60*1000*cfg.ShardDurationDays))
	fmt.Printf("shard retention: %d ms\n", uint64(24*60*60*1000*cfg.ShardRetentionDays))

	st, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	allKeys := map[int]string{}
	allData := map[[20]byte][]byte{}
	allRefKeysA := [][20]byte{}
	allRefKeysB := [][20]byte{}

	var fatal atomic.Pointer[error]
	var mu sync.Mutex
	var wg sync.WaitGroup
	recordNumber := 0
	retrievals := 0
	purges := 0
	var pending atomic.Int32
	completions := map[int]struct{}{}
	var completionsMu sync.Mutex
	removedKeys := map[[20]byte]struct{}{}
	removedRefKeys := map[[20]byte]struct{}{}

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
				if recordNumber == 0 {
					mu.Unlock()
				} else {
					var content []byte
					var refKeys [][20]byte
					var expectedContent []byte
					var expectedRef bool
					var pkey *[20]byte
					var keySlice []byte
					var refKey [20]byte
					var refKeyType string

					subop := mrand.Intn(100)
					if subop < 50 {
						// get a random document
						idx := mrand.Intn(recordNumber)
						keyStr := allKeys[idx]
						keySlice, err = hex.DecodeString(keyStr)
						if err != nil {
							fatal.Store(&err)
							return
						}
						key := [20]byte{}
						copy(key[:], keySlice)
						pkey = &key
						expectedContent = allData[key]
					} else if subop < 54 {
						// look up a random missing ref key
						refKeyType = "A"
						refKey = [20]byte{}
						rand.Read(refKey[:])
					} else if subop < 74 && len(allRefKeysA) > 0 {
						// get a document from a random ref key A
						refKeyType = "A"
						refKey = allRefKeysA[mrand.Intn(len(allRefKeysA))]
						expectedRef = true
					} else if subop < 94 && len(allRefKeysB) > 0 {
						// get a document from a random ref key B
						refKeyType = "B"
						refKey = allRefKeysB[mrand.Intn(len(allRefKeysB))]
						expectedRef = true
					} else {
						// look up a random non existent key
						key := [20]byte{}
						rand.Read(key[:])
						pkey = &key
					}

					err = func() (failure error) {
						if refKeyType != "" {
							if refKeys, failure = st.RetrieveReferences(refKeyType, keyGroupFromKey(refKey), refKey); failure != nil {
								return
							}

							if len(refKeys) != 0 && !expectedRef {
								return errors.New("didn't expect to find reference")
							}

							if expectedRef {
								if len(refKeys) == 0 {
									_, removed := removedRefKeys[refKey]
									if !removed {
										return errors.New("expected a reference key")
									}
								} else {
									key := refKeys[mrand.Intn(len(refKeys))]
									pkey = &key
									expectedContent = allData[key]
								}
							}
						}
						return
					}()

					mu.Unlock()

					if err != nil {
						fatal.Store(&err)
						return
					}

					if pkey != nil {
						content, err = st.RetrieveContent(keyGroupFromKey(*pkey), *pkey)
						if err != nil {
							fatal.Store(&err)
							return
						}
					}

					if !bytes.Equal(expectedContent, content) {
						// old content is removed
						var removed bool
						if pkey != nil {
							_, removed = removedKeys[*pkey]
						}
						if len(content) > 0 || (pkey != nil && !removed) {
							err := errors.New("content not equal")
							fatal.Store(&err)
							return
						}
					}
					retrievals++
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
					// make a random document
					key := [20]byte{}
					rand.Read(key[:])
					datalen := mrand.Intn(256) + 1
					data := make([]byte, datalen)
					rand.Read(data)

					// make two random reference keys
					refLists := map[string][]StoreReference{}
					if mrand.Intn(10) > 3 {
						if len(allRefKeysA) == 0 || mrand.Intn(10) > 3 {
							ref1 := [20]byte{}
							rand.Read(ref1[:])
							allRefKeysA = append(allRefKeysA, ref1)
							refLists["A"] = []StoreReference{{keyGroupFromKey(ref1), ref1}}
						} else {
							ref1 := allRefKeysA[mrand.Intn(len(allRefKeysA))]
							refLists["A"] = []StoreReference{{keyGroupFromKey(ref1), ref1}}
						}
					}
					if mrand.Intn(10) > 3 {
						if len(allRefKeysB) == 0 || mrand.Intn(10) > 3 {
							ref2 := [20]byte{}
							rand.Read(ref2[:])
							allRefKeysB = append(allRefKeysB, ref2)
							refLists["B"] = []StoreReference{{keyGroupFromKey(ref2), ref2}}
						} else {
							ref2 := allRefKeysB[mrand.Intn(len(allRefKeysB))]
							refLists["B"] = []StoreReference{{keyGroupFromKey(ref2), ref2}}
						}
					}

					records = append(records, StoreRecord{KeyGroup: keyGroupFromKey(key), Key: key, Content: data, RefLists: refLists})

					keyStr := hex.EncodeToString(key[:])
					allKeys[recordNumber] = keyStr
					allData[key] = data
					recordNumber++
				}
				mu.Unlock()

				err = st.StoreContent(records, func(failure error) {
					// ensure completion routine is called only once
					completionsMu.Lock()
					defer completionsMu.Unlock()

					if _, found := completions[round]; found {
						fail := errors.New("already completed")
						fatal.Store(&fail)
					} else {
						completions[round] = struct{}{}
					}

					if err == nil {
						err = failure
					}
				})
				if err != nil {
					fatal.Store(&err)
					return
				}
			}()
		} else {
			p := st.(*store)
			p.ai.removed = removedKeys
			for _, refTable := range p.refTables {
				refTable.index.removed = removedRefKeys
			}

			if _, err = st.PurgeOld(nil); err != nil {
				t.Fatal(&err)
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
	stats := st.Stats()
	fmt.Printf("current keys: %d\n", stats.Keys)

	cfg.StoreKeyInData = true
	cfg.BaseName += "-copy"
	st2, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	err = CopyStore(st, st2, &CopyConfig{Progress: func(index, saves int64) { fmt.Printf("index:%d saves:%d\n", index, saves) }})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStorePurge(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.00000116,
		ShardRetentionDays: 0.00000579,
		RecoveryEnabled:    true,
		ReferenceTables:    []string{"a"},
	}

	fmt.Printf("shard life: %d ms\n", uint64(24*60*60*1000*cfg.ShardDurationDays))
	fmt.Printf("shard retention: %d ms\n", uint64(24*60*60*1000*cfg.ShardRetentionDays))

	st, err := newStoreInternal(&cfg, func(st *store) {
		st.cleanupInterval = time.Millisecond * 25
		st.idleFileHandle = time.Millisecond * 75
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	var mu sync.Mutex
	readShard := map[uint64][20]byte{}
	lastShard := uint64(0)
	add := true

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		end := time.Now().Add(time.Millisecond * 700)
		for {
			if time.Now().After(end) {
				return
			}

			key := [20]byte{}
			rand.Read(key[:])
			data := make([]byte, 200)
			rand.Read(data)

			shard := st.cfg.calcShard(time.Now().UTC())
			if shard != lastShard {
				if add {
					mu.Lock()
					readShard[shard] = key
					mu.Unlock()
					add = false
				} else {
					add = true
				}
				lastShard = shard
			}

			records := []StoreRecord{{KeyGroup: "a", Key: key, Content: data}}
			err := st.StoreContent(records, nil)
			if err != nil {
				panic(err)
			}

			time.Sleep(time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		end := time.Now().Add(time.Millisecond * 700)
		for {
			if time.Now().After(end) {
				return
			}

			cutoff, err := st.PurgeOld(nil)
			if err != nil {
				panic(err)
			}

			period := time.Since(cutoff)
			if period.Milliseconds() < 450 {
				panic("unexpected cutoff")
			}

			mu.Lock()
			for _, k := range readShard {
				if _, err = st.RetrieveContent("a", k); err != nil {
					panic(err)
				}
			}
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()

	stats := st.Stats()
	fmt.Printf("opened: %d closed: %d removed: %d\n", stats.ShardsOpened, stats.ShardsClosed, stats.ShardsRemoved)

	if stats.ShardsClosed > 8 {
		t.Fatal("too many shards closed")
	}
	delta := stats.ShardsOpened - stats.ShardsClosed
	if delta < 2 {
		t.Fatal("not enough older shards still open")
	}
	if stats.ShardsRemoved < 3 {
		t.Fatal("too few shards removed")
	}
}

func TestStoreAndCopyMultiShard(t *testing.T) {
	ts := testInitialize(t, false)

	srcPath := path.Join(ts.testDir, "source")
	err := AppFs.MkdirAll(srcPath, 0755)
	if err != nil {
		t.Fatal(err)
	}

	srcCfg := VfsConfig{
		IndexDir:           srcPath,
		DataDir:            srcPath,
		BaseName:           "the.test",
		ShardDurationDays:  0.000004630,
		ShardRetentionDays: 1,
		ReferenceTables:    []string{"mycol"},
	}
	fmt.Printf("shard life: %d ms\n", uint64(24*60*60*1000*srcCfg.ShardDurationDays))

	srcSt, err := NewStore(&srcCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srcSt.Close()

	priorKeys := [][20]byte{}

	testShards := 3

	for {
		key := [20]byte{}
		rand.Read(key[:])
		datalen := mrand.Intn(2048) + 80
		data := make([]byte, datalen)
		rand.Read(data)

		valueKey1 := [20]byte{}
		copy(valueKey1[:], data[:20])
		valueKey2 := [20]byte{}
		copy(valueKey2[:], data[20:40])

		if len(priorKeys) < 30 {
			priorKeys = append(priorKeys, key)
		} else if mrand.Intn(10) < 2 {
			key = priorKeys[mrand.Intn(len(priorKeys))]
		}

		refLists := map[string][]StoreReference{}
		refLists["mycol"] = []StoreReference{
			{KeyGroup: testValueKeyGroupFromKey(valueKey1), ValueKey: valueKey1},
			{KeyGroup: testValueKeyGroupFromKey(valueKey2), ValueKey: valueKey2},
		}

		records := []StoreRecord{{KeyGroup: kTestKeyGroup, Key: key, Content: data, RefLists: refLists}}

		if err = srcSt.StoreContent(records, nil); err != nil {
			t.Fatal(err)
		}

		files, err := afero.ReadDir(AppFs, srcPath)
		if err != nil {
			t.Fatal(err)
		}

		dt3s := 0
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "the.test.") && strings.HasSuffix(file.Name(), ".dt3") {
				dt3s++
			}
		}
		if dt3s >= testShards {
			break
		}
	}

	stats := srcSt.Stats()
	fmt.Printf("keys: %d\nsets: %d\n", stats.Keys, stats.Sets)

	destPath := path.Join(ts.testDir, "dest")
	err = AppFs.MkdirAll(destPath, 0755)
	if err != nil {
		t.Fatal(err)
	}

	destCfg := VfsConfig{
		IndexDir:           destPath,
		DataDir:            destPath,
		BaseName:           srcCfg.BaseName,
		ShardDurationDays:  srcCfg.ShardDurationDays,
		ShardRetentionDays: srcCfg.ShardRetentionDays,
		ReferenceTables:    srcCfg.ReferenceTables,
	}

	destSt, err := NewStore(&destCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer destSt.Close()

	copyCfg := CopyConfig{
		Reindex: func(name string, content []byte) (refs []StoreReference, err error) {
			valueKey1 := [20]byte{}
			copy(valueKey1[:], content[:20])
			valueKey2 := [20]byte{}
			copy(valueKey2[:], content[20:40])

			refs = []StoreReference{
				{KeyGroup: testValueKeyGroupFromKey(valueKey1), ValueKey: valueKey1},
				{KeyGroup: testValueKeyGroupFromKey(valueKey2), ValueKey: valueKey2},
			}
			return
		},
	}
	err = CopyStore(srcSt, destSt, &copyCfg)
	if err != nil {
		t.Fatal(err)
	}

	destStats := destSt.Stats()
	fmt.Printf("dest keys: %d  sets: %d\n", destStats.Keys, destStats.Sets)

	srcFiles, err := afero.ReadDir(AppFs, srcPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range srcFiles {
		fmt.Println("source: " + file.Name())
	}
	for _, file := range srcFiles {
		fi, err := AppFs.Stat(path.Join(destPath, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("dest:   " + fi.Name())
	}

	sourceData := []testStoreKey{}
	err = srcSt.IterateByKeys(func(keyGroup string, key [20]byte, shard, position uint64, timestamp time.Time) (err error) {
		tsk := testStoreKey{keyGroup: keyGroup, key: key, timestamp: timestamp}
		sourceData = append(sourceData, tsk)
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sourceData) != int(destStats.Keys) {
		t.Fatal("not expected number of keys")
	}

	i := 0
	err = destSt.IterateByKeys(func(keyGroup string, key [20]byte, shard, position uint64, timestamp time.Time) (err error) {
		tsk := sourceData[i]
		i++

		if tsk.keyGroup != keyGroup {
			err = fmt.Errorf("keygroup mismatch: expected %s, got %s", tsk.keyGroup, keyGroup)
			return
		}
		srcKey := hex.EncodeToString(tsk.key[:])
		destKey := hex.EncodeToString(key[:])

		if srcKey != destKey {
			err = fmt.Errorf("key mismatch: expected %s, got %s", srcKey, destKey)
			return
		}
		if !tsk.timestamp.Equal(timestamp) {
			err = fmt.Errorf("timestamp mismatch: expected %s, got %s", tsk.timestamp, timestamp)
			return
		}

		content, err := destSt.RetrieveContent(keyGroup, key)
		if err != nil {
			return
		}
		valueKey1 := [20]byte{}
		copy(valueKey1[:], content[:20])
		valueKey2 := [20]byte{}
		copy(valueKey2[:], content[20:40])

		refKeys, err := destSt.RetrieveReferences("mycol", testValueKeyGroupFromKey(valueKey1), valueKey1)
		if err != nil {
			return
		}
		if len(refKeys) != 1 {
			err = fmt.Errorf("value 1 ref not found")
			return
		}
		if !bytes.Equal(refKeys[0][:], key[:]) {
			err = fmt.Errorf("value 1 ref not equal")
			return
		}

		refKeys, err = destSt.RetrieveReferences("mycol", testValueKeyGroupFromKey(valueKey2), valueKey2)
		if err != nil {
			return
		}
		if len(refKeys) != 1 {
			err = fmt.Errorf("value 2 ref not found")
			return
		}
		if !bytes.Equal(refKeys[0][:], key[:]) {
			err = fmt.Errorf("value 2 ref not equal")
			return
		}

		return
	})
	if err != nil {
		t.Fatal(err)
	}

	sourceData = []testStoreKey{}
	err = srcSt.IterateByTimestamp(func(keyGroup string, key [20]byte, shard, position uint64, timestamp time.Time) (err error) {
		tsk := testStoreKey{keyGroup: keyGroup, key: key, timestamp: timestamp}
		sourceData = append(sourceData, tsk)
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sourceData) != int(destStats.Keys) {
		t.Fatal("not expected number of keys")
	}

	i = 0
	err = destSt.IterateByTimestamp(func(keyGroup string, key [20]byte, shard, position uint64, timestamp time.Time) (err error) {
		tsk := sourceData[i]
		i++

		if tsk.keyGroup != keyGroup {
			err = fmt.Errorf("keygroup mismatch: expected %s, got %s", tsk.keyGroup, keyGroup)
			return
		}
		srcKey := hex.EncodeToString(tsk.key[:])
		destKey := hex.EncodeToString(key[:])

		if srcKey != destKey {
			err = fmt.Errorf("key mismatch: expected %s, got %s", srcKey, destKey)
			return
		}
		if !tsk.timestamp.Equal(timestamp) {
			err = fmt.Errorf("timestamp mismatch: expected %s, got %s", tsk.timestamp, timestamp)
			return
		}
		return
	})
	if err != nil {
		t.Fatal(err)
	}

	vcfg := VerifyConfig{
		CompareContent: true,
		Progress:       func(index, compares int64) { fmt.Printf("verify: index=%d  compares=%d\n", index, compares) },
	}
	err = VerifyStore(srcSt, destSt, &vcfg)
	if err != nil {
		t.Fatal(err)
	}
}

func testValueKeyGroupFromKey(valueKey [20]byte) string {
	return string('A' + (valueKey[0] % 3))
}

func TestStoreAndGetOneUpdate(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
	}

	st, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key := [20]byte{}
	rand.Read(key[:])
	datalen := mrand.Intn(16384) + 1
	data := make([]byte, datalen)
	rand.Read(data)

	records := []StoreRecord{{KeyGroup: kTestKeyGroup, Key: key, Content: data}}

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	now := time.Now().UTC()

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	var storedTs time.Time
	err = st.IterateByTimestamp(func(keyGroup string, key [20]byte, shard, position uint64, timestamp time.Time) (err error) {
		if !storedTs.IsZero() {
			err = errors.New("already set")
			return
		}

		storedTs = timestamp
		return
	})
	if err != nil {
		t.Fatal(err)
	}

	delta := storedTs.Sub(now)
	if delta.Milliseconds() > 200 {
		t.Fatal("timestamp not updated")
	}
}

func TestStoreTwoAndTouchFirst(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
	}

	st, err := NewStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key1 := [20]byte{}
	rand.Read(key1[:])
	datalen1 := mrand.Intn(16384) + 1
	data1 := make([]byte, datalen1)
	rand.Read(data1)

	key2 := [20]byte{}
	rand.Read(key2[:])
	datalen2 := mrand.Intn(16384) + 1
	data2 := make([]byte, datalen2)
	rand.Read(data2)

	records := []StoreRecord{
		{KeyGroup: kTestKeyGroup, Key: key1, Content: data1},
		{KeyGroup: kTestKeyGroup, Key: key2, Content: data2},
	}

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	now := time.Now().UTC()

	records = []StoreRecord{
		{KeyGroup: kTestKeyGroup, Key: key1, Content: data1},
	}

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	first := true
	var storedTs time.Time
	err = st.IterateByTimestamp(func(keyGroup string, key [20]byte, shard, position uint64, timestamp time.Time) (err error) {
		if first {
			// key1 should have moved to the end
			first = false
			return
		}

		if !storedTs.IsZero() {
			err = errors.New("already set")
			return
		}

		storedTs = timestamp
		return
	})
	if err != nil {
		t.Fatal(err)
	}

	delta := storedTs.Sub(now)
	if delta.Milliseconds() > 200 {
		t.Fatal("timestamp not updated")
	}
}

func TestStoreTwoAndTouchFirstAndCopy(t *testing.T) {
	ts := testInitialize(t, false)

	srcPath := path.Join(ts.testDir, "source")
	err := AppFs.MkdirAll(srcPath, 0755)
	if err != nil {
		t.Fatal(err)
	}

	srcCfg := VfsConfig{
		IndexDir:           srcPath,
		DataDir:            srcPath,
		BaseName:           "the.test",
		ShardDurationDays:  0.000004630,
		ShardRetentionDays: 1,
	}

	fmt.Printf("shard life: %d ms\n", uint64(24*60*60*1000*srcCfg.ShardDurationDays))

	st, err := NewStore(&srcCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key1 := [20]byte{}
	rand.Read(key1[:])
	data1 := make([]byte, 2048)
	rand.Read(data1)

	key2 := [20]byte{}
	rand.Read(key2[:])
	data2 := make([]byte, 2048)
	rand.Read(data2)

	records := []StoreRecord{
		{KeyGroup: kTestKeyGroup, Key: key1, Content: data1},
		{KeyGroup: kTestKeyGroup, Key: key2, Content: data2},
	}

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	now := time.Now().UTC()

	records = []StoreRecord{
		{KeyGroup: kTestKeyGroup, Key: key1, Content: data1},
	}

	if err = st.StoreContent(records, nil); err != nil {
		t.Fatal(err)
	}

	destPath := path.Join(ts.testDir, "dest")
	err = AppFs.MkdirAll(destPath, 0755)
	if err != nil {
		t.Fatal(err)
	}

	destCfg := VfsConfig{
		IndexDir:           destPath,
		DataDir:            destPath,
		BaseName:           srcCfg.BaseName,
		ShardDurationDays:  srcCfg.ShardDurationDays,
		ShardRetentionDays: srcCfg.ShardRetentionDays,
	}

	st2, err := NewStore(&destCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	copyCfg := CopyConfig{}
	err = CopyStore(st, st2, &copyCfg)
	if err != nil {
		t.Fatal(err)
	}

	srcFiles, err := afero.ReadDir(AppFs, srcPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range srcFiles {
		fmt.Printf("source: %s %d\n", file.Name(), file.Size())
	}
	for _, file := range srcFiles {
		fi, err := AppFs.Stat(path.Join(destPath, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("dest:   %s %d\n", fi.Name(), fi.Size())
	}

	first := true
	var storedTs time.Time
	err = st2.IterateByTimestamp(func(keyGroup string, key [20]byte, shard, position uint64, timestamp time.Time) (err error) {
		if first {
			// key1 should have moved to the end
			first = false
			return
		}

		if !storedTs.IsZero() {
			err = errors.New("already set")
			return
		}

		storedTs = timestamp
		return
	})
	if err != nil {
		t.Fatal(err)
	}

	delta := storedTs.Sub(now)
	if delta.Milliseconds() > 200 {
		t.Fatal("timestamp not updated")
	}

	err = VerifyStore(st, st2, &VerifyConfig{})
	if err != nil {
		t.Fatal(err)
	}
}
