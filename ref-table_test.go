package vfs

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jimsnab/afero"
)

type (
	refTestState struct {
		originalFs afero.Fs
		testDir    string
		testKeys   [][]byte
		cfg        VfsConfig
	}
)

func refTestInitialize(t *testing.T) (rts *refTestState) {
	return refTestInitializeEx(t, false)
}

func refTestInitializeEx(t *testing.T, multishard bool) (rts *refTestState) {
	rts = &refTestState{}
	rts.originalFs = AppFs
	AppFs = afero.NewMemMapFs()

	rts.testDir = "/afero/data"
	err := AppFs.MkdirAll(rts.testDir, 0744)
	if err != nil {
		t.Fatal(err)
	}

	testKeys := make([][]byte, 0, 10)
	for i := 0; i < 10; i++ {
		key := make([]byte, 20)
		n, err := rand.Read(key)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(key) {
			t.Fatal("rand is short")
		}
		testKeys = append(testKeys, key)
	}
	rts.testKeys = testKeys

	rts.cfg = VfsConfig{
		IndexDir:           rts.testDir,
		DataDir:            rts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.03,
		ShardRetentionDays: 0.06,
		RecoveryEnabled:    true,
	}

	if multishard {
		rts.cfg.ShardDurationDays = 0.000002315
		rts.cfg.ShardRetentionDays = 0.00000926
		rts.cfg.Sync = true

		fmt.Printf("shard duration: %d ms\n", uint64(24*60*60*1000*rts.cfg.ShardDurationDays))
		fmt.Printf("shard retention: %d ms\n", uint64(24*60*60*1000*rts.cfg.ShardRetentionDays))
	}

	t.Cleanup(func() {
		AppFs = rts.originalFs
	})
	return
}

func TestRefAndGetOne(t *testing.T) {
	rts := refTestInitialize(t)

	tbl, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	valueKey := rts.testKeys[0]
	storeKey := rts.testKeys[1]

	records := []RefRecord{{kTestKeyGroup, valueKey, storeKey}}
	terr := tbl.AddReferences(records)
	if terr != ErrNotStarted {
		t.Fatal(terr)
	}

	tbl.Start()
	if err = tbl.AddReferences(records); err != nil {
		t.Fatal(err)
	}

	refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey) {
		t.Fatal("refs not equal")
	}
}

func TestRefAndGetTwo(t *testing.T) {
	rts := refTestInitialize(t)

	tbl, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	tbl.Start()
	defer tbl.Close()

	valueKey1 := rts.testKeys[0]
	valueKey2 := rts.testKeys[1]
	storeKey := rts.testKeys[2]

	records := []RefRecord{{kTestKeyGroup, valueKey1, storeKey}, {kTestKeyGroup, valueKey2, storeKey}}
	if err = tbl.AddReferences(records); err != nil {
		t.Fatal(err)
	}

	refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey1)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey) {
		t.Fatal("refs not equal")
	}

	refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey2)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey) {
		t.Fatal("refs not equal")
	}
}

func TestRefAndGetOneWithDup(t *testing.T) {
	rts := refTestInitialize(t)

	tbl, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	tbl.Start()
	defer tbl.Close()

	valueKey := rts.testKeys[0]
	storeKey := rts.testKeys[1]

	records := []RefRecord{{kTestKeyGroup, valueKey, storeKey}, {kTestKeyGroup, valueKey, storeKey}}
	if err = tbl.AddReferences(records); err != nil {
		t.Fatal(err)
	}

	refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey) {
		t.Fatal("refs[0] not equal")
	}
}

func TestRefTwoByTwo(t *testing.T) {
	rts := refTestInitialize(t)

	tbl, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	tbl.Start()
	defer tbl.Close()

	valueKey1 := rts.testKeys[0]
	valueKey2 := rts.testKeys[1]
	storeKey1 := rts.testKeys[2]
	storeKey2 := rts.testKeys[3]

	records := []RefRecord{{kTestKeyGroup, valueKey1, storeKey1}, {kTestKeyGroup, valueKey2, storeKey2}}
	if err = tbl.AddReferences(records); err != nil {
		t.Fatal(err)
	}

	refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey1)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey1) {
		t.Fatal("refs[0] not equal")
	}

	refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey2)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey2) {
		t.Fatal("refs[0] not equal")
	}
}

func TestRefThreeByTwo(t *testing.T) {
	rts := refTestInitialize(t)

	tbl, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	tbl.Start()
	defer tbl.Close()

	valueKey1 := rts.testKeys[0]
	valueKey2 := rts.testKeys[1]
	valueKey3 := rts.testKeys[2]
	storeKey1 := rts.testKeys[3]
	storeKey2 := rts.testKeys[4]

	records := []RefRecord{{kTestKeyGroup, valueKey1, storeKey1}, {kTestKeyGroup, valueKey2, storeKey2}, {kTestKeyGroup, valueKey3, storeKey2}}
	if err = tbl.AddReferences(records); err != nil {
		t.Fatal(err)
	}

	refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey1)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey1) {
		t.Fatal("refs[0] not equal")
	}

	refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey2)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey2) {
		t.Fatal("refs[0] not equal")
	}

	refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey3)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey2) {
		t.Fatal("refs[0] not equal")
	}
}

func TestRefTwoByThree(t *testing.T) {
	rts := refTestInitialize(t)

	tbl, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	tbl.Start()
	defer tbl.Close()

	valueKey1 := rts.testKeys[0]
	valueKey2 := rts.testKeys[1]
	storeKey1 := rts.testKeys[2]
	storeKey2 := rts.testKeys[3]
	storeKey3 := rts.testKeys[4]

	records := []RefRecord{{kTestKeyGroup, valueKey1, storeKey1}, {kTestKeyGroup, valueKey2, storeKey2}, {kTestKeyGroup, valueKey1, storeKey3}}
	if err = tbl.AddReferences(records); err != nil {
		t.Fatal(err)
	}

	refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey1)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 2 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey1) {
		t.Fatal("refs[0] not equal")
	}

	if !bytes.Equal(refs[1], storeKey3) {
		t.Fatal("refs[1] not equal")
	}

	refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey2)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey2) {
		t.Fatal("refs[0] not equal")
	}
}

func TestRefAndGetOneThenOne(t *testing.T) {
	rts := refTestInitialize(t)

	tbl, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	tbl.Start()
	defer tbl.Close()

	valueKey1 := rts.testKeys[0]
	valueKey2 := rts.testKeys[1]
	storeKey := rts.testKeys[2]

	records := []RefRecord{{kTestKeyGroup, valueKey1, storeKey}}
	if err = tbl.AddReferences(records); err != nil {
		t.Fatal(err)
	}

	records = []RefRecord{{kTestKeyGroup, valueKey2, storeKey}}
	if err = tbl.AddReferences(records); err != nil {
		t.Fatal(err)
	}

	refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey1)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey) {
		t.Fatal("refs not equal")
	}

	refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey2)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey) {
		t.Fatal("refs not equal")
	}
}

func TestRefAndGetOneThenAppend(t *testing.T) {
	rts := refTestInitialize(t)

	tbl, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	tbl.Start()
	defer tbl.Close()

	valueKey := rts.testKeys[0]
	storeKey1 := rts.testKeys[1]
	storeKey2 := rts.testKeys[2]

	records := []RefRecord{{kTestKeyGroup, valueKey, storeKey1}}
	if err = tbl.AddReferences(records); err != nil {
		t.Fatal(err)
	}

	records = []RefRecord{{kTestKeyGroup, valueKey, storeKey2}}
	if err = tbl.AddReferences(records); err != nil {
		t.Fatal(err)
	}

	refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 2 {
		t.Fatal("wrong length")
	}

	if !bytes.Equal(refs[0], storeKey1) {
		t.Fatal("refs[0] not equal")
	}

	if !bytes.Equal(refs[1], storeKey2) {
		t.Fatal("refs[1] not equal")
	}
}

func TestRefAndGet5(t *testing.T) {
	rts := refTestInitialize(t)

	tbl, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	tbl.Start()
	defer tbl.Close()

	m := map[[20]byte][]byte{}
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		valueKey := rts.testKeys[i]
		storeKey := rts.testKeys[mrand.Intn(5)+5]

		m[[20]byte(valueKey)] = storeKey

		wg.Add(1)
		go func() {
			defer wg.Done()
			records := []RefRecord{{kTestKeyGroup, valueKey, storeKey}}
			if err = tbl.AddReferences(records); err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()

	for valueKey, storeKey := range m {
		refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey[:])
		if err != nil {
			t.Fatal(err)
		}

		if len(refs) != 1 {
			t.Fatal("wrong length")
		}

		if !bytes.Equal(refs[0], storeKey) {
			t.Fatal("refs[0] not equal")
		}
	}
}

func TestRefAndGet5000(t *testing.T) {
	rts := refTestInitialize(t)

	tbl, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	tbl.Start()
	defer tbl.Close()

	var wg sync.WaitGroup

	for i := 0; i < 5000; i++ {
		valueKey := rts.testKeys[mrand.Intn(5)]
		storeKey := rts.testKeys[mrand.Intn(5)+5]

		wg.Add(1)
		go func() {
			defer wg.Done()
			records := []RefRecord{{kTestKeyGroup, valueKey, storeKey}}
			if err = tbl.AddReferences(records); err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()

	for _, valueKey := range rts.testKeys[0:5] {
		refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey)
		if err != nil {
			t.Fatal(err)
		}

		if len(refs) > 5 {
			t.Fatal("wrong length")
		}

		for _, ref := range refs {
			found := false
			for _, sk := range rts.testKeys[5:10] {
				if bytes.Equal(ref, sk) {
					found = true
					break
				}
			}

			if !found {
				t.Fatal("ref not equal")
			}
		}
	}
}

func TestRefAndGetMany(t *testing.T) {
	rts := refTestInitializeEx(t, true)

	count := 12500

	table, err := newRefTable(&rts.cfg, "example")
	if err != nil {
		t.Fatal(err)
	}
	table.Start()
	defer table.Close()

	allValueKeys := map[int][]byte{}
	allValueKeysList := [][]byte{}
	allStoreKeys := map[[20]byte][][]byte{}
	allStoreKeysList := [][]byte{}

	var fatal atomic.Pointer[error]
	var mu sync.Mutex
	var wg sync.WaitGroup
	recordNumber := 0
	setNew := 0
	setExisting := 0
	findMissing := 0
	findLocated := 0
	retrievals := 0
	purges := 0
	var pending atomic.Int32
	completions := map[int]struct{}{}
	var completionsMu sync.Mutex
	lastUpdate := time.Now()
	largestList := 0

	for i := 0; i < count; i++ {
		if fatal.Load() != nil {
			break
		}

		if time.Since(lastUpdate).Seconds() >= 1 {
			fmt.Printf("Processing %d\n", recordNumber)
			lastUpdate = time.Now()
		}

		// pick an operation at random, with 3% lookup miss, 17% lookup, 40% set new, 40% set existing, and purge every 500
		//
		// N.B. the afero ram disk uses a memmove on a single allocation to expand a file,
		//      and if the index gets too large, the test will slow to a crawl
		op := 4
		if i%500 != 0 {
			r := mrand.Intn(100)
			if r < 3 {
				op = 0
			} else if r < 20 {
				op = 1
			} else if r < 60 {
				op = 2
			} else {
				op = 3
			}
		}

		// thrashing in go when there are too many go routines waiting on the same mutex
		if pending.Load() == 25 {
			wg.Wait()
		}

		if op == 0 || op == 1 {
			pending.Add(1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer pending.Add(-1)

				mu.Lock()
				defer mu.Unlock()

				var valueKey []byte
				var shouldExist bool
				if op == 0 || len(allValueKeysList) == 0 {
					// try a key that won't exist
					buf := make([]byte, 20)
					rand.Read(buf)
					valueKey = buf
					shouldExist = false
				} else {
					// select a key known to be stored
					idx := mrand.Intn(len(allValueKeysList))
					valueKey = allValueKeysList[idx]
					shouldExist = true
				}

				if len(valueKey) > 0 {

					refs, err := table.RetrieveReferences(keyGroupFromKey(valueKey), valueKey)
					if err != nil {
						fatal.Store(&err)
						return
					}

					if shouldExist {
						// might have been purged - ensure it still should exist
						shouldExist = false
						for _, key := range allValueKeysList {
							if bytes.Equal(valueKey, key) {
								shouldExist = true
								break
							}
						}
					}

					if !shouldExist {
						if len(refs) != 0 {
							err = errors.New("found value key that should be missing")
							fatal.Store(&err)
							return
						}
						findMissing++
					} else {
						if len(refs) == 0 {
							err = fmt.Errorf("[%d] didn't find value key previously stored", recordNumber)
							fatal.Store(&err)
							return
						}

						if len(refs) > largestList {
							largestList = len(refs)
						}

						expectedStoreKeys := allStoreKeys[[20]byte(valueKey)]
						if len(expectedStoreKeys) == 0 {
							err = errors.New("should have at least one expected store key")
							fatal.Store(&err)
							return
						}

						found := map[[20]byte]struct{}{}
						for _, ref := range refs {
							for _, sk := range expectedStoreKeys {
								if bytes.Equal(ref, sk) {
									if _, prior := found[[20]byte(sk)]; prior {
										err = errors.New("store key array should not have dups")
										fatal.Store(&err)
										return
									}
									found[[20]byte(sk)] = struct{}{}
									break
								}
							}
						}
						if len(found) != len(refs) {
							err = errors.New("didn't get the expected reference array")
							fatal.Store(&err)
							return
						}
						findLocated++
					}

					retrievals++
				}
			}()
		} else if op == 2 || op == 3 {
			wg.Add(1)
			pending.Add(1)
			go func() {
				defer wg.Done()
				defer pending.Add(-1)

				mu.Lock()

				records := make([]RefRecord, 0, 48)
				round := recordNumber
				recordNumber++
				newValueKeys := [][]byte{}

				setSize := mrand.Intn(8) + 2
				for i := 0; i < setSize; i++ {
					// select a value key - op=2 for new value keys, op=3 for existing
					var valueKey []byte
					if op == 2 || len(allValueKeysList) == 0 {
						valueKey = make([]byte, 20)
						rand.Read(valueKey)
						newValueKeys = append(newValueKeys, valueKey)
					} else {
						valueKey = allValueKeysList[mrand.Intn(len(allValueKeysList))]
					}

					// select a store key - 1/3 existing store key, 2/3 new store key
					var storeKey []byte
					if mrand.Intn(3) == 0 && len(allStoreKeysList) > 0 {
						storeKey = allStoreKeysList[mrand.Intn(len(allStoreKeysList))][:]
					} else {
						storeKey = make([]byte, 20)
						rand.Read(storeKey)
						allStoreKeysList = append(allStoreKeysList, storeKey)
					}

					// append an operation
					records = append(records, RefRecord{keyGroupFromKey(valueKey), valueKey, storeKey})

					// track the expected storage
					allValueKeys[recordNumber] = valueKey

					vk := [20]byte(valueKey)
					list := allStoreKeys[vk]
					if len(list) == 0 {
						list = make([][]byte, 0, 1)
					}

					prior := false
					for _, sk := range list {
						if bytes.Equal(sk, storeKey) {
							// already stored
							prior = true
							break
						}
					}

					if !prior {
						allStoreKeys[vk] = append(allStoreKeys[vk], storeKey)
					}
				}

				var err error
				tm := newTransactionManager(func(failure error) {
					// ensure completion routine is called only once
					completionsMu.Lock()
					defer completionsMu.Unlock()

					if _, found := completions[round]; found {
						fail := errors.New("already completed")
						fatal.Store(&fail)
					} else {
						completions[round] = struct{}{}
					}

					if failure != nil {
						err = failure
					} else {
						if len(newValueKeys) > 0 {
							allValueKeysList = append(allValueKeysList, newValueKeys...)
							setNew++
						} else {
							setExisting++
						}
					}

					mu.Unlock()
				})

				var txn *refTableTransaction
				txn, err = table.BeginTransaction(tm)

				if err == nil {
					err = txn.AddReferences(records)
				}

				if err == nil {
					// we made tm, so we call tm.Resolve() instead of txn.EndTransaction()
					err = tm.Resolve(nil)

					// operation continues in a separate go routine with mu locked; the
					// tm completionFn releases the lock
				}

				if err != nil {
					fatal.Store(&err)
					mu.Unlock()
					return
				}
			}()
		} else {
			mu.Lock()

			err = table.PurgeOld(time.Now().Add(-time.Millisecond * 800))
			if err != nil {
				t.Fatal(err)
			}

			survivors := map[[20]byte]struct{}{}
			err = table.index.IterateByKeys(func(node *avlNode) error {
				survivors[[20]byte(node.key)] = struct{}{}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}

			purged := map[[20]byte]struct{}{}
			toRemove := []int{}
			newValueKeyList := [][]byte{}
			for i, valueKey := range allValueKeys {
				_, survivor := survivors[[20]byte(valueKey)]
				if survivor {
					newValueKeyList = append(newValueKeyList, valueKey)
				} else {
					purged[[20]byte(valueKey)] = struct{}{}
					toRemove = append(toRemove, i)
				}
			}

			allValueKeysList = newValueKeyList
			for _, i := range toRemove {
				delete(allValueKeys, i)
			}

			mu.Unlock()

			purges++
		}
	}

	wg.Wait()
	perr := fatal.Load()
	if perr != nil {
		t.Fatal(*perr)
	}

	fmt.Printf("records set: %d, records retrieved: %d\n", recordNumber, retrievals)
	fmt.Printf("set existing: %d, new: %d\n", setExisting, setNew)
	fmt.Printf("find not stored: %d, stored: %d\n", findMissing, findLocated)
	fmt.Printf("purges: %d, files removed: %d, value keys removed: %d\n", purges, table.shardsRemoved, table.indexKeysRemoved)
	fmt.Printf("largest ref array read: %d\n", largestList)

	if setExisting == 0 {
		t.Fatal("no set of existing value key")
	}
	if setNew == 0 {
		t.Fatal("no set of a new value key")
	}
	if findMissing == 0 {
		t.Fatal("no find of a missing value key")
	}
	if findLocated == 0 {
		t.Fatal("no find of a value key")
	}
	if table.shardsRemoved == 0 {
		t.Fatal("a shard was not removed")
	}
	if table.indexKeysRemoved == 0 {
		t.Fatal("index keys were not removed")
	}

}
