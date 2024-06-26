package vfs

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand"
	"runtime"
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
		testKeys   [][20]byte
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

	testKeys := make([][20]byte, 0, 10)
	for i := 0; i < 10; i++ {
		key := [20]byte{}
		n, err := rand.Read(key[:])
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

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
		if err != nil {
			t.Fatal(err)
		}
		defer tbl.Close()

		valueKey := rts.testKeys[0]
		storeKey := rts.testKeys[1]

		records := []refRecord{{kTestKeyGroup, valueKey, storeKey}}
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

		if !keysEqual(refs[0], storeKey) {
			t.Fatal("refs not equal")
		}
	}
}

func TestRefAndGetOneReopen(t *testing.T) {
	rts := refTestInitialize(t)

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
		if err != nil {
			t.Fatal(err)
		}
		defer tbl.Close()

		valueKey := rts.testKeys[0]
		storeKey := rts.testKeys[1]

		records := []refRecord{{kTestKeyGroup, valueKey, storeKey}}
		terr := tbl.AddReferences(records)
		if terr != ErrNotStarted {
			t.Fatal(terr)
		}

		tbl.cleanupInterval = time.Millisecond * 20
		tbl.idleFileHandle = time.Millisecond * 40

		tbl.Start()
		if err = tbl.AddReferences(records); err != nil {
			t.Fatal(err)
		}

		// wait for file handle close
		for {
			stats := tbl.Stats()
			if stats.ShardsClosed == 1 {
				break
			}
			time.Sleep(time.Millisecond * 20)
		}

		refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey)
		if err != nil {
			t.Fatal(err)
		}

		if len(refs) != 1 {
			t.Fatal("wrong length")
		}

		if !keysEqual(refs[0], storeKey) {
			t.Fatal("refs not equal")
		}

		stats := tbl.Stats()
		if stats.ShardsOpened != 2 || stats.ShardsClosed != 1 {
			t.Fatal("unexpected open/close counts")
		}
	}
}

func TestRefAndGetTwo(t *testing.T) {
	rts := refTestInitialize(t)

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
		if err != nil {
			t.Fatal(err)
		}
		tbl.Start()
		defer tbl.Close()

		valueKey1 := rts.testKeys[0]
		valueKey2 := rts.testKeys[1]
		storeKey := rts.testKeys[2]

		records := []refRecord{{kTestKeyGroup, valueKey1, storeKey}, {kTestKeyGroup, valueKey2, storeKey}}
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

		if !keysEqual(refs[0], storeKey) {
			t.Fatal("refs not equal")
		}

		refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey2)
		if err != nil {
			t.Fatal(err)
		}

		if len(refs) != 1 {
			t.Fatal("wrong length")
		}

		if !keysEqual(refs[0], storeKey) {
			t.Fatal("refs not equal")
		}
	}
}

func TestRefAndGetOneWithDup(t *testing.T) {
	rts := refTestInitialize(t)

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
		if err != nil {
			t.Fatal(err)
		}
		tbl.Start()
		defer tbl.Close()

		valueKey := rts.testKeys[0]
		storeKey := rts.testKeys[1]

		records := []refRecord{{kTestKeyGroup, valueKey, storeKey}, {kTestKeyGroup, valueKey, storeKey}}
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

		if !keysEqual(refs[0], storeKey) {
			t.Fatal("refs[0] not equal")
		}
	}
}

func TestRefTwoByTwo(t *testing.T) {
	rts := refTestInitialize(t)

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
		if err != nil {
			t.Fatal(err)
		}
		tbl.Start()
		defer tbl.Close()

		valueKey1 := rts.testKeys[0]
		valueKey2 := rts.testKeys[1]
		storeKey1 := rts.testKeys[2]
		storeKey2 := rts.testKeys[3]

		records := []refRecord{{kTestKeyGroup, valueKey1, storeKey1}, {kTestKeyGroup, valueKey2, storeKey2}}
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

		if !keysEqual(refs[0], storeKey1) {
			t.Fatal("refs[0] not equal")
		}

		refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey2)
		if err != nil {
			t.Fatal(err)
		}

		if len(refs) != 1 {
			t.Fatal("wrong length")
		}

		if !keysEqual(refs[0], storeKey2) {
			t.Fatal("refs[0] not equal")
		}
	}
}

func TestRefThreeByTwo(t *testing.T) {
	rts := refTestInitialize(t)

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
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

		records := []refRecord{{kTestKeyGroup, valueKey1, storeKey1}, {kTestKeyGroup, valueKey2, storeKey2}, {kTestKeyGroup, valueKey3, storeKey2}}
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

		if !keysEqual(refs[0], storeKey1) {
			t.Fatal("refs[0] not equal")
		}

		refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey2)
		if err != nil {
			t.Fatal(err)
		}

		if len(refs) != 1 {
			t.Fatal("wrong length")
		}

		if !keysEqual(refs[0], storeKey2) {
			t.Fatal("refs[0] not equal")
		}

		refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey3)
		if err != nil {
			t.Fatal(err)
		}

		if len(refs) != 1 {
			t.Fatal("wrong length")
		}

		if !keysEqual(refs[0], storeKey2) {
			t.Fatal("refs[0] not equal")
		}
	}
}

func TestRefTwoByThree(t *testing.T) {
	rts := refTestInitialize(t)

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
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

		records := []refRecord{{kTestKeyGroup, valueKey1, storeKey1}, {kTestKeyGroup, valueKey2, storeKey2}, {kTestKeyGroup, valueKey1, storeKey3}}
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

		if !keysEqual(refs[0], storeKey1) {
			t.Fatal("refs[0] not equal")
		}

		if !keysEqual(refs[1], storeKey3) {
			t.Fatal("refs[1] not equal")
		}

		refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey2)
		if err != nil {
			t.Fatal(err)
		}

		if len(refs) != 1 {
			t.Fatal("wrong length")
		}

		if !keysEqual(refs[0], storeKey2) {
			t.Fatal("refs[0] not equal")
		}
	}
}

func TestRefAndGetOneThenOne(t *testing.T) {
	rts := refTestInitialize(t)

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
		if err != nil {
			t.Fatal(err)
		}
		tbl.Start()
		defer tbl.Close()

		valueKey1 := rts.testKeys[0]
		valueKey2 := rts.testKeys[1]
		storeKey := rts.testKeys[2]

		records := []refRecord{{kTestKeyGroup, valueKey1, storeKey}}
		if err = tbl.AddReferences(records); err != nil {
			t.Fatal(err)
		}

		records = []refRecord{{kTestKeyGroup, valueKey2, storeKey}}
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

		if !keysEqual(refs[0], storeKey) {
			t.Fatal("refs not equal")
		}

		refs, err = tbl.RetrieveReferences(kTestKeyGroup, valueKey2)
		if err != nil {
			t.Fatal(err)
		}

		if len(refs) != 1 {
			t.Fatal("wrong length")
		}

		if !keysEqual(refs[0], storeKey) {
			t.Fatal("refs not equal")
		}
	}
}

func TestRefAndGetOneThenAppend(t *testing.T) {
	rts := refTestInitialize(t)

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
		if err != nil {
			t.Fatal(err)
		}
		tbl.Start()
		defer tbl.Close()

		valueKey := rts.testKeys[0]
		storeKey1 := rts.testKeys[1]
		storeKey2 := rts.testKeys[2]

		records := []refRecord{{kTestKeyGroup, valueKey, storeKey1}}
		if err = tbl.AddReferences(records); err != nil {
			t.Fatal(err)
		}

		records = []refRecord{{kTestKeyGroup, valueKey, storeKey2}}
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

		if !keysEqual(refs[0], storeKey1) {
			t.Fatal("refs[0] not equal")
		}

		if !keysEqual(refs[1], storeKey2) {
			t.Fatal("refs[1] not equal")
		}
	}
}

func TestRefAndGet5(t *testing.T) {
	rts := refTestInitialize(t)

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
		if err != nil {
			t.Fatal(err)
		}
		tbl.Start()
		defer tbl.Close()

		m := map[[20]byte][20]byte{}
		var wg sync.WaitGroup

		for i := 0; i < 5; i++ {
			valueKey := rts.testKeys[i]
			storeKey := rts.testKeys[mrand.Intn(5)+5]

			m[valueKey] = storeKey

			wg.Add(1)
			go func() {
				defer wg.Done()
				records := []refRecord{{kTestKeyGroup, valueKey, storeKey}}
				if err = tbl.AddReferences(records); err != nil {
					panic(err)
				}
			}()
		}
		wg.Wait()

		for valueKey, storeKey := range m {
			refs, err := tbl.RetrieveReferences(kTestKeyGroup, valueKey)
			if err != nil {
				t.Fatal(err)
			}

			if len(refs) != 1 {
				t.Fatal("wrong length")
			}

			if !keysEqual(refs[0], storeKey) {
				t.Fatal("refs[0] not equal")
			}
		}
	}
}

func TestRefAndGet5Capped(t *testing.T) {
	rts := refTestInitialize(t)
	rts.cfg.ReferenceLimit = 3

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
		if err != nil {
			t.Fatal(err)
		}
		tbl.Start()
		defer tbl.Close()

		for i := 1; i < len(rts.testKeys); i++ {
			valueKey := rts.testKeys[0]
			storeKey := rts.testKeys[i]

			records := []refRecord{{kTestKeyGroup, valueKey, storeKey}}
			if err = tbl.AddReferences(records); err != nil {
				panic(err)
			}
		}

		refs, err := tbl.RetrieveReferences(kTestKeyGroup, rts.testKeys[0])
		if err != nil {
			t.Fatal(err)
		}

		if len(refs) != 3 {
			t.Fatal("wrong length")
		}

		if !keysEqual(refs[0], rts.testKeys[1]) {
			t.Fatal("refs[0] not equal")
		}
		if !keysEqual(refs[1], rts.testKeys[2]) {
			t.Fatal("refs[1] not equal")
		}
		if !keysEqual(refs[2], rts.testKeys[3]) {
			t.Fatal("refs[2] not equal")
		}
	}
}

func TestRefAndGet5000(t *testing.T) {
	rts := refTestInitialize(t)

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		tbl, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%d", pass))
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
				records := []refRecord{{kTestKeyGroup, valueKey, storeKey}}
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
					if !keysEqual(ref, sk) {
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
}

func TestRefAndGetMany(t *testing.T) {
	rts := refTestInitializeEx(t, true)

	count := 12500

	for pass := range 2 {
		rts.cfg.StoreKeyInData = (pass == 1)

		testRefAndGetManyWorker(t, rts, count)
	}
}

func testRefAndGetManyWorker(t *testing.T, rts *refTestState, count int) {

	table, err := newRefTable(&rts.cfg, fmt.Sprintf("example-%t", rts.cfg.StoreKeyInData))
	if err != nil {
		t.Fatal(err)
	}
	table.Start()
	defer table.Close()

	storedValueKeys := map[[20]byte]struct{}{}
	allValueKeysList := [][20]byte{}
	valKeyToStoreKeys := map[[20]byte][][20]byte{}
	allStoreKeysList := [][20]byte{}

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
	rounds := 0

	//
	// This is a load test to exercise overlap of add, append, retrieve and purge operations.
	//
	// There are "store keys" that represent a hypothetical main data record. In this test,
	// store keys are just random values.
	//
	// And there are "value keys" that simulate extracts of main data for the purpose of value
	// indexing. A value key is added to the store with a reference to at least one store key.
	//
	// A verification map is adjusted when value keys are added to the store. Upon retrieval,
	// the value key's references are checked against the verification map.
	//
	// Purging activity will discard value keys.
	//

	for i := 0; i < count; i++ {
		if fatal.Load() != nil {
			break
		}

		if time.Since(lastUpdate).Seconds() >= 1 {
			fmt.Printf("Processing %d\n", rounds)
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
			// retrieve references of a value key
			pending.Add(1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer pending.Add(-1)

				mu.Lock()
				defer mu.Unlock()

				var valueKey [20]byte
				var shouldExist bool
				if op == 0 || len(allValueKeysList) == 0 {
					// try a value key that won't exist
					buf := [20]byte{}
					rand.Read(buf[:])
					valueKey = buf
					shouldExist = false
				} else {
					// select a value key known to be stored
					idx := mrand.Intn(len(allValueKeysList))
					valueKey = allValueKeysList[idx]
					shouldExist = true
				}

				if len(valueKey) == 0 {
					panic("value key should have been created or selected")
				}

				refs, err := table.RetrieveReferences(keyGroupFromKey(valueKey), valueKey)
				if err != nil {
					fatal.Store(&err)
					return
				}

				if !shouldExist {
					if len(refs) != 0 {
						err = errors.New("found value key that should be missing")
						fatal.Store(&err)
						return
					}
					findMissing++
				} else {
					expectedStoreKeys := valKeyToStoreKeys[valueKey]

					if len(refs) != len(expectedStoreKeys) {
						table.index.IterateByKeys(func(keyGroup string, node *avlNode) error {
							if keysEqual(node.key, valueKey) {
								fmt.Printf("indexed:%s\n", hex.EncodeToString(node.key[:]))
								table.RetrieveReferences(keyGroupFromKey(valueKey), valueKey)
							}
							return nil
						})
						err = fmt.Errorf("[%s] didn't find value key previously stored", hex.EncodeToString(valueKey[:]))
						fatal.Store(&err)
						return
					}

					if len(refs) > largestList {
						largestList = len(refs)
					}

					found := map[[20]byte]struct{}{}
					for _, ref := range refs {
						for _, sk := range expectedStoreKeys {
							if keysEqual(ref, sk) {
								if _, prior := found[sk]; prior {
									err = errors.New("store key array should not have dups")
									fatal.Store(&err)
									return
								}
								found[sk] = struct{}{}
								break
							}
						}
					}
					if len(found) != len(expectedStoreKeys) {
						err = fmt.Errorf("got %d references to store keys, not %d", len(found), len(expectedStoreKeys))
						fatal.Store(&err)
						return
					}
					findLocated++
				}

				retrievals++
			}()
		} else if op == 2 || op == 3 {
			// add or append a reference from value key to store key
			wg.Add(1)
			pending.Add(1)
			go func() {
				defer wg.Done()
				defer pending.Add(-1)

				mu.Lock()

				records := make([]refRecord, 0, 48)
				round := rounds
				rounds++
				newValueKeys := [][20]byte{}

				// make a batch request
				setSize := mrand.Intn(8) + 2
				for i := 0; i < setSize; i++ {
					// select a value key - op=2 for new value keys, op=3 for existing
					var valueKey [20]byte
					if op == 2 || len(allValueKeysList) == 0 {
						valueKey = [20]byte{}
						rand.Read(valueKey[:])
						newValueKeys = append(newValueKeys, valueKey)
					} else {
						valueKey = allValueKeysList[mrand.Intn(len(allValueKeysList))]
					}

					// select a store key - 1/3 existing store key, 2/3 new store key
					var storeKey [20]byte
					if mrand.Intn(3) == 0 && len(allStoreKeysList) > 0 {
						storeKey = allStoreKeysList[mrand.Intn(len(allStoreKeysList))]
					} else {
						storeKey = [20]byte{}
						rand.Read(storeKey[:])
						allStoreKeysList = append(allStoreKeysList, storeKey)
					}

					// append an operation
					records = append(records, refRecord{keyGroupFromKey(valueKey), valueKey, storeKey})

					// track the expected storage
					storedValueKeys[valueKey] = struct{}{}
					list := valKeyToStoreKeys[valueKey]

					prior := false
					for _, sk := range list {
						if keysEqual(sk, storeKey) {
							// already stored
							prior = true
							break
						}
					}

					if !prior {
						valKeyToStoreKeys[valueKey] = append(valKeyToStoreKeys[valueKey], storeKey)
					}
				}

				// submit the batch request
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

			// iterate the index to find which values remain, and then take out
			// purged value keys from the verification map
			survivors := map[[20]byte]struct{}{}
			err = table.index.IterateByKeys(func(keyGroup string, node *avlNode) error {
				survivors[node.key] = struct{}{}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}

			// reference shards can get removed while a value key holds on to
			// a reference that is not removed - fix up the verification map
			for survivor := range survivors {
				refs, err := table.RetrieveReferences(keyGroupFromKey(survivor), survivor)
				if err != nil {
					fatal.Store(&err)
					return
				}

				verifList := valKeyToStoreKeys[survivor]
				newList := [][20]byte{}
				for _, verifSk := range verifList {
					for _, ref := range refs {
						if keysEqual(verifSk, ref) {
							newList = append(newList, verifSk)
						}
					}
				}

				valKeyToStoreKeys[survivor] = newList
			}

			toRemove := [][20]byte{}
			newValueKeyList := [][20]byte{}
			for valueKey := range storedValueKeys {
				_, survivor := survivors[valueKey]
				if survivor {
					newValueKeyList = append(newValueKeyList, valueKey)
				} else {
					toRemove = append(toRemove, valueKey)
					delete(valKeyToStoreKeys, valueKey)
				}
			}

			allValueKeysList = newValueKeyList
			for _, vk := range toRemove {
				delete(storedValueKeys, vk)
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

func TestStoreRefTablePurge(t *testing.T) {
	ts := testInitialize(t, false)

	cfg := VfsConfig{
		IndexDir:           ts.testDir,
		DataDir:            ts.testDir,
		BaseName:           "the.test",
		Sync:               true,
		ShardDurationDays:  0.00000116,
		ShardRetentionDays: 0.00000463,
		RecoveryEnabled:    true,
		ReferenceTables:    []string{"a"},
	}

	fmt.Printf("shard life: %d ms\n", uint64(24*60*60*1000*cfg.ShardDurationDays))
	fmt.Printf("shard retention: %d ms\n", uint64(24*60*60*1000*cfg.ShardRetentionDays))

	for pass := range 2 {
		cfg.StoreKeyInData = (pass == 1)

		testStoreRefTablePurgeWorker(t, ts, &cfg)
	}
}

func testStoreRefTablePurgeWorker(t *testing.T, ts *testState, cfg *VfsConfig) {
	_ = ts

	table, err := newRefTable(cfg, "a")
	if err != nil {
		t.Fatal(err)
	}
	table.cleanupInterval = time.Millisecond * 25
	table.idleFileHandle = time.Millisecond * 75
	table.Start()
	defer table.Close()

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
			valueKey := [20]byte{}
			rand.Read(valueKey[:])

			shard := table.cfg.calcShard(time.Now().UTC())
			if shard != lastShard {
				if add {
					mu.Lock()
					readShard[shard] = valueKey
					mu.Unlock()
					add = false
				} else {
					add = true
				}
				lastShard = shard
			}

			records := []refRecord{{"a", valueKey, key}}
			err := table.AddReferences(records)
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

			err := table.PurgeOld(time.Now().UTC().Add(-400 * time.Millisecond))
			if err != nil {
				panic(err)
			}

			mu.Lock()
			for _, vk := range readShard {
				if _, err = table.RetrieveReferences("a", vk); err != nil {
					panic(err)
				}
			}
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()

	stats := table.Stats()
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
	fmt.Printf("opened: %d closed: %d removed: %d\n", stats.ShardsOpened, stats.ShardsClosed, stats.ShardsRemoved)
}

func TestRefCleanCollisions(t *testing.T) {
	rts := refTestInitialize(t)
	rts.cfg.StoreKeyInData = true

	tbl, err := newRefTable(&rts.cfg, "refs")
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	tbl.cleanupInterval = time.Millisecond * 5
	tbl.idleFileHandle = time.Millisecond * 13
	tbl.Start()

	var wg sync.WaitGroup
	var mu sync.Mutex

	var ct atomic.Int32
	var gen atomic.Uint64
	gen.Store(1)

	fn := func() {
		defer wg.Done()

		runtime.LockOSThread() // ensure concurrent execution

		readBacks := [][20]byte{}
		tick := time.Now().Add(time.Millisecond * time.Duration(mrand.Intn(30)+10))

		for range 10000 {
			g := gen.Load()

			key := [20]byte{}
			rand.Read(key[:])
			shard := (uint64(key[0]) % 10)
			if shard == 0 {
				// for one of the shards, have it accessed infrequently
				if time.Now().After(tick) {
					tick = time.Now().Add(time.Millisecond * time.Duration(mrand.Intn(30)+10))
				} else {
					shard = 1
				}
			}
			shard += gen.Load()

			group := fmt.Sprintf("%s%d", kTestKeyGroup, ct.Load()/5000)

			records := []refRecord{{group, key, key}}

			var txn *refTableTransaction
			mu.Lock()

			if g != gen.Load() {
				// An old shard may have been removed by another go routine in the
				// purge call. Don't recreate it, because shards are supposed to
				// have a time element that ensures an old shard is never reused.
				// In this test the shard numbering is specifically crafted to
				// challenge the race potential of API calls and background cleanup.
				mu.Unlock()
				continue
			}

			txn, err = tbl.BeginTransaction(nil)
			if err != nil {
				panic(err)
			}
			if err = txn.AddReferencesAtShard(records, shard); err != nil {
				panic(err)
			}
			txn.EndTransaction()
			mu.Unlock()

			if len(readBacks) > 0 {
				mu.Lock()
				lookupKey := readBacks[mrand.Intn(len(readBacks))]
				txn, err = tbl.BeginTransaction(nil)
				if err != nil {
					panic(err)
				}
				refs, err := txn.RetrieveReferences(group, lookupKey)
				if err != nil {
					panic(err)
				}
				txn.EndTransaction()

				if len(refs) == 1 {
					if !keysEqual(refs[0], lookupKey) {
						panic("refs not equal")
					}
				}
				mu.Unlock()
			}

			count := ct.Add(1)
			if count%1000 == 0 {
				readBacks = append(readBacks, key)
				gen.Add(100)
				mu.Lock()
				err = tbl.PurgeOld(time.Now().UTC().Add(-time.Second))
				mu.Unlock()
				if err != nil {
					panic(err)
				}
			}

			if count%5000 == 0 {
				fmt.Printf("wrote %d\n", count)
			}
		}
	}

	n := 3
	wg.Add(n)
	for range n {
		go fn()
	}
	wg.Wait()
}
