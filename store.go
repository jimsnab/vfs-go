package vfs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jimsnab/afero"
)

type (
	Store interface {
		// Saves a batch of records. Existing records with the same key are replaced.
		// The caller can optionally provide a callback that is invoked after the disk
		// is synchronized.
		StoreContent(records []StoreRecord, onComplete CommitCompleted) (err error)

		// Retrieve a specific record
		RetrieveContent(keyGroup string, key [20]byte) (content []byte, err error)

		// Retrieve the referenced keys from a value key of type 'name'
		RetrieveReferences(name, keyGroup string, valueKey [20]byte) (refKeys [][20]byte, err error)

		// Gets the index timestamp for the specified key; returns zero time if not found.
		GetKeyTimestamp(keyGroup string, key [20]byte) (ts time.Time, err error)

		// Discard all records that fall out of the retention period specified in the config.
		// The caller can optionally provide a callback that is invoked after the disk
		// is synchronized. If onComplete is nil, the function blocks until the deletion
		// is complete.
		PurgeOld(onComplete CommitCompleted) (cutoff time.Time, err error)

		// The store metrics
		Stats() StoreStats

		// Ensures disk files are synchronized
		Sync() error

		// Close I/O.
		Close() error

		// Iterates the index and returns each key
		IterateByKeys(iter StoreIterator) (err error)

		// Iterates the keys in oldest to newest order
		IterateByTimestamp(iter StoreIterator) (err error)
	}

	StoreRecord struct {
		shard     uint64 // if 0, the shard is computed
		timestamp int64  // if 0, the current timestamp is used
		KeyGroup  string
		Key       [20]byte
		Content   []byte
		RefLists  map[string][]StoreReference
	}

	StoreReference struct {
		KeyGroup string
		ValueKey [20]byte
	}

	StoreStats struct {
		Sets          uint64
		Deletes       uint64
		Keys          uint64
		KeysRemoved   uint64
		ShardsOpened  uint64
		ShardsClosed  uint64
		ShardsRemoved uint64
	}

	StoreIterator func(keyGroup string, key [20]byte, shard, position uint64, timestamp time.Time) (err error)

	store struct {
		docMu             sync.Mutex
		iterMu            sync.Mutex
		ai                *avlIndex
		shards            map[uint64]afero.File
		accessed          map[uint64]time.Time
		cfg               VfsConfig
		keysRemoved       uint64
		shardsOpened      uint64
		shardsClosed      uint64
		shardsRemoved     uint64
		refTables         map[string]*refTable
		idleFileHandle    time.Duration
		cleanupInterval   time.Duration
		wg                sync.WaitGroup
		cancelFn          context.CancelFunc
		storeKeyInData    bool
		testRemovedShards map[uint64]struct{}
	}

	prestartCallback func(st *store)

	openShard struct {
		shard uint64
		f     afero.File
	}
)

func NewStore(cfg *VfsConfig) (st Store, err error) {
	return newStoreInternal(cfg, nil)
}

func newStoreInternal(cfg *VfsConfig, psfn prestartCallback) (st *store, err error) {
	ai, err := newIndex(cfg, kMainIndexExt)
	if err != nil {
		return
	}

	s := &store{
		ai:              ai,
		cfg:             *cfg,
		shards:          map[uint64]afero.File{},
		accessed:        map[uint64]time.Time{},
		refTables:       map[string]*refTable{},
		idleFileHandle:  time.Minute * 15,
		cleanupInterval: time.Minute,
		storeKeyInData:  cfg.StoreKeyInData,
	}

	if s.cfg.ShardDurationDays == 0 {
		s.cfg.ShardDurationDays = 1
	}
	if s.cfg.ShardRetentionDays == 0 {
		s.cfg.ShardRetentionDays = 7
	}
	if s.cfg.CacheSize == 0 {
		s.cfg.CacheSize = 1024
	}

	for _, name := range s.cfg.ReferenceTables {
		var tbl *refTable
		tbl, err = newRefTable(&s.cfg, name)
		if err != nil {
			ai.Close()
			for _, table := range s.refTables {
				table.Close()
			}
			return
		}

		s.refTables[name] = tbl
	}

	if psfn != nil {
		psfn(s)
	}

	for _, tbl := range s.refTables {
		tbl.Start()
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	s.cancelFn = cancelFn

	s.wg.Add(1)
	go s.run(ctx)

	st = s
	return
}

func (st *store) openShard(request uint64, forRead bool) (f afero.File, shard uint64, err error) {
	if request == 0 {
		shard = st.cfg.calcShard(time.Now().UTC())
	} else {
		shard = request
	}

	f, exists := st.shards[shard]
	if !exists {
		shardPath := path.Join(st.cfg.DataDir, fmt.Sprintf("%s.%d.dt3", st.cfg.BaseName, shard))

		f, err = createOrOpenFile(shardPath, forRead)
		if err != nil {
			err = fmt.Errorf("error opening store shard %s forRead=%t: %v", shardPath, forRead, err)
			return
		}

		st.shards[shard] = f
		st.shardsOpened++
	}
	st.accessed[shard] = time.Now().UTC()

	return
}

// Background task worker
func (st *store) closeIdleShards() {
	st.docMu.Lock()
	defer st.docMu.Unlock()

	cutoff := time.Now().UTC().Add(-st.idleFileHandle)
	for sh, ts := range st.accessed {
		if ts.Before(cutoff) {
			f, exists := st.shards[sh]
			if exists {
				f.Close()
				delete(st.shards, sh)
				st.shardsClosed++
			}
			delete(st.accessed, sh)
		}
	}
}

func (st *store) purgeShards(cutoff time.Time) (err error) {
	files, err := afero.ReadDir(AppFs, st.cfg.DataDir)
	if err != nil {
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()
		prefix := st.cfg.BaseName + "."
		if !strings.HasPrefix(fileName, prefix) {
			continue
		}

		afterBase := fileName[len(prefix):]
		cutPoint := strings.Index(afterBase, ".")
		if cutPoint <= 0 {
			continue
		}

		if afterBase[cutPoint:] != ".dt3" {
			continue
		}

		shard, terr := strconv.ParseUint(afterBase[:cutPoint], 10, 64)
		if terr != nil {
			continue
		}

		ts := st.cfg.timeFromShard(shard)
		if ts.Before(cutoff) {
			if err = AppFs.Remove(path.Join(st.cfg.DataDir, fileName)); err != nil {
				return
			}

			st.shardsRemoved++
			if st.testRemovedShards != nil {
				st.testRemovedShards[shard] = struct{}{}
			}
		}
	}

	return
}

func (st *store) StoreContent(records []StoreRecord, onComplete CommitCompleted) (err error) {
	st.docMu.Lock()
	if st.iterMu.TryLock() {
		st.iterMu.Unlock()
	} else {
		err = errors.New("can't store content during iteration")
		st.docMu.Unlock()
		return
	}

	wrapped := onComplete
	if wrapped != nil {
		wrapped = func(failure error) {
			onComplete(failure)
			st.docMu.Unlock()
		}
	}

	err = st.doStoreContent(records, wrapped)
	if err != nil {
		// immediate error
		if onComplete != nil {
			onComplete(err)
		}
		st.docMu.Unlock()
	} else if wrapped == nil {
		st.docMu.Unlock()
	}
	return
}

func (st *store) doStoreContent(records []StoreRecord, onComplete CommitCompleted) (err error) {
	modifiedShards := make(map[uint64]openShard, len(records))

	// begin outer transaction
	tm := newTransactionManager(onComplete)
	txn, err := st.ai.BeginTransaction(tm)
	if err != nil {
		return
	}
	defer func() {
		// Where one of those who directly make the transaction manager and don't call EndTransaction,
		// but instead call tm.Resolve.
		err = tm.Resolve(err)
	}()

	// determine which ref tables are being used and attach update instances to the transaction
	refTableTxns := map[string]*refTableTransaction{}
	for _, record := range records {
		// if requested shard is 0, access the latest shard; otherwise, access the specified shard
		shard := record.shard
		_, opened := modifiedShards[shard]
		if !opened {
			var f afero.File
			f, shard, err = st.openShard(record.shard, false)
			if err != nil {
				err = fmt.Errorf("store-content: %v", err)
				return
			}
			modifiedShards[record.shard] = openShard{f: f, shard: shard}
		}

		for name := range record.RefLists {
			_, exists := refTableTxns[name]
			if !exists {
				refTable := st.refTables[name]
				if refTable == nil {
					err = fmt.Errorf("reference table %s is not specified in the configuration reference_tables array", name)
					return
				}

				var refTableTxn *refTableTransaction
				refTableTxn, err = refTable.BeginTransaction(tm)
				if err != nil {
					return
				}
				refTableTxns[name] = refTableTxn
			}
		}
	}

	for _, record := range records {
		tuple := modifiedShards[record.shard]
		f := tuple.f
		shard := tuple.shard

		//
		// Store the main document.
		//

		var compressed []byte
		if compressed, err = compress(record.Content); err != nil {
			return
		}

		compressedLen := len(compressed)
		var sizedContent []byte
		if !st.storeKeyInData {
			sizedContent = make([]byte, compressedLen+4)
			binary.BigEndian.PutUint32(sizedContent[0:4], uint32(compressedLen))
			copy(sizedContent[4:], compressed)
		} else {
			sizedContent = make([]byte, compressedLen+24)
			binary.BigEndian.PutUint32(sizedContent[0:4], uint32(compressedLen))
			copy(sizedContent[4:24], record.Key[:])
			copy(sizedContent[24:], compressed)
		}

		var offset int64
		if offset, err = f.Seek(0, io.SeekEnd); err != nil {
			return
		}

		var n int
		n, err = f.Write(sizedContent)
		if err != nil {
			return
		}

		if n < len(sizedContent) {
			err = errors.New("unable to write all data") // should be unreachable
			return
		}

		if txn.setWithTimestamp(record.KeyGroup, record.Key, shard, uint64(offset), record.timestamp); err != nil {
			return
		}

		//
		// Add references.
		//

		for name, refList := range record.RefLists {
			rtxn := refTableTxns[name]
			var refRecords []refRecord
			for _, refKey := range refList {
				refRecords = append(refRecords, refRecord{refKey.KeyGroup, refKey.ValueKey, record.Key})
			}
			if err = rtxn.AddReferencesAtShard(refRecords, shard); err != nil {
				return
			}
		}
	}

	if st.cfg.Sync {
		for _, ms := range modifiedShards {
			if err = ms.f.Sync(); err != nil {
				return
			}
		}
	}

	return
}

func (st *store) RetrieveContent(keyGroup string, key [20]byte) (content []byte, err error) {
	st.docMu.Lock()
	defer st.docMu.Unlock()

	txn, err := st.ai.BeginTransaction(nil)
	if err != nil {
		return
	}
	defer func() {
		terr := st.ai.txn.EndTransaction()
		if err == nil {
			err = terr
		}
	}()

	found, shard, position, _, err := txn.Get(keyGroup, key)
	if err != nil {
		return
	}

	if found {
		content, err = st.doLoadContent(shard, position)
	}

	return
}

func (st *store) doLoadContent(shard uint64, position uint64) (content []byte, err error) {
	var f afero.File
	f, _, err = st.openShard(shard, true)
	if err != nil {
		// key indexed but shard does not exist; treat if not indexed
		if isFileNotFound(err) {
			err = nil
		} else {
			err = fmt.Errorf("load-content: %v", err)
		}
		return
	}

	hdr := make([]byte, 4)
	var n int
	if n, err = f.ReadAt(hdr, int64(position)); err != nil {
		return
	}

	if n != 4 {
		err = errors.New("short read for content length")
		return
	}

	length := binary.BigEndian.Uint32(hdr)
	if length > 128*1024 {
		err = errors.New("content length too long")
		return
	}

	contentPos := int64(position)
	if st.storeKeyInData {
		contentPos += 24
	} else {
		contentPos += 4
	}

	data := make([]byte, length)
	if n, err = f.ReadAt(data, contentPos); err != nil {
		return
	}

	if n != int(length) {
		err = errors.New("short read for content")
		return
	}

	if content, err = uncompress(data); err != nil {
		return
	}

	return
}

func (st *store) GetKeyTimestamp(keyGroup string, key [20]byte) (ts time.Time, err error) {
	st.docMu.Lock()
	defer st.docMu.Unlock()

	txn, err := st.ai.BeginTransaction(nil)
	if err != nil {
		return
	}
	defer func() {
		terr := st.ai.txn.EndTransaction()
		if err == nil {
			err = terr
		}
	}()

	_, _, _, ts, err = txn.Get(keyGroup, key)
	return
}

// Retrieves reference array where 'name' is the kind of reference (e.g., a field within the data),
// 'keyGroup' is the reference index group, and 'valueKey' is up to 20 bytes of the value. The
// return 'refKeys' provides an array of store keys. This allows index lookup of a value that is
// inside the stored content.
func (st *store) RetrieveReferences(name, keyGroup string, valueKey [20]byte) (refKeys [][20]byte, err error) {
	st.docMu.Lock()
	defer st.docMu.Unlock()

	table := st.refTables[name]
	if table != nil {
		table.testRemovedShards = st.testRemovedShards
		refKeys, err = table.RetrieveReferences(keyGroup, valueKey)
	}
	return
}

func (st *store) PurgeOld(onComplete CommitCompleted) (cutoff time.Time, err error) {
	st.docMu.Lock()
	defer st.docMu.Unlock()

	if st.iterMu.TryLock() {
		st.iterMu.Unlock()
	} else {
		err = errors.New("can't delete content during iteration")
		return
	}

	retentionPeriod := time.Duration(float64(time.Hour*24) * st.cfg.ShardRetentionDays)
	cutoff = time.Now().UTC().Add(-retentionPeriod)

	if onComplete == nil {
		err = st.doPurgeOld(cutoff)
	} else {
		go func() {
			failure := st.doPurgeOld(cutoff)
			onComplete(failure)
		}()
	}
	return
}

func (st *store) doPurgeOld(cutoff time.Time) (err error) {
	before := st.ai.Stats()

	if err = st.ai.doRemoveBefore(cutoff); err != nil {
		return
	}

	if err = st.purgeShards(cutoff); err != nil {
		return
	}

	after := st.ai.Stats()
	st.keysRemoved += after.Deletes - before.Deletes

	for _, refTable := range st.refTables {
		if err = refTable.PurgeOld(cutoff); err != nil {
			return
		}
	}

	return
}

func (st *store) Stats() StoreStats {
	indexStats := st.ai.Stats()

	return StoreStats{
		Sets:          indexStats.Sets,
		Deletes:       indexStats.Deletes,
		Keys:          indexStats.NodeCount,
		KeysRemoved:   st.keysRemoved,
		ShardsOpened:  st.shardsOpened,
		ShardsClosed:  st.shardsClosed,
		ShardsRemoved: st.shardsRemoved,
	}
}

func (st *store) Sync() (err error) {
	st.docMu.Lock()
	defer st.docMu.Unlock()

	for _, f := range st.shards {
		terr := f.Sync()
		if terr != nil && err == nil {
			err = terr
		}
	}

	terr := st.ai.Sync()
	if terr != nil && err == nil {
		err = terr
	}

	return
}

func (st *store) run(ctx context.Context) {
	defer st.wg.Done()

	ticker1 := time.NewTicker(st.cleanupInterval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker1.C:
			st.closeIdleShards()
		}
	}
}

func (st *store) IterateByKeys(iter StoreIterator) (err error) {
	st.iterMu.Lock()
	defer st.iterMu.Unlock()

	if st.cancelFn == nil {
		err = os.ErrClosed
		return
	}

	return st.ai.IterateByKeys(func(keyGroup string, node *avlNode) error {
		return iter(keyGroup, node.key, node.shard, node.position, node.Epoch())
	})
}

func (st *store) IterateByTimestamp(iter StoreIterator) (err error) {
	st.iterMu.Lock()
	defer st.iterMu.Unlock()

	if st.cancelFn == nil {
		err = os.ErrClosed
		return
	}

	return st.ai.IterateByTimestamp(func(keyGroup string, node *avlNode) error {
		return iter(keyGroup, node.key, node.shard, node.position, node.Epoch())
	})
}

func (st *store) Close() (err error) {
	// stop the run() goroutine
	st.docMu.Lock()
	if st.cancelFn == nil {
		err = os.ErrClosed
		st.docMu.Unlock()
		return
	}
	st.cancelFn()
	st.cancelFn = nil
	st.docMu.Unlock()
	st.wg.Wait()

	// close the resources
	st.docMu.Lock()
	defer st.docMu.Unlock()

	terr := st.ai.Close()
	if terr != nil {
		err = terr
	}
	for _, f := range st.shards {
		terr = f.Close()
		if err == nil && terr != nil {
			err = terr
		}
	}
	st.shards = map[uint64]afero.File{}
	st.accessed = map[uint64]time.Time{}
	for _, tbl := range st.refTables {
		tbl.Close()
	}

	return err
}
