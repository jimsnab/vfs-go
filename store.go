package vfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
		RetrieveContent(keyGroup string, key []byte) (content []byte, err error)

		// Retrieve the referenced keys from a value key of type 'name'
		RetrieveReferences(name, keyGroup string, valueKey []byte) (refKeys [][]byte, err error)

		// Discard all records that fall out of the retention period specified in the config.
		// The caller can optionally provide a callback that is invoked after the disk
		// is synchronized. If onComplete is nil, the function blocks until the deletion
		// is complete.
		PurgeOld(onComplete CommitCompleted) (err error)

		// The store metrics
		Stats() StoreStats

		// Ensures disk files are synchronized
		Sync() error

		// Close I/O.
		Close() error
	}

	StoreRecord struct {
		KeyGroup string
		Key      []byte
		Content  []byte
		RefKeys  map[string]StoreReference
	}

	StoreReference struct {
		KeyGroup string
		ValueKey []byte
	}

	StoreStats struct {
		Sets           uint64
		Deletes        uint64
		Keys           uint64
		KeysRemoved    uint64
		ShardsAccessed uint64
		ShardsRemoved  uint64
	}

	store struct {
		mu             sync.Mutex
		ai             *avlIndex
		shards         map[uint64]afero.File
		accessed       map[uint64]time.Time
		cfg            VfsConfig
		keysRemoved    uint64
		shardsAccessed uint64
		shardsRemoved  uint64
		refTables      map[string]*refTable
	}
)

func NewStore(cfg *VfsConfig) (st Store, err error) {
	ai, err := newIndex(cfg, kMainIndexExt)
	if err != nil {
		return
	}

	s := &store{
		ai:        ai,
		cfg:       *cfg,
		shards:    map[uint64]afero.File{},
		accessed:  map[uint64]time.Time{},
		refTables: map[string]*refTable{},
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
		tbl.Start()
	}

	st = s
	return
}

func (st *store) calcShard(when time.Time) uint64 {
	// convert time to an integral
	divisor := uint64(24 * 60 * 60 * 1000 * st.cfg.ShardDurationDays)
	if divisor < 1 {
		divisor = 1
	}
	shard := uint64(when.UnixMilli())
	shard = shard / divisor

	// multiply by 10 to leave some numeric space between shards
	//
	// for example, it may be desired to compact old shards, and having space to insert
	// another allows transactions to be moved one by one, while the rest of the system
	// continues to operate
	//
	// it also leaves space for migration to a new format
	shard *= 10
	return shard
}

func (st *store) timeFromShard(shard uint64) time.Time {
	ms := int64(shard/10) * int64(24*60*60*1000*st.cfg.ShardDurationDays)
	return time.Unix(int64(ms/1000), (ms%1000)*1000*1000)
}

func (st *store) openShard(request uint64) (f afero.File, shard uint64, err error) {
	if request == 0 {
		shard = st.calcShard(time.Now().UTC())
	} else {
		shard = request
	}

	f, exists := st.shards[shard]
	if !exists {
		shardPath := path.Join(st.cfg.DataDir, fmt.Sprintf("%s.%d.dt3", st.cfg.BaseName, shard))

		f, err = openFile(shardPath)
		if err != nil {
			err = fmt.Errorf("error opening shard %s: %v", shardPath, err)
			return
		}

		st.shards[shard] = f
		st.accessed[shard] = time.Now().UTC()
		st.shardsAccessed++

		cutoff := time.Now().UTC().Add(-time.Minute)
		for sh, ts := range st.accessed {
			if ts.Before(cutoff) {
				f2, exists := st.shards[sh]
				if exists {
					f2.Close()
					delete(st.shards, sh)
				}
				delete(st.accessed, sh)
			}
		}
	}

	return
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

		ts := st.timeFromShard(shard)
		if ts.Before(cutoff) {
			if err = AppFs.Remove(path.Join(st.cfg.DataDir, fileName)); err != nil {
				return
			}

			st.shardsRemoved++
		}
	}

	return
}

func (st *store) StoreContent(records []StoreRecord, onComplete CommitCompleted) (err error) {
	st.mu.Lock()

	err = st.doStoreContent(records, func(failure error) {
		// after i/o
		if onComplete != nil {
			onComplete(failure)
		}
		st.mu.Unlock()
	})
	if err != nil {
		// immediate error
		if onComplete != nil {
			onComplete(err)
		}
		st.mu.Unlock()
	}
	return
}

func (st *store) doStoreContent(records []StoreRecord, onComplete CommitCompleted) (err error) {
	f, shard, err := st.openShard(0)
	if err != nil {
		return
	}

	// begin outer transaction
	tm := newTransactionManager(onComplete)
	txn, err := st.ai.BeginTransaction(tm)
	if err != nil {
		return
	}
	defer func() {
		// Those who directly make the transaction manager don't call EndTransaction,
		// but instead call tm.Resolve.
		err = tm.Resolve(err)
	}()

	// determine which ref tables are being used and attach update instances to the transaction
	refTableTxns := map[string]*refTableTransaction{}
	for _, record := range records {
		for name := range record.RefKeys {
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

	if len(tm.dataStores) > 3 {
		panic("BUGBUG bad")
	}

	for _, record := range records {
		//
		// Store the main document.
		//

		sizedContent := make([]byte, len(record.Content)+4)
		binary.BigEndian.PutUint32(sizedContent[0:4], uint32(len(record.Content)))
		copy(sizedContent[4:], record.Content)

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

		if txn.Set(record.KeyGroup, record.Key, shard, uint64(offset)); err != nil {
			return
		}

		//
		// Add references.
		//

		for name, refKey := range record.RefKeys {
			rtxn := refTableTxns[name]
			refRecords := []RefRecord{{refKey.KeyGroup, refKey.ValueKey, record.Key}}
			if err = rtxn.AddReferences(refRecords); err != nil {
				return
			}
		}
	}

	if st.cfg.Sync {
		if err = f.Sync(); err != nil {
			return
		}
	}

	return
}

func (st *store) RetrieveContent(keyGroup string, key []byte) (content []byte, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

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

	found, shard, position, err := txn.Get(keyGroup, key)
	if err != nil {
		return
	}

	if found {
		var f afero.File
		f, _, err = st.openShard(shard)
		if err != nil {
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

		data := make([]byte, length)
		if n, err = f.ReadAt(data, int64(position+4)); err != nil {
			return
		}

		if n != int(length) {
			err = errors.New("short read for content")
			return
		}

		content = data
	}

	return
}

func (st *store) RetrieveReferences(name, keyGroup string, valueKey []byte) (refKeys [][]byte, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	table := st.refTables[name]
	if table != nil {
		refKeys, err = table.RetrieveReferences(keyGroup, valueKey)
	}
	return
}

func (st *store) PurgeOld(onComplete CommitCompleted) (err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if onComplete == nil {
		err = st.doPurgeOld()
	} else {
		go func() {
			failure := st.doPurgeOld()
			onComplete(failure)
		}()
	}
	return
}

func (st *store) doPurgeOld() (err error) {
	cutoff := time.Now().UTC().Add(-(time.Duration(time.Hour * 24 * time.Duration(st.cfg.ShardRetentionDays))))

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
		Sets:           indexStats.Sets,
		Deletes:        indexStats.Deletes,
		Keys:           indexStats.NodeCount,
		KeysRemoved:    st.keysRemoved,
		ShardsAccessed: st.shardsAccessed,
		ShardsRemoved:  st.shardsRemoved,
	}
}

func (st *store) Sync() (err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

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

func (st *store) Close() (err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

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
