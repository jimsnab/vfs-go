package vfs

import (
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

	"github.com/spf13/afero"
)

type (
	Store interface {
		// Saves a batch of records. Existing records with the same key are replaced.
		// The caller can optionally provide a callback that is invoked after the disk
		// is synchronized.
		StoreContent(records []StoreRecord, onComplete CommitCompleted) (err error)

		// Retrieve a specific record
		RetrieveContent(key []byte) (content []byte, err error)

		// Discard all records that fall out of the retention period specified in the config.
		// The caller can optionally provide a callback that is invoked after the disk
		// is synchronized.
		PurgeOld(onComplete CommitCompleted) (err error)

		// The store metrics
		Stats() StoreStats

		// Ensures disk files are synchronized
		Sync() error

		// Close I/O.
		Close() error
	}

	StoreRecord struct {
		Key     []byte
		Content []byte
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
	}
)

func NewStore(cfg *VfsConfig) (st Store, err error) {
	ai, err := newIndex(cfg)
	if err != nil {
		return
	}

	s := &store{
		ai:       ai,
		cfg:      *cfg,
		shards:   map[uint64]afero.File{},
		accessed: map[uint64]time.Time{},
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

		f, err = AppFs.OpenFile(shardPath, os.O_CREATE|os.O_RDWR, 0644)
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
		if !strings.HasPrefix(fileName, st.cfg.BaseName+".") {
			continue
		}

		afterBase := fileName[len(st.cfg.BaseName)+1:]
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
	err = st.doStoreContent(records, onComplete)
	if err != nil && onComplete != nil {
		onComplete(err)
	}
	return
}

func (st *store) doStoreContent(records []StoreRecord, onComplete CommitCompleted) (err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	f, shard, err := st.openShard(0)
	if err != nil {
		return
	}

	txn, err := st.ai.BeginTransaction()
	if err != nil {
		return
	}

	for _, record := range records {
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

		if txn.Set(record.Key, shard, uint64(offset)); err != nil {
			return
		}
	}

	if st.cfg.SyncTask {
		go func() {
			if err = f.Sync(); err != nil {
				return
			}

		}()
	} else if st.cfg.Sync {
		if err = f.Sync(); err != nil {
			return
		}
	}

	if err = txn.doEndTransaction(onComplete); err != nil {
		return
	}

	return
}

func (st *store) RetrieveContent(key []byte) (content []byte, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	txn, err := st.ai.BeginTransaction()
	if err != nil {
		return
	}

	found, shard, position, err := txn.Get(key)
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

	if err = txn.EndTransaction(nil); err != nil {
		return
	}

	return
}

func (st *store) PurgeOld(onComplete CommitCompleted) (err error) {
	err = st.doPurgeOld(onComplete)
	if err != nil && onComplete != nil {
		onComplete(err)
	}
	return
}

func (st *store) doPurgeOld(onComplete CommitCompleted) (err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	cutoff := time.Now().UTC().Add(-(time.Duration(time.Hour * 24 * time.Duration(st.cfg.ShardRetentionDays))))

	before := st.ai.tree.Stats()

	if err = st.ai.doRemoveBefore(cutoff, func(err error) { st.doPurgeShards(cutoff, onComplete) }); err != nil {
		return
	}

	after := st.ai.tree.Stats()
	st.keysRemoved += after.Deletes - before.Deletes

	return nil
}

func (st *store) doPurgeShards(cutoff time.Time, onComplete CommitCompleted) {
	err := st.purgeShards(cutoff)

	if onComplete != nil {
		onComplete(err)
	}
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

	return err
}
