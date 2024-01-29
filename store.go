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
		StoreContent(records []StoreRecord) (err error)

		// Retrieve a specific record
		RetrieveContent(key []byte) (content []byte, err error)

		// Discard all records that fall out of the retention period specified in the config.
		PurgeOld() (err error)

		// Close I/O.
		Close()
	}

	StoreRecord struct {
		key     []byte
		content []byte
	}

	store struct {
		mu       sync.Mutex
		index    Index
		shards   map[uint64]afero.File
		accessed map[uint64]time.Time
		cfg      VfsConfig
	}
)

func NewStore(cfg *VfsConfig) (st Store, err error) {
	index, err := NewIndex(cfg)
	if err != nil {
		return
	}

	s := &store{
		index:    index,
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
	shard := uint64(when.Unix())
	shard = shard / uint64(24*60*60*st.cfg.ShardDurationDays)
	return shard
}

func (st *store) timeFromShard(shard uint64) time.Time {
	secs := shard * uint64(24*60*60*st.cfg.ShardDurationDays)
	return time.Unix(int64(secs), 0)
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
		}
	}

	return
}

func (st *store) StoreContent(records []StoreRecord) (err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	f, shard, err := st.openShard(0)
	if err != nil {
		return
	}

	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}

	txn, err := st.index.BeginTransaction()
	if err != nil {
		return
	}

	for _, record := range records {
		sizedContent := make([]byte, len(record.content)+4)
		binary.BigEndian.PutUint32(sizedContent[0:4], uint32(len(record.content)))
		copy(sizedContent[4:], record.content)

		var n int
		n, err = f.Write(sizedContent)
		if err != nil {
			return
		}

		if n < len(sizedContent) {
			err = errors.New("unable to write all data") // should be unreachable
			return
		}

		if txn.Set(record.key, shard, uint64(offset)); err != nil {
			return
		}
	}

	if err = txn.EndTransaction(); err != nil {
		return
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

	return
}

func (st *store) RetrieveContent(key []byte) (content []byte, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	txn, err := st.index.BeginTransaction()
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

	if err = txn.EndTransaction(); err != nil {
		return
	}

	return
}

func (st *store) PurgeOld() (err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	cutoff := time.Now().UTC().Add(-(time.Duration(time.Hour * 24 * time.Duration(st.cfg.ShardRetentionDays))))

	if err = st.index.RemoveBefore(cutoff); err != nil {
		return
	}

	return st.purgeShards(cutoff)
}

func (st *store) Close() {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.index.Close()
	for _, f := range st.shards {
		f.Close()
	}
	st.shards = map[uint64]afero.File{}
	st.accessed = map[uint64]time.Time{}
}
