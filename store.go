package vfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/spf13/afero"
)

type (
	Store interface {
		StoreContent(key, content []byte) (err error)
		RetrieveContent(key []byte) (content []byte, err error)
		Close()
	}

	store struct {
		mu       sync.Mutex
		index    Index
		shards   map[uint64]afero.File
		accessed map[uint64]time.Time
		cfg      *VfsConfig
	}
)

func NewStore(cfg *VfsConfig) (st Store, err error) {
	index, err := NewIndex(cfg)
	if err != nil {
		return
	}

	st = &store{
		index:    index,
		cfg:      cfg,
		shards:   map[uint64]afero.File{},
		accessed: map[uint64]time.Time{},
	}
	return
}

func (st *store) openShard(request uint64) (f afero.File, shard uint64, err error) {
	if request == 0 {
		shard = uint64(time.Now().UTC().Unix())
		shard = shard / (60 * uint64(st.cfg.ShardDurationMins))
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

func (st *store) StoreContent(key, content []byte) (err error) {
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

	sizedContent := make([]byte, len(content)+4)
	binary.BigEndian.PutUint32(sizedContent[0:4], uint32(len(content)))
	copy(sizedContent[4:], content)

	n, err := f.Write(sizedContent)
	if err != nil {
		return
	}

	if n < len(sizedContent) {
		err = errors.New("unable to write all data") // should be unreachable
		return
	}

	txn, err := st.index.BeginTransaction()
	if err != nil {
		return
	}

	if txn.Set(key, shard, uint64(offset)); err != nil {
		return
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
