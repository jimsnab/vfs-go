package vfs

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jimsnab/afero"
)

type (
	refTable struct {
		mu               sync.Mutex
		name             string
		extension        string
		cfg              VfsConfig
		shards           map[uint64]afero.File
		accessed         map[uint64]time.Time
		index            *avlIndex
		indexKeysRemoved uint64
		shardsAccessed   uint64
		shardsRemoved    uint64
		wg               sync.WaitGroup
		cancelFn         context.CancelFunc
		cleanupInterval  time.Duration
		txn              *refTableTransaction
	}

	refTableStats struct {
		ShardsAccessed   uint64
		IndexKeysRemoved uint64
		ShardsRemoved    uint64
	}

	RefRecord struct {
		KeyGroup string // the key group for ValueKey
		ValueKey []byte // the indexed key, up to 20 bytes of data extracted from the history doc
		StoreKey []byte // the history doc id
	}
)

const kRefTableDataExt = "dt4"
const kRefTableIndexExt = "dt5"

var ErrNotStarted = errors.New("not started")

func newRefTable(cfg *VfsConfig, name string) (tbl *refTable, err error) {
	index, err := newIndex(cfg, fmt.Sprintf("%s.%s", name, kRefTableIndexExt))
	if err != nil {
		return
	}

	t := refTable{
		name:            name,
		extension:       kRefTableDataExt,
		cfg:             *cfg,
		index:           index,
		shards:          map[uint64]afero.File{},
		accessed:        map[uint64]time.Time{},
		cleanupInterval: time.Minute,
	}

	if t.cfg.ShardDurationDays == 0 {
		t.cfg.ShardDurationDays = 1
	}
	if t.cfg.ShardRetentionDays == 0 {
		t.cfg.ShardRetentionDays = 7
	}
	if t.cfg.CacheSize == 0 {
		t.cfg.CacheSize = 1024
	}

	tbl = &t
	return
}

func (table *refTable) Start() {
	ctx, cancelFn := context.WithCancel(context.Background())
	table.cancelFn = cancelFn
	table.wg.Add(1)
	go table.run(ctx)
}

func (table *refTable) Stats() (stats refTableStats) {
	stats.ShardsAccessed = table.shardsAccessed
	stats.IndexKeysRemoved = table.indexKeysRemoved
	stats.ShardsRemoved = table.shardsRemoved
	return
}

func (table *refTable) run(ctx context.Context) {
	defer table.wg.Done()

	ticker1 := time.NewTicker(table.cleanupInterval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker1.C:
			table.closeIdleShards()
		}
	}
}

func (table *refTable) Close() {
	table.mu.Lock()
	if table.cancelFn == nil {
		table.mu.Unlock()
		return
	}
	table.cancelFn()
	table.cancelFn = nil
	table.mu.Unlock()
	table.wg.Wait()

	for _, f := range table.shards {
		f.Close()
	}
	table.shards = nil

	table.index.Close()
	table.index = nil
}

func (table *refTable) calcShard(when time.Time) uint64 {
	// convert time to an integral
	divisor := uint64(24 * 60 * 60 * 1000 * table.cfg.ShardDurationDays)
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

func (table *refTable) timeFromShard(shard uint64) time.Time {
	ms := int64(shard/10) * int64(24*60*60*1000*table.cfg.ShardDurationDays)
	return time.Unix(int64(ms/1000), (ms%1000)*1000*1000)
}

// Background task worker
func (table *refTable) closeIdleShards() {
	table.mu.Lock()
	defer table.mu.Unlock()

	cutoff := time.Now().UTC().Add(-time.Minute * 15)
	for sh, ts := range table.accessed {
		if ts.Before(cutoff) {
			f, exists := table.shards[sh]
			if exists {
				f.Close()
				delete(table.shards, sh)
			}
			delete(table.accessed, sh)
		}
	}
}

func (table *refTable) PurgeOld(cutoff time.Time) (err error) {
	table.mu.Lock()
	defer table.mu.Unlock()

	startStats := table.index.Stats()

	var wg sync.WaitGroup
	wg.Add(1)
	table.index.RemoveBefore(cutoff, func(failure error) {
		defer wg.Done()
		endStats := table.index.Stats()
		table.indexKeysRemoved += (startStats.NodeCount - endStats.NodeCount)

		if failure != nil {
			err = failure
		} else {
			err = table.purgeShards(cutoff)
		}
	})
	wg.Wait()

	return
}

// API task worker (caller holds the lock)
func (table *refTable) openShard(request uint64, forRead bool) (f afero.File, shard uint64, err error) {
	if request == 0 {
		shard = table.calcShard(time.Now().UTC())
	} else {
		shard = request
	}

	f, exists := table.shards[shard]
	if !exists {
		shardPath := path.Join(table.cfg.DataDir, fmt.Sprintf("%s.%s.%d.%s", table.cfg.BaseName, table.name, shard, table.extension))

		f, err = createOrOpenFile(shardPath, forRead)
		if err != nil {
			err = fmt.Errorf("error opening shard %s: %v", shardPath, err)
			return
		}

		table.shards[shard] = f
		table.accessed[shard] = time.Now().UTC()
		table.shardsAccessed++
	}

	return
}

// cleanup worker - caller holds the mutex
func (table *refTable) purgeShards(cutoff time.Time) (err error) {
	files, err := afero.ReadDir(AppFs, table.cfg.DataDir)
	if err != nil {
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()
		prefix := table.cfg.BaseName + "." + table.name + "."
		if !strings.HasPrefix(fileName, prefix) {
			continue
		}

		afterBase := fileName[len(prefix):]
		cutPoint := strings.Index(afterBase, ".")
		if cutPoint <= 0 {
			continue
		}

		if afterBase[cutPoint:] != "."+table.extension {
			continue
		}

		shard, terr := strconv.ParseUint(afterBase[:cutPoint], 10, 64)
		if terr != nil {
			continue
		}

		ts := table.timeFromShard(shard)
		if ts.Before(cutoff) {
			if err = AppFs.Remove(path.Join(table.cfg.DataDir, fileName)); err != nil {
				return
			}

			table.shardsRemoved++
		}
	}

	return
}

func (table *refTable) AddReferences(refRecords []RefRecord) (err error) {
	table.mu.Lock()
	defer table.mu.Unlock()

	txn, err := table.doBeginTransaction(nil)
	if err != nil {
		return
	}

	if err = txn.doAddReferences(refRecords); err != nil {
		return
	}

	err = txn.doEndTransaction()
	return
}

func (table *refTable) RetrieveReferences(keyGroup string, valueKey []byte) (refs [][]byte, err error) {
	table.mu.Lock()
	defer table.mu.Unlock()

	txn, err := table.doBeginTransaction(nil)
	if err != nil {
		return
	}

	defer func() {
		failure := txn.doEndTransaction()
		if err == nil {
			err = failure
		}
	}()

	if refs, err = txn.doRetrieveReferences(keyGroup, valueKey); err != nil {
		return
	}

	return
}
