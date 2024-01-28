package vfs

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/spf13/afero"
)

type (
	VfsConfig struct {
		IndexDir          string `json:"index_dir"`
		DataDir           string `json:"data_dir"`
		BaseName          string `json:"base_name"`
		CacheSize         int    `json:"cache_size"`
		Sync              bool   `json:"sync"`
		SyncTask          bool   `json:"sync_task"`
		ShardDurationMins int    `json:"shard_duration_mins"`
		RecoveryEnabled   bool   `json:"recovery_enabled"`
	}

	Index interface {
		// Starts a transaction for getting or setting index values. Only one
		// transaction can be active at a time.
		BeginTransaction() (txn IndexTransaction, err error)

		// Removes all of the index nodes with a timestamp older than start.
		PurgeOlder(start time.Time) (err error)

		// Closes the file resources of the index.
		Close()

		Check()
	}

	IndexTransaction interface {
		Set(key []byte, shard, position uint64) (err error)
		Get(key []byte) (found bool, shard, position uint64, err error)
		EndTransaction() (err error)
	}

	avlIndex struct {
		mu   sync.Mutex
		tree avlTree
		txn  *avlTransaction
		cfg  *VfsConfig
	}
)

var AppFs = afero.NewOsFs()

var ErrTransactionStarted = errors.New("transaction in progress")

func NewIndex(cfg *VfsConfig) (index Index, err error) {
	tree, err := newAvlTree(cfg)
	if err != nil {
		return
	}

	index = &avlIndex{
		tree: tree,
		cfg:  cfg,
	}
	return
}

func (ai *avlIndex) Close() {
	ai.mu.Lock()
	defer ai.mu.Unlock()

	if ai.tree != nil {
		ai.tree.Close()
	}
}

func (ai *avlIndex) BeginTransaction() (txn IndexTransaction, err error) {
	ai.mu.Lock()
	defer ai.mu.Unlock()

	if ai.txn != nil {
		err = ErrTransactionStarted
		return
	}

	t := avlTransaction{
		ai:   ai,
		tree: ai.tree,
	}
	txn = &t
	ai.txn = &t
	return
}

func (ai *avlIndex) PurgeOlder(cutoff time.Time) (err error) {
	ai.mu.Lock()
	defer ai.mu.Unlock()

	if ai.txn != nil {
		err = ErrTransactionStarted
		return
	}

	cutoffNs := cutoff.UnixNano()
	deletions := 0
	var order int64
	ai.tree.IterateByTimestamp(func(node avlNode) bool {
		createdNs := node.Timestamp()

		if createdNs < order {
			panic("out of order")
		}
		order = createdNs

		if createdNs >= cutoffNs {
			return false
		}

		ai.tree.Delete(node.Key())
		deletions++
		if deletions == 1000 {
			// flush every 1000 removals
			deletions = 0
			ai.tree.flush()
		}

		return true
	})

	return ai.tree.flush()
}

func (ai *avlIndex) Check() {
	count1 := 0
	ai.tree.IterateByKeys(func(node avlNode) bool {
		count1++
		return true
	})

	count2 := 0
	ai.tree.IterateByTimestamp(func(node avlNode) bool {
		count2++
		return true
	})
	if count1 != count2 {
		fmt.Printf("key count %d different than timestamp count %d\n", count1, count2)
		panic("check failure")
	}
}
