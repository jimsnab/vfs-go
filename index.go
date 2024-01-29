package vfs

import (
	"errors"
	"sync"
	"time"

	"github.com/spf13/afero"
)

type (
	VfsConfig struct {
		IndexDir           string  `json:"index_dir"`
		DataDir            string  `json:"data_dir"`
		BaseName           string  `json:"base_name"`
		CacheSize          int     `json:"cache_size"`
		Sync               bool    `json:"sync"`
		SyncTask           bool    `json:"sync_task"`
		ShardDurationDays  float64 `json:"shard_duration_days"`
		ShardRetentionDays float64 `json:"shard_retention_days"`
		RecoveryEnabled    bool    `json:"recovery_enabled"`
	}

	Index interface {
		// Starts a transaction for getting or setting index values. Only one
		// transaction can be active at a time.
		BeginTransaction() (txn IndexTransaction, err error)

		// Removes all of the index nodes with a timestamp older than start.
		RemoveBefore(start time.Time) (err error)

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

func (ai *avlIndex) RemoveBefore(cutoff time.Time) (err error) {
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

		// delete invalidates node
		if !ai.tree.Delete(node.Key()) {
			panic("didn't find node in the tree")
		}

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
	if !ai.tree.isValid() {
		panic("check failure")
	}
}
