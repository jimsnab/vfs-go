package vfs

import (
	"errors"
	"time"

	"github.com/spf13/afero"
)

type (
	avlIndex struct {
		tree *avlTree
		txn  *avlTransaction
		cfg  *VfsConfig
	}
)

var AppFs = afero.NewOsFs()

var ErrTransactionStarted = errors.New("transaction in progress")

func newIndex(cfg *VfsConfig) (index *avlIndex, err error) {
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
	if ai.tree != nil {
		ai.tree.Close()
	}
}

func (ai *avlIndex) BeginTransaction() (txn *avlTransaction, err error) {
	if ai.txn != nil {
		err = ErrTransactionStarted
		return
	}

	txn = &avlTransaction{
		ai:   ai,
		tree: ai.tree,
	}
	ai.txn = txn
	return
}

func (ai *avlIndex) RemoveBefore(cutoff time.Time) (err error) {
	if ai.txn != nil {
		err = ErrTransactionStarted
		return
	}

	cutoffNs := cutoff.UnixNano()
	deletions := 0
	var order int64
	ai.tree.IterateByTimestamp(func(node *avlNode) bool {
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

func (ai *avlIndex) Stats() avlTreeStats {
	return ai.tree.Stats()
}

func (ai *avlIndex) Check() {
	if !ai.tree.isValid() {
		panic("check failure")
	}
}
