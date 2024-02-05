package vfs

import (
	"errors"
	"os"
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

func (ai *avlIndex) Sync() (err error) {
	return ai.tree.Sync()
}

func (ai *avlIndex) Close() (err error) {
	ai.tree.dt1sync.Wait()
	ai.tree.dt2sync.Wait()

	if ai.tree != nil {
		ai.tree.Close()
	} else {
		err = os.ErrClosed
	}
	return
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

func (ai *avlIndex) RemoveBefore(cutoff time.Time, onComplete CommitCompleted) (err error) {
	err = ai.doRemoveBefore(cutoff, onComplete)
	if err != nil && onComplete != nil {
		onComplete(err)
	}
	return
}

func (ai *avlIndex) doRemoveBefore(cutoff time.Time, onComplete CommitCompleted) (err error) {
	if ai.txn != nil {
		err = ErrTransactionStarted
		return
	}

	cutoffNs := cutoff.UnixNano()
	deletions := 0
	var order int64
	terr := ai.tree.IterateByTimestamp(func(node *avlNode) error {
		createdNs := node.Timestamp()

		if createdNs < order {
			panic("out of order")
		}
		order = createdNs

		if createdNs >= cutoffNs {
			return ErrIteratorAbort
		}

		// delete invalidates node
		wasDeleted, err := ai.tree.Delete(node.Key())
		if err != nil {
			return err
		}

		if !wasDeleted {
			return ErrDamagedIndex
		}

		deletions++
		if deletions == 1000 {
			// flush every 1000 removals
			deletions = 0
			ai.tree.flush(nil)
		}

		return nil
	})

	if terr != nil && terr != ErrIteratorAbort {
		err = terr
		return
	}

	return ai.tree.flush(onComplete)
}

func (ai *avlIndex) Stats() avlTreeStats {
	return ai.tree.Stats()
}

func (ai *avlIndex) Check() (err error) {
	valid, err := ai.tree.isValid()
	if err != nil { // couldn't determine validity
		return
	}

	if !valid {
		panic("check failure")
	}
	return
}
