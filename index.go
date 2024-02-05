package vfs

import (
	"errors"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/afero"
)

type (
	avlIndex struct {
		trees map[string]*avlTree
		txn   *avlTransaction
		cfg   *VfsConfig
	}
)

var AppFs = afero.NewOsFs()

var ErrTransactionStarted = errors.New("transaction in progress")

func newIndex(cfg *VfsConfig) (index *avlIndex, err error) {
	ai := &avlIndex{
		trees: map[string]*avlTree{},
		cfg:   cfg,
	}

	if err = ai.OpenAll(); err != nil {
		return
	}

	index = ai
	return
}

func (ai *avlIndex) OpenAll() (err error) {
	prefix := ai.cfg.BaseName + "."
	terr := afero.Walk(AppFs, ai.cfg.IndexDir, func(path string, info fs.FileInfo, err error) error {
		name := info.Name()
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ".dt1") {
			keyGroup := name[len(prefix) : len(name)-4]
			_, err = ai.getTree(keyGroup)
		}
		return err
	})
	if err == nil {
		err = terr
	}
	return
}

func (ai *avlIndex) getTree(keyGroup string) (tree *avlTree, err error) {
	if ai.trees == nil {
		err = os.ErrClosed
		return
	}

	t := ai.trees[keyGroup]
	if t == nil {
		t, err = newAvlTree(ai.cfg, keyGroup)
		if err != nil {
			return
		}

		ai.trees[keyGroup] = t
	}

	tree = t

	return
}

func (ai *avlIndex) Sync() (err error) {
	if ai.trees == nil {
		return os.ErrClosed
	}

	for _, tree := range ai.trees {
		terr := tree.Sync()
		if terr != nil {
			err = terr
		}
	}

	return
}

func (ai *avlIndex) Close() (err error) {
	if ai.trees == nil {
		return os.ErrClosed
	}

	for _, tree := range ai.trees {
		tree.dt1sync.Wait()
		tree.dt2sync.Wait()
		tree.Close()
	}

	ai.trees = nil
	return
}

func (ai *avlIndex) BeginTransaction() (txn *avlTransaction, err error) {
	if ai.txn != nil {
		err = ErrTransactionStarted
		return
	}

	txn = &avlTransaction{
		ai:      ai,
		touched: map[string]struct{}{},
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

	var wg sync.WaitGroup
	onTreeComplete := func(failed error) {
		if err == nil {
			err = failed
		}
		wg.Done()
	}

	for _, tree := range ai.trees {
		cutoffNs := cutoff.UnixNano()
		deletions := 0
		var order int64
		terr := tree.IterateByTimestamp(func(node *avlNode) error {
			createdNs := node.Timestamp()

			if createdNs < order {
				panic("out of order")
			}
			order = createdNs

			if createdNs >= cutoffNs {
				return ErrIteratorAbort
			}

			// delete invalidates node
			wasDeleted, err := tree.Delete(node.Key())
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
				tree.flush(nil)
			}

			return nil
		})

		if terr != nil && terr != ErrIteratorAbort {
			err = terr
			return
		}

		wg.Add(1)
		if err = tree.flush(onTreeComplete); err != nil {
			return
		}
	}

	wg.Wait()
	if err == nil && onComplete != nil {
		onComplete(nil)
	}

	return
}

func (ai *avlIndex) Stats() (stats avlTreeStats) {
	for _, tree := range ai.trees {
		s := tree.Stats()
		stats.Deletes += s.Deletes
		stats.FreeCount += s.FreeCount
		stats.NodeCount += s.NodeCount
		stats.Sets += s.Sets
	}
	return
}

func (ai *avlIndex) Check() (err error) {
	for _, tree := range ai.trees {
		var valid bool
		valid, err = tree.isValid()
		if err != nil { // couldn't determine validity
			return
		}

		if !valid {
			panic("check failure")
		}
	}
	return
}
