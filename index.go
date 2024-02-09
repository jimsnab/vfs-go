package vfs

import (
	"errors"
	"io/fs"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jimsnab/afero"
)

type (
	avlIndex struct {
		mu        sync.Mutex
		extension string
		trees     map[string]*avlTree
		txn       *avlTransaction
		cfg       *VfsConfig
		removed   map[[20]byte]struct{}
	}

	indexIterator struct {
		done        bool
		availableCh chan struct{}
		releaseCh   chan error
		node        *avlNode
		failure     error
	}
)

var AppFs = afero.NewOsFs()

var ErrTransactionStarted = errors.New("transaction in progress")

const kMainIndexExt = "dt1"

func newIndex(cfg *VfsConfig, extension string) (index *avlIndex, err error) {
	ai := &avlIndex{
		trees:     map[string]*avlTree{},
		cfg:       cfg,
		extension: extension,
	}

	if err = ai.OpenAll(); err != nil {
		return
	}

	index = ai
	return
}

func (ai *avlIndex) OpenAll() (err error) {
	ai.mu.Lock()
	defer ai.mu.Unlock()

	prefix := ai.cfg.BaseName + "."
	suffix := "." + ai.extension
	terr := afero.Walk(AppFs, ai.cfg.IndexDir, func(path string, info fs.FileInfo, err error) error {
		name := info.Name()
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix) {
			keyGroup := name[len(prefix) : len(name)-len(suffix)]
			_, err = ai.getTree(keyGroup)
		}
		return err
	})
	if err == nil {
		err = terr
	}
	return
}

// worker to get the avl tree for the key group, creating it if necessary
func (ai *avlIndex) getTree(keyGroup string) (tree *avlTree, err error) {
	if ai.trees == nil {
		err = os.ErrClosed
		return
	}

	t := ai.trees[keyGroup]
	if t == nil {
		t, err = newAvlTree(ai.cfg, keyGroup, ai.extension)
		if err != nil {
			return
		}

		ai.trees[keyGroup] = t
	}

	tree = t
	return
}

func (ai *avlIndex) Sync() (err error) {
	ai.mu.Lock()
	defer ai.mu.Unlock()

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
	ai.mu.Lock()
	defer ai.mu.Unlock()

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

// Starts a transaction. If tm is nil, a transactionManager is allocated.
func (ai *avlIndex) BeginTransaction(tm *transactionManager) (txn *avlTransaction, err error) {
	ai.mu.Lock()
	defer ai.mu.Unlock()

	if ai.txn != nil {
		err = ErrTransactionStarted
		return
	}

	var ownedTm *transactionManager
	if tm == nil {
		ownedTm = newTransactionManager(nil)
		tm = ownedTm
	}

	txn = &avlTransaction{
		ai:      ai,
		ownedTm: ownedTm,
		touched: map[string]struct{}{},
	}
	ai.txn = txn
	tm.Attach(txn)
	return
}

func (ai *avlIndex) RemoveBefore(cutoff time.Time, onComplete CommitCompleted) (err error) {
	// if onComplete is nil, perform operation in blocking mode
	var wg sync.WaitGroup
	if onComplete == nil {
		wg.Add(1)
		onComplete = func(failure error) {
			err = failure
			wg.Done()
		}
	}

	go func() {
		onComplete(ai.doRemoveBefore(cutoff))
	}()

	wg.Wait()
	return
}

func (ai *avlIndex) doRemoveBefore(cutoff time.Time) (err error) {
	if ai.txn != nil {
		err = ErrTransactionStarted
		return
	}

	cutoffNs := cutoff.UnixNano()

	for _, tree := range ai.trees {
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

			// testing support
			if ai.removed != nil {
				ai.removed[node.key] = struct{}{}
			}

			// delete invalidates node
			wasDeleted, err := tree.Delete(node.key)
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
				hasBackup, err := tree.flush()
				if err != nil {
					return err
				}
				err = tree.commit(hasBackup)
				if err != nil {
					return err
				}
			}

			return nil
		})

		if terr != nil && terr != ErrIteratorAbort {
			err = terr
			return
		}

		var hasBackup bool
		hasBackup, err = tree.flush()
		if err != nil {
			return
		}
		if err = tree.commit(hasBackup); err != nil {
			return
		}
	}

	return
}

func (ai *avlIndex) IterateByTimestamp(iter avlIterator) (err error) {
	var wg sync.WaitGroup
	var mu sync.Mutex // synchronizes 'done' state
	nodes := map[*indexIterator]bool{}

	for _, t := range ai.trees {
		ii := indexIterator{
			availableCh: make(chan struct{}),
			releaseCh:   make(chan error),
		}

		wrapper := func(node *avlNode) (err error) {
			// post the node
			ii.failure = err
			ii.node = node
			ii.availableCh <- struct{}{}

			// wait for aggregation to process the result
			err = <-ii.releaseCh
			return
		}

		wg.Add(1)
		nodes[&ii] = false
		go func(tree *avlTree) {
			defer wg.Done()

			ierr := tree.IterateByTimestamp(wrapper)

			mu.Lock()
			ii.done = true
			ii.failure = ierr
			mu.Unlock()
		}(t)
	}

	for {
		// capture the next node from each index iterator
		bestTs := int64(math.MaxInt64)
		var bestIter *indexIterator
		for ii, captured := range nodes {
			if !captured {
				mu.Lock()
				iiDone := ii.done
				mu.Unlock()

				if iiDone {
					continue
				}

				<-ii.availableCh
				nodes[ii] = true
			}

			if ii.failure != nil {
				bestIter = ii
				break
			}
			if ii.node.timestamp < bestTs {
				bestTs = ii.node.timestamp
				bestIter = ii
			}
		}

		if bestIter == nil {
			// nothing more to iterate
			wg.Wait()
			return
		}

		if bestIter.failure != nil {
			err = bestIter.failure

			// release the unfailed iterators
			for ii := range nodes {
				mu.Lock()
				iiDone := ii.done
				mu.Unlock()
				if !iiDone {
					ii.releaseCh <- err
				}
			}

			wg.Wait()
			return
		}

		// invoke callback
		iter(bestIter.node)

		// release the processed iterator
		nodes[bestIter] = false
		bestIter.releaseCh <- nil
	}
}

func (ai *avlIndex) IterateByKeys(iter avlIterator) (err error) {
	keyGroups := make([]string, 0, len(ai.trees))
	for keyGroup := range ai.trees {
		keyGroups = append(keyGroups, keyGroup)
	}
	slices.Sort(keyGroups)
	for _, keyGroup := range keyGroups {
		tree := ai.trees[keyGroup]
		err = tree.IterateByKeys(iter)
		if err != nil {
			return
		}
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
