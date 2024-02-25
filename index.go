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

	timestampIterator struct {
		keyGroup    string
		availableCh chan bool
		releaseCh   chan struct{}
		mu          sync.Mutex
		node        *avlNode
		failure     error
	}

	indexIteratorFn func(keyGroup string, node *avlNode) (err error)
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
			createdNs := node.timestamp

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

func (ai *avlIndex) IterateByTimestamp(iter indexIteratorFn) (err error) {
	var wg sync.WaitGroup
	nodes := map[*timestampIterator]bool{}

	for keyGroup, t := range ai.trees {
		ti := timestampIterator{
			availableCh: make(chan bool),
			releaseCh:   make(chan struct{}),
			keyGroup:    keyGroup,
		}

		wrapper := func(node *avlNode) (err error) {
			// post the node
			ti.node = node
			ti.availableCh <- true

			// wait for aggregation to process the result
			<-ti.releaseCh
			if ti.failure != nil {
				err = ti.failure
			}
			return
		}

		wg.Add(1)
		nodes[&ti] = false
		go func(tree *avlTree) {
			ti.failure = tree.IterateByTimestamp(wrapper)
			close(ti.availableCh)
			wg.Done()
		}(t)
	}

	for {
		// capture the next node from each index iterator
		bestTs := int64(math.MaxInt64)
		var chosen *timestampIterator
		for ti, captured := range nodes {
			if !captured {
				ready := <-ti.availableCh
				if !ready {
					if ti.failure != nil {
						err = ti.failure
						break
					}
					continue
				}

				nodes[ti] = true
			}

			if ti.node.timestamp < bestTs {
				bestTs = ti.node.timestamp
				chosen = ti
			}
		}

		if chosen == nil || err != nil {
			// finish
			for ti := range nodes {
				ti.failure = err
				close(ti.releaseCh)
			}
			wg.Wait()
			return
		}

		// invoke callback
		iter(chosen.keyGroup, chosen.node)

		// release the processed iterator
		nodes[chosen] = false
		chosen.releaseCh <- struct{}{}
	}
}

func (ai *avlIndex) IterateByKeys(iter indexIteratorFn) (err error) {
	keyGroups := make([]string, 0, len(ai.trees))
	for keyGroup := range ai.trees {
		keyGroups = append(keyGroups, keyGroup)
	}
	slices.Sort(keyGroups)
	for _, keyGroup := range keyGroups {
		tree := ai.trees[keyGroup]
		err = tree.IterateByKeys(func(node *avlNode) error {
			return iter(keyGroup, node)
		})
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
