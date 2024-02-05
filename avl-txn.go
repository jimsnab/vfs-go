package vfs

import (
	"sync"
	"sync/atomic"
)

type (
	avlTransaction struct {
		mu      sync.Mutex
		ai      *avlIndex
		touched map[string]struct{}
	}
)

func (txn *avlTransaction) Set(keyGroup string, key []byte, shard, position uint64) (err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	tree, err := txn.ai.getTree(keyGroup)
	if err != nil {
		return
	}
	txn.touched[keyGroup] = struct{}{}

	_, _, err = tree.Set(key, shard, position)
	return
}

func (txn *avlTransaction) Get(keyGroup string, key []byte) (found bool, shard, position uint64, err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	tree, err := txn.ai.getTree(keyGroup)
	if err != nil {
		return
	}

	node, err := tree.Find(key)
	if err != nil {
		return
	}
	if node != nil {
		found = true
		shard = node.Shard()
		position = node.Position()
	}

	return
}

func (txn *avlTransaction) EndTransaction(onComplete CommitCompleted) (err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	err = txn.doEndTransaction(onComplete)
	if err != nil && onComplete != nil {
		onComplete(err)
	}
	return
}

func (txn *avlTransaction) partiallyComplete(refs *atomic.Int32, onComplete CommitCompleted, failure error) {
	if refs.Add(-1) == 0 {
		if onComplete != nil {
			onComplete(failure)
		}
	}
}

func (txn *avlTransaction) doEndTransaction(onComplete CommitCompleted) (err error) {
	var refs atomic.Int32

	refs.Add(int32(len(txn.touched)))
	for keyGroup := range txn.touched {
		var tree *avlTree
		if tree, err = txn.ai.getTree(keyGroup); err != nil {
			return
		}

		err = tree.flush(func(err error) {
			txn.partiallyComplete(&refs, onComplete, err)
		})
		if err != nil {
			return
		}
	}

	txn.ai.txn = nil
	return
}
