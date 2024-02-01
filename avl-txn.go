package vfs

import "sync"

type (
	avlTransaction struct {
		mu   sync.Mutex
		ai   *avlIndex
		tree *avlTree
	}
)

func (txn *avlTransaction) Set(key []byte, shard, position uint64) (err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	_, _, err = txn.tree.Set(key, shard, position)
	return
}

func (txn *avlTransaction) Get(key []byte) (found bool, shard, position uint64, err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	node, err := txn.tree.Find(key)
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

func (txn *avlTransaction) doEndTransaction(onComplete CommitCompleted) (err error) {
	if err = txn.tree.flush(onComplete); err != nil {
		return
	}

	txn.ai.txn = nil
	return
}
