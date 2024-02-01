package vfs

type (
	avlTransaction struct {
		ai   *avlIndex
		tree *avlTree
	}
)

func (txn *avlTransaction) Set(key []byte, shard, position uint64) (err error) {
	txn.tree.Lock()
	defer txn.tree.Unlock()

	_, _, err = txn.tree.Set(key, shard, position)
	return
}

func (txn *avlTransaction) Get(key []byte) (found bool, shard, position uint64, err error) {
	txn.tree.Lock()
	defer txn.tree.Unlock()

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
	err = txn.doEndTransaction(onComplete)
	if err != nil && onComplete != nil {
		onComplete(err)
	}
	return
}

func (txn *avlTransaction) doEndTransaction(onComplete CommitCompleted) (err error) {
	txn.tree.Lock()
	defer txn.tree.Unlock()

	if err = txn.tree.flush(onComplete); err != nil {
		return
	}

	txn.ai.txn = nil
	return
}
