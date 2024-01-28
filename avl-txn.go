package vfs

type (
	avlTransaction struct {
		ai   *avlIndex
		tree avlTree
	}
)

func (txn *avlTransaction) Set(key []byte, shard, position uint64) (err error) {
	if err = txn.tree.lock(); err != nil {
		return
	}
	defer txn.tree.unlock()

	txn.tree.Set(key, shard, position)
	return txn.tree.lastError()
}

func (txn *avlTransaction) Get(key []byte) (found bool, shard, position uint64, err error) {
	if err = txn.tree.lock(); err != nil {
		return
	}
	defer txn.tree.unlock()

	node := txn.tree.Find(key)
	if node != nil {
		found = true
		shard = node.Shard()
		position = node.Position()
	}

	err = txn.tree.lastError()
	return
}

func (txn *avlTransaction) EndTransaction() (err error) {
	if err = txn.tree.lock(); err != nil {
		return
	}
	defer txn.tree.unlock()

	if err = txn.tree.flush(); err != nil {
		return
	}

	txn.ai.txn = nil
	return
}
