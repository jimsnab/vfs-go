package vfs

import (
	"sync"
)

type (
	avlTransaction struct {
		mu      sync.Mutex
		ai      *avlIndex
		ownedTm *transactionManager
		touched map[string]struct{}
	}

	avlTransactionResolver struct {
		tree      *avlTree
		keyGroup  string
		hasBackup bool
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

func (txn *avlTransaction) EndTransaction() (err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.ownedTm != nil {
		// this transaction owns a transaction manager - call resolve
		err = txn.ownedTm.Resolve(nil)
	}
	return
}

func (txn *avlTransaction) Register(tc *transactionCommit) (err error) {
	if len(txn.touched) > 0 {
		for keyGroup := range txn.touched {
			atr := avlTransactionResolver{
				keyGroup: keyGroup,
			}

			atr.tree, err = txn.ai.getTree(keyGroup)
			if err != nil {
				return
			}

			tc.Bind(&atr)
		}
	}
	return
}

func (txn *avlTransaction) Detach() {
	txn.ai.mu.Lock()
	txn.ai.txn = nil
	txn.ai.mu.Unlock()
}

func (resolver *avlTransactionResolver) Flush() (err error) {
	resolver.hasBackup, err = resolver.tree.flush()
	return
}

func (resolver *avlTransactionResolver) Commit() (err error) {
	return resolver.tree.commit(resolver.hasBackup)
}
