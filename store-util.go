package vfs

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

type (
	CopyProgressFn   func(index int64, saves int64)
	VerifyProgressFn func(index int64, comparisons int64)
	ReindexFn        func(name string, content []byte) (refs []StoreReference, err error)

	CopyConfig struct {
		Progress  CopyProgressFn
		FromShard uint64
		ToShard   uint64
		Reindex   ReindexFn
	}

	VerifyConfig struct {
		Progress       VerifyProgressFn
		FromShard      uint64
		ToShard        uint64
		CompareContent bool
	}
)

// Copies data from the source store to the target store. cfg can be nil,
// or can specify copy options.
func CopyStore(source, target Store, cfg *CopyConfig) (err error) {
	if cfg == nil {
		cfg = &CopyConfig{}
	}

	endShard := cfg.ToShard
	if endShard == 0 {
		endShard = 0xFFFFFFFFFFFFFFFF
	}

	src := source.(*store)
	src.docMu.Lock()
	defer src.docMu.Unlock()

	dest := target.(*store)
	dest.docMu.Lock()
	defer dest.docMu.Unlock()

	srcTxn, err := src.ai.BeginTransaction(nil)
	if err != nil {
		return
	}

	index := int64(0)
	copied := int64(0)
	records := make([]StoreRecord, 0, 10000)

	nextUpdate := time.Now().Add(time.Second)

	err = srcTxn.ai.IterateByTimestamp(func(keyGroup string, node *avlNode) error {
		if node.shard > endShard {
			return ErrIteratorAbort
		}

		if node.shard >= cfg.FromShard {
			content, err := src.doLoadContent(node.shard, node.position)
			if err != nil {
				return err
			}

			// recompute refs, because there's no way to determine value key group
			// from the reference array; something to fix in v2
			refLists := make(map[string][]StoreReference, len(src.refTables))
			if cfg.Reindex != nil {
				for name := range src.refTables {
					refs, err := cfg.Reindex(name, content)
					if err != nil {
						return err
					}

					if len(refs) > 0 {
						refLists[name] = refs
					}
				}
			}

			record := StoreRecord{
				shard:     dest.cfg.calcShard(node.Epoch()),
				timestamp: node.timestamp,
				KeyGroup:  keyGroup,
				Key:       node.key,
				Content:   content,
				RefLists:  refLists,
			}
			records = append(records, record)
			if len(records) == 10000 {
				err := dest.doStoreContent(records, nil)
				if err != nil {
					return err
				}
				copied += int64(len(records))
				records = records[:0]
			}
		}

		index++
		if nextUpdate.Before(time.Now()) {
			nextUpdate = nextUpdate.Add(time.Second)
			if cfg.Progress != nil {
				cfg.Progress(index, copied)
			}
		}

		return nil
	})

	if errors.Is(err, ErrIteratorAbort) {
		err = nil
	}

	if err != nil {
		return
	}

	if len(records) > 0 {
		err = dest.doStoreContent(records, nil)
		if err != nil {
			return
		}
		copied += int64(len(records))
		if cfg.Progress != nil {
			cfg.Progress(index, copied)
		}
	}

	err = srcTxn.EndTransaction()
	return
}

// Reads from the source store and verifies it is present in the destination.
func VerifyStore(source, target Store, cfg *VerifyConfig) (err error) {
	if cfg == nil {
		cfg = &VerifyConfig{}
	}

	endShard := cfg.ToShard
	if endShard == 0 {
		endShard = 0xFFFFFFFFFFFFFFFF
	}

	src := source.(*store)
	src.docMu.Lock()
	defer src.docMu.Unlock()

	dest := target.(*store)
	dest.docMu.Lock()
	defer dest.docMu.Unlock()

	srcTxn, err := src.ai.BeginTransaction(nil)
	if err != nil {
		return
	}

	destTxn, err := dest.ai.BeginTransaction(nil)
	if err != nil {
		return
	}

	index := int64(0)
	compared := int64(0)

	nextUpdate := time.Now().Add(time.Second)

	err = srcTxn.ai.IterateByTimestamp(func(keyGroup string, srcNode *avlNode) (err error) {
		if srcNode.shard > endShard {
			err = ErrIteratorAbort
			return
		}

		if srcNode.shard >= cfg.FromShard {
			var tree *avlTree
			tree, err = destTxn.ai.getTree(keyGroup)
			if err != nil {
				return
			}

			var destNode *avlNode
			destNode, err = tree.Find(srcNode.key)
			if err != nil {
				return
			}
			if destNode == nil {
				err = fmt.Errorf("did not find key %s in destination store", hex.EncodeToString(srcNode.key[:]))
				return
			}

			if cfg.CompareContent {
				var srcContent []byte
				srcContent, err = src.doLoadContent(srcNode.shard, srcNode.position)
				if err != nil {
					return err
				}

				var destContent []byte
				destContent, err = dest.doLoadContent(destNode.shard, destNode.position)
				if err != nil {
					return err
				}
				if !bytes.Equal(srcContent, destContent) {
					err = fmt.Errorf("content of key %s does not match", hex.EncodeToString(srcNode.key[:]))
					return
				}
				compared++
			}
		}

		index++
		if nextUpdate.Before(time.Now()) {
			nextUpdate = nextUpdate.Add(time.Second)
			if cfg.Progress != nil {
				cfg.Progress(index, compared)
			}
		}

		return nil
	})

	if errors.Is(err, ErrIteratorAbort) {
		err = nil
	}

	if err != nil {
		return
	}

	if cfg.Progress != nil {
		cfg.Progress(index, compared)
	}

	err = srcTxn.EndTransaction()
	err2 := destTxn.EndTransaction()
	if err == nil {
		err = err2
	}
	return
}

// Reads from the source store and verifies it is present in the destination. It uses key iteration,
// so shard start and end aren't available.
func VerifyStoreByKeys(source, target Store, cfg *VerifyConfig) (err error) {
	if cfg == nil {
		cfg = &VerifyConfig{}
	}

	src := source.(*store)
	src.docMu.Lock()
	defer src.docMu.Unlock()

	dest := target.(*store)
	dest.docMu.Lock()
	defer dest.docMu.Unlock()

	srcTxn, err := src.ai.BeginTransaction(nil)
	if err != nil {
		return
	}

	destTxn, err := dest.ai.BeginTransaction(nil)
	if err != nil {
		return
	}

	index := int64(0)
	compared := int64(0)

	nextUpdate := time.Now().Add(time.Second)

	err = srcTxn.ai.IterateByKeys(func(keyGroup string, srcNode *avlNode) (err error) {
		var tree *avlTree
		tree, err = destTxn.ai.getTree(keyGroup)
		if err != nil {
			return
		}

		var destNode *avlNode
		destNode, err = tree.Find(srcNode.key)
		if err != nil {
			return
		}
		if destNode == nil {
			err = fmt.Errorf("did not find key %s in destination store", hex.EncodeToString(srcNode.key[:]))
			return
		}

		if cfg.CompareContent {
			var srcContent []byte
			srcContent, err = src.doLoadContent(srcNode.shard, srcNode.position)
			if err != nil {
				return err
			}

			var destContent []byte
			destContent, err = dest.doLoadContent(destNode.shard, destNode.position)
			if err != nil {
				return err
			}
			if !bytes.Equal(srcContent, destContent) {
				err = fmt.Errorf("content of key %s does not match", hex.EncodeToString(srcNode.key[:]))
				return
			}
			compared++
		}

		index++
		if nextUpdate.Before(time.Now()) {
			nextUpdate = nextUpdate.Add(time.Second)
			if cfg.Progress != nil {
				cfg.Progress(index, compared)
			}
		}

		return nil
	})

	if errors.Is(err, ErrIteratorAbort) {
		err = nil
	}

	if err != nil {
		return
	}

	if cfg.Progress != nil {
		cfg.Progress(index, compared)
	}

	err = srcTxn.EndTransaction()
	err2 := destTxn.EndTransaction()
	if err == nil {
		err = err2
	}
	return
}

// Iterates the source store and checks the dest index for the key. If it is missing,
// the data is copied.
func CopyMissing(source, target Store, cfg *CopyConfig) (err error) {
	if cfg == nil {
		cfg = &CopyConfig{}
	}

	endShard := cfg.ToShard
	if endShard == 0 {
		endShard = 0xFFFFFFFFFFFFFFFF
	}

	src := source.(*store)
	src.docMu.Lock()
	defer src.docMu.Unlock()

	dest := target.(*store)
	dest.docMu.Lock()
	defer dest.docMu.Unlock()

	srcTxn, err := src.ai.BeginTransaction(nil)
	if err != nil {
		return
	}

	destTxn, err := dest.ai.BeginTransaction(nil)
	if err != nil {
		return
	}

	index := int64(0)
	stored := int64(0)
	records := make([]StoreRecord, 0, 10000)

	nextUpdate := time.Now().Add(time.Second)

	err = srcTxn.ai.IterateByTimestamp(func(keyGroup string, srcNode *avlNode) (err error) {
		if srcNode.shard > endShard {
			err = ErrIteratorAbort
			return
		}

		if srcNode.shard >= cfg.FromShard {
			var tree *avlTree
			tree, err = destTxn.ai.getTree(keyGroup)
			if err != nil {
				return
			}

			var destNode *avlNode
			destNode, err = tree.Find(srcNode.key)
			if err != nil {
				return
			}
			if destNode == nil {
				var srcContent []byte
				srcContent, err = src.doLoadContent(srcNode.shard, srcNode.position)
				if err != nil {
					return err
				}

				// recompute refs, because there's no way to determine value key group
				// from the reference array; something to fix in v2
				refLists := make(map[string][]StoreReference, len(src.refTables))
				if cfg.Reindex != nil {
					for name := range src.refTables {
						refs, err := cfg.Reindex(name, srcContent)
						if err != nil {
							return err
						}

						if len(refs) > 0 {
							refLists[name] = refs
						}
					}
				}

				record := StoreRecord{
					shard:     dest.cfg.calcShard(srcNode.Epoch()),
					timestamp: srcNode.timestamp,
					KeyGroup:  keyGroup,
					Key:       srcNode.key,
					Content:   srcContent,
					RefLists:  refLists,
				}
				records = append(records, record)
				if len(records) == 10000 {
					err = destTxn.EndTransaction()
					if err != nil {
						return
					}

					err = dest.doStoreContent(records, nil)
					if err != nil {
						return
					}
					stored += int64(len(records))
					records = records[:0]

					destTxn, err = dest.ai.BeginTransaction(nil)
					if err != nil {
						return
					}
				}
			}
		}

		index++
		if nextUpdate.Before(time.Now()) {
			nextUpdate = nextUpdate.Add(time.Second)
			if cfg.Progress != nil {
				cfg.Progress(index, stored)
			}
		}

		return nil
	})

	if errors.Is(err, ErrIteratorAbort) {
		err = nil
	}

	if err != nil {
		return
	}

	if len(records) > 0 {
		err = destTxn.EndTransaction()
		if err != nil {
			return
		}

		err = dest.doStoreContent(records, nil)
		if err != nil {
			return
		}
		stored += int64(len(records))
		if cfg.Progress != nil {
			cfg.Progress(index, stored)
		}
	} else {
		err = destTxn.EndTransaction()
	}

	err2 := srcTxn.EndTransaction()
	if err == nil {
		err = err2
	}
	return
}
