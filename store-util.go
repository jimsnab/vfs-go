package vfs

import (
	"errors"
	"time"
)

type (
	CopyProgressFn func(index int64, saves int64)
	ReindexFn      func(name string, content []byte) (ref *StoreReference, err error)

	CopyConfig struct {
		Progress  CopyProgressFn
		FromShard uint64
		ToShard   uint64
		Reindex   ReindexFn
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
	dest := target.(*store)

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
			refKeys := make(map[string]StoreReference, len(src.refTables))
			if cfg.Reindex != nil {
				for name := range src.refTables {
					ref, err := cfg.Reindex(name, content)
					if err != nil {
						return err
					}

					if ref != nil {
						refKeys[name] = *ref
					}
				}
			}

			record := StoreRecord{
				KeyGroup: keyGroup,
				Key:      node.key,
				Content:  content,
				RefKeys:  refKeys,
			}
			records = append(records, record)
			if len(records) == 10000 {
				err := dest.StoreContent(records, nil)
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
		err = dest.StoreContent(records, nil)
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
