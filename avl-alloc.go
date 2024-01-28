package vfs

import (
	"io"
	"time"
)

func (af *avlTreeS) loadFreeNode(offset uint64) *freeNodeS {
	if af.err != nil {
		return &freeNodeS{}
	}

	fn, exists := af.freeNodes[offset]
	if !exists {
		var err error
		fn, err = af.readFreeNode(offset)
		if err != nil {
			af.err = err
			return &freeNodeS{}
		}

		af.freeNodes[offset] = fn
	} else if fn == nil {
		panic("reclaiming a free node that was already reclaimed")
	}

	fn.touch()
	return fn
}

func (af *avlTreeS) alloc(key []byte, shard, position uint64) (node avlNode) {
	an := avlNodeS{
		tree: af,
		key:  make([]byte, len(key)),
	}

	if af.err != nil {
		node = &an
		return
	}

	//
	// Find some space.
	//

	offset := af.firstFreeOffset
	if offset != 0 {
		// reclaim a deleted node
		reclaimed := af.loadFreeNode(offset)
		af.firstFreeOffset = reclaimed.NextOffset()
		af.dirty = true
		af.originalRaw = reclaimed.originalRaw

		af.freeNodes[offset] = nil
		af.freeCount--
		af.freeLru.Remove(reclaimed.lru)
	} else {
		// allocate a new node at the end of the file
		size, err := af.f.Seek(0, io.SeekEnd)
		if err != nil {
			af.err = err
			node = &an
			return
		}
		offset = uint64(size)

		if err = af.f.Truncate(size + kRecordSize); err != nil {
			return
		}
	}
	af.nodeCount++

	//
	// Initialize the new node.
	//

	af.nodeCache[offset] = &an
	an.nodeDirty()
	an.offset = offset
	copy(an.key, key)
	an.shard = shard
	an.position = position
	an.timestamp = time.Now().UTC().UnixNano()

	//
	// Link the new node in time history.
	//

	an.prevOffset = af.newestOffset

	if af.newestOffset == 0 {
		af.oldestOffset = offset
	} else {
		prev := af.loadNode(af.newestOffset)
		prev.SetNext(&an)
	}

	af.newestOffset = offset

	an.lru = af.allocLru.Add(&an)
	node = &an
	return
}

// Converts an allocated node into a free node.
func (an *avlNodeS) Free() {
	af := an.tree
	if af.err != nil {
		return
	}

	// removing := an.offset
	// af.IterateByKeys(func(node avlNode) bool {
	// 	if offsetOf(node.Parent()) == removing ||
	// 	 offsetOf(node.Left()) == removing ||
	// 	 offsetOf(node.Right()) == removing {
	// 		panic("removing a referenced node")
	// 	 }
	// 	 return true
	// })

	//
	// Delink from time history and convert to a free node.
	//

	if an.prevOffset == 0 {
		af.oldestOffset = an.nextOffset
	} else {
		prev := an.Prev()
		prev.SetNext(an.Next())
	}

	if an.nextOffset == 0 {
		af.newestOffset = an.prevOffset
	} else {
		next := an.Next()
		next.SetPrev(an.Prev())
	}

	if an.dirty {
		// for the case of set then delete in the same transaction; should be rare
		for i, n := range af.writtenNodes {
			if n == an {
				af.writtenNodes = append(af.writtenNodes[:i], af.writtenNodes[i+1:]...)
				break
			}
		}
	}

	fn := &freeNodeS{
		tree:        an.tree,
		dirty:       true,
		originalRaw: an.originalRaw,
		offset:      an.offset,
		next:        nil,
		nextOffset:  af.firstFreeOffset,
	}
	af.freeCount++
	af.freeNodes[fn.offset] = fn
	fn.lru = af.freeLru.Add(fn)

	af.nodeCount--
	af.nodeCache[an.offset] = nil
	af.allocLru.Remove(an.lru)

	af.firstFreeOffset = fn.offset
	af.dirty = true
}

func (af *avlTreeS) loadNode(offset uint64) (node avlNode) {
	if af == nil {
		return nil
	}

	if af.err != nil {
		return af.zeroNode
	}

	if offset != 0 {
		an, exists := af.nodeCache[offset]
		if !exists {
			var err error
			an, err = af.readAvlNode(offset)
			if err != nil {
				af.err = err
				return af.zeroNode
			}
			af.nodeCache[offset] = an
			an.lru = af.allocLru.Add(an)
		} else if an == nil {
			panic("load of a node that was deleted")
		} else if an.lru == nil {
			panic("ejected from lru but still in nodeCache")
		}
		node = an
		node.touch()
	}

	return
}
