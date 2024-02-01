package vfs

import (
	"time"
)

func (tree *avlTree) loadFreeNode(offset uint64) (node *freeNode, err error) {
	fn, exists := tree.freeNodes[offset]
	if !exists {
		if fn, err = tree.readFreeNode(offset); err != nil {
			return
		}

		tree.freeNodes[offset] = fn
	} else if fn == nil {
		panic("reclaiming a free node that was already reclaimed")
	}

	node = fn
	return
}

func (tree *avlTree) alloc(key []byte, shard, position uint64) (node *avlNode, err error) {
	an := avlNode{
		tree: tree,
		key:  make([]byte, len(key)),
	}

	//
	// Find some space.
	//

	offset := tree.firstFreeOffset
	if offset != 0 {
		// reclaim a deleted node
		var reclaimed *freeNode
		if reclaimed, err = tree.loadFreeNode(offset); err != nil {
			return
		}
		tree.firstFreeOffset = reclaimed.NextOffset()
		tree.dirty = true
		an.originalRawNode = reclaimed.originalRawFree

		delete(tree.freeNodes, offset)
		tree.freeCount.Add(-1)
		reclaimed.dirty = false
	} else {
		// allocate a new node at the end of the file
		offset = tree.allocatedSize
		tree.allocatedSize += kRecordSize
	}
	tree.nodeCount.Add(1)

	//
	// Initialize the new node.
	//

	tree.nodeCache[offset] = &an
	an.nodeDirty()
	an.offset = offset
	copy(an.key, key)
	an.shard = shard
	an.position = position
	an.timestamp = time.Now().UTC().UnixNano()

	//
	// Link the new node in time history.
	//

	an.prevOffset = tree.newestOffset

	if tree.newestOffset == 0 {
		tree.oldestOffset = offset
	} else {
		var prev *avlNode
		if prev, err = tree.loadNode(tree.newestOffset); err != nil {
			return
		}
		prev.SetNextOffset(offset)
	}

	tree.newestOffset = offset

	an.lru = tree.allocLru.Add(&an)
	node = &an
	return
}

// Converts an allocated node into a free node.
func (an *avlNode) Free() (err error) {
	tree := an.tree

	//
	// Delink from time history and convert to a free node.
	//

	if an.prevOffset == 0 {
		tree.oldestOffset = an.nextOffset
	} else {
		var prev *avlNode
		if prev, err = tree.loadNode(an.PrevOffset()); err != nil {
			return
		}
		prev.SetNextOffset(an.NextOffset())
	}

	if an.nextOffset == 0 {
		tree.newestOffset = an.prevOffset
	} else {
		var next *avlNode
		if next, err = tree.loadNode(an.NextOffset()); err != nil {
			return
		}
		next.SetPrevOffset(an.PrevOffset())
	}

	if an.dirty {
		// for the case of set then delete in the same transaction; should be rare
		for i, n := range tree.writtenNodes {
			if n == an {
				tree.writtenNodes = append(tree.writtenNodes[:i], tree.writtenNodes[i+1:]...)
				break
			}
		}
	}

	fn := &freeNode{
		tree:            an.tree,
		dirty:           true,
		originalRawFree: an.originalRawNode,
		offset:          an.offset,
		next:            nil,
		nextOffset:      tree.firstFreeOffset,
	}
	tree.freeCount.Add(1)
	tree.freeNodes[fn.offset] = fn

	tree.nodeCount.Add(-1)
	delete(tree.nodeCache, an.offset)
	an.dirty = false
	tree.allocLru.Remove(an.lru)

	tree.firstFreeOffset = fn.offset
	tree.dirty = true
	return
}

func (tree *avlTree) loadNode(offset uint64) (node *avlNode, err error) {
	if offset != 0 {
		an, exists := tree.nodeCache[offset]
		if !exists {
			an, err = tree.readAvlNode(offset)
			if err != nil {
				return
			}
			tree.nodeCache[offset] = an
			an.lru = tree.allocLru.Add(an)
		} else if an == nil {
			panic("load of a node that was deleted")
		} else if an.lru == nil {
			panic("ejected from lru but still in nodeCache")
		} else if an.lru.discarded {
			panic("ejected from lru but still holding reference")
		}

		node = an
		node.touch()
	}

	return
}
