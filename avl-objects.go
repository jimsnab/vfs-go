package vfs

import (
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/spf13/afero"
)

type (
	avlTree interface {
		Find(key []byte) avlNode
		Set(key []byte, shard, position uint64) (node avlNode, added bool)
		Delete(key []byte) (deleted bool)
		IterateByKeys(iter AvlIterator)
		IterateByTimestamp(iter AvlIterator)
		NodeCount() uint64
		FreeCount() uint64
		Close()

		lock() error
		unlock()
		lastError() error
		flush() error
		getRoot() avlNode
		setRoot(root avlNode)
		setOldest(node avlNode)
		getOldest() avlNode
		setNewest(node avlNode)
		getNewest() avlNode
		isValid() bool

		countEach() int
		printTree(header string)
		printTreeValues(enable bool)
	}

	AvlIterator func(node avlNode) bool

	avlTreeS struct {
		mu              sync.Mutex
		err             error
		f               afero.File
		rf              afero.File
		dirty           bool
		originalRaw     []byte
		rootOffset      uint64
		firstFreeOffset uint64
		committedSize   uint64
		oldestOffset    uint64
		newestOffset    uint64
		syncEnabled     bool
		writtenNodes    []*avlNodeS
		nodeCache       map[uint64]*avlNodeS
		allocLru        *lruStack[*avlNodeS]
		nodeCount       uint64
		freeNodes       map[uint64]*freeNodeS
		freeLru         *lruStack[*freeNodeS]
		freeCount       uint64
		zeroNode        *avlNodeS
		printValues     bool
	}

	avlNode interface {
		Offset() uint64
		Left() avlNode
		SetLeft(parent avlNode)
		Right() avlNode
		SetRight(parent avlNode)
		Parent() avlNode
		SetParent(parent avlNode)
		Key() []byte
		Shard() uint64
		Position() uint64
		SetValues(shard, position uint64)
		Balance() int
		SetBalance(balance int)
		AddBalance(delta int)
		Timestamp() int64
		SetTimestamp(int64)
		Prev() avlNode
		SetPrev(prev avlNode)
		Next() avlNode
		SetNext(prev avlNode)
		Free()
		CopyKeyAndValues(other avlNode)
		SwapTimestamp(other avlNode)

		adjustBalance(second avlNode, third avlNode, direction int)
		rotateLeft(middle avlNode) avlNode
		rotateRight(middle avlNode) avlNode
		deleteRotateLeft(middle avlNode) (out avlNode, rebalanced bool)
		deleteRotateRight(middle avlNode) (out avlNode, rebalanced bool)
		iterateNext(iter AvlIterator) bool
		touch()
		dump(prefix string)
	}

	avlNodeS struct {
		tree         *avlTreeS
		offset       uint64
		dirty        bool
		originalRaw  []byte
		lru          *lruStackElement[*avlNodeS]
		balance      int
		key          []byte
		shard        uint64
		position     uint64
		leftOffset   uint64
		rightOffset  uint64
		parentOffset uint64
		timestamp    int64
		prevOffset   uint64
		nextOffset   uint64
	}

	freeNode interface {
		Offset() uint64
		Next() freeNode
		NextOffset() uint64
		SetNext(next freeNode)
	}

	freeNodeS struct {
		tree        *avlTreeS
		dirty       bool
		lru         *lruStackElement[*freeNodeS]
		originalRaw []byte
		offset      uint64
		next        freeNode
		nextOffset  uint64
	}
)

const kAllocCacheSize = int(1e5)
const kFreeCacheSize = 1024
const kTransactionAverageSize = 256

func newAvlTree(cfg *IndexConfig) (tree avlTree, err error) {
	filePath := path.Join(cfg.DataDir, fmt.Sprintf("%s.dt1", cfg.BaseName))
	f, err := AppFs.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("error opening index file %s: %v", filePath, err)
		return
	}

	filePath = path.Join(cfg.DataDir, fmt.Sprintf("%s.dt2", cfg.BaseName))
	rf, err := AppFs.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		f.Close()
		err = fmt.Errorf("error opening index recovery file %s: %v", filePath, err)
		return
	}

	acs := kAllocCacheSize
	if cfg.CacheSize != 0 {
		acs = cfg.CacheSize
	}

	af := avlTreeS{
		f:            f,
		rf:           rf,
		writtenNodes: make([]*avlNodeS, 0, kTransactionAverageSize),
		nodeCache:    make(map[uint64]*avlNodeS, acs),
		freeNodes:    make(map[uint64]*freeNodeS, kFreeCacheSize),
		syncEnabled:  cfg.Sync,
	}
	af.allocLru = newLruStack[*avlNodeS](kAllocCacheSize, af.collectNode)
	af.freeLru = newLruStack[*freeNodeS](kFreeCacheSize, af.collectFreeNode)
	af.zeroNode = &avlNodeS{tree: &af}

	err = func() (err error) {
		// recover from interrupted operations
		if err = af.recover(); err != nil {
			return
		}

		// load the header
		if err = af.readAvlHeader(); err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		f.Close()
		rf.Close()
		return
	}

	tree = &af
	return
}

func (af *avlTreeS) Close() {
	af.f.Close()
	af.rf.Close()
	if af.err == nil {
		af.err = os.ErrClosed
	}
}

func (af *avlTreeS) collectNode(an *avlNodeS) bool {
	if !an.dirty {
		delete(af.nodeCache, an.offset)
		return true
	}

	return false
}

func (af *avlTreeS) collectFreeNode(fn *freeNodeS) bool {
	if !fn.dirty {
		delete(af.nodeCache, fn.offset)
		return true
	}

	return false
}

func (af *avlTreeS) NodeCount() uint64 {
	return af.nodeCount
}

func (af *avlTreeS) FreeCount() uint64 {
	return af.freeCount
}

func (an *avlNodeS) nodeDirty() {
	if !an.dirty {
		an.dirty = true
		an.tree.writtenNodes = append(an.tree.writtenNodes, an)
	}
}

func (an *avlNodeS) Balance() int {
	return an.balance
}

func (an *avlNodeS) SetBalance(balance int) {
	an.nodeDirty()
	an.balance = balance
}

func (an *avlNodeS) AddBalance(delta int) {
	an.nodeDirty()
	an.balance += delta
}

func (an *avlNodeS) Key() []byte {
	return an.key
}

func (an *avlNodeS) Shard() uint64 {
	return an.shard
}

func (an *avlNodeS) Position() uint64 {
	return an.position
}

func (an *avlNodeS) SetValues(shard, position uint64) {
	an.nodeDirty()
	an.shard = shard
	an.position = position
}

func (an *avlNodeS) CopyKeyAndValues(bn avlNode) {
	an.nodeDirty()
	copy(an.key, bn.Key())
	an.shard = bn.Shard()
	an.position = bn.Position()
}

func (an *avlNodeS) SetTimestamp(ts int64) {
	an.nodeDirty()
	an.timestamp = ts
}

func (an *avlNodeS) SwapTimestamp(bn avlNode) {
	ts := an.timestamp
	an.nodeDirty()
	an.timestamp = bn.Timestamp()
	bn.SetTimestamp(ts)

	tree := an.tree

	offsetA := an.Offset()
	offsetB := bn.Offset()

	// saveprev = B.prev or B if B.prev=A, B.prev set to A.prev, A.prev.next point to B
	saveprev := bn.Prev()
	if offsetOf(saveprev) == offsetA {
		saveprev = bn
	}
	link := an.Prev()
	bn.SetPrev(link)
	if link == nil {
		tree.setOldest(bn)
	} else {
		link.SetNext(bn)
	}

	// savenext = A.next or A if A.next==B, A.next set to B.next, B.next.prev point to A
	savenext := an.Next()
	if offsetOf(savenext) == offsetB {
		savenext = an
	}
	link = bn.Next()
	an.SetNext(link)
	if link == nil {
		tree.setNewest(an)
	} else {
		link.SetPrev(an)
	}

	// set B.next to savenext, savenext.prev to B
	bn.SetNext(savenext)
	if savenext == nil {
		tree.setNewest(an)
	} else {
		savenext.SetPrev(bn)
	}

	// set A.prev to saveprev, saveprev.next to A
	an.SetPrev(saveprev)
	if saveprev == nil {
		tree.setOldest(an)
	} else {
		saveprev.SetNext(an)
	}
}

func (an *avlNodeS) Timestamp() int64 {
	return an.timestamp
}

func (af *avlTreeS) lock() error {
	af.mu.Lock()
	if af.err != nil {
		err := af.err
		af.mu.Unlock()
		return err
	}

	return nil
}

func (af *avlTreeS) unlock() {
	af.mu.Unlock()
}

func (af *avlTreeS) lastError() error {
	return af.err
}

func (af *avlTreeS) getRoot() avlNode {
	return af.loadNode(af.rootOffset)
}

func (af *avlTreeS) setRoot(root avlNode) {
	af.dirty = true
	af.rootOffset = offsetOf(root)
}

func (af *avlTreeS) getOldest() avlNode {
	return af.loadNode(af.oldestOffset)
}
func (af *avlTreeS) setOldest(node avlNode) {
	af.dirty = true
	af.oldestOffset = offsetOf(node)
}

func (af *avlTreeS) getNewest() avlNode {
	return af.loadNode(af.newestOffset)
}

func (af *avlTreeS) setNewest(node avlNode) {
	af.dirty = true
	af.newestOffset = offsetOf(node)
}

func (an *avlNodeS) Offset() uint64 {
	return an.offset
}

func (an *avlNodeS) Left() avlNode {
	if an == nil {
		return nil
	}
	return an.tree.loadNode(an.leftOffset)
}

func (an *avlNodeS) SetLeft(left avlNode) {
	an.nodeDirty()
	an.leftOffset = offsetOf(left)
}

func (an *avlNodeS) Right() avlNode {
	if an == nil {
		return nil
	}

	return an.tree.loadNode(an.rightOffset)
}

func (an *avlNodeS) SetRight(right avlNode) {
	an.nodeDirty()
	an.rightOffset = offsetOf(right)
}

func (an *avlNodeS) Parent() avlNode {
	if an == nil {
		return nil
	}
	return an.tree.loadNode(an.parentOffset)
}

func (an *avlNodeS) SetParent(parent avlNode) {
	an.nodeDirty()
	an.parentOffset = offsetOf(parent)
}

func (an *avlNodeS) Next() avlNode {
	if an == nil {
		return nil
	}
	return an.tree.loadNode(an.nextOffset)
}

func (an *avlNodeS) SetNext(next avlNode) {
	an.nodeDirty()
	an.nextOffset = offsetOf(next)
}

func (an *avlNodeS) Prev() avlNode {
	if an == nil {
		return nil
	}
	return an.tree.loadNode(an.prevOffset)
}

func (an *avlNodeS) SetPrev(prev avlNode) {
	an.nodeDirty()
	an.prevOffset = offsetOf(prev)
}

func (an *avlNodeS) touch() {
	an.tree.allocLru.Promote(an.lru)
}

func (an *avlNodeS) dump(prefix string) {
	fmt.Printf("%s: %d parent=%d left=%d right=%d\n", prefix, an.timestamp, an.parentOffset, an.leftOffset, an.rightOffset)
}

func offsetOf(node avlNode) uint64 {
	if node == nil {
		return 0
	}
	return node.Offset()
}

func (fn *freeNodeS) Offset() uint64 {
	return fn.offset
}

func (fn *freeNodeS) Next() freeNode {
	if fn.tree.err != nil {
		return &freeNodeS{}
	}

	if fn.next == nil {
		fn.next = fn.tree.loadFreeNode(fn.nextOffset)
	}
	return fn.next
}

func (fn *freeNodeS) NextOffset() uint64 {
	return fn.nextOffset
}

func (fn *freeNodeS) SetNext(next freeNode) {
	fn.dirty = true
	fn.next = next
	fn.nextOffset = next.Offset()
}

func (fn *freeNodeS) touch() {
	fn.tree.freeLru.Promote(fn.lru)
}
