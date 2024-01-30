package vfs

import (
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/spf13/afero"
)

type (
	AvlIterator func(node *avlNode) bool

	avlTree struct {
		mu              sync.Mutex
		cfg             *VfsConfig
		err             atomic.Pointer[error]
		f               afero.File
		rf              afero.File
		dirty           bool
		originalRaw     []byte
		rootOffset      uint64
		firstFreeOffset uint64
		committedSize   uint64
		oldestOffset    uint64
		newestOffset    uint64
		writtenNodes    []*avlNode
		nodeCache       map[uint64]*avlNode
		allocLru        *lruStack[*avlNode]
		nodeCount       uint64
		freeNodes       map[uint64]*freeNodeS
		freeCount       uint64
		setCount        uint64
		deleteCount     uint64
		zeroNode        *avlNode
		printValues     bool
		dt1sync         sync.WaitGroup
		dt2sync         sync.WaitGroup
		nextByTime      uint64
		keyIteration    bool
	}

	avlNode struct {
		tree         *avlTree
		offset       uint64
		dirty        bool
		originalRaw  []byte
		lru          *lruStackElement[*avlNode]
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

	freeNodeS struct {
		tree        *avlTree
		dirty       bool
		originalRaw []byte
		offset      uint64
		next        *freeNodeS
		nextOffset  uint64
	}

	avlTreeStats struct {
		Sets    uint64
		Deletes uint64
	}
)

const kAllocCacheSize = int(1e5)
const kFreeCacheSize = 1024
const kTransactionAverageSize = 256

func newAvlTree(cfg *VfsConfig) (tree *avlTree, err error) {
	filePath := path.Join(cfg.IndexDir, fmt.Sprintf("%s.dt1", cfg.BaseName))
	f, err := AppFs.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("error opening index file %s: %v", filePath, err)
		return
	}

	filePath = path.Join(cfg.IndexDir, fmt.Sprintf("%s.dt2", cfg.BaseName))
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

	af := avlTree{
		f:            f,
		rf:           rf,
		writtenNodes: make([]*avlNode, 0, kTransactionAverageSize),
		nodeCache:    make(map[uint64]*avlNode, acs),
		freeNodes:    make(map[uint64]*freeNodeS, kFreeCacheSize),
		cfg:          cfg,
	}
	af.allocLru = newLruStack[*avlNode](acs, af.collectNode)
	af.zeroNode = &avlNode{tree: &af}

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

func (af *avlTree) Close() {
	af.f.Close()
	af.rf.Close()
	if af.lastError() == nil {
		af.err.Store(&os.ErrClosed)
	}
}

func (af *avlTree) collectNode(an *avlNode) bool {
	if !an.dirty {
		p := af.nodeCache[an.offset]
		if p != nil {
			delete(af.nodeCache, an.offset)
		}
		return true
	}

	return false
}

func (af *avlTree) NodeCount() uint64 {
	return af.nodeCount
}

func (af *avlTree) FreeCount() uint64 {
	return af.freeCount
}

func (af *avlTree) Stats() avlTreeStats {
	return avlTreeStats{
		Sets:    af.setCount,
		Deletes: af.deleteCount,
	}
}

func (an *avlNode) nodeDirty() {
	if !an.dirty {
		an.dirty = true
		an.tree.writtenNodes = append(an.tree.writtenNodes, an)
	}
}

func (an *avlNode) Balance() int {
	return an.balance
}

func (an *avlNode) SetBalance(balance int) {
	an.nodeDirty()
	an.balance = balance
}

func (an *avlNode) AddBalance(delta int) {
	an.nodeDirty()
	an.balance += delta
}

func (an *avlNode) Key() []byte {
	return an.key
}

func (an *avlNode) Shard() uint64 {
	return an.shard
}

func (an *avlNode) Position() uint64 {
	return an.position
}

func (an *avlNode) SetValues(shard, position uint64) {
	an.nodeDirty()
	an.shard = shard
	an.position = position
}

func (an *avlNode) CopyKeyAndValues(bn *avlNode) {
	an.nodeDirty()
	copy(an.key, bn.Key())
	an.shard = bn.Shard()
	an.position = bn.Position()
}

func (an *avlNode) SetTimestamp(ts int64) {
	an.nodeDirty()
	an.timestamp = ts
}

func (an *avlNode) SwapTimestamp(bn *avlNode) {
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

func (an *avlNode) Timestamp() int64 {
	return an.timestamp
}

func (af *avlTree) lock() error {
	af.mu.Lock()
	err := af.lastError()
	if err != nil {
		af.mu.Unlock()
		return err
	}

	return nil
}

func (af *avlTree) unlock() {
	af.mu.Unlock()
}

func (af *avlTree) lastError() error {
	perr := af.err.Load()
	if perr != nil {
		return *perr
	}
	return nil
}

func (af *avlTree) getRoot() *avlNode {
	return af.loadNode(af.rootOffset)
}

func (af *avlTree) setRoot(root *avlNode) {
	af.dirty = true
	af.rootOffset = offsetOf(root)
}

func (af *avlTree) getOldest() *avlNode {
	return af.loadNode(af.oldestOffset)
}
func (af *avlTree) setOldest(node *avlNode) {
	af.dirty = true
	af.oldestOffset = offsetOf(node)
}

func (af *avlTree) getNewest() *avlNode {
	return af.loadNode(af.newestOffset)
}

func (af *avlTree) setNewest(node *avlNode) {
	af.dirty = true
	af.newestOffset = offsetOf(node)
}

func (an *avlNode) Offset() uint64 {
	return an.offset
}

func (an *avlNode) Left() *avlNode {
	if an == nil {
		return nil
	}
	return an.tree.loadNode(an.leftOffset)
}

func (an *avlNode) SetLeft(left *avlNode) {
	an.nodeDirty()
	an.leftOffset = offsetOf(left)
}

func (an *avlNode) Right() *avlNode {
	if an == nil {
		return nil
	}

	return an.tree.loadNode(an.rightOffset)
}

func (an *avlNode) SetRight(right *avlNode) {
	an.nodeDirty()
	an.rightOffset = offsetOf(right)
}

func (an *avlNode) Parent() *avlNode {
	if an == nil {
		return nil
	}
	return an.tree.loadNode(an.parentOffset)
}

func (an *avlNode) SetParent(parent *avlNode) {
	an.nodeDirty()
	an.parentOffset = offsetOf(parent)
}

func (an *avlNode) Next() *avlNode {
	if an == nil {
		return nil
	}
	return an.tree.loadNode(an.nextOffset)
}

func (an *avlNode) SetNext(next *avlNode) {
	an.nodeDirty()
	an.nextOffset = offsetOf(next)
}

func (an *avlNode) Prev() *avlNode {
	if an == nil {
		return nil
	}
	return an.tree.loadNode(an.prevOffset)
}

func (an *avlNode) SetPrev(prev *avlNode) {
	an.nodeDirty()
	an.prevOffset = offsetOf(prev)
}

func (an *avlNode) touch() {
	an.tree.allocLru.Promote(an.lru)
}

func (an *avlNode) dump(prefix string) {
	fmt.Printf("%s: %d parent=%d left=%d right=%d\n", prefix, an.timestamp, an.parentOffset, an.leftOffset, an.rightOffset)
}

func offsetOf(node *avlNode) uint64 {
	if node == nil {
		return 0
	}
	return node.Offset()
}

func (fn *freeNodeS) Offset() uint64 {
	return fn.offset
}

func (fn *freeNodeS) Next() *freeNodeS {
	if fn.tree.err.Load() != nil {
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

func (fn *freeNodeS) SetNext(next *freeNodeS) {
	fn.dirty = true
	fn.next = next
	fn.nextOffset = next.Offset()
}
