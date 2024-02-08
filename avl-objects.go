package vfs

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/jimsnab/afero"
)

type (
	avlIterator func(node *avlNode) error

	avlTree struct {
		cfg             *VfsConfig
		f               afero.File
		rf              afero.File
		dirty           bool
		originalRawHdr  []byte
		rootOffset      uint64
		firstFreeOffset uint64
		committedSize   uint64
		allocatedSize   uint64
		oldestOffset    uint64
		newestOffset    uint64
		writtenNodes    []*avlNode
		nodeCache       map[uint64]*avlNode
		allocLru        *lruStack[*avlNode]
		nodeCount       atomic.Int64
		freeNodes       map[uint64]*freeNode
		freeCount       atomic.Int64
		setCount        atomic.Uint64
		deleteCount     atomic.Uint64
		printType       testPrintType
		dt1sync         sync.WaitGroup
		dt2sync         sync.WaitGroup
		nextByTime      uint64
		keyIteration    bool
		closed          bool
	}

	avlNode struct {
		tree            *avlTree
		offset          uint64
		dirty           bool
		originalRawNode []byte
		lru             *lruStackElement[*avlNode]
		balance         int
		key             []byte
		shard           uint64
		position        uint64
		leftOffset      uint64
		rightOffset     uint64
		parentOffset    uint64
		timestamp       int64
		prevOffset      uint64
		nextOffset      uint64
	}

	freeNode struct {
		tree            *avlTree
		dirty           bool
		originalRawFree []byte
		offset          uint64
		next            *freeNode
		nextOffset      uint64
	}

	avlTreeStats struct {
		NodeCount uint64
		FreeCount uint64
		Sets      uint64
		Deletes   uint64
	}

	testPrintType int
)

var ErrIteratorAbort = errors.New("iteration aborted")
var ErrDamagedIndex = errors.New("damaged index")

const kAllocCacheSize = int(1e5)
const kFreeCacheSize = 1024
const kTransactionAverageSize = 256

const (
	testPtBalance testPrintType = iota
	testPtKey
	testPtPosition
)

func newAvlTree(cfg *VfsConfig, keyGroup, extension string) (tree *avlTree, err error) {
	filePath := path.Join(cfg.IndexDir, fmt.Sprintf("%s.%s.%s", cfg.BaseName, keyGroup, extension))
	f, err := createOrOpenFile(filePath, false)
	if err != nil {
		err = fmt.Errorf("error opening index file %s: %v", filePath, err)
		return
	}

	if err = f.Sync(); err != nil {
		return
	}

	end := len(extension) - 1
	extension2 := extension[:end] + string(extension[end]+1)

	filePath = path.Join(cfg.IndexDir, fmt.Sprintf("%s.%s.%s", cfg.BaseName, keyGroup, extension2))
	rf, err := createOrOpenFile(filePath, false)
	if err != nil {
		f.Close()
		err = fmt.Errorf("error opening index recovery file %s: %v", filePath, err)
		return
	}

	if err = rf.Sync(); err != nil {
		f.Close()
		rf.Close()
		return
	}

	acs := kAllocCacheSize
	if cfg.CacheSize != 0 {
		acs = cfg.CacheSize
	}

	at := avlTree{
		f:            f,
		rf:           rf,
		writtenNodes: make([]*avlNode, 0, kTransactionAverageSize),
		nodeCache:    make(map[uint64]*avlNode, acs),
		freeNodes:    make(map[uint64]*freeNode, kFreeCacheSize),
		cfg:          cfg,
	}
	at.allocLru = newLruStack[*avlNode](acs, at.collectNode)

	err = func() (err error) {
		// recover from interrupted operations
		if err = at.recover(); err != nil {
			return
		}

		// load the header
		if err = at.readAvlHeader(); err != nil {
			return err
		}

		at.allocatedSize = at.committedSize

		return nil
	}()
	if err != nil {
		f.Close()
		rf.Close()
		return
	}

	tree = &at
	return
}

func (tree *avlTree) Sync() (err error) {
	if tree.closed {
		err = os.ErrClosed
		return
	}

	err1 := tree.f.Sync()
	err2 := tree.rf.Sync()

	if err1 != nil {
		return err1
	}
	return err2
}

func (tree *avlTree) Close() (err error) {
	if tree.closed {
		err = os.ErrClosed
		return
	}

	tree.f.Close()
	tree.rf.Close()
	tree.closed = true
	return
}

func (tree *avlTree) collectNode(an *avlNode) bool {
	// stop "unused" warning for this development function
	_ = an.dump

	if !an.dirty {
		p := tree.nodeCache[an.offset]
		if p != nil {
			delete(tree.nodeCache, an.offset)
		}
		return true
	}

	return false
}

func (tree *avlTree) Stats() avlTreeStats {
	return avlTreeStats{
		NodeCount: uint64(tree.nodeCount.Load()),
		FreeCount: uint64(tree.freeCount.Load()),
		Sets:      tree.setCount.Load(),
		Deletes:   tree.deleteCount.Load(),
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

func (an *avlNode) SwapTimestamp(bn *avlNode) (err error) {
	ts := an.timestamp
	an.nodeDirty()
	an.timestamp = bn.Timestamp()
	bn.SetTimestamp(ts)

	tree := an.tree

	offsetA := an.Offset()
	offsetB := bn.Offset()

	// saveprev = B.prev or B if B.prev=A, B.prev set to A.prev, A.prev.next point to B
	saveprev, err := an.tree.loadNode(bn.PrevOffset())
	if err != nil {
		return
	}
	if offsetOf(saveprev) == offsetA {
		saveprev = bn
	}
	link, err := an.tree.loadNode(an.PrevOffset())
	if err != nil {
		return
	}
	bn.SetPrevOffset(offsetOf(link))
	if link == nil {
		tree.setOldestOffset(offsetOf(bn))
	} else {
		link.SetNextOffset(offsetOf(bn))
	}

	// savenext = A.next or A if A.next==B, A.next set to B.next, B.next.prev point to A
	savenext, err := an.tree.loadNode(an.NextOffset())
	if err != nil {
		return
	}
	if offsetOf(savenext) == offsetB {
		savenext = an
	}
	if link, err = an.tree.loadNode(bn.NextOffset()); err != nil {
		return
	}
	an.SetNextOffset(offsetOf(link))
	if link == nil {
		tree.setNewestOffset(offsetOf(an))
	} else {
		link.SetPrevOffset(offsetOf(an))
	}

	// set B.next to savenext, savenext.prev to B
	bn.SetNextOffset(offsetOf(savenext))
	if savenext == nil {
		tree.setNewestOffset(offsetOf(an))
	} else {
		savenext.SetPrevOffset(offsetOf(bn))
	}

	// set A.prev to saveprev, saveprev.next to A
	an.SetPrevOffset(offsetOf(saveprev))
	if saveprev == nil {
		tree.setOldestOffset(offsetOf(an))
	} else {
		saveprev.SetNextOffset(offsetOf(an))
	}

	return
}

func (an *avlNode) Timestamp() int64 {
	return an.timestamp
}

func (tree *avlTree) getRootOffset() uint64 {
	return tree.rootOffset
}

func (tree *avlTree) setRootOffset(rootOffset uint64) {
	tree.dirty = true
	tree.rootOffset = rootOffset
}

func (tree *avlTree) getOldestOffset() uint64 {
	return tree.oldestOffset
}
func (tree *avlTree) setOldestOffset(nodeOffset uint64) {
	tree.dirty = true
	tree.oldestOffset = nodeOffset
}

func (tree *avlTree) getNewestOffset() uint64 {
	return tree.newestOffset
}

func (tree *avlTree) setNewestOffset(nodeOffset uint64) {
	tree.dirty = true
	tree.newestOffset = nodeOffset
}

func (an *avlNode) Offset() uint64 {
	if an == nil {
		return 0
	}
	return an.offset
}

func (an *avlNode) LeftOffset() uint64 {
	if an == nil {
		return 0
	}
	return an.leftOffset
}

func (an *avlNode) SetLeftOffset(leftOffset uint64) {
	an.nodeDirty()
	an.leftOffset = leftOffset
}

func (an *avlNode) RightOffset() uint64 {
	if an == nil {
		return 0
	}
	return an.rightOffset
}

func (an *avlNode) SetRightOffset(rightOffset uint64) {
	an.nodeDirty()
	an.rightOffset = rightOffset
}

func (an *avlNode) ParentOffset() uint64 {
	if an == nil {
		return 0
	}
	return an.parentOffset
}

func (an *avlNode) SetParentOffset(parentOffset uint64) {
	an.nodeDirty()
	an.parentOffset = parentOffset
}

func (an *avlNode) NextOffset() uint64 {
	if an == nil {
		return 0
	}
	return an.nextOffset
}

func (an *avlNode) SetNextOffset(nextOffset uint64) {
	an.nodeDirty()
	an.nextOffset = nextOffset
}

func (an *avlNode) PrevOffset() uint64 {
	if an == nil {
		return 0
	}
	return an.prevOffset
}

func (an *avlNode) SetPrevOffset(prevOffset uint64) {
	an.nodeDirty()
	an.prevOffset = prevOffset
}

func (an *avlNode) touch() {
	an.tree.allocLru.Promote(an.lru)
}

func (an *avlNode) dump(prefix string) {
	fmt.Printf("%s: %d parent=%d left=%d right=%d\n", prefix, an.timestamp, an.parentOffset, an.leftOffset, an.rightOffset)
}

func (an *avlNode) String() string {
	return fmt.Sprintf(
		"key:%s shard:%d position:%d ts:%d parent:%d left:%d right:%d next:%d prev:%d",
		hex.EncodeToString(an.key),
		an.shard,
		an.position,
		an.timestamp,
		an.parentOffset,
		an.leftOffset,
		an.rightOffset,
		an.leftOffset,
		an.rightOffset,
	)
}

func offsetOf(node *avlNode) uint64 {
	if node == nil {
		return 0
	}
	return node.Offset()
}

func (fn *freeNode) Offset() uint64 {
	return fn.offset
}

func (fn *freeNode) NextOffset() uint64 {
	return fn.nextOffset
}

func (fn *freeNode) SetNext(next *freeNode) {
	fn.dirty = true
	fn.next = next
	fn.nextOffset = next.Offset()
}
