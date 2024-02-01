package vfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type (
	// see kRecordSize below, which is the size of this structure in bytes
	diskAvlNode struct {
		Rt        diskRecordType
		Balance   int8
		Key       [20]byte
		Shard     uint64
		Position  uint64
		Left      uint64
		Right     uint64
		Parent    uint64
		Timestamp int64
		Prev      uint64
		Next      uint64
	}

	diskHeader struct {
		Version         uint64
		FirstFreeOffset uint64
		CommittedSize   uint64
		RootOffset      uint64
		Oldest          uint64
		Newest          uint64
		NodeCount       uint64
		FreeCount       uint64
		SetCount        uint64
		DeleteCount     uint64
		Unused          [kRecordSize - kHeaderSize]byte // sized to match diskAvlNode
	}

	diskFreeNode struct {
		Rt     diskRecordType
		Next   uint64
		Unused [kRecordSize - kFreeNodeSize]byte // sized to match diskAvlNode
	}

	diskRecordType byte
)

// size of diskAvlNode (the biggest disk record)
const kHeaderSize = (8 * 10)
const kFreeNodeSize = 1 + 8
const kRecordSize = 1 + 1 + 20 + (8 * 8)

// expose the on-disk size of the index header and index record for external tests
var IndexHeaderSize = kRecordSize
var IndexRecordSize = kRecordSize

const (
	rtAvlNode diskRecordType = iota
	rtFreeNode
	rtRecoveryNode
)

func (tree *avlTree) headerFromRaw(raw []byte, hdr *diskHeader) (err error) {
	r := bytes.NewReader(raw)
	return binary.Read(r, binary.BigEndian, hdr)
}

func (tree *avlTree) readAvlHeader() (err error) {
	raw := make([]byte, kRecordSize)
	n, terr := tree.f.ReadAt(raw, 0)
	if terr != nil {
		if !errors.Is(terr, io.EOF) {
			err = terr
			return
		}

		newHdr := diskHeader{
			Version:       1,
			CommittedSize: kRecordSize,
		}
		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, &newHdr)
		raw = buf.Bytes()

		tree.dirty = true

		if _, err = tree.f.WriteAt(raw, 0); err != nil {
			return
		}
	} else {
		if n != kRecordSize {
			err = errors.New("insufficient header bytes")
			return
		}
	}

	var hdr diskHeader
	_ = hdr.Unused // this is to work around an absolutely stupid Go team decision
	if err = tree.headerFromRaw(raw, &hdr); err != nil {
		return
	}

	tree.originalRawHdr = raw

	if hdr.Version != 1 {
		err = fmt.Errorf("unsupported version %d", hdr.Version)
		return
	}

	tree.firstFreeOffset = hdr.FirstFreeOffset
	tree.committedSize = hdr.CommittedSize
	tree.rootOffset = hdr.RootOffset
	tree.oldestOffset = hdr.Oldest
	tree.newestOffset = hdr.Newest
	tree.nodeCount = hdr.NodeCount
	tree.freeCount = hdr.FreeCount
	tree.setCount = hdr.SetCount
	tree.deleteCount = hdr.DeleteCount

	totalRecords := 1 + tree.nodeCount + tree.freeCount
	totalSize := totalRecords * kRecordSize
	if tree.committedSize != totalSize {
		err = fmt.Errorf("committed size %d != size of records %d", tree.committedSize, totalSize)
		return
	}

	return
}

func (tree *avlTree) write() (err error) {
	if !tree.dirty {
		panic("hdr dirty flag must be set")
	}

	hdr := diskHeader{
		Version:         1,
		FirstFreeOffset: tree.firstFreeOffset,
		CommittedSize:   tree.committedSize,
		RootOffset:      tree.rootOffset,
		Oldest:          tree.oldestOffset,
		Newest:          tree.newestOffset,
		NodeCount:       tree.nodeCount,
		FreeCount:       tree.freeCount,
		SetCount:        tree.setCount,
		DeleteCount:     tree.deleteCount,
	}

	var record bytes.Buffer
	if err = binary.Write(&record, binary.BigEndian, &hdr); err != nil {
		return
	}

	raw := record.Bytes()
	n, err := tree.f.WriteAt(raw, 0)
	if err != nil {
		return
	}
	if n != kRecordSize {
		err = errors.New("incomplete header write")
		return
	}

	tree.originalRawHdr = raw
	tree.dirty = false
	return
}

func (tree *avlTree) readAvlNode(offset uint64) (an *avlNode, err error) {
	raw := make([]byte, kRecordSize)
	n, err := tree.f.ReadAt(raw, int64(offset))
	if err != nil {
		return
	}
	if n != kRecordSize {
		err = errors.New("insufficient header bytes")
		return
	}

	var node diskAvlNode
	r := bytes.NewReader(raw)
	if err = binary.Read(r, binary.BigEndian, &node); err != nil {
		return
	}

	// sanity check
	if node.Rt != rtAvlNode {
		err = fmt.Errorf("invalid node type %d at offset %d", node.Rt, offset)
		return
	}

	an = &avlNode{
		tree:            tree,
		offset:          offset,
		originalRawNode: raw,
		balance:         int(node.Balance),
		key:             node.Key[:],
		shard:           node.Shard,
		position:        node.Position,
		leftOffset:      node.Left,
		rightOffset:     node.Right,
		parentOffset:    node.Parent,
		timestamp:       node.Timestamp,
		prevOffset:      node.Prev,
		nextOffset:      node.Next,
	}
	return
}

func (an *avlNode) write() (err error) {
	if !an.dirty {
		panic("node dirty flag must be set")
	}

	node := diskAvlNode{
		Rt:        rtAvlNode,
		Balance:   int8(an.balance),
		Shard:     an.shard,
		Position:  an.position,
		Left:      an.leftOffset,
		Right:     an.rightOffset,
		Parent:    an.parentOffset,
		Timestamp: an.timestamp,
		Prev:      an.prevOffset,
		Next:      an.nextOffset,
	}
	copy(node.Key[:], an.key)

	var record bytes.Buffer
	if err = binary.Write(&record, binary.BigEndian, &node); err != nil {
		return
	}

	raw := record.Bytes()
	n, err := an.tree.f.WriteAt(raw, int64(an.offset))
	if err != nil {
		return
	}
	if n != kRecordSize {
		err = errors.New("incomplete node write")
		return
	}

	an.originalRawNode = raw
	an.dirty = false
	return
}

func (tree *avlTree) readFreeNode(offset uint64) (fn *freeNode, err error) {
	raw := make([]byte, kRecordSize)
	n, err := tree.f.ReadAt(raw, int64(offset))
	if err != nil {
		return
	}
	if n != kRecordSize {
		err = errors.New("insufficient header bytes")
		return
	}

	var node diskFreeNode
	_ = node.Unused // this is to work around an absolutely stupid Go team decision

	r := bytes.NewReader(raw)
	if err = binary.Read(r, binary.BigEndian, &node); err != nil {
		return
	}

	// sanity check
	if node.Rt != rtFreeNode {
		err = fmt.Errorf("invalid free node type %d at offset %d", node.Rt, offset)
		return
	}

	fn = &freeNode{
		tree:            tree,
		nextOffset:      node.Next,
		offset:          offset,
		originalRawFree: raw,
	}
	return
}

func (fn *freeNode) write() (err error) {
	if !fn.dirty {
		panic("node dirty flag must be set")
	}

	node := diskFreeNode{
		Rt:   rtFreeNode,
		Next: fn.nextOffset,
	}

	var record bytes.Buffer
	if err = binary.Write(&record, binary.BigEndian, &node); err != nil {
		return
	}

	raw := record.Bytes()
	n, err := fn.tree.f.WriteAt(raw, int64(fn.offset))
	if err != nil {
		return
	}
	if n != kRecordSize {
		err = errors.New("incomplete free node write")
		return
	}

	fn.originalRawFree = raw
	fn.dirty = false
	return
}
