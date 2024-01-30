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
		Unused [kRecordSize - (8 + 8)]byte // sized to match diskAvlNode
	}

	diskRecordType byte
)

// size of diskAvlNode (the biggest disk record)
const kHeaderSize = (8 * 10)
const kRecordSize = 1 + 1 + 20 + (8 * 8)

const (
	rtAvlNode diskRecordType = iota
	rtFreeNode
	rtRecoveryNode
)

func (af *avlTree) readAvlHeader() (err error) {
	raw := make([]byte, kRecordSize)
	n, terr := af.f.ReadAt(raw, 0)
	if terr != nil {
		if !errors.Is(terr, io.EOF) {
			err = terr
			return
		}

		if _, err = af.f.WriteAt(raw, 0); err != nil {
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

	r := bytes.NewReader(raw)
	if err = binary.Read(r, binary.BigEndian, &hdr); err != nil {
		return
	}

	if terr != nil {
		hdr.Version = 1
		hdr.CommittedSize = kRecordSize
		af.dirty = true
	} else {
		af.originalRaw = raw
	}

	if hdr.Version != 1 {
		err = fmt.Errorf("unsupported version %d", hdr.Version)
		return
	}

	af.firstFreeOffset = hdr.FirstFreeOffset
	af.committedSize = hdr.CommittedSize
	af.rootOffset = hdr.RootOffset
	af.oldestOffset = hdr.Oldest
	af.newestOffset = hdr.Newest
	af.nodeCount = hdr.NodeCount
	af.freeCount = hdr.FreeCount
	af.setCount = hdr.SetCount
	af.deleteCount = hdr.DeleteCount

	totalRecords := 1 + af.nodeCount + af.freeCount
	totalSize := totalRecords * kRecordSize
	if af.committedSize != totalSize {
		err = fmt.Errorf("committed size %d != size of records %d", af.committedSize, totalSize)
		return
	}

	return
}

func (af *avlTree) write() (err error) {
	hdr := diskHeader{
		Version:         1,
		FirstFreeOffset: af.firstFreeOffset,
		CommittedSize:   af.committedSize,
		RootOffset:      af.rootOffset,
		Oldest:          af.oldestOffset,
		Newest:          af.newestOffset,
		NodeCount:       af.nodeCount,
		FreeCount:       af.freeCount,
		SetCount:        af.setCount,
		DeleteCount:     af.deleteCount,
	}

	if _, err = af.f.Seek(0, io.SeekStart); err != nil {
		return
	}

	return binary.Write(af.f, binary.BigEndian, &hdr)
}

func (af *avlTree) readAvlNode(offset uint64) (an *avlNode, err error) {
	raw := make([]byte, kRecordSize)
	n, err := af.f.ReadAt(raw, int64(offset))
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
		tree:         af,
		offset:       offset,
		originalRaw:  raw,
		balance:      int(node.Balance),
		key:          node.Key[:],
		shard:        node.Shard,
		position:     node.Position,
		leftOffset:   node.Left,
		rightOffset:  node.Right,
		parentOffset: node.Parent,
		timestamp:    node.Timestamp,
		prevOffset:   node.Prev,
		nextOffset:   node.Next,
	}
	return
}

func (an *avlNode) write() (err error) {
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

	if _, err = an.tree.f.Seek(int64(an.offset), io.SeekStart); err != nil {
		return
	}

	return binary.Write(an.tree.f, binary.BigEndian, &node)
}

func (af *avlTree) readFreeNode(offset uint64) (fn *freeNodeS, err error) {
	raw := make([]byte, kRecordSize)
	n, err := af.f.ReadAt(raw, int64(offset))
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

	fn = &freeNodeS{
		tree:        af,
		nextOffset:  node.Next,
		originalRaw: raw,
	}
	return
}

func (fn *freeNodeS) write() (err error) {
	node := diskFreeNode{
		Rt:   rtFreeNode,
		Next: fn.nextOffset,
	}

	if _, err = fn.tree.f.Seek(int64(fn.offset), io.SeekStart); err != nil {
		return
	}

	return binary.Write(fn.tree.f, binary.BigEndian, &node)
}
