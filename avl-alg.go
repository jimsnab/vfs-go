package vfs

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
)

type (
	avlOperation struct {
		tree     *avlTree
		key      [20]byte
		shard    uint64
		position uint64
		leaf     *avlNode
		added    bool
	}
)

// locates a key in the AVL tree
func (tree *avlTree) Find(key [20]byte) (node *avlNode, err error) {
	n, err := tree.loadNode(tree.getRootOffset())
	if err != nil {
		return
	}

	for {
		if n == nil {
			return
		}

		cmp := bytes.Compare(key[:], n.key[:])

		if cmp == 0 {
			node = n
			return
		}

		if cmp < 0 {
			if n, err = tree.loadNode(n.LeftOffset()); err != nil {
				return
			}
		} else {
			if n, err = tree.loadNode(n.RightOffset()); err != nil {
				return
			}
		}
	}
}

// adds a key to the AVL tree, or finds the existing node
func (tree *avlTree) Set(key [20]byte, shard, position uint64) (node *avlNode, added bool, err error) {
	op := &avlOperation{
		tree:     tree,
		key:      key,
		shard:    shard,
		position: position,
	}

	root, err := tree.loadNode(tree.getRootOffset())
	if err != nil {
		return
	}

	var rootOffset uint64
	if rootOffset, _, err = op.insertNode(0, root); err != nil {
		return
	}
	tree.setRootOffset(rootOffset)
	tree.setCount.Add(1)

	node = op.leaf
	added = op.added
	return
}

// removes a key from the AVL tree, returing true if the key was found and deleted
func (tree *avlTree) Delete(key [20]byte) (wasDeleted bool, err error) {
	delKey := [20]byte(key)

	if tree.keyIteration {
		panic("delete is not permitted during iteration by keys")
	}
	op := &avlOperation{
		tree: tree,
		key:  delKey,
	}

	root, err := tree.loadNode(tree.getRootOffset())
	if err != nil {
		return
	}

	var rootOffset uint64
	if rootOffset, _, err = op.deleteNode(root); err != nil {
		return
	}

	if op.leaf != nil {
		tree.setRootOffset(rootOffset)
		if err = op.leaf.Free(); err != nil {
			return
		}
		tree.deleteCount.Add(1)
		wasDeleted = true
	}

	return
}

// convenience wrapper to insert a node when caller hasn't yet loaded the node
func (op *avlOperation) loadAndInsertNode(parentOffset uint64, nodeOffset uint64) (outOffset uint64, balanced bool, err error) {
	node, err := op.tree.loadNode(nodeOffset)
	if err != nil {
		return
	}

	return op.insertNode(parentOffset, node)
}

// recursive worker that searches for the insertion position for a new node, and adds and rebalances the tree if key doesn't already exist
func (op *avlOperation) insertNode(parentOffset uint64, node *avlNode) (outOffset uint64, balanced bool, err error) {
	if node == nil {
		var out *avlNode
		if out, err = op.tree.alloc(op.key, op.shard, op.position); err != nil {
			return
		}
		out.SetParentOffset(parentOffset)
		outOffset = out.Offset()
		op.leaf = out
		op.added = true
		return
	}

	cmp := bytes.Compare(op.key[:], node.key[:])

	if cmp == 0 {
		op.leaf = node
		balanced = true
		node.SetValues(op.shard, op.position)
	} else {
		if cmp < 0 {
			var leftOffset uint64
			if leftOffset, balanced, err = op.loadAndInsertNode(offsetOf(node), node.LeftOffset()); err != nil {
				return
			}
			node.SetLeftOffset(leftOffset)

			if !balanced {
				node.AddBalance(-1)
				if node.Balance() < -1 {
					if node, err = node.loadAndRotateLeft(node.LeftOffset()); err != nil {
						return
					}
				}
				balanced = (node.Balance() == 0)
			}
		} else {
			var rightOffset uint64
			if rightOffset, balanced, err = op.loadAndInsertNode(offsetOf(node), node.RightOffset()); err != nil {
				return
			}
			node.SetRightOffset(rightOffset)

			if !balanced {
				node.AddBalance(1)
				if node.Balance() > 1 {
					if node, err = node.loadAndRotateRight(node.RightOffset()); err != nil {
						return
					}
				}
				balanced = (node.Balance() == 0)
			}
		}
	}

	outOffset = offsetOf(node)
	return
}

// convenience wrapper on deleteNode when caller only has the node offset
func (op *avlOperation) loadAndDeleteNode(nodeOffset uint64) (outOffset uint64, rebalanced bool, err error) {
	node, err := op.tree.loadNode(nodeOffset)
	if err != nil {
		return
	}

	return op.deleteNode(node)
}

// recursive worker that searches for a node, and if found, deletes and rebalances the tree
func (op *avlOperation) deleteNode(node *avlNode) (outOffset uint64, rebalanced bool, err error) {
	if node == nil {
		rebalanced = true
		return
	}

	cmp := bytes.Compare(node.key[:], op.key[:])

	if cmp == 0 {
		if node.Offset() == op.tree.nextByTime {
			op.tree.nextByTime = node.NextOffset()
		}

		op.leaf = node
		if node.LeftOffset() == 0 {
			outOffset = node.RightOffset()
			if outOffset != 0 {
				var out *avlNode
				if out, err = op.tree.loadNode(outOffset); err != nil {
					return
				}
				out.SetParentOffset(node.ParentOffset())
			}
			return
		}

		var left *avlNode
		if left, err = op.tree.loadNode(node.LeftOffset()); err != nil {
			return
		}

		if node.RightOffset() == 0 {
			outOffset = left.Offset()
			left.SetParentOffset(node.ParentOffset())
			return
		}

		replacement := left
		for replacement.RightOffset() != 0 {
			if replacement, err = op.tree.loadNode(replacement.RightOffset()); err != nil {
				return
			}
		}

		node.CopyKeyAndValues(replacement)
		node.SwapTimestamp(replacement)

		// key to delete now becomes the replacement - the original one further down in the tree
		if op.tree.nextByTime == node.Offset() {
			op.tree.nextByTime = replacement.Offset() // delete during iteration - fix up next pointer
		}
		copy(op.key[:], replacement.key[:])
		cmp = bytes.Compare(node.key[:], op.key[:])
	}

	if cmp >= 0 {
		var leftOffset uint64
		if leftOffset, rebalanced, err = op.loadAndDeleteNode(node.LeftOffset()); err != nil {
			return
		}
		node.SetLeftOffset(leftOffset)

		if !rebalanced {
			node.AddBalance(1)
			if node.Balance() > 1 {
				if node, rebalanced, err = node.loadAndDeleteRotateRight(node.RightOffset()); err != nil {
					return
				}
			} else {
				rebalanced = (node.Balance() != 0)
			}
		}
	} else {
		var rightOffset uint64
		if rightOffset, rebalanced, err = op.loadAndDeleteNode(node.RightOffset()); err != nil {
			return
		}
		node.SetRightOffset(rightOffset)

		if !rebalanced {
			node.AddBalance(-1)
			if node.Balance() < -1 {
				if node, rebalanced, err = node.loadAndDeleteRotateLeft(node.LeftOffset()); err != nil {
					return
				}
			} else {
				rebalanced = (node.Balance() != 0)
			}
		}
	}

	outOffset = offsetOf(node)
	return
}

// worker to update the balance factor
func (node *avlNode) adjustBalance(second *avlNode, third *avlNode, direction int) {
	switch third.Balance() {
	case 0:
		node.SetBalance(0)
		second.SetBalance(0)
	case direction:
		node.SetBalance(0)
		second.SetBalance(-direction)
	default:
		node.SetBalance(direction)
		second.SetBalance(0)
	}
	third.SetBalance(0)
}

// convenience wrapper for rotateLeft when caller has not yet loaded the middle node
func (node *avlNode) loadAndRotateLeft(middleOffset uint64) (rotated *avlNode, err error) {
	middle, err := node.tree.loadNode(middleOffset)
	if err != nil {
		return
	}

	return node.rotateLeft(middle)
}

// worker to balance the tree after left insertion makes the tree left heavy
func (node *avlNode) rotateLeft(middle *avlNode) (rotated *avlNode, err error) {
	nodeParentOffset := node.ParentOffset()
	if middle.Balance() < 0 {
		// left-left rotation
		subtreeCOffset := middle.RightOffset()
		middle.SetRightOffset(node.Offset())
		node.SetLeftOffset(subtreeCOffset)
		node.SetParentOffset(middle.Offset())
		middle.SetParentOffset(nodeParentOffset)
		node.SetBalance(0)
		middle.SetBalance(0)
		rotated = middle
	} else if middle.RightOffset() != 0 {
		// left-right rotation
		var third *avlNode
		if third, err = node.tree.loadNode(middle.RightOffset()); err != nil {
			return
		}

		node.adjustBalance(middle, third, 1)
		subtreeBOffset := third.LeftOffset()
		subtreeCOffset := third.RightOffset()
		third.SetLeftOffset(middle.Offset())
		third.SetRightOffset(node.Offset())
		middle.SetRightOffset(subtreeBOffset)
		node.SetLeftOffset(subtreeCOffset)
		node.SetParentOffset(third.Offset())
		middle.SetParentOffset(third.Offset())
		third.SetParentOffset(nodeParentOffset)
		rotated = third
	} else {
		rotated = node
	}

	return
}

// convenience wrapper for rotateRight when caller has not yet loaded the middle node
func (node *avlNode) loadAndRotateRight(middleOffset uint64) (rotated *avlNode, err error) {
	middle, err := node.tree.loadNode(middleOffset)
	if err != nil {
		return
	}

	return node.rotateRight(middle)
}

// worker to balance the tree after right insertion makes the tree right heavy
func (node *avlNode) rotateRight(middle *avlNode) (rotated *avlNode, err error) {
	nodeParentOffset := node.ParentOffset()
	if middle.Balance() > 0 {
		// right-right rotation
		subtreeBOffset := middle.LeftOffset()
		middle.SetLeftOffset(node.Offset())
		node.SetRightOffset(subtreeBOffset)
		node.SetParentOffset(middle.Offset())
		middle.SetParentOffset(nodeParentOffset)
		node.SetBalance(0)
		middle.SetBalance(0)
		rotated = middle
	} else if middle.LeftOffset() != 0 {
		// right-left rotation
		var third *avlNode
		if third, err = node.tree.loadNode(middle.LeftOffset()); err != nil {
			return
		}

		node.adjustBalance(middle, third, -1)
		subtreeBOffset := third.LeftOffset()
		subtreeCOffset := third.RightOffset()
		third.SetLeftOffset(node.Offset())
		third.SetRightOffset(middle.Offset())
		node.SetRightOffset(subtreeBOffset)
		middle.SetLeftOffset(subtreeCOffset)
		node.SetParentOffset(third.Offset())
		middle.SetParentOffset(third.Offset())
		third.SetParentOffset(nodeParentOffset)
		rotated = third
	} else {
		rotated = node
	}
	return
}

// convenience wrapper for deleteRotateLeft when caller hasn't loaded the node yet
func (node *avlNode) loadAndDeleteRotateLeft(middleOffset uint64) (rotated *avlNode, rebalanced bool, err error) {
	middle, err := node.tree.loadNode(middleOffset)
	if err != nil {
		return
	}

	return node.deleteRotateLeft(middle)
}

// worker to rotate after a right node deletion leaves the tree unbalanced
func (node *avlNode) deleteRotateLeft(middle *avlNode) (rotated *avlNode, rebalanced bool, err error) {
	if middle.Balance() == 0 {
		nodeParentOffset := node.ParentOffset()
		subtreeCOffset := middle.RightOffset()
		middle.SetRightOffset(node.Offset())
		node.SetLeftOffset(subtreeCOffset)
		node.SetParentOffset(middle.Offset())
		middle.SetParentOffset(nodeParentOffset)
		node.SetBalance(-1)
		middle.SetBalance(1)
		rotated = middle
		rebalanced = true
	} else {
		rotated, err = node.rotateLeft(middle)
	}
	return
}

// convenience wrapper for deleteRotateRight when caller hasn't loaded the node yet
func (node *avlNode) loadAndDeleteRotateRight(middleOffset uint64) (rotated *avlNode, rebalanced bool, err error) {
	middle, err := node.tree.loadNode(middleOffset)
	if err != nil {
		return
	}

	return node.deleteRotateRight(middle)
}

// worker to rotate after a left node deletion leaves the tree unbalanced
func (node *avlNode) deleteRotateRight(middle *avlNode) (rotated *avlNode, rebalanced bool, err error) {
	if middle.Balance() == 0 {
		nodeParentOffset := node.ParentOffset()
		subtreeBOffset := middle.LeftOffset()
		middle.SetLeftOffset(node.Offset())
		node.SetRightOffset(subtreeBOffset)
		node.SetParentOffset(middle.Offset())
		middle.SetParentOffset(nodeParentOffset)
		node.SetBalance(1)
		middle.SetBalance(-1)
		rotated = middle
		rebalanced = true
	} else {
		rotated, err = node.rotateRight(middle)
	}
	return
}

// iterates the AVL tree in the order of node creation
func (tree *avlTree) IterateByTimestamp(iter avlIterator) (err error) {
	node, err := tree.loadNode(tree.getOldestOffset())
	if err != nil {
		return
	}

	for node != nil {
		tree.nextByTime = node.NextOffset()
		err = iter(node)
		nextOffset := tree.nextByTime
		tree.nextByTime = 0

		if err != nil {
			return
		}

		if node, err = tree.loadNode(nextOffset); err != nil {
			return
		}
	}
	return
}

// iterates the AVL tree in sorted order
func (tree *avlTree) IterateByKeys(iter avlIterator) (err error) {
	root, err := tree.loadNode(tree.getRootOffset())
	if err != nil {
		return
	}

	tree.keyIteration = true
	err = root.iterateNext(iter)
	tree.keyIteration = false

	return
}

func (node *avlNode) iterateNext(iter avlIterator) (err error) {
	if node == nil {
		return
	}

	left, err := node.tree.loadNode(node.LeftOffset())
	if err != nil {
		return
	}
	if left != nil {
		if err = left.iterateNext(iter); err != nil {
			return
		}
	}
	if err = iter(node); err != nil {
		return
	}

	right, err := node.tree.loadNode(node.RightOffset())
	if right != nil {
		if err = right.iterateNext(iter); err != nil {
			return
		}
	}
	return
}

// testing function
func (tree *avlTree) loadAndSubtreeHeight(nodeOffset uint64) (height int, err error) {
	node, err := tree.loadNode(nodeOffset)
	if err != nil {
		return
	}
	return tree.subtreeHeight(node)
}

// testing function
func (tree *avlTree) subtreeHeight(node *avlNode) (height int, err error) {
	if node == nil {
		return
	}

	leftHeight, err := tree.loadAndSubtreeHeight(node.LeftOffset())
	if err != nil {
		return
	}
	rightHeight, err := tree.loadAndSubtreeHeight(node.RightOffset())
	if err != nil {
		return
	}

	if leftHeight >= rightHeight {
		height = leftHeight + 1
	} else {
		height = rightHeight + 1
	}
	return
}

// testing function
func (tree *avlTree) isBalanced(node *avlNode) (balanced bool, err error) {
	if node == nil {
		balanced = true
		return
	}

	leftHeight, err := tree.loadAndSubtreeHeight(node.LeftOffset())
	if err != nil {
		return
	}
	rightHeight, err := tree.loadAndSubtreeHeight(node.RightOffset())
	if err != nil {
		return
	}

	delta := leftHeight - rightHeight
	balanced = (delta >= -1 && delta <= 1)
	return
}

// testing function
func (tree *avlTree) checkBalanceFactors(node *avlNode) (balanced bool, err error) {
	if node == nil {
		balanced = true
		return
	}

	left, err := node.tree.loadNode(node.LeftOffset())
	if err != nil {
		return
	}
	right, err := node.tree.loadNode(node.RightOffset())
	if err != nil {
		return
	}

	childBalanced, err := tree.checkBalanceFactors(left)
	if err != nil {
		return
	}
	if !childBalanced {
		return
	}

	childBalanced, err = tree.checkBalanceFactors(right)
	if err != nil {
		return
	}
	if !childBalanced {
		return
	}

	lh, err := tree.subtreeHeight(left)
	if err != nil {
		return
	}
	rh, err := tree.subtreeHeight(right)
	if err != nil {
		return
	}

	balance := rh - lh
	balanced = (node.Balance() == balance)
	return
}

// testing function
func (tree *avlTree) checkParentLinks(node *avlNode) (valid bool, err error) {
	if node == nil {
		valid = true
	}

	left, err := tree.loadNode(node.LeftOffset())
	if err != nil {
		return
	}
	if left != nil && left.ParentOffset() != node.Offset() {
		return
	}

	right, err := tree.loadNode(node.RightOffset())
	if err != nil {
		return
	}
	if right != nil && right.ParentOffset() != node.Offset() {
		return
	}

	valid = true
	return
}

// testing function
func (tree *avlTree) checkTimestampLinks() (valid bool, err error) {
	p, err := tree.loadNode(tree.getOldestOffset())
	if err != nil {
		return
	}

	var prior *avlNode
	ts := int64(0)
	for p != nil {
		if p.timestamp < ts {
			return
		}
		ts = p.timestamp

		if ts == 0 {
			err = errors.New("timestamp not set")
			return
		}

		if p.PrevOffset() != prior.Offset() {
			return
		}
		prior = p
		if p, err = tree.loadNode(p.NextOffset()); err != nil {
			return
		}
	}

	valid = (tree.getNewestOffset() == offsetOf(prior))
	return
}

// testing function
func (tree *avlTree) isValid() (valid bool, err error) {
	root, err := tree.loadNode(tree.getRootOffset())
	if err != nil {
		return
	}

	if valid, err = tree.checkBalanceFactors(root); !valid || err != nil {
		return
	}
	if valid, err = tree.checkParentLinks(root); !valid || err != nil {
		return
	}
	if valid, err = tree.checkTimestampLinks(); !valid || err != nil {
		return
	}
	if valid, err = tree.isBalanced(root); !valid || err != nil {
		return
	}
	return
}

// testing function
func (tree *avlTree) countEach() (count int, err error) {
	err = tree.IterateByKeys(func(node *avlNode) error {
		count++
		return nil
	})
	return
}

// testing function
func (tree *avlTree) testNodeString(node *avlNode) string {
	switch tree.printType {
	case testPtPosition:
		return fmt.Sprintf("%d", node.Position())
	case testPtBalance:
		return fmt.Sprintf("%v", node.Balance())
	case testPtKey:
		return hex.EncodeToString(node.key[:4])
	default:
		panic("unexpected test print type")
	}
}

// testing function
func (tree *avlTree) printTree(header string) {

	// workaround for go's nag of unused debugging code
	_ = tree.printTreePositions
	_ = tree.printTreeKeys

	fmt.Println(header)

	root, err := tree.loadNode(tree.getRootOffset())
	if err != nil {
		panic(err)
	}

	if root == nil {
		fmt.Println("(nil)")
		return
	}
	maxWidth := 0
	err = tree.IterateByKeys(func(node *avlNode) error {
		width := len(tree.testNodeString(node))
		if width > maxWidth {
			maxWidth = width
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	height, err := tree.subtreeHeight(root)
	if err != nil {
		panic(err)
	}
	if height > 5 {
		fmt.Printf("height %d is too big to print\n", height)
		return
	}

	heightExp := math.Pow(2, float64(height)) / 2
	nodeWidth := maxWidth + 2
	fieldWidth := int(heightExp) * nodeWidth

	nextLineNodes := []*avlNode{}
	nextLineNodes = append(nextLineNodes, root)

	for {
		lineNodes := nextLineNodes
		nextLineNodes = []*avlNode{}

		keyLine := ""
		connectorLine := ""
		more := false
		for _, node := range lineNodes {
			kl, cl := tree.nodeText(node, fieldWidth, nodeWidth)
			keyLine += kl
			connectorLine += cl

			if node != nil {
				var left *avlNode
				if left, err = tree.loadNode(node.LeftOffset()); err != nil {
					panic(err)
				}
				var right *avlNode
				if right, err = tree.loadNode(node.RightOffset()); err != nil {
					panic(err)
				}
				nextLineNodes = append(nextLineNodes, left, right)
				more = more || (left != nil || right != nil)
			} else {
				nextLineNodes = append(nextLineNodes, nil, nil)
			}
		}

		fmt.Println(keyLine)
		if strings.ContainsAny(connectorLine, "/\\") {
			fmt.Println(connectorLine)
		}

		fieldWidth /= 2

		if !more {
			break
		}
	}
}

// testing function
func (tree *avlTree) nodeText(node *avlNode, fieldWidth, nodeWidth int) (keyLine, connectorLine string) {
	spaces := " "
	if node == nil {
		keyLine = strings.Repeat(spaces, fieldWidth)
		connectorLine = keyLine
	} else {
		leftSpaces := (fieldWidth - nodeWidth) / 2
		connectorLine = strings.Repeat(spaces, leftSpaces)

		if node.LeftOffset() != 0 {
			connectorLine += "/"
		} else {
			connectorLine += spaces
		}

		connectorLine += strings.Repeat(spaces, nodeWidth-2)

		if node.RightOffset() != 0 {
			connectorLine += "\\"
		} else {
			connectorLine += spaces
		}
		connectorLine += strings.Repeat(spaces, fieldWidth-len(connectorLine))

		keyText := tree.testNodeString(node)
		leftSpaces += (nodeWidth - len(keyText)) / 2
		rightSpaces := fieldWidth - leftSpaces - len(keyText)

		keyLine = strings.Repeat(spaces, leftSpaces)
		keyLine += keyText
		keyLine += strings.Repeat(spaces, rightSpaces)
	}
	return
}

// testing function
func (tree *avlTree) printTreePositions(prefix string) {
	tree.printType = testPtPosition
	tree.printTree(prefix)
}

// testing function
func (tree *avlTree) printTreeKeys(prefix string) {
	tree.printType = testPtKey
	tree.printTree(prefix)
}
