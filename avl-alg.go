package vfs

import (
	"bytes"
	"fmt"
	"math"
	"strings"
)

type (
	avlOperation struct {
		tree     *avlTreeS
		key      []byte
		shard    uint64
		position uint64
		leaf     avlNode
		added    bool
	}
)

// locates a key in the AVL tree
func (tree *avlTreeS) Find(key []byte) (node avlNode) {
	n := tree.getRoot()

	for {
		if n == nil || tree.err.Load() != nil {
			return
		}

		cmp := bytes.Compare(key, n.Key())

		if cmp == 0 {
			node = n
			return
		}

		if cmp < 0 {
			n = n.Left()
		} else {
			n = n.Right()
		}
	}
}

// adds a key to the AVL tree, or finds the existing node
func (tree *avlTreeS) Set(key []byte, shard, position uint64) (node avlNode, added bool) {
	op := &avlOperation{
		tree:     tree,
		key:      key,
		shard:    shard,
		position: position,
	}

	root, _ := op.insertNode(nil, tree.getRoot())
	tree.setRoot(root)

	return op.leaf, op.added
}

// removes a key from the AVL tree, returing true if the key was found and deleted
func (tree *avlTreeS) Delete(key []byte) bool {
	op := &avlOperation{
		tree: tree,
		key:  key,
	}

	root, _ := op.deleteNode(tree.getRoot())

	if op.leaf != nil {
		tree.setRoot(root)
		op.leaf.Free()
		return true
	}

	return false
}

// recursive worker that searches for the insertion position for a new node, and adds and rebalances the tree if key doesn't already exist
func (op *avlOperation) insertNode(parent avlNode, node avlNode) (out avlNode, balanced bool) {
	if node == nil {
		out = op.tree.alloc(op.key, op.shard, op.position)
		out.SetParent(parent)
		op.leaf = out
		op.added = true
		return
	}

	cmp := bytes.Compare(op.key, node.Key())

	if cmp == 0 {
		op.leaf = node
		balanced = true
		node.SetValues(op.shard, op.position)
	} else {
		if cmp < 0 {
			var left avlNode
			left, balanced = op.insertNode(node, node.Left())
			node.SetLeft(left)

			if !balanced {
				node.AddBalance(-1)
				if node.Balance() < -1 {
					node = node.rotateLeft(node.Left())
				}
				balanced = (node.Balance() == 0)
			}
		} else {
			var right avlNode
			right, balanced = op.insertNode(node, node.Right())
			node.SetRight(right)

			if !balanced {
				node.AddBalance(1)
				if node.Balance() > 1 {
					node = node.rotateRight(node.Right())
				}
				balanced = (node.Balance() == 0)
			}
		}
	}

	out = node
	return
}

// recursive worker that searches for a node, and if found, deletes and rebalances the tree
func (op *avlOperation) deleteNode(node avlNode) (out avlNode, rebalanced bool) {
	if node == nil {
		rebalanced = true
		return
	}

	cmp := bytes.Compare(node.Key(), op.key)

	if cmp == 0 {
		op.leaf = node
		if node.Left() == nil {
			out = node.Right()
			if out != nil {
				out.SetParent(node.Parent())
			}
			return
		}
		if node.Right() == nil {
			out = node.Left()
			out.SetParent(node.Parent())
			return
		}

		replacement := node.Left()
		for replacement.Right() != nil {
			replacement = replacement.Right()
		}

		node.CopyKeyAndValues(replacement)
		node.SwapTimestamp(replacement)

		// key to delete now becomes the replacement - the original one further down in the tree
		copy(op.key, replacement.Key())
		cmp = bytes.Compare(node.Key(), op.key)
	}

	if cmp >= 0 {
		var left avlNode
		left, rebalanced = op.deleteNode(node.Left())
		node.SetLeft(left)

		if !rebalanced {
			node.AddBalance(1)
			if node.Balance() > 1 {
				node, rebalanced = node.deleteRotateRight(node.Right())
			} else {
				rebalanced = (node.Balance() != 0)
			}
		}
	} else {
		var right avlNode
		right, rebalanced = op.deleteNode(node.Right())
		node.SetRight(right)

		if !rebalanced {
			node.AddBalance(-1)
			if node.Balance() < -1 {
				node, rebalanced = node.deleteRotateLeft(node.Left())
			} else {
				rebalanced = (node.Balance() != 0)
			}
		}
	}

	out = node
	return
}

// worker to update the balance factor
func (an *avlNodeS) adjustBalance(second avlNode, third avlNode, direction int) {
	node := avlNode(an)

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

// worker to balance the tree after left insertion makes the tree left heavy
func (an *avlNodeS) rotateLeft(middle avlNode) avlNode {
	node := avlNode(an)

	nodeParent := node.Parent()
	if middle.Balance() < 0 {
		// left-left rotation
		subtreeC := middle.Right()
		middle.SetRight(node)
		node.SetLeft(subtreeC)
		node.SetParent(middle)
		middle.SetParent(nodeParent)
		node.SetBalance(0)
		middle.SetBalance(0)
		return middle
	} else {
		// left-right rotation
		third := middle.Right()
		if third == nil {
			return node
		}
		node.adjustBalance(middle, third, 1)
		subtreeB := third.Left()
		subtreeC := third.Right()
		third.SetLeft(middle)
		third.SetRight(node)
		middle.SetRight(subtreeB)
		node.SetLeft(subtreeC)
		node.SetParent(third)
		middle.SetParent(third)
		third.SetParent(nodeParent)
		return third
	}
}

// worker to balance the tree after right insertion makes the tree right heavy
func (an *avlNodeS) rotateRight(middle avlNode) avlNode {
	node := avlNode(an)

	nodeParent := node.Parent()
	if middle.Balance() > 0 {
		// right-right rotation
		subtreeB := middle.Left()
		middle.SetLeft(node)
		node.SetRight(subtreeB)
		node.SetParent(middle)
		middle.SetParent(nodeParent)
		node.SetBalance(0)
		middle.SetBalance(0)
		return middle
	} else {
		// right-left rotation
		third := middle.Left()
		if third == nil {
			return node
		}
		node.adjustBalance(middle, third, -1)
		subtreeB := third.Left()
		subtreeC := third.Right()
		third.SetLeft(node)
		third.SetRight(middle)
		node.SetRight(subtreeB)
		middle.SetLeft(subtreeC)
		node.SetParent(third)
		middle.SetParent(third)
		third.SetParent(nodeParent)
		return third
	}
}

// worker to rotate after a right node deletion leaves the tree unbalanced
func (an *avlNodeS) deleteRotateLeft(middle avlNode) (out avlNode, rebalanced bool) {
	node := avlNode(an)

	if middle.Balance() == 0 {
		nodeParent := node.Parent()
		subtreeC := middle.Right()
		middle.SetRight(node)
		node.SetLeft(subtreeC)
		node.SetParent(middle)
		middle.SetParent(nodeParent)
		node.SetBalance(-1)
		middle.SetBalance(1)
		return middle, true
	} else {
		return node.rotateLeft(middle), false
	}
}

// worker to rotate after a left node deletion leaves the tree unbalanced
func (an *avlNodeS) deleteRotateRight(middle avlNode) (out avlNode, rebalanced bool) {
	node := avlNode(an)

	if middle.Balance() == 0 {
		nodeParent := node.Parent()
		subtreeB := middle.Left()
		middle.SetLeft(node)
		node.SetRight(subtreeB)
		node.SetParent(middle)
		middle.SetParent(nodeParent)
		node.SetBalance(1)
		middle.SetBalance(-1)
		return middle, true
	} else {
		return node.rotateRight(middle), false
	}
}

// iterates the AVL tree in the order of node creation
func (tree *avlTreeS) IterateByTimestamp(iter AvlIterator) {
	node := tree.getOldest()
	for node != nil {
		if tree.err.Load() != nil {
			return
		}
		next := node.Next() // iterator might delete node
		if !iter(node) {
			break
		}

		node = next
	}
}

// iterates the AVL tree in sorted order
func (tree *avlTreeS) IterateByKeys(iter AvlIterator) {
	tree.getRoot().iterateNext(iter)
}

func (an *avlNodeS) iterateNext(iter AvlIterator) bool {
	if an == nil {
		return true
	}

	if an.tree.err.Load() != nil {
		return false
	}

	node := avlNode(an)

	if node.Left() != nil {
		if !node.Left().iterateNext(iter) {
			return false
		}
	}
	if !iter(node) {
		return false
	}
	if node.Right() != nil {
		if !node.Right().iterateNext(iter) {
			return false
		}
	}
	return true
}

// testing function
func (tree *avlTreeS) subtreeHeight(node avlNode) (height int) {
	if node == nil {
		return
	}

	leftHeight := tree.subtreeHeight(node.Left())
	rightHeight := tree.subtreeHeight(node.Right())

	if leftHeight >= rightHeight {
		return leftHeight + 1
	} else {
		return rightHeight + 1
	}
}

// testing function
func (tree *avlTreeS) isBalanced(node avlNode) bool {
	if node == nil {
		return true
	}

	delta := tree.subtreeHeight(node.Left()) - tree.subtreeHeight(node.Right())
	return delta >= -1 && delta <= 1
}

// testing function
func (tree *avlTreeS) checkBalanceFactors(node avlNode) bool {
	if node == nil {
		return true
	}

	left := node.Left()
	right := node.Right()

	if !tree.checkBalanceFactors(left) || !tree.checkBalanceFactors(right) {
		return false
	}

	lh := tree.subtreeHeight(left)
	rh := tree.subtreeHeight(right)

	balance := rh - lh
	return node.Balance() == balance
}

// testing function
func (tree *avlTreeS) checkParentLinks(node avlNode) bool {
	if node == nil {
		return true
	}

	if node.Left() != nil && node.Left().Parent() != node {
		return false
	}
	if node.Right() != nil && node.Right().Parent() != node {
		return false
	}

	return true
}

// testing function
func (tree *avlTreeS) checkTimestampLinks() bool {
	p := tree.getOldest()

	var prior avlNode
	ts := int64(0)
	for p != nil {
		if p.Timestamp() < ts {
			return false
		}
		ts = p.Timestamp()

		if p.Prev() != prior {
			return false
		}
		prior = p
		p = p.Next()
	}

	return tree.getNewest() == prior
}

// testing function
func (tree *avlTreeS) isValid() bool {
	if !tree.checkBalanceFactors(tree.getRoot()) {
		return false
	}
	if !tree.checkParentLinks(tree.getRoot()) {
		return false
	}
	if !tree.checkTimestampLinks() {
		return false
	}
	return tree.isBalanced(tree.getRoot())
}

// testing function
func (tree *avlTreeS) countEach() int {
	count := 0
	tree.IterateByKeys(func(node avlNode) bool {
		count++
		return true
	})
	return count
}

// testing function
func (tree *avlTreeS) printTree(header string) {
	fmt.Println(header)
	if tree.getRoot() == nil {
		fmt.Println("(nil)")
		return
	}
	maxWidth := 0
	tree.IterateByKeys(func(node avlNode) bool {
		var width int
		if tree.printValues {
			width = len(fmt.Sprintf("%v", node.Position()))
		} else {
			width = len(fmt.Sprintf("%v", node.Balance()))
		}
		if width > maxWidth {
			maxWidth = width
		}
		return true
	})

	height := tree.subtreeHeight(tree.getRoot())
	if height > 5 {
		fmt.Printf("height %d is too big to print\n", height)
		return
	}

	heightExp := math.Pow(2, float64(height)) / 2
	nodeWidth := maxWidth + 2
	fieldWidth := int(heightExp) * nodeWidth

	nextLineNodes := []avlNode{}
	nextLineNodes = append(nextLineNodes, tree.getRoot())

	for {
		lineNodes := nextLineNodes
		nextLineNodes = []avlNode{}

		keyLine := ""
		connectorLine := ""
		more := false
		for _, node := range lineNodes {
			kl, cl := tree.nodeText(node, fieldWidth, nodeWidth)
			keyLine += kl
			connectorLine += cl

			if node != nil {
				nextLineNodes = append(nextLineNodes, node.Left(), node.Right())
				more = more || (node.Left() != nil || node.Right() != nil)
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
func (an *avlTreeS) nodeText(node avlNode, fieldWidth, nodeWidth int) (keyLine, connectorLine string) {
	spaces := " "
	if node == nil {
		keyLine = strings.Repeat(spaces, fieldWidth)
		connectorLine = keyLine
	} else {
		leftSpaces := (fieldWidth - nodeWidth) / 2
		connectorLine = strings.Repeat(spaces, leftSpaces)
		if node.Left() != nil {
			connectorLine += "/"
		} else {
			connectorLine += spaces
		}
		connectorLine += strings.Repeat(spaces, nodeWidth-2)
		if node.Right() != nil {
			connectorLine += "\\"
		} else {
			connectorLine += spaces
		}
		connectorLine += strings.Repeat(spaces, fieldWidth-len(connectorLine))

		var keyText string
		if an.printValues {
			keyText = fmt.Sprintf("%d", node.Position())
		} else {
			keyText = fmt.Sprintf("%v", node.Balance())
		}
		leftSpaces += (nodeWidth - len(keyText)) / 2
		rightSpaces := fieldWidth - leftSpaces - len(keyText)

		keyLine = strings.Repeat(spaces, leftSpaces)
		keyLine += keyText
		keyLine += strings.Repeat(spaces, rightSpaces)
	}
	return
}

// testing function
func (tree *avlTreeS) printTreeValues(enable bool) {
	tree.printValues = enable
}
