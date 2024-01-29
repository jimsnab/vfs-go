package vfs

type (
	lruStack[T comparable] struct {
		head         *lruStackElement[T]
		tail         *lruStackElement[T]
		advisedLimit int
		count        int
		blocked      int
		canDel       lruStackCanDelete[T]
	}

	lruStackElement[T comparable] struct {
		elem      T
		prev      *lruStackElement[T]
		next      *lruStackElement[T]
		discarded bool
	}

	lruStackCanDelete[T comparable] func(elem T) bool
)

func newLruStack[T comparable](advisedLimit int, canDel lruStackCanDelete[T]) *lruStack[T] {
	if canDel == nil {
		canDel = func(elem T) bool { return true }
	}

	if advisedLimit > 500 {
		// for big lru lists, suggest 25% headroom
		advisedLimit = (3 * advisedLimit) / 4
	}

	return &lruStack[T]{
		advisedLimit: advisedLimit,
		canDel:       canDel,
	}
}

// Adds an element to the lru stack. The caller should ensure
// the element hasn't been added before.
func (lru *lruStack[T]) Add(elem T) *lruStackElement[T] {
	e := &lruStackElement[T]{
		elem: elem,
		prev: lru.tail,
	}
	if lru.head == nil {
		lru.head = e
	} else {
		lru.tail.next = e
	}
	lru.tail = e
	lru.count++
	return e
}

// Moves an element to the head of the list.
func (lru *lruStack[T]) Promote(p *lruStackElement[T]) {
	if p.discarded {
		panic("discarded")
	}
	if p.next != nil {
		if p.prev == nil {
			lru.head = p.next
		} else {
			p.prev.next = p.next
		}

		p.next.prev = p.prev

		p.next = nil
		p.prev = lru.tail
		p.prev.next = p
		lru.tail = p
	}
}

// Takes element out of the lru stack.
func (lru *lruStack[T]) Remove(p *lruStackElement[T]) bool {
	if p.discarded {
		panic("already removed")
	}
	if lru.canDel(p.elem) {
		if p.prev == nil {
			lru.head = p.next
		} else {
			p.prev.next = p.next
		}

		if p.next == nil {
			lru.tail = p.prev
		} else {
			p.next.prev = p.prev
		}

		var zero T
		p.elem = zero

		p.discarded = true
		lru.count--
		return true
	} else {
		lru.blocked++
	}

	return false
}

// Remove items if possible to bring the stack under the advised size limit
func (lru *lruStack[T]) Collect() {
	if lru.count >= lru.advisedLimit {
		p := lru.head
		for p != nil {
			next := p.next
			if lru.Remove(p) {
				if lru.count <= lru.advisedLimit {
					break
				}
			}
			p = next
		}
	}
}

// Diagnostic - search linearly for the element
func (lru *lruStack[T]) Find(elem T) *lruStackElement[T] {
	p := lru.head
	for p != nil {
		if p.elem == elem {
			return p
		}
		p = p.next
	}
	return nil
}

// Diagnostic - check integrity
func (lru *lruStack[T]) Check() bool {
	count := 0
	p := lru.head
	p2 := lru.head
	var prior *lruStackElement[T]
	for p != nil {
		count++
		if p.prev != prior {
			return false
		}
		prior = p

		if p2 != nil {
			p2 = p2.next
			if p2 == p {
				return false
			}
			if p2 != nil {
				p2 = p2.next
				if p2 == p {
					return false
				}
			}
		}

		p = p.next
	}

	if prior != lru.tail {
		return false
	}

	if count != lru.count {
		return false
	}

	return true
}
