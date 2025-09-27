package btree

// BIter represents a B-tree iterator
type BIter struct {
	Tree *BTree
	Path []BNode  // from root to leaf
	Pos  []uint16 // indexes into nodes
}

// SeekLE finds the closest position that is less or equal to the input key
func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{Tree: tree}
	for ptr := tree.GetRoot(); ptr != 0; {
		node := tree.GetNode(ptr)
		idx := nodeLookupLE(node, key)
		iter.Path = append(iter.Path, node)
		iter.Pos = append(iter.Pos, idx)
		if node.BType() == BNODE_NODE {
			ptr = node.GetPtr(idx)
		} else {
			ptr = 0
		}
	}
	return iter
}

// get the current KV pair
func (iter *BIter) Deref() ([]byte, []byte) {
	if !iter.Valid() {
		return nil, nil
	}
	leaf := iter.Path[len(iter.Path)-1]
	idx := iter.Pos[len(iter.Pos)-1]
	return leaf.GetKey(idx), leaf.GetVal(idx)
}

// precondition of the Deref()
func (iter *BIter) Valid() bool {
	return len(iter.Path) > 0 && len(iter.Pos) > 0
}

// moving backward and forward
func (iter *BIter) Prev() {
	iterPrev(iter, len(iter.Path)-1)
}

func (iter *BIter) Next() {
	iterNext(iter, len(iter.Path)-1)
}

func iterPrev(iter *BIter, level int) {
	if iter.Pos[level] > 0 {
		iter.Pos[level]-- // move within this node
	} else if level > 0 {
		iterPrev(iter, level-1) // move to a sibling node
	} else {
		return // dummy key
	}
	if level+1 < len(iter.Pos) {
		// update the kid node
		node := iter.Path[level]
		kid := iter.Tree.GetNode(node.GetPtr(iter.Pos[level]))
		iter.Path[level+1] = kid
		iter.Pos[level+1] = kid.NKeys() - 1
	}
}

func iterNext(iter *BIter, level int) {
	if iter.Pos[level]+1 < iter.Path[level].NKeys() {
		iter.Pos[level]++ // move within this node
	} else if level > 0 {
		iterNext(iter, level-1) // move to a sibling node
	} else {
		return // dummy key
	}
	if level+1 < len(iter.Pos) {
		// update the kid node
		node := iter.Path[level]
		kid := iter.Tree.GetNode(node.GetPtr(iter.Pos[level]))
		iter.Path[level+1] = kid
		iter.Pos[level+1] = 0
	}
}
