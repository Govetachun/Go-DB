package btree

import (
	"bytes"
	"fmt"
	"govetachun/go-mini-db/utils"
)

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)                       // copy keys before idx
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1)) // copy keys after idx
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// find the key position
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // not found
		}
		// delete the key in the leaf node
		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(new, node, idx)
		return new

	case BNODE_NODE:
		// recursively delete from child
		return nodeDelete(tree, node, idx, key)
	default:
		panic("invalid node type")
	}
}

// delete a key from an internal node; part of the treeDelete()
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)

	// check for merging
	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0:
		utils.Assert(updated.nkeys() > 0, "updated.nkeys() > 0") // no merge
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// The conditions for merging are:
// 1. The node is smaller than 1/4 of a page (this is arbitrary).
// 2. Has a sibling and the merged result does not exceed one page.
// should the updated kid be merged with a sibling?
func shouldMerge(
	tree *BTree, node BNode,
	idx uint16, updated BNode,
) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}
	return 0, BNode{}
}

// checkLimit validates key and value sizes
func checkLimit(key []byte, val []byte) error {
	if len(key) > BTREE_MAX_KEY_SIZE {
		return fmt.Errorf("key too large")
	}
	if len(val) > BTREE_MAX_VAL_SIZE {
		return fmt.Errorf("value too large")
	}
	return nil
}

// delete a key and returns whether the key was there
func (tree *BTree) Delete(key []byte) bool {
	if tree.root == 0 {
		return false
	}

	// check key size limit
	if err := checkLimit(key, nil); err != nil {
		return false
	}

	// perform deletion
	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false // key not found
	}

	// update root if needed
	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

// 1. A new root node is created when the old root is split into multiple nodes.
// 2. When inserting the first key, create the first leaf node as the root.
// insert a new key or update an existing key
func (tree *BTree) Insert(key []byte, val []byte) error {
	// 1. check the length limit imposed by the node format
	if err := checkLimit(key, val); err != nil {
		return err // the only way for an update to fail
	}
	// 2. create the first node
	if tree.root == 0 {
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return nil
	}

	node := tree.get(tree.root)
	tree.del(tree.root)
	// 3. insert the key
	node = treeInsert(tree, node, key, val)
	// 4. grow the tree if the root is split
	nsplit, split := nodeSplit3(node)
	if nsplit > 1 { // the root was split, add a new level.
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
	return nil
}

// get a key and returns whether the key was there
func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}

	node := tree.get(tree.root)
	idx := nodeLookupLE(node, key)

	// check if key exists
	if idx >= 0 && idx < node.nkeys() && bytes.Equal(node.getKey(idx), key) {
		return node.getVal(idx), true
	}

	return nil, false
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())              // copy all keys from left
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys()) // copy all keys from right
}

// replace 2 adjacent links with 1
func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)                         // copy keys before idx
	nodeAppendKV(new, idx, ptr, key, nil)                        // insert the merged node
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2)) // copy keys after idx+1
}
