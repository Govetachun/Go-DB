package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"govetachun/go-mini-db/refactor_code/pkg/utils"
)

// BTree represents a B-tree data structure
type BTree struct {
	// root pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // read data from a page number, dereference a pointer
	new func(BNode) uint64 // allocate a new page number with data
	del func(uint64)       // deallocate a page number
}

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
	for {
		idx := nodeLookupLE(node, key)
		switch node.btype() {
		case BNODE_LEAF:
			if idx < node.nkeys() && bytes.Equal(node.getKey(idx), key) {
				return node.getVal(idx), true
			}
			return nil, false
		case BNODE_NODE:
			node = tree.get(node.getPtr(idx))
		default:
			panic("invalid node type")
		}
	}
}

func (tree *BTree) Update(key []byte, val []byte, mode int) (bool, error) {
	switch mode {
	case 0: // MODE_UPSERT - insert or replace
		return true, tree.Insert(key, val)
	case 1: // MODE_UPDATE_ONLY - update existing keys
		if _, exists := tree.Get(key); exists {
			return true, tree.Insert(key, val)
		}
		return false, nil
	case 2: // MODE_INSERT_ONLY - only add new keys
		if _, exists := tree.Get(key); !exists {
			return true, tree.Insert(key, val)
		}
		return false, nil
	default:
		return false, fmt.Errorf("invalid mode: %d", mode)
	}
}

// GetNode returns a BNode by pointer (exported for iterator use)
func (tree *BTree) GetNode(ptr uint64) BNode {
	return tree.get(ptr)
}

// GetRoot returns the root pointer (exported for iterator use)
func (tree *BTree) GetRoot() uint64 {
	return tree.root
}

// SetRoot sets the root pointer (exported for transaction use)
func (tree *BTree) SetRoot(root uint64) {
	tree.root = root
}

// SetGet sets the get function (exported for transaction use)
func (tree *BTree) SetGet(get func(uint64) BNode) {
	tree.get = get
}

// SetNew sets the new function (exported for transaction use)
func (tree *BTree) SetNew(new func(BNode) uint64) {
	tree.new = new
}

// SetDel sets the del function (exported for transaction use)
func (tree *BTree) SetDel(del func(uint64)) {
	tree.del = del
}

// Helper functions that need to be implemented
// These are placeholder implementations - you'll need to add the actual logic

func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	// the first key is a copy from the parent node,
	// thus it's always less than or equal to the key.
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	utils.Assert(srcOld+n <= old.nkeys(), "srcOld+n <= old.nkeys()")
	utils.Assert(dstNew+n <= new.nkeys(), "dstNew+n <= new.nkeys()")
	if n == 0 {
		return
	}
	// pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}
	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ { // NOTE: the range is [1, n]
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}
	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

func nodeAppendKV(node BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	node.setPtr(idx, ptr)

	// KVs
	pos := node.kvPos(idx) // use the offset value of the previous key

	// 4-bytes KV sizes
	binary.LittleEndian.PutUint16(node.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node.data[pos+2:], uint16(len(val)))

	// KV data
	copy(node.data[pos+4:], key)
	copy(node.data[pos+4+uint16(len(key)):], val)

	// update offset value for the next key
	node.setOffset(idx+1, node.getOffset(idx)+4+uint16(len(key)+len(val)))
}

func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	// where to insert the key?
	idx := nodeLookupLE(node, key) // node.getKey(idx) <= key
	switch node.btype() {
	case BNODE_LEAF: // leaf node
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val) // found, update it
		} else {
			leafInsert(new, node, idx+1, key, val) // not found, insert
		}
	case BNODE_NODE:
		// recursive insertion to the kid node
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("invalid node type")
	}
	return new
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is still too large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	utils.Assert(leftleft.nbytes() <= BTREE_PAGE_SIZE, "leftleft.nbytes() <= BTREE_PAGE_SIZE")
	return 3, [3]BNode{leftleft, middle, right}
}

func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())              // copy all keys from left
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys()) // copy all keys from right
}

func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)                         // copy keys before idx
	nodeAppendKV(new, idx, ptr, key, nil)                        // insert the merged node
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2)) // copy keys after idx+1
}

// Helper functions for insertion operations
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)                   // copy the keys before `idx`
	nodeAppendKV(new, idx, 0, key, val)                    // the new key
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) // keys from `idx`
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	// get and deallocate the kid node
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)
	// recursively insert the key into the kid node
	knode = treeInsert(tree, knode, key, val)
	nsplit, split := nodeSplit3(knode)
	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}

// split a bigger-than-allowed node into two.
// the second node always fits on a page.
func nodeSplit2(left BNode, right BNode, old BNode) {
	utils.Assert(old.nkeys() >= 2, "old.nkeys() >= 2")
	// the initial guess
	nleft := old.nkeys() / 2
	// try to fit the left half
	left_bytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	utils.Assert(nleft >= 1, "nleft >= 1")
	// try to fit the right half
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + 4
	}
	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	utils.Assert(nleft < old.nkeys(), "nleft < old.nkeys()")
	nright := old.nkeys() - nleft
	// new nodes
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	// NOTE: the left half may be still too big
	utils.Assert(right.nbytes() <= BTREE_PAGE_SIZE, "right.nbytes() <= BTREE_PAGE_SIZE")
}
