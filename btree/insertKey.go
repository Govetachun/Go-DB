package btree

import (
	"bytes"
	"encoding/binary"
	"govetachun/go-mini-db/utils"
)

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {

	// ptrs
	new.setPtr(idx, ptr)

	// KVs
	pos := new.kvPos(idx) // use the offest value of the previous key

	// 4-bytes KV sizes
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))

	// 4-bytes KV data
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	
	//update offsets value for the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
	
}

func leafInsert(
    new BNode, old BNode, idx uint16, key []byte, val []byte,
) {
    new.setHeader(BNODE_LEAF, old.nkeys()+1)
    nodeAppendRange(new, old, 0, 0, idx)    // copy the keys before `idx`
    nodeAppendKV(new, idx, 0, key, val)     // the new key
    nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)  // keys from `idx`
}

func leafUpdate(
    new BNode, old BNode, idx uint16, key []byte, val []byte,
) {
    new.setHeader(BNODE_LEAF, old.nkeys())
    nodeAppendRange(new, old, 0, 0, idx)
    nodeAppendKV(new, idx, 0, key, val)
    nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}
// copy multiple keys, values, and pointers into the position
func nodeAppendRange(
    new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16,
) {
    for i := uint16(0); i < n; i++ {
        dst, src := dstNew+i, srcOld+i
        nodeAppendKV(new, dst,
            old.getPtr(src), old.getKey(src), old.getVal(src))
    }
}
// find the last postion that is less than or equal to the key
func nodeLookupLE(node BNode, key []byte) uint16 {
    nkeys := node.nkeys()
    var i uint16
    for i = 0; i < nkeys; i++ {
        cmp := bytes.Compare(node.getKey(i), key)
        if cmp == 0 {
            return i
        }
        if cmp > 0 {
            return i - 1
        }
    }
    return i - 1
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
    // The extra size allows it to exceed 1 page temporarily.
    new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
    // where to insert the key?
    idx := nodeLookupLE(node, key) // node.getKey(idx) <= key
    switch node.btype() {
    case BNODE_LEAF: // leaf node
        if bytes.Equal(key, node.getKey(idx)) {
            leafUpdate(new, node, idx, key, val)   // found, update it
        } else {
            leafInsert(new, node, idx+1, key, val) // not found, insert
        }
    case BNODE_NODE:
        // recursive insertion to the kid node
        kptr := node.getPtr(idx)
        knode := treeInsert(tree, tree.get(kptr), key, val)
        // after insertion, split the result
        nsplit, split := nodeSplit3(knode)
        // deallocate the old kid node
        tree.del(kptr)
        // update the kid links
        nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
    }
    return new
}
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
func nodeReplaceKidN(
    tree *BTree, new BNode, old BNode, idx uint16,
    kids ...BNode,
) {
    inc := uint16(len(kids))
    new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
    nodeAppendRange(new, old, 0, 0, idx)
    for i, node := range kids {
        nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
    }
    nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}
func nodeSplit3(old BNode) (uint16, [3]BNode) {
    if old.nbytes() <= BTREE_PAGE_SIZE {
        old = old[:BTREE_PAGE_SIZE]
        return 1, [3]BNode{old} // not split
    }
    left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
    right := BNode(make([]byte, BTREE_PAGE_SIZE))
    nodeSplit2(left, right, old)
    if left.nbytes() <= BTREE_PAGE_SIZE {
        left = left[:BTREE_PAGE_SIZE]
        return 2, [3]BNode{left, right} // 2 nodes
    }
    leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
    middle := BNode(make([]byte, BTREE_PAGE_SIZE))
    nodeSplit2(leftleft, middle, left)
    utils.Assert(leftleft.nbytes() <= BTREE_PAGE_SIZE, "leftleft.nbytes() <= BTREE_PAGE_SIZE")
    return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}