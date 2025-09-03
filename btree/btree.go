package btree

import (
	"encoding/binary"
	"govetachun/go-mini-db/utils"
)

const (
	BNODE_NODE = 1 //internal nodes with pointers
	BNODE_LEAF = 2 // leaf nodes with values
)

type Node struct {
	keys [][]byte

	vals [][]byte

	children []*Node
}

const BTREE_PAGE_SIZE = 4096

// size constraints
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

const HEADER = 4 // type and nkeys
func init() {
	// | type | nkeys |  pointers  |  offsets   | key-values | unused |
	// |  2B  |   2B  | nkeys × 8B | nkeys × 2B |     ...    |        |
	POINTERS := 1 * 8
	OFFSETS := 1 * 2

	// | key_size | val_size | key | val |
	// |    2B    |    2B    | ... | ... |
	KEY_SIZE := 2
	VAL_SIZE := 2
	node1max := HEADER + POINTERS + OFFSETS + KEY_SIZE + VAL_SIZE + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		panic("Exceeded page size!")
	}
}
type BTree struct {
    // root pointer (a nonzero page number)
    root uint64
    // callbacks for managing on-disk pages
    get func(uint64) []byte // read data from a page number
    new func([]byte) uint64 // allocate a new page number with data
    del func(uint64)        // deallocate a page number
}
func Encode(node *Node) []byte

func Decode(page []byte) (*Node, error)

type BNode []byte // can be dumped to the disk

// Read the fixed-size header.
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}



// Write the fixed-size header.
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}



// Read and write child pointers array (for internal nodes).
func (node BNode) getPtr(index uint16) uint64 {
	utils.Assert(index >= node.nkeys(), "Index out of bounds")
	pos := HEADER + index*8
	return binary.LittleEndian.Uint64(node[pos:])
}
func (node BNode) setPtr(index uint16, val uint64) {
	utils.Assert(index >= node.nkeys(), "Index out of bounds")
	pos := HEADER + index*8
	binary.LittleEndian.PutUint64(node[pos:], val)
}



// read the Offsets array
func (node BNode) getOffset(idx uint16) uint16 {
	utils.Assert(idx >= node.nkeys(), "Index out of bounds")
	if idx == 0 {
		return 0
	}
	pos := HEADER + node.nkeys()*8 + (idx-1)*2
	return binary.LittleEndian.Uint16(node[pos:])
}

// write the Offsets array
func (node BNode) setOffset(idx uint16, val uint16) {
	utils.Assert(idx >= node.nkeys(), "Index out of bounds")
	pos := HEADER + node.nkeys()*8 + (idx-1)*2
	binary.LittleEndian.PutUint16(node[pos:], val)
}


// Return the position of the nth key using getOffset().
func (node BNode) kvPos(idx uint16) uint16 {
	utils.Assert(idx > node.nkeys(), "Index out of bounds")
	return HEADER + node.nkeys()*8 + node.nkeys()*2 + node.getOffset(idx)
}

// Get the nth key data as a slice.
func (node BNode) getKey(idx uint16) []byte {
	utils.Assert(idx > node.nkeys(), "Index out of bounds")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

// Get the nth value data as a slice (for leaf nodes).
func (node BNode) getVal(idx uint16) []byte {
	utils.Assert(idx > node.nkeys(), "Index out of bounds")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
    return node.kvPos(node.nkeys()) // uses the offset value of the last key
}
