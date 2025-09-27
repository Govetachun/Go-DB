package btree

import (
	"encoding/binary"
	"govetachun/go-mini-db/refactor_code/pkg/utils"
)

// Import constants from storage package
const (
	BNODE_NODE = 1 // internal nodes with pointers
	BNODE_LEAF = 2 // leaf nodes with values
)

const BTREE_PAGE_SIZE = 4096
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

// BNode represents a B-tree node
type BNode struct {
	data []byte // can be dumped to the disk
}

// header operations
// Read the fixed-size header.
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

// Write the fixed-size header.
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointer operations
// Read and write child pointers array (for internal nodes).
func (node BNode) getPtr(index uint16) uint64 {
	utils.Assert(index < node.nkeys(), "Index out of bounds")
	pos := HEADER + index*8
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(index uint16, val uint64) {
	utils.Assert(index < node.nkeys(), "Index out of bounds")
	pos := HEADER + index*8
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset operations
// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	utils.Assert(1 <= idx && idx <= node.nkeys(), "1<=idx && idx <= node.nkeys()")
	return HEADER + node.nkeys()*8 + (idx-1)*2
}

// read the Offsets array
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0 // idx=0: Always returns 0 (special case)
	}
	pos := offsetPos(node, idx)
	return binary.LittleEndian.Uint16(node.data[pos:])
}

// write the Offsets array
func (node BNode) setOffset(idx uint16, val uint16) {
	pos := offsetPos(node, idx)
	binary.LittleEndian.PutUint16(node.data[pos:], val)
}

// key-value operations
// Return the position of the nth key using getOffset().
func (node BNode) kvPos(idx uint16) uint16 {
	utils.Assert(idx <= node.nkeys(), "idx <= node.nkeys()")
	return HEADER + node.nkeys()*8 + node.nkeys()*2 + node.getOffset(idx)
}

// Get the nth key data as a slice.
func (node BNode) getKey(idx uint16) []byte {
	utils.Assert(idx < node.nkeys(), "idx < node.nkeys()")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

// Get the nth value data as a slice (for leaf nodes).
func (node BNode) getVal(idx uint16) []byte {
	utils.Assert(idx < node.nkeys(), "idx < node.nkeys()")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys()) // uses the offset value of the last key
}

// Exported methods for iterator use
func (node BNode) GetKey(idx uint16) []byte {
	return node.getKey(idx)
}

func (node BNode) GetVal(idx uint16) []byte {
	return node.getVal(idx)
}

func (node BNode) GetPtr(idx uint16) uint64 {
	return node.getPtr(idx)
}

func (node BNode) NKeys() uint16 {
	return node.nkeys()
}

func (node BNode) BType() uint16 {
	return node.btype()
}

// GetData returns the node data (exported for transaction use)
func (node BNode) GetData() []byte {
	return node.data
}

// NewBNode creates a new BNode with the given data (exported for transaction use)
func NewBNode(data []byte) BNode {
	return BNode{data: data}
}
