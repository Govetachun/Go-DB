package storage

import (
	"govetachun/go-mini-db/refactor_code/internal/storage/btree"
	"os"
)

// B-tree node types
const (
	BNODE_NODE = 1 // internal nodes with pointers
	BNODE_LEAF = 2 // leaf nodes with values
)

// Size constraints
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

const HEADER = 4 // type and nkeys

const DB_SIG = "BuildYourOwnDB06" // not compatible between chapters

const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4 + 8 + 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

// Insert modes
const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

// Data types
const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// BTree and BNode are defined in btree package

// KV represents the key-value store
type KV struct {
	Path string
	// internals
	fp   *os.File
	tree btree.BTree
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64 // database size in number of pages
		nfree   int    // number of pages taken from the free list
		nappend int    // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer.
		// nil value denotes a deallocated page.
		updates map[uint64][]byte
	}
	free FreeList
}

// FreeList represents the free list for page management
type FreeList struct {
	head uint64
	// callbacks for managing on-disk pages
	get func(uint64) btree.BNode  // dereference a pointer
	new func(btree.BNode) uint64  // append a new page
	use func(uint64, btree.BNode) // reuse a page
}

// BIter is defined in btree package
