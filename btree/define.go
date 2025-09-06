package btree

import "os"

const (
	BNODE_NODE = 1 //internal nodes with pointers
	BNODE_LEAF = 2 // leaf nodes with values
)

// size constraints
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

const HEADER = 4 // type and nkeys

const DB_SIG = "BuildYourOwnDB06" // not compatible between chapters


const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4 + 8 + 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8


type BTree struct {
	// root pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // read data from a page number, dereference a pointer
	new func(BNode) uint64 // allocate a new page number with data
	del func(uint64)       // deallocate a page number
}

type BNode struct {
	data []byte // can be dumped to the disk
}

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree BTree
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


type FreeList struct {
	head uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode  // dereference a pointer
	new func(BNode) uint64  // append a new page
	use func(uint64, BNode) // reuse a page
}
