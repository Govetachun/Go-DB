package disk

import (
	"govetachun/go-mini-db/refactor_code/internal/storage/btree"
	"govetachun/go-mini-db/refactor_code/pkg/utils"
	"os"
)

// KV represents the key-value store with page management
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
	head      uint64
	total     int
	freePages []uint64 // Simple in-memory list of free pages
	// callbacks for managing on-disk pages
	get func(uint64) btree.BNode  // dereference a pointer
	new func(btree.BNode) uint64  // append a new page
	use func(uint64, btree.BNode) // reuse a page
}

// callback for BTree & FreeList, dereference a pointer.
func (db *KV) pageGet(ptr uint64) btree.BNode {
	if page, ok := db.page.updates[ptr]; ok {
		utils.Assert(page != nil, "page != nil")
		return btree.NewBNode(page) // for new pages
	}
	return pageGetMapped(db, ptr) // for written pages
}

func pageGetMapped(db *KV, ptr uint64) btree.BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return btree.NewBNode(chunk[offset : offset+BTREE_PAGE_SIZE])
		}
		start = end
	}
	panic("bad ptr")
}

// callback for BTree, read a page.
func (db *KV) pageRead(ptr uint64) btree.BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return btree.NewBNode(chunk[offset : offset+BTREE_PAGE_SIZE])
		}
		start = end
	}
	panic("bad ptr")
}

// callback for FreeList, allocate a new page.
func (db *KV) pageAppend(node btree.BNode) uint64 {
	utils.Assert(len(node.GetData()) <= BTREE_PAGE_SIZE, "len(node.data) <= BTREE_PAGE_SIZE")
	ptr := db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	db.page.updates[ptr] = node.GetData()
	return ptr
}

// callback for FreeList, reuse a page.
func (db *KV) pageUse(ptr uint64, node btree.BNode) {
	db.page.updates[ptr] = node.GetData()
}

// callback for BTree, allocate a new page.
func (db *KV) pageNew(node btree.BNode) uint64 {
	utils.Assert(len(node.GetData()) <= BTREE_PAGE_SIZE, "len(node.data) <= BTREE_PAGE_SIZE")
	ptr := uint64(0)
	if db.page.nfree < db.free.Total() {
		// reuse a deallocated page
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree++
	} else {
		// append a new page
		ptr = db.page.flushed + uint64(db.page.nappend)
		db.page.nappend++
	}
	db.page.updates[ptr] = node.GetData()
	return ptr
}

// callback for BTree, deallocate a page.
func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

// Helper methods for FreeList
func (fl *FreeList) Total() int {
	return fl.total
}

func (fl *FreeList) Get(index int) uint64 {
	if index >= len(fl.freePages) || index < 0 {
		return 0 // No free pages available
	}
	return fl.freePages[index]
}

func (fl *FreeList) Update(nfree int, freed []uint64) {
	// Add newly freed pages to the free list
	for _, ptr := range freed {
		if ptr == 0 {
			continue // Skip invalid pointers
		}

		// Add to our simple in-memory list
		fl.freePages = append(fl.freePages, ptr)
		fl.total++
	}

	// Remove pages that were consumed (first nfree pages)
	if nfree > 0 && nfree <= len(fl.freePages) {
		fl.freePages = fl.freePages[nfree:]
		fl.total -= nfree
	}
}
