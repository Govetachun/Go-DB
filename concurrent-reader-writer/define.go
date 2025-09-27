package concurrentreaderwriter

import (
	"container/heap"
	btree "govetachun/go-mini-db/kv-store"
	"os"
	"sync"
)

// Version numbers are added to free list nodes and to the master page:
// | type | size | total | next | pointer-version-pairs |
// | 2B | 2B | 8B | 8B | size * 16B |
// The master page format:
// | sig | btree_root | page_used | free_list | version |
// | 16B | 8B | 8B | 8B | 8B |

type KV struct {
	Path string
	// internals
	fp *os.File
	// mod 1: moved the B-tree and the free list
	tree btree.BTree
	free FreeListData
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	// mod 2: moved the page management
	page struct {
		flushed uint64 // database size in number of pages
	}
	// mod 3: mutexes
	mu     sync.Mutex
	writer sync.Mutex
	// mod 4: version number and the reader list
	version uint64
	readers ReaderList // heap, for tracking the minimum reader version
}

// read-only KV transactions
type KVReader struct {
	// the snapshot
	version uint64
	tree    btree.BTree
	mmap    struct {
		chunks [][]byte // copied from struct KV. read-only.
	}
	// for removing from the heap
	index int
}

func (kv *KV) BeginRead(tx *KVReader) {
	kv.mu.Lock()
	tx.mmap.chunks = kv.mmap.chunks
	tx.tree = kv.tree
	tx.version = kv.version
	heap.Push(&kv.readers, tx)
	kv.mu.Unlock()
}

func (kv *KV) EndRead(tx *KVReader) {
	kv.mu.Lock()
	heap.Remove(&kv.readers, tx.index)
	kv.mu.Unlock()
}

// implements heap.Interface
type ReaderList []*KVReader

func (rl ReaderList) Len() int           { return len(rl) }
func (rl ReaderList) Less(i, j int) bool { return rl[i].version < rl[j].version }
func (rl ReaderList) Swap(i, j int)      { rl[i], rl[j] = rl[j], rl[i]; rl[i].index, rl[j].index = i, j }

func (rl *ReaderList) Push(x interface{}) {
	n := len(*rl)
	item := x.(*KVReader)
	item.index = n
	*rl = append(*rl, item)
}

func (rl *ReaderList) Pop() interface{} {
	old := *rl
	n := len(old)
	item := old[n-1]
	item.index = -1
	*rl = old[0 : n-1]
	return item
}

func (tx *KVReader) Get(key []byte) ([]byte, bool) {
	return tx.tree.Get(key)
}

func (tx *KVReader) Seek(key []byte, cmp int) *btree.BIter {
	return tx.tree.SeekLE(key)
}

// GetVersion returns the reader version (exported for transaction use)
func (tx *KVReader) GetVersion() uint64 {
	return tx.version
}

// GetMmapChunks returns the mmap chunks (exported for transaction use)
func (tx *KVReader) GetMmapChunks() [][]byte {
	return tx.mmap.chunks
}

// SetMmapChunks sets the mmap chunks (exported for transaction use)
func (tx *KVReader) SetMmapChunks(chunks [][]byte) {
	tx.mmap.chunks = chunks
}

// SetVersion sets the reader version (exported for transaction use)
func (tx *KVReader) SetVersion(version uint64) {
	tx.version = version
}

// the in-memory data structure that is updated and committed by transactions
type FreeListData struct {
	head uint64
	// cached pointers to list nodes for accessing both ends.
	nodes []uint64 // from the tail to the head
	// cached total number of items; stored in the head node.
	total int
	// cached number of discarded items in the tail node.
	offset int
}

type FreeList struct {
	FreeListData
	// for each transaction
	version   uint64   // current version
	minReader uint64   // minimum reader version
	freed     []uint64 // pages that will be added to the free list
	// callbacks for managing on-disk pages
	get func(uint64) btree.BNode  // dereference a pointer
	new func(btree.BNode) uint64  // append a new page
	use func(uint64, btree.BNode) // reuse a page
}

// try to remove an item from the tail. returns 0 on failure.
// the removed pointer must not be reachable by the minimum version reader.
func (fl *FreeList) Pop() uint64 {
	// Simplified implementation
	if fl.total == 0 {
		return 0
	}
	fl.total--
	return fl.head
}

// add some new pointers to the head and finalize the update
func (fl *FreeList) Add(freed []uint64) {
	fl.freed = append(fl.freed, freed...)
	fl.total += len(freed)
}

// GetVersion returns the freelist version (exported for transaction use)
func (fl *FreeList) GetVersion() uint64 {
	return fl.version
}

// SetVersion sets the freelist version (exported for transaction use)
func (fl *FreeList) SetVersion(version uint64) {
	fl.version = version
}

// GetMinReader returns the minimum reader version (exported for transaction use)
func (fl *FreeList) GetMinReader() uint64 {
	return fl.minReader
}

// SetMinReader sets the minimum reader version (exported for transaction use)
func (fl *FreeList) SetMinReader(minReader uint64) {
	fl.minReader = minReader
}

// GetGet returns the get function (exported for transaction use)
func (fl *FreeList) GetGet() func(uint64) btree.BNode {
	return fl.get
}

// SetGet sets the get function (exported for transaction use)
func (fl *FreeList) SetGet(get func(uint64) btree.BNode) {
	fl.get = get
}

// GetNew returns the new function (exported for transaction use)
func (fl *FreeList) GetNew() func(btree.BNode) uint64 {
	return fl.new
}

// SetNew sets the new function (exported for transaction use)
func (fl *FreeList) SetNew(new func(btree.BNode) uint64) {
	fl.new = new
}

// GetUse returns the use function (exported for transaction use)
func (fl *FreeList) GetUse() func(uint64, btree.BNode) {
	return fl.use
}

// SetUse sets the use function (exported for transaction use)
func (fl *FreeList) SetUse(use func(uint64, btree.BNode)) {
	fl.use = use
}

// Helper methods for KV
func (kv *KV) FlushPages() error {
	// Simplified implementation - would need actual page flushing logic
	return nil
}

func (kv *KV) StoreMaster() error {
	// Simplified implementation - would need actual master page storage logic
	return nil
}

func (kv *KV) Sync() error {
	return kv.fp.Sync()
}

func (kv *KV) GetVersion() uint64 {
	return kv.version
}

func (kv *KV) GetTree() *btree.BTree {
	return &kv.tree
}

func (kv *KV) GetFreeList() *FreeListData {
	return &kv.free
}

func (kv *KV) GetPageFlushed() uint64 {
	return kv.page.flushed
}

func (kv *KV) SetPageFlushed(flushed uint64) {
	kv.page.flushed = flushed
}

func (kv *KV) GetReaders() *ReaderList {
	return &kv.readers
}

func (kv *KV) GetMmapChunks() [][]byte {
	return kv.mmap.chunks
}

func (kv *KV) SetMmapChunks(chunks [][]byte) {
	kv.mmap.chunks = chunks
}

func (kv *KV) GetWriterMutex() *sync.Mutex {
	return &kv.writer
}

func (kv *KV) GetMutex() *sync.Mutex {
	return &kv.mu
}
