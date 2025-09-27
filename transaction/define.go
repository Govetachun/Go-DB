package transaction

import (
	"fmt"
	concurrentreaderwriter "govetachun/go-mini-db/concurrent-reader-writer"
	btree "govetachun/go-mini-db/kv-store"
)

// KV transaction
type KVTX struct {
	concurrentreaderwriter.KVReader
	db *concurrentreaderwriter.KV
	// for the rollback
	tree struct {
		root uint64
		get  func(uint64) btree.BNode
		new  func(btree.BNode) uint64
		del  func(uint64)
	}
	free concurrentreaderwriter.FreeList
	page struct {
		nappend int // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer.
		// nil value denotes a deallocated page.
		updates map[uint64][]byte
	}
}

// begin a transaction
func Begin(kv *concurrentreaderwriter.KV, tx *KVTX) {
	tx.db = kv
	tx.page.updates = map[uint64][]byte{}
	// Copy mmap chunks to transaction
	tx.SetMmapChunks(kv.GetMmapChunks())
	kv.GetWriterMutex().Lock()
	// Copy version to transaction
	tx.SetVersion(kv.GetVersion())
	// btree
	tx.tree.root = kv.GetTree().GetRoot()
	tx.tree.get = tx.pageGet
	tx.tree.new = tx.pageNew
	tx.tree.del = tx.pageDel
	// freelist
	tx.free.FreeListData = *kv.GetFreeList()
	// Set freelist callbacks and version
	tx.free.SetVersion(kv.GetVersion())
	tx.free.SetGet(tx.pageGet)
	tx.free.SetNew(tx.pageAppend)
	tx.free.SetUse(tx.pageUse)
	tx.free.SetMinReader(kv.GetVersion())
	kv.GetMutex().Lock()
	readers := kv.GetReaders()
	if len(*readers) > 0 {
		// Get minimum reader version
		tx.free.SetMinReader((*readers)[0].GetVersion())
	}
	kv.GetMutex().Unlock()
}

// end a transaction: commit updates
func Commit(kv *concurrentreaderwriter.KV, tx *KVTX) error {
	defer kv.GetWriterMutex().Unlock()
	if kv.GetTree().GetRoot() == tx.tree.root {
		return nil // no updates?
	}
	// phase 1: persist the page data to disk.
	if err := kv.FlushPages(); err != nil {
		rollbackTX(tx)
		return err
	}
	// the page data must reach disk before the master page.
	// the `fsync` serves as a barrier here.
	if err := kv.Sync(); err != nil {
		rollbackTX(tx)
		return fmt.Errorf("fsync: %w", err)
	}
	// the transaction is visible at this point.
	kv.SetPageFlushed(kv.GetPageFlushed() + uint64(tx.page.nappend))
	*kv.GetFreeList() = tx.free.FreeListData
	kv.GetMutex().Lock()
	kv.GetTree().SetRoot(tx.tree.root)
	// kv.version++ - would need a setter for version
	kv.GetMutex().Unlock()
	// phase 2: update the master page to point to the new tree.
	// NOTE: Cannot rollback the tree to the old version if phase 2 fails.
	// Because there is no way to know the state of the master page.
	// Updating from an old root can cause corruption.
	if err := kv.StoreMaster(); err != nil {
		return err
	}
	if err := kv.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

// rollback the tree and other in-memory data structures.
func rollbackTX(tx *KVTX) {
	kv := tx.db
	kv.GetTree().SetRoot(tx.tree.root)
	// Reset page tracking
	kv.SetPageFlushed(0)
}

// end a transaction: rollback
func Abort(kv *concurrentreaderwriter.KV, tx *KVTX) {
	kv.GetWriterMutex().Unlock()
}

// KV operations
func (tx *KVTX) Get(key []byte) ([]byte, bool) {
	// Create a temporary BTree with transaction's callbacks
	tempTree := btree.BTree{}
	tempTree.SetRoot(tx.tree.root)
	tempTree.SetGet(tx.tree.get)
	tempTree.SetNew(tx.tree.new)
	tempTree.SetDel(tx.tree.del)
	return tempTree.Get(key)
}

func (tx *KVTX) Seek(key []byte, cmp int) *btree.BIter {
	// Create a temporary BTree with transaction's callbacks
	tempTree := btree.BTree{}
	tempTree.SetRoot(tx.tree.root)
	tempTree.SetGet(tx.tree.get)
	tempTree.SetNew(tx.tree.new)
	tempTree.SetDel(tx.tree.del)
	return tempTree.SeekLE(key)
}

func (tx *KVTX) Update(key []byte, val []byte, mode int) (bool, error) {
	// Create a temporary BTree with transaction's callbacks
	tempTree := btree.BTree{}
	tempTree.SetRoot(tx.tree.root)
	tempTree.SetGet(tx.tree.get)
	tempTree.SetNew(tx.tree.new)
	tempTree.SetDel(tx.tree.del)

	switch mode {
	case 0: // MODE_UPSERT
		tempTree.Insert(key, val)
		tx.tree.root = tempTree.GetRoot()
		return true, nil
	case 1: // MODE_UPDATE_ONLY
		if _, exists := tempTree.Get(key); exists {
			tempTree.Insert(key, val)
			tx.tree.root = tempTree.GetRoot()
			return true, nil
		}
		return false, nil
	case 2: // MODE_INSERT_ONLY
		if _, exists := tempTree.Get(key); !exists {
			tempTree.Insert(key, val)
			tx.tree.root = tempTree.GetRoot()
			return true, nil
		}
		return false, nil
	default:
		return false, fmt.Errorf("invalid mode: %d", mode)
	}
}

func (tx *KVTX) Del(key []byte) (bool, error) {
	// Create a temporary BTree with transaction's callbacks
	tempTree := btree.BTree{}
	tempTree.SetRoot(tx.tree.root)
	tempTree.SetGet(tx.tree.get)
	tempTree.SetNew(tx.tree.new)
	tempTree.SetDel(tx.tree.del)
	deleted := tempTree.Delete(key)
	tx.tree.root = tempTree.GetRoot()
	return deleted, nil
}

// Page management methods for transaction
func (tx *KVTX) pageGet(ptr uint64) btree.BNode {
	// Check if page is in transaction updates
	if data, exists := tx.page.updates[ptr]; exists {
		if data == nil {
			return btree.BNode{} // deallocated page
		}
		// Create BNode with data using constructor
		return btree.NewBNode(data)
	}
	// Use the original page getter
	return tx.db.GetTree().GetNode(ptr)
}

func (tx *KVTX) pageNew(node btree.BNode) uint64 {
	// Allocate new page in transaction
	ptr := uint64(tx.db.GetPageFlushed() + uint64(tx.page.nappend))
	tx.page.updates[ptr] = node.GetData()
	tx.page.nappend++
	return ptr
}

func (tx *KVTX) pageDel(ptr uint64) {
	// Mark page as deallocated in transaction
	tx.page.updates[ptr] = nil
}

func (tx *KVTX) pageAppend(node btree.BNode) uint64 {
	return tx.pageNew(node)
}

func (tx *KVTX) pageUse(ptr uint64, node btree.BNode) {
	tx.page.updates[ptr] = node.GetData()
}
