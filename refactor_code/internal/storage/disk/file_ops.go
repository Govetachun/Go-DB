package disk

import (
	"fmt"
	"govetachun/go-mini-db/refactor_code/internal/storage/btree"
	"govetachun/go-mini-db/refactor_code/pkg/utils"
	"os"
	"syscall"
)

const DB_SIG = "BuildYourOwnDB06"

func (db *KV) Open() error {
	// open or create the DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp
	// create the initial mmap
	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}
	// btree callbacks
	db.tree.SetGet(db.pageGet)
	db.tree.SetNew(db.pageNew)
	db.tree.SetDel(db.pageDel)
	// read the master page
	err = masterLoad(db)
	if err != nil {
		goto fail
	}
	// done
	return nil
fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		utils.Assert(err == nil, "err == nil")
	}
	_ = db.fp.Close()
}

func writePages(db *KV) error {
	// update the free list
	freed := []uint64{}
	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.free.Update(db.page.nfree, freed)
	// extend the file & mmap if needed
	npages := int(db.page.flushed) + db.free.Total()
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}
	// copy data to the file
	for ptr, page := range db.page.updates {
		if page != nil {
			copy(pageGetMapped(db, ptr).GetData(), page)
		}
	}
	return nil
}

// persist the newly allocated pages after updates
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |
func syncPages(db *KV) error {
	// flush data to the disk. must be done before updating the master page.
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	db.page.flushed += uint64(db.free.Total())
	db.free.head = db.free.head
	db.page.nfree = 0
	db.page.nappend = 0
	// update & flush the master page
	if err := masterStore(db); err != nil {
		return err
	}
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

// read the db
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

func (db *KV) Update(key []byte, val []byte, mode int) (bool, error) {
	switch mode {
	case 0: // MODE_UPSERT - insert or replace
		db.tree.Insert(key, val)
		return true, flushPages(db)
	case 1: // MODE_UPDATE_ONLY - update existing keys
		if _, exists := db.tree.Get(key); exists {
			db.tree.Insert(key, val)
			return true, flushPages(db)
		}
		return false, nil
	case 2: // MODE_INSERT_ONLY - only add new keys
		if _, exists := db.tree.Get(key); !exists {
			db.tree.Insert(key, val)
			return true, flushPages(db)
		}
		return false, nil
	default:
		return false, fmt.Errorf("invalid mode: %d", mode)
	}
}

// Helper functions for master page operations (simplified implementations)
func masterLoad(db *KV) error {
	// Load master page - simplified implementation
	if db.mmap.file == 0 {
		// Empty file, initialize
		db.page.updates = make(map[uint64][]byte)
		return nil
	}
	return nil
}

func masterStore(db *KV) error {
	// Store master page - simplified implementation
	return nil
}

// Exported methods for transaction use
func (db *KV) GetRoot() uint64 {
	return db.tree.GetRoot()
}

func (db *KV) SetRoot(root uint64) {
	db.tree.SetRoot(root)
}

func (db *KV) GetFreeHead() uint64 {
	return db.free.head
}

func (db *KV) SetFreeHead(head uint64) {
	db.free.head = head
}

func (db *KV) FlushPages() error {
	return flushPages(db)
}

func (db *KV) Sync() error {
	return db.fp.Sync()
}

func (db *KV) StoreMaster() error {
	return masterStore(db)
}

func (db *KV) ResetPages() {
	db.page.nfree = 0
	db.page.nappend = 0
	db.page.updates = map[uint64][]byte{}
}

func (db *KV) GetTree() *btree.BTree {
	return &db.tree
}
