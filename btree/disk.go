package btree

import (
	"errors"
	"fmt"
	"govetachun/go-mini-db/utils"
	"os"
	"syscall"
)

// create the initial mmap that covers the whole file.
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}
	if fi.Size()%BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("File size is not a multiple of page size.")
	}
	mmapSize := 64 << 20
	utils.Assert(mmapSize%BTREE_PAGE_SIZE == 0, "mmapSize%BTREE_PAGE_SIZE == 0")
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}
	// mmapSize can be larger than the file
	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}
	return int(fi.Size()), chunk, nil
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
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}
}

// extend the mmap by adding new mappings.

func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil // enough range
	}
	alloc := max(db.mmap.total, 64<<20) // double the current address space
	for db.mmap.total+alloc < size {
		alloc *= 2 // still not enough?
	}
	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), alloc,
		syscall.PROT_READ, syscall.MAP_SHARED, // read-only
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

// callback for BTree, read a page.
func (db *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

// callback for BTree, append a page.
func (db *KV) pageAppend(node []byte) uint64 {
	ptr := db.page.flushed + uint64(len(db.page.temp)) // just append
	db.page.temp = append(db.page.temp, node)
	return ptr
}

func writePages(db *KV) error {
	// extend the file & mmap if needed
	npages := int(db.page.flushed) + len(db.page.temp)
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}
	// copy data to the file
	for i, page := range db.page.temp {
		ptr := db.page.flushed + uint64(i)
		copy(db.pageRead(ptr), page)
	}
	return nil
}

// callback for BTree, allocate a new page.
func (db *KV) pageNew(node BNode) uint64 {
	// TODO: reuse deallocated pages
	utils.Assert(len(node.data) <= BTREE_PAGE_SIZE, "len(node.data) <= BTREE_PAGE_SIZE")
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node.data)
	return ptr
}

// callback for BTree, deallocate a page.
func (db *KV) pageDel(uint64) {
	// TODO: implement this
}

// extend the file to at least `npages`.
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}
	for filePages < npages {
		// the file size is increased exponentially,
		// so that we don't have to extend the file for every update.
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}
	fileSize := filePages * BTREE_PAGE_SIZE
	err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}
	db.mmap.file = fileSize
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
    db.page.flushed += uint64(len(db.page.temp))
    db.page.temp = db.page.temp[:0]
    // update & flush the master page
    if err := masterStore(db); err != nil {
    return err
    }
    if err := db.fp.Sync(); err != nil {
    return fmt.Errorf("fsync: %w", err)
    }
    return nil
    }
    