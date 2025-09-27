package disk

import (
	"errors"
	"fmt"
	"govetachun/go-mini-db/refactor_code/pkg/utils"
	"os"
	"syscall"
)

const BTREE_PAGE_SIZE = 4096

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

// Remove KV definition from here as it's defined in page_manager.go
