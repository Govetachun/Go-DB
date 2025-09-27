package storage

import (
	"govetachun/go-mini-db/refactor_code/internal/storage/btree"
	"govetachun/go-mini-db/refactor_code/internal/storage/disk"
)

// KVStore represents the main key-value store interface
type KVStore interface {
	Open() error
	Close()
	Get(key []byte) ([]byte, bool)
	Set(key []byte, val []byte) error
	Del(key []byte) (bool, error)
	Update(key []byte, val []byte, mode int) (bool, error)
}

// NewKVStore creates a new key-value store
func NewKVStore(path string) KVStore {
	return &disk.KV{Path: path}
}

// Re-export important types from btree package
type BTree = btree.BTree
type BNode = btree.BNode
type BIter = btree.BIter
