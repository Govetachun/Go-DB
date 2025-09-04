package btree

import (
	"govetachun/go-mini-db/utils"
	"unsafe"
	"fmt"
)

type C struct {
    tree  BTree
    ref   map[string]string // the reference data
    pages map[uint64]BNode  // in-memory pages
}

func newC() *C {
    pages := map[uint64]BNode{}
    return &C{
        tree: BTree{
            get: func(ptr uint64) []byte {
                node, ok := pages[ptr]
                utils.Assert(ok, "node not found")
                return node
            },
            new: func(node []byte) uint64 {
                utils.Assert(BNode(node).nbytes() <= BTREE_PAGE_SIZE, "node size exceeds page size")
                ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
                utils.Assert(pages[ptr] == nil, "node already exists")
                pages[ptr] = node
                return ptr
            },
            del: func(ptr uint64) {
                utils.Assert(pages[ptr] != nil, "node not found")
                delete(pages, ptr)
            },
        },
        ref:   map[string]string{},
        pages: pages,
    }
}

func (c *C) Add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) Del(key string) bool {
	delete(c.ref, key)
	_, err := c.tree.Delete([]byte(key))
	return err == nil
}

func (c *C) PrintTree() {
	// fmt.Printf("Root page: %d\n", c.pages[c.tree.root])
	fmt.Println("Pages:")
	for pt, node := range c.pages {
		fmt.Println("Pointer:", pt)
		fmt.Println("BNode data:", node)
	}
}