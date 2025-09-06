package btree

import (
	"fmt"
	"govetachun/go-mini-db/utils"
	"unsafe"
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
				return node.data
			},
			new: func(data []byte) uint64 {
				node := BNode{data: data}
				utils.Assert(node.nbytes() <= BTREE_PAGE_SIZE, "node too large")
				key := uint64(uintptr(unsafe.Pointer(&data[0])))
				utils.Assert(len(pages[key].data) == 0, "node already exists")
				pages[key] = node
				return key
			},

			del: func(ptr uint64) {
				_, ok := pages[ptr]
				utils.Assert(ok, "node not found")
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) del(key string) bool {
	found := c.tree.Delete([]byte(key))
	if found {
		delete(c.ref, key)
	}
	return found
}

func (c *C) PrintTree() {
	// fmt.Printf("Root page: %d\n", c.pages[c.tree.root])
	fmt.Println("Pages:")
	for pt, node := range c.pages {
		fmt.Println("Pointer:", pt)
		fmt.Println("BNode data:", node)
	}
}
