package relationaldb

import btree "govetachun/go-mini-db/kv-store"

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)
const (
	CMP_GE = +3 // >=
	CMP_GT = +2 // >
	CMP_LT = -2 // <
	CMP_LE = -3 // <=
)
const TABLE_PREFIX_MIN = 3
const (
	INDEX_ADD = 1
	INDEX_DEL = 2
)

// table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}
type DB struct {
	Path string
	// internals
	kv     *btree.BTree
	tables map[string]*TableDef // cached table definition
}

// GetKV returns the underlying KV store (exported for transaction use)
func (db *DB) GetKV() *btree.BTree {
	return db.kv
}

// GetTableDef returns a table definition by name (exported for query execution use)
func (db *DB) GetTableDef(name string) *TableDef {
	return db.tables[name]
}

// table definition
type TableDef struct {
	// user defined
	Name    string
	Types   []uint32 // column types
	Cols    []string // column names
	PKeys   int      // the first `PKeys` columns are the primary key
	Indexes [][]string
	// auto-assigned B-tree key prefixes for different tables/indexes
	Prefix        uint32
	IndexPrefixes []uint32
}

// internal table: metadata
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

// table row
type Record struct {
	Cols []string
	Vals []Value
}

// modes of the updates
const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

type InsertReq struct {
	tree *btree.BTree
	// out
	Added   bool   // added a new key
	Updated bool   // added a new key or an old key was changed
	Old     []byte // the value before the update
	// in
	Key  []byte
	Val  []byte
	Mode int
}
type DeleteReq struct {
	tree *btree.BTree
	// in
	Key []byte
	// out
	Old []byte
}
