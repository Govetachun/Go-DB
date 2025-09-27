package query

// Value represents a value in the query system
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// Data types
const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// Insert modes
const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

// QLNode represents a node in the query language AST
type QLNode struct {
	Value Value // Type, I64, Str
	Kids  []QLNode
}

// syntax tree node types
const (
	QL_UNINIT = 0
	// scalar
	QL_STR = TYPE_BYTES
	QL_I64 = TYPE_INT64
	// binary ops
	QL_CMP_GE  = 10 // >=
	QL_CMP_GT  = 11 // >
	QL_CMP_LT  = 12 // <
	QL_CMP_LE  = 13 // <=
	QL_CMP_EQ  = 14 // =
	QL_CMP_NE  = 15 // !=
	QL_CMP_OR  = 16 // OR
	QL_CMP_AND = 17 // AND
	QL_CMP_ADD = 18 // +
	QL_CMP_SUB = 19 // -
	QL_CMP_MUL = 20 // *
	QL_CMP_DIV = 21 // /
	QL_CMP_MOD = 22 // %
	// unary ops
	QL_NOT = 50
	QL_NEG = 51
	// others
	QL_SYM = 100 // column
	QL_TUP = 101 // tuple
	QL_ERR = 200 // error; from parsing or evaluation
)

// common structure for queries: `INDEX BY`, `FILTER`, `LIMIT`
type QLScan struct {
	Table string
	// INDEX BY xxx
	Key1 QLNode // comparison, optional
	Key2 QLNode // comparison, optional
	// FILTER xxx
	Filter QLNode // boolean, optional
	// LIMIT x, y
	Offset int64
	Limit  int64
}

// stmt: select
type QLSelect struct {
	QLScan
	Names  []string // expr AS name
	Output []QLNode // expression list
}

// stmt: update
type QLUpdate struct {
	QLScan
	Names  []string
	Values []QLNode
}

// stmt: insert
type QLInsert struct {
	Table  string
	Mode   int
	Names  []string
	Values [][]QLNode
}

// stmt: delete
type QLDelete struct {
	QLScan
}

// stmt: create table
type QLCreateTable struct {
	Def TableDef // This will need to be imported from database package
}

// Parser represents the SQL parser state
type Parser struct {
	Input []byte
	Idx   int
	Err   error
}

// Placeholder for TableDef - this will be properly defined in database package
type TableDef struct {
	Name  string
	Cols  []string
	Types []uint32
	PKeys int
}
