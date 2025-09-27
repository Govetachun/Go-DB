package executor

import (
	"fmt"
	"govetachun/go-mini-db/refactor_code/internal/database"
	"govetachun/go-mini-db/refactor_code/internal/query"
)

// Re-export types
type Record = database.Record
type Value = database.Value
type Scanner = database.Scanner
type TableDef = database.TableDef

type QLNode = query.QLNode
type QLSelect = query.QLSelect
type QLInsert = query.QLInsert
type QLUpdate = query.QLUpdate
type QLDelete = query.QLDelete
type QLCreateTable = query.QLCreateTable
type QLScan = query.QLScan

// Re-export constants
const (
	TYPE_INT64 = database.TYPE_INT64
	TYPE_BYTES = database.TYPE_BYTES
	TYPE_ERROR = database.TYPE_ERROR

	CMP_GE = database.CMP_GE
	CMP_LE = database.CMP_LE

	QL_SYM    = query.QL_SYM
	QL_I64    = query.QL_I64
	QL_STR    = query.QL_STR
	QL_NEG    = query.QL_NEG
	QL_CMP_EQ = query.QL_CMP_EQ
	QL_UNINIT = query.QL_UNINIT
)

// DBTX represents a database transaction interface
type DBTX interface {
	TableNew(def *TableDef) error
	Scan(table string, scanner *Scanner) error
	Delete(table string, key Record) (bool, error)
	GetDB() DB
	// Additional methods needed for full functionality
	Insert(table string, record Record) error
	Get(table string, key Record) (*Record, error)
	Update(table string, key Record, record Record) error
	DropTable(tableName string) error
	AlterTable(tableName string, newDef *TableDef) error
	CreateIndex(indexName string, tableName string, columnNames []string) error
	DropIndex(indexName string) error
	TruncateTable(tableName string) error
	RenameTable(oldName string, newName string) error
}

// DB represents a database interface
type DB interface {
	GetTableDef(name string) *TableDef
	ListTables() ([]string, error)
}

// QLEvalContex represents the evaluation context for expressions
type QLEvalContex struct {
	env Record // optional row values
	out Value
	err error
}

// ExecuteQuery is the main entry point for query execution
func ExecuteQuery(stmt interface{}, tx DBTX) (interface{}, error) {
	switch s := stmt.(type) {
	case *QLSelect:
		return ExecuteSelect(s, tx)
	case *QLInsert:
		return ExecuteInsert(s, tx)
	case *QLUpdate:
		return ExecuteUpdate(s, tx)
	case *QLDelete:
		return ExecuteDelete(s, tx)
	case *QLCreateTable:
		return nil, ExecuteCreateTable(s, tx)
	default:
		return nil, fmt.Errorf("unknown statement type")
	}
}
