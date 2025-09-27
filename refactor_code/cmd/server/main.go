package main

import (
	"fmt"
	"log"

	"govetachun/go-mini-db/refactor_code/internal/query/executor"
	"govetachun/go-mini-db/refactor_code/internal/query/parser"
	"govetachun/go-mini-db/refactor_code/internal/storage"
	"govetachun/go-mini-db/refactor_code/internal/transaction"
)

// SimpleDB represents a simple database implementation
type SimpleDB struct {
	store  storage.KVStore
	tables map[string]*executor.TableDef
}

// GetTableDef returns a table definition
func (db *SimpleDB) GetTableDef(name string) *executor.TableDef {
	return db.tables[name]
}

// ListTables returns a list of all table names
func (db *SimpleDB) ListTables() ([]string, error) {
	tables := make([]string, 0, len(db.tables))
	for name := range db.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

// ExecutorTX wraps transaction.DBTX to implement executor.DBTX
type ExecutorTX struct {
	tx *transaction.DBTX
}

func (etx *ExecutorTX) TableNew(def *executor.TableDef) error {
	// Convert to transaction.TableDef
	txDef := &transaction.TableDef{
		Name:  def.Name,
		Cols:  def.Cols,
		Types: def.Types,
		PKeys: def.PKeys,
	}
	return etx.tx.TableNew(txDef)
}

func (etx *ExecutorTX) Scan(table string, scanner *executor.Scanner) error {
	// Convert to transaction.Scanner
	txScanner := &transaction.Scanner{
		Key1: transaction.Record{Cols: scanner.Key1.Cols, Vals: convertValues(scanner.Key1.Vals)},
		Key2: transaction.Record{Cols: scanner.Key2.Cols, Vals: convertValues(scanner.Key2.Vals)},
		Cmp1: scanner.Cmp1,
		Cmp2: scanner.Cmp2,
	}
	return etx.tx.Scan(table, txScanner)
}

func (etx *ExecutorTX) Delete(table string, key executor.Record) (bool, error) {
	// Convert to transaction.Record
	txKey := transaction.Record{Cols: key.Cols, Vals: convertValues(key.Vals)}
	return etx.tx.Delete(table, txKey)
}

func (etx *ExecutorTX) Insert(table string, record executor.Record) error {
	// Convert to transaction.Record
	txRecord := transaction.Record{Cols: record.Cols, Vals: convertValues(record.Vals)}
	return etx.tx.Insert(table, txRecord)
}

func (etx *ExecutorTX) Get(table string, key executor.Record) (*executor.Record, error) {
	// Convert to transaction.Record
	txKey := transaction.Record{Cols: key.Cols, Vals: convertValues(key.Vals)}
	txRecord, err := etx.tx.Get(table, txKey)
	if err != nil {
		return nil, err
	}
	if txRecord == nil {
		return nil, nil
	}

	// Convert back to executor.Record
	result := executor.Record{Cols: txRecord.Cols, Vals: convertValuesFromTransaction(txRecord.Vals)}
	return &result, nil
}

func (etx *ExecutorTX) Update(table string, key executor.Record, record executor.Record) error {
	// Convert to transaction.Record
	txKey := transaction.Record{Cols: key.Cols, Vals: convertValues(key.Vals)}
	txRecord := transaction.Record{Cols: record.Cols, Vals: convertValues(record.Vals)}
	return etx.tx.Update(table, txKey, txRecord)
}

func (etx *ExecutorTX) DropTable(tableName string) error {
	return etx.tx.DropTable(tableName)
}

func (etx *ExecutorTX) AlterTable(tableName string, newDef *executor.TableDef) error {
	// Convert to transaction.TableDef
	txDef := &transaction.TableDef{
		Name:  newDef.Name,
		Cols:  newDef.Cols,
		Types: newDef.Types,
		PKeys: newDef.PKeys,
	}
	return etx.tx.AlterTable(tableName, txDef)
}

func (etx *ExecutorTX) CreateIndex(indexName string, tableName string, columnNames []string) error {
	return etx.tx.CreateIndex(indexName, tableName, columnNames)
}

func (etx *ExecutorTX) DropIndex(indexName string) error {
	return etx.tx.DropIndex(indexName)
}

func (etx *ExecutorTX) TruncateTable(tableName string) error {
	return etx.tx.TruncateTable(tableName)
}

func (etx *ExecutorTX) RenameTable(oldName string, newName string) error {
	return etx.tx.RenameTable(oldName, newName)
}

func (etx *ExecutorTX) GetDB() executor.DB {
	return &ExecutorDBWrapper{db: etx.tx.GetDB()}
}

// ExecutorDBWrapper wraps transaction.DB to implement executor.DB
type ExecutorDBWrapper struct {
	db transaction.DB
}

func (w *ExecutorDBWrapper) GetTableDef(name string) *executor.TableDef {
	txDef := w.db.GetTableDef(name)
	if txDef == nil {
		return nil
	}

	// Convert transaction.TableDef to executor.TableDef
	return &executor.TableDef{
		Name:  txDef.Name,
		Cols:  txDef.Cols,
		Types: txDef.Types,
		PKeys: txDef.PKeys,
	}
}

func (w *ExecutorDBWrapper) ListTables() ([]string, error) {
	return w.db.ListTables()
}

// Helper function to convert values from executor to transaction
func convertValues(vals []executor.Value) []transaction.Value {
	result := make([]transaction.Value, len(vals))
	for i, v := range vals {
		result[i] = transaction.Value{Type: v.Type, I64: v.I64, Str: v.Str}
	}
	return result
}

// Helper function to convert values from transaction to executor
func convertValuesFromTransaction(vals []transaction.Value) []executor.Value {
	result := make([]executor.Value, len(vals))
	for i, v := range vals {
		result[i] = executor.Value{Type: v.Type, I64: v.I64, Str: v.Str}
	}
	return result
}

func main() {
	fmt.Println("Starting Go Mini DB Server...")

	// Initialize the key-value store
	store := storage.NewKVStore("./test.db")

	// Open the store
	if err := store.Open(); err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer store.Close()

	// Example usage
	fmt.Println("Database opened successfully!")

	// Example SQL query
	sqlQuery := `SELECT id, name FROM users WHERE age > 18`

	// Parse the query
	stmt, err := parser.Parse([]byte(sqlQuery))
	if err != nil {
		log.Printf("Parse error: %v", err)
		return
	}

	fmt.Printf("Parsed statement type: %T\n", stmt)

	// Create a simple database instance
	db := &SimpleDB{
		store:  store,
		tables: make(map[string]*executor.TableDef),
	}

	// Create a transaction
	txImpl := transaction.NewDBTX(db)

	// Create executor-compatible transaction wrapper
	tx := &ExecutorTX{txImpl}

	// Execute the query (this would fail in current implementation as it's simplified)
	result, err := executor.ExecuteQuery(stmt, tx)
	if err != nil {
		log.Printf("Execution error: %v", err)
		return
	}

	fmt.Printf("Query result: %v\n", result)

	// Example key-value operations
	fmt.Println("\nTesting key-value operations:")

	// Set a key-value pair
	err = store.Set([]byte("test_key"), []byte("test_value"))
	if err != nil {
		log.Printf("Set error: %v", err)
	} else {
		fmt.Println("Successfully set test_key")
	}

	// Get the value
	value, found := store.Get([]byte("test_key"))
	if found {
		fmt.Printf("Retrieved value: %s\n", string(value))
	} else {
		fmt.Println("Key not found")
	}

	// Delete the key
	deleted, err := store.Del([]byte("test_key"))
	if err != nil {
		log.Printf("Delete error: %v", err)
	} else if deleted {
		fmt.Println("Successfully deleted test_key")
	} else {
		fmt.Println("Key was not found for deletion")
	}

	fmt.Println("Go Mini DB Server completed successfully!")
}
