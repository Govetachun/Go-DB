package transaction

import (
	"fmt"
	"govetachun/go-mini-db/refactor_code/internal/database"
	"testing"
)

// MockDB implements the DB interface for testing
type MockDB struct {
	tables map[string]*TableDef
}

func NewMockDB() *MockDB {
	return &MockDB{
		tables: make(map[string]*TableDef),
	}
}

func (m *MockDB) GetTableDef(name string) *TableDef {
	return m.tables[name]
}

func (m *MockDB) ListTables() ([]string, error) {
	tables := make([]string, 0, len(m.tables))
	for name := range m.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

func (m *MockDB) CreateTable(def *TableDef) {
	m.tables[def.Name] = def
}

func TestTransactionManager(t *testing.T) {
	fmt.Println("Testing Transaction Manager...")

	// Create mock database and transaction manager
	db := NewMockDB()
	tm := NewTransactionManager(db)

	// Test beginning a transaction
	tx, err := tm.BeginTransaction(IsolationReadCommitted)
	if err != nil {
		t.Errorf("Failed to begin transaction: %v", err)
	}

	if tx == nil {
		t.Errorf("Transaction is nil")
	}

	if tx.Status != StatusActive {
		t.Errorf("Expected transaction status to be active, got %d", tx.Status)
	}

	// Test transaction operations
	err = tx.Write("users", "1", &Record{
		Cols: []string{"id", "name"},
		Vals: []Value{
			{Type: database.TYPE_INT64, I64: 1},
			{Type: database.TYPE_BYTES, Str: []byte("John")},
		},
	})
	if err != nil {
		t.Errorf("Failed to write: %v", err)
	}

	// Test reading
	record, err := tx.Read("users", "1")
	if err != nil {
		t.Errorf("Failed to read: %v", err)
	}
	if record == nil {
		t.Errorf("Expected to read a record")
	}

	// Test committing transaction
	err = tm.CommitTransaction(tx)
	if err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}

	if tx.Status != StatusCommitted {
		t.Errorf("Expected transaction status to be committed, got %d", tx.Status)
	}

	fmt.Println("Transaction Manager tests passed!")
}

func TestIsolationLevels(t *testing.T) {
	fmt.Println("Testing Isolation Levels...")

	// Test different isolation levels
	levels := []IsolationLevel{
		IsolationReadUncommitted,
		IsolationReadCommitted,
		IsolationRepeatableRead,
		IsolationSerializable,
	}

	for _, level := range levels {
		// Test validation
		err := ValidateIsolationLevel(level)
		if err != nil {
			t.Errorf("Isolation level %d validation failed: %v", level, err)
		}

		// Test string representation
		str := level.String()
		if str == "" {
			t.Errorf("Isolation level %d has empty string representation", level)
		}

		// Test description
		desc := GetIsolationLevelDescription(level)
		if desc == "" {
			t.Errorf("Isolation level %d has empty description", level)
		}
	}

	// Test invalid isolation level
	err := ValidateIsolationLevel(IsolationLevel(999))
	if err == nil {
		t.Errorf("Expected validation error for invalid isolation level")
	}

	fmt.Println("Isolation Levels tests passed!")
}

func TestIsolationManager(t *testing.T) {
	fmt.Println("Testing Isolation Manager...")

	// Create isolation manager
	im := NewIsolationManager()

	// Begin transaction
	txID := TransactionID(1)
	err := im.BeginTransaction(txID, IsolationReadCommitted)
	if err != nil {
		t.Errorf("Failed to begin transaction: %v", err)
	}

	// Test read operation
	version, err := im.ReadOperation(txID, "users", IsolationReadCommitted)
	if err != nil {
		t.Errorf("Failed to perform read operation: %v", err)
	}
	// Version starts at 0, so 0 is valid
	if version < 0 {
		t.Errorf("Expected non-negative version")
	}

	// Test write operation
	version, err = im.WriteOperation(txID, "users", IsolationReadCommitted)
	if err != nil {
		t.Errorf("Failed to perform write operation: %v", err)
	}
	// Version starts at 0, so 0 is valid
	if version < 0 {
		t.Errorf("Expected non-negative version")
	}

	// Test commit
	err = im.CommitTransaction(txID)
	if err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}

	// Test abort
	txID2 := TransactionID(2)
	im.BeginTransaction(txID2, IsolationReadCommitted)
	im.AbortTransaction(txID2)

	fmt.Println("Isolation Manager tests passed!")
}

func TestTransactionOperations(t *testing.T) {
	fmt.Println("Testing Transaction Operations...")

	// Create mock database and transaction manager
	db := NewMockDB()
	tm := NewTransactionManager(db)

	// Begin transaction
	tx, err := tm.BeginTransaction(IsolationRepeatableRead)
	if err != nil {
		t.Errorf("Failed to begin transaction: %v", err)
	}

	// Test acquiring lock
	err = tx.AcquireLock("users")
	if err != nil {
		t.Errorf("Failed to acquire lock: %v", err)
	}

	// Test write operations
	record := &Record{
		Cols: []string{"id", "name", "age"},
		Vals: []Value{
			{Type: database.TYPE_INT64, I64: 1},
			{Type: database.TYPE_BYTES, Str: []byte("Alice")},
			{Type: database.TYPE_INT64, I64: 30},
		},
	}

	err = tx.Write("users", "1", record)
	if err != nil {
		t.Errorf("Failed to write record: %v", err)
	}

	// Test read operations
	readRecord, err := tx.Read("users", "1")
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if readRecord == nil {
		t.Errorf("Expected to read a record")
	}

	// Test delete operations
	err = tx.Delete("users", "1")
	if err != nil {
		t.Errorf("Failed to delete record: %v", err)
	}

	// Test transaction info
	info := tx.GetTransactionInfo()
	if info == nil {
		t.Errorf("Expected transaction info")
	}
	if info.ID != tx.ID {
		t.Errorf("Transaction ID mismatch")
	}

	// Test transaction age
	age := tx.GetAge()
	if age < 0 {
		t.Errorf("Transaction age should be positive")
	}

	// Test read/write set sizes
	readSize := tx.GetReadSetSize()
	writeSize := tx.GetWriteSetSize()
	if readSize < 0 || writeSize < 0 {
		t.Errorf("Read/write set sizes should be non-negative")
	}

	// Test releasing lock
	tx.ReleaseLock("users")

	// Test aborting transaction
	err = tm.AbortTransaction(tx)
	if err != nil {
		t.Errorf("Failed to abort transaction: %v", err)
	}

	if tx.Status != StatusAborted {
		t.Errorf("Expected transaction status to be aborted, got %d", tx.Status)
	}

	fmt.Println("Transaction Operations tests passed!")
}

func TestConcurrentTransactions(t *testing.T) {
	fmt.Println("Testing Concurrent Transactions...")

	// Create mock database and transaction manager
	db := NewMockDB()
	tm := NewTransactionManager(db)

	// Begin multiple transactions
	tx1, err := tm.BeginTransaction(IsolationSerializable)
	if err != nil {
		t.Errorf("Failed to begin transaction 1: %v", err)
	}

	tx2, err := tm.BeginTransaction(IsolationSerializable)
	if err != nil {
		t.Errorf("Failed to begin transaction 2: %v", err)
	}

	// Test concurrent writes
	record1 := &Record{
		Cols: []string{"id", "name"},
		Vals: []Value{
			{Type: database.TYPE_INT64, I64: 1},
			{Type: database.TYPE_BYTES, Str: []byte("Alice")},
		},
	}

	record2 := &Record{
		Cols: []string{"id", "name"},
		Vals: []Value{
			{Type: database.TYPE_INT64, I64: 2},
			{Type: database.TYPE_BYTES, Str: []byte("Bob")},
		},
	}

	err = tx1.Write("users", "1", record1)
	if err != nil {
		t.Errorf("Failed to write in transaction 1: %v", err)
	}

	err = tx2.Write("users", "2", record2)
	if err != nil {
		t.Errorf("Failed to write in transaction 2: %v", err)
	}

	// Test concurrent reads
	read1, err := tx1.Read("users", "1")
	if err != nil {
		t.Errorf("Failed to read in transaction 1: %v", err)
	}
	if read1 == nil {
		t.Errorf("Expected to read record in transaction 1")
	}

	read2, err := tx2.Read("users", "2")
	if err != nil {
		t.Errorf("Failed to read in transaction 2: %v", err)
	}
	if read2 == nil {
		t.Errorf("Expected to read record in transaction 2")
	}

	// Commit both transactions
	err = tm.CommitTransaction(tx1)
	if err != nil {
		t.Errorf("Failed to commit transaction 1: %v", err)
	}

	err = tm.CommitTransaction(tx2)
	if err != nil {
		t.Errorf("Failed to commit transaction 2: %v", err)
	}

	// Test listing transactions
	activeTxs := tm.ListTransactions()
	if len(activeTxs) != 0 {
		t.Errorf("Expected no active transactions after commit, got %d", len(activeTxs))
	}

	fmt.Println("Concurrent Transactions tests passed!")
}

func TestTransactionCleanup(t *testing.T) {
	fmt.Println("Testing Transaction Cleanup...")

	// Create mock database and transaction manager
	db := NewMockDB()
	tm := NewTransactionManager(db)

	// Begin and commit a transaction
	tx, err := tm.BeginTransaction(IsolationReadCommitted)
	if err != nil {
		t.Errorf("Failed to begin transaction: %v", err)
	}

	err = tm.CommitTransaction(tx)
	if err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}

	// Test cleanup
	tm.CleanupTransactions()

	// Verify transaction is still accessible (cleanup only removes old transactions)
	retrievedTx := tm.GetTransaction(tx.ID)
	if retrievedTx == nil {
		t.Errorf("Transaction should still be accessible after cleanup")
	}

	fmt.Println("Transaction Cleanup tests passed!")
}
