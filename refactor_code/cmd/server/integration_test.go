package main

import (
	"fmt"
	"testing"

	"govetachun/go-mini-db/refactor_code/internal/query/executor"
	"govetachun/go-mini-db/refactor_code/internal/query/parser"
	"govetachun/go-mini-db/refactor_code/internal/storage"
	"govetachun/go-mini-db/refactor_code/internal/transaction"
)

func TestDatabaseIntegration(t *testing.T) {
	fmt.Println("Testing Database Integration...")

	// Initialize the key-value store
	store := storage.NewKVStore("./test_integration.db")

	// Open the store
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer store.Close()

	// Create a simple database instance
	db := &SimpleDB{
		store:  store,
		tables: make(map[string]*executor.TableDef),
	}

	// Create a transaction
	txImpl := transaction.NewDBTX(db)

	// Begin the transaction
	if err := txImpl.Begin(); err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create executor-compatible transaction wrapper
	tx := &ExecutorTX{txImpl}

	// Ensure transaction is committed at the end
	defer func() {
		if err := txImpl.Commit(); err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}
	}()

	// Test 1: Create a table
	fmt.Println("Test 1: Creating a table...")
	tableDef := &executor.TableDef{
		Name:  "users",
		Cols:  []string{"id", "name", "age"},
		Types: []uint32{executor.TYPE_INT64, executor.TYPE_BYTES, executor.TYPE_INT64},
		PKeys: 1,
	}

	err := tx.TableNew(tableDef)
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	// Test 2: Insert a record
	fmt.Println("Test 2: Inserting a record...")
	record := executor.Record{
		Cols: []string{"id", "name", "age"},
		Vals: []executor.Value{
			{Type: executor.TYPE_INT64, I64: 1},
			{Type: executor.TYPE_BYTES, Str: []byte("John")},
			{Type: executor.TYPE_INT64, I64: 25},
		},
	}

	err = tx.Insert("users", record)
	if err != nil {
		t.Errorf("Failed to insert record: %v", err)
	}

	// Test 3: Get a record
	fmt.Println("Test 3: Getting a record...")
	key := executor.Record{
		Cols: []string{"id"},
		Vals: []executor.Value{
			{Type: executor.TYPE_INT64, I64: 1},
		},
	}

	retrievedRecord, err := tx.Get("users", key)
	if err != nil {
		t.Errorf("Failed to get record: %v", err)
	}
	if retrievedRecord != nil {
		fmt.Printf("Retrieved record: %+v\n", retrievedRecord)
	}

	// Test 4: Update a record
	fmt.Println("Test 4: Updating a record...")
	updatedRecord := executor.Record{
		Cols: []string{"id", "name", "age"},
		Vals: []executor.Value{
			{Type: executor.TYPE_INT64, I64: 1},
			{Type: executor.TYPE_BYTES, Str: []byte("John Updated")},
			{Type: executor.TYPE_INT64, I64: 26},
		},
	}

	err = tx.Update("users", key, updatedRecord)
	if err != nil {
		t.Errorf("Failed to update record: %v", err)
	}

	// Test 5: Delete a record
	fmt.Println("Test 5: Deleting a record...")
	deleted, err := tx.Delete("users", key)
	if err != nil {
		t.Errorf("Failed to delete record: %v", err)
	}
	if deleted {
		fmt.Println("Record deleted successfully")
	}

	// Test 6: List tables
	fmt.Println("Test 6: Listing tables...")
	tables, err := db.ListTables()
	if err != nil {
		t.Errorf("Failed to list tables: %v", err)
	}
	fmt.Printf("Tables: %v\n", tables)

	// Test 7: Parse and execute a simple query
	fmt.Println("Test 7: Parsing and executing a query...")
	sqlQuery := `SELECT id, name FROM users WHERE age > 18`

	stmt, err := parser.Parse([]byte(sqlQuery))
	if err != nil {
		t.Errorf("Parse error: %v", err)
		return
	}

	fmt.Printf("Parsed statement type: %T\n", stmt)

	result, err := executor.ExecuteQuery(stmt, tx)
	if err != nil {
		t.Errorf("Execution error: %v", err)
		return
	}

	fmt.Printf("Query result: %v\n", result)

	fmt.Println("All tests completed successfully!")
}

func TestScannerMethods(t *testing.T) {
	fmt.Println("Testing Scanner Methods...")

	// Create a scanner
	scanner := &executor.Scanner{}

	// Create some test records
	records := []executor.Record{
		{
			Cols: []string{"id", "name"},
			Vals: []executor.Value{
				{Type: executor.TYPE_INT64, I64: 1},
				{Type: executor.TYPE_BYTES, Str: []byte("Alice")},
			},
		},
		{
			Cols: []string{"id", "name"},
			Vals: []executor.Value{
				{Type: executor.TYPE_INT64, I64: 2},
				{Type: executor.TYPE_BYTES, Str: []byte("Bob")},
			},
		},
	}

	// Set records for scanning
	scanner.SetRecords(records)

	// Test scanning
	var scannedRecords []executor.Record
	for scanner.Valid() {
		var record executor.Record
		scanner.Deref(&record)
		scannedRecords = append(scannedRecords, record)
		fmt.Printf("Scanned record: %+v\n", record)
		scanner.Next()
	}

	if len(scannedRecords) != len(records) {
		t.Errorf("Expected %d records, got %d", len(records), len(scannedRecords))
	}

	fmt.Println("Scanner test completed successfully!")
}
