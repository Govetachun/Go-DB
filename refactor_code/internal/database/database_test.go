package database

import (
	"fmt"
	"testing"
)

func TestTableManager(t *testing.T) {
	fmt.Println("Testing Table Manager...")

	// Create table manager
	tm := NewTableManager()

	// Create a table definition
	tableDef := &TableDef{
		Name:  "users",
		Cols:  []string{"id", "name", "age"},
		Types: []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64},
		PKeys: 1,
	}

	// Test table creation
	err := tm.CreateTable(tableDef)
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	// Test getting table
	table := tm.GetTable("users")
	if table == nil {
		t.Errorf("Failed to get table")
	}

	// Test listing tables
	tables := tm.ListTables()
	if len(tables) != 1 || tables[0] != "users" {
		t.Errorf("Expected 1 table named 'users', got %v", tables)
	}

	fmt.Println("Table Manager tests passed!")
}

func TestTableOperations(t *testing.T) {
	fmt.Println("Testing Table Operations...")

	// Create table manager and table
	tm := NewTableManager()
	tableDef := &TableDef{
		Name:  "users",
		Cols:  []string{"id", "name", "age"},
		Types: []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64},
		PKeys: 1,
	}
	tm.CreateTable(tableDef)
	table := tm.GetTable("users")

	// Test record insertion
	record := Record{
		Cols: []string{"id", "name", "age"},
		Vals: []Value{
			{Type: TYPE_INT64, I64: 1},
			{Type: TYPE_BYTES, Str: []byte("John")},
			{Type: TYPE_INT64, I64: 25},
		},
	}

	err := table.Insert(record)
	if err != nil {
		t.Errorf("Failed to insert record: %v", err)
	}

	// Test record retrieval
	key := Record{
		Cols: []string{"id"},
		Vals: []Value{{Type: TYPE_INT64, I64: 1}},
	}

	retrievedRecord, err := table.Get(key)
	if err != nil {
		t.Errorf("Failed to get record: %v", err)
	}
	if retrievedRecord == nil {
		t.Errorf("Record not found")
	} else {
		fmt.Printf("Retrieved record: %+v\n", retrievedRecord)
	}

	// Test record update
	updatedRecord := Record{
		Cols: []string{"id", "name", "age"},
		Vals: []Value{
			{Type: TYPE_INT64, I64: 1},
			{Type: TYPE_BYTES, Str: []byte("John Updated")},
			{Type: TYPE_INT64, I64: 26},
		},
	}

	err = table.Update(key, updatedRecord)
	if err != nil {
		t.Errorf("Failed to update record: %v", err)
	}

	// Test record deletion
	deleted, err := table.Delete(key)
	if err != nil {
		t.Errorf("Failed to delete record: %v", err)
	}
	if !deleted {
		t.Errorf("Record deletion failed")
	}

	fmt.Println("Table Operations tests passed!")
}

func TestIndexManager(t *testing.T) {
	fmt.Println("Testing Index Manager...")

	// Create index manager
	im := NewIndexManager()

	// Create an index
	err := im.CreateIndex("idx_name", "users", []string{"name"}, []uint32{TYPE_BYTES})
	if err != nil {
		t.Errorf("Failed to create index: %v", err)
	}

	// Test getting index
	index := im.GetIndex("idx_name")
	if index == nil {
		t.Errorf("Failed to get index")
	}

	// Test listing indexes
	indexes := im.ListIndexes()
	if len(indexes) != 1 || indexes[0] != "idx_name" {
		t.Errorf("Expected 1 index named 'idx_name', got %v", indexes)
	}

	// Test listing indexes for table
	tableIndexes := im.ListIndexesForTable("users")
	if len(tableIndexes) != 1 || tableIndexes[0] != "idx_name" {
		t.Errorf("Expected 1 index for table 'users', got %v", tableIndexes)
	}

	fmt.Println("Index Manager tests passed!")
}

func TestIndexOperations(t *testing.T) {
	fmt.Println("Testing Index Operations...")

	// Create index manager and index
	im := NewIndexManager()
	im.CreateIndex("idx_name", "users", []string{"name"}, []uint32{TYPE_BYTES})
	index := im.GetIndex("idx_name")

	// Create a record
	record := &Record{
		Cols: []string{"id", "name", "age"},
		Vals: []Value{
			{Type: TYPE_INT64, I64: 1},
			{Type: TYPE_BYTES, Str: []byte("John")},
			{Type: TYPE_INT64, I64: 25},
		},
	}

	// Test adding record to index
	err := index.Add(record)
	if err != nil {
		t.Errorf("Failed to add record to index: %v", err)
	}

	// Test lookup
	lookupKey := Record{
		Cols: []string{"name"},
		Vals: []Value{{Type: TYPE_BYTES, Str: []byte("John")}},
	}

	records, err := index.Lookup(lookupKey)
	if err != nil {
		t.Errorf("Failed to lookup records: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("Expected 1 record, got %d", len(records))
	}

	// Test removing record from index
	err = index.Remove(record)
	if err != nil {
		t.Errorf("Failed to remove record from index: %v", err)
	}

	fmt.Println("Index Operations tests passed!")
}

func TestSchemaManager(t *testing.T) {
	fmt.Println("Testing Schema Manager...")

	// Create schema manager
	sm := NewSchemaManager()

	// Create a table definition
	tableDef := &TableDef{
		Name:  "users",
		Cols:  []string{"id", "name", "age"},
		Types: []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64},
		PKeys: 1,
	}

	// Test table creation
	err := sm.CreateTable(tableDef)
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	// Test getting table definition
	retrievedDef := sm.GetTableDef("users")
	if retrievedDef == nil {
		t.Errorf("Failed to get table definition")
	}

	// Test listing tables
	tables := sm.ListTables()
	if len(tables) != 1 || tables[0] != "users" {
		t.Errorf("Expected 1 table named 'users', got %v", tables)
	}

	// Test adding column
	err = sm.AddColumn("users", "email", TYPE_BYTES)
	if err != nil {
		t.Errorf("Failed to add column: %v", err)
	}

	// Test dropping column
	err = sm.DropColumn("users", "email")
	if err != nil {
		t.Errorf("Failed to drop column: %v", err)
	}

	// Test renaming table
	err = sm.RenameTable("users", "customers")
	if err != nil {
		t.Errorf("Failed to rename table: %v", err)
	}

	// Test dropping table
	err = sm.DropTable("customers")
	if err != nil {
		t.Errorf("Failed to drop table: %v", err)
	}

	fmt.Println("Schema Manager tests passed!")
}

func TestIntegration(t *testing.T) {
	fmt.Println("Testing Database Integration...")

	// Create managers
	tm := NewTableManager()
	im := NewIndexManager()
	sm := NewSchemaManager()

	// Create table schema
	tableDef := &TableDef{
		Name:  "products",
		Cols:  []string{"id", "name", "price", "category"},
		Types: []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64, TYPE_BYTES},
		PKeys: 1,
	}

	// Create table in schema manager
	err := sm.CreateTable(tableDef)
	if err != nil {
		t.Errorf("Failed to create table schema: %v", err)
	}

	// Create table in table manager
	err = tm.CreateTable(tableDef)
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	// Create index
	err = im.CreateIndex("idx_category", "products", []string{"category"}, []uint32{TYPE_BYTES})
	if err != nil {
		t.Errorf("Failed to create index: %v", err)
	}

	// Get table and index
	table := tm.GetTable("products")
	index := im.GetIndex("idx_category")

	// Insert records
	records := []Record{
		{
			Cols: []string{"id", "name", "price", "category"},
			Vals: []Value{
				{Type: TYPE_INT64, I64: 1},
				{Type: TYPE_BYTES, Str: []byte("Laptop")},
				{Type: TYPE_INT64, I64: 999},
				{Type: TYPE_BYTES, Str: []byte("Electronics")},
			},
		},
		{
			Cols: []string{"id", "name", "price", "category"},
			Vals: []Value{
				{Type: TYPE_INT64, I64: 2},
				{Type: TYPE_BYTES, Str: []byte("Book")},
				{Type: TYPE_INT64, I64: 29},
				{Type: TYPE_BYTES, Str: []byte("Books")},
			},
		},
	}

	for _, record := range records {
		err := table.Insert(record)
		if err != nil {
			t.Errorf("Failed to insert record: %v", err)
		}

		err = index.Add(&record)
		if err != nil {
			t.Errorf("Failed to add record to index: %v", err)
		}
	}

	// Test index lookup
	lookupKey := Record{
		Cols: []string{"category"},
		Vals: []Value{{Type: TYPE_BYTES, Str: []byte("Electronics")}},
	}

	indexedRecords, err := index.Lookup(lookupKey)
	if err != nil {
		t.Errorf("Failed to lookup records: %v", err)
	}
	if len(indexedRecords) != 1 {
		t.Errorf("Expected 1 record in Electronics category, got %d", len(indexedRecords))
	}

	// Test table scan
	scanner := &Scanner{}
	err = table.Scan(scanner)
	if err != nil {
		t.Errorf("Failed to scan table: %v", err)
	}

	var scannedRecords []Record
	for scanner.Valid() {
		var record Record
		scanner.Deref(&record)
		scannedRecords = append(scannedRecords, record)
		scanner.Next()
	}

	if len(scannedRecords) != 2 {
		t.Errorf("Expected 2 records from scan, got %d", len(scannedRecords))
	}

	fmt.Println("Database Integration tests passed!")
	fmt.Printf("Final state: %d tables, %d indexes, %d records\n",
		len(sm.ListTables()), len(im.ListIndexes()), table.GetRecordCount())
}
