package database

import (
	"fmt"
	"sync"
)

// TableManager handles table operations
type TableManager struct {
	tables map[string]*Table
	mu     sync.RWMutex
}

// Table represents a database table
type Table struct {
	Def     *TableDef
	Data    map[string]*Record // Primary key -> Record mapping
	Indexes map[string]*Index  // Index name -> Index mapping
	mu      sync.RWMutex
}

// NewTableManager creates a new table manager
func NewTableManager() *TableManager {
	return &TableManager{
		tables: make(map[string]*Table),
	}
}

// CreateTable creates a new table
func (tm *TableManager) CreateTable(def *TableDef) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if def == nil {
		return fmt.Errorf("table definition cannot be nil")
	}

	if def.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if _, exists := tm.tables[def.Name]; exists {
		return fmt.Errorf("table %s already exists", def.Name)
	}

	// Validate table definition
	if err := validateTableDef(def); err != nil {
		return fmt.Errorf("invalid table definition: %w", err)
	}

	// Create new table
	table := &Table{
		Def:     def,
		Data:    make(map[string]*Record),
		Indexes: make(map[string]*Index),
	}

	tm.tables[def.Name] = table
	return nil
}

// DropTable drops a table
func (tm *TableManager) DropTable(name string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.tables[name]; !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	delete(tm.tables, name)
	return nil
}

// GetTable returns a table by name
func (tm *TableManager) GetTable(name string) *Table {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.tables[name]
}

// ListTables returns all table names
func (tm *TableManager) ListTables() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tables := make([]string, 0, len(tm.tables))
	for name := range tm.tables {
		tables = append(tables, name)
	}
	return tables
}

// Table Operations

// Insert inserts a record into the table
func (t *Table) Insert(record Record) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Validate record
	if err := validateRecord(record, t.Def); err != nil {
		return fmt.Errorf("record validation failed: %w", err)
	}

	// Generate primary key
	key := t.generatePrimaryKey(record)
	if key == "" {
		return fmt.Errorf("failed to generate primary key")
	}

	// Check if record already exists
	if _, exists := t.Data[key]; exists {
		return fmt.Errorf("record with primary key already exists")
	}

	// Create a copy of the record
	recordCopy := t.copyRecord(record)
	t.Data[key] = &recordCopy

	// Update indexes
	if err := t.updateIndexes(&recordCopy, true); err != nil {
		// Rollback the insert
		delete(t.Data, key)
		return fmt.Errorf("index update failed: %w", err)
	}

	return nil
}

// Get retrieves a record by primary key
func (t *Table) Get(key Record) (*Record, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	keyStr := t.generatePrimaryKey(key)
	if keyStr == "" {
		return nil, fmt.Errorf("invalid primary key")
	}

	record, exists := t.Data[keyStr]
	if !exists {
		return nil, nil
	}

	// Return a copy
	result := t.copyRecord(*record)
	return &result, nil
}

// Update updates a record
func (t *Table) Update(key Record, newRecord Record) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	keyStr := t.generatePrimaryKey(key)
	if keyStr == "" {
		return fmt.Errorf("invalid primary key")
	}

	// Check if record exists
	oldRecord, exists := t.Data[keyStr]
	if !exists {
		return fmt.Errorf("record not found")
	}

	// Validate new record
	if err := validateRecord(newRecord, t.Def); err != nil {
		return fmt.Errorf("record validation failed: %w", err)
	}

	// Update indexes (remove old, add new)
	if err := t.updateIndexes(oldRecord, false); err != nil {
		return fmt.Errorf("index update failed: %w", err)
	}

	// Create a copy of the new record
	recordCopy := t.copyRecord(newRecord)
	t.Data[keyStr] = &recordCopy

	// Update indexes with new record
	if err := t.updateIndexes(&recordCopy, true); err != nil {
		// Rollback the update
		t.Data[keyStr] = oldRecord
		t.updateIndexes(oldRecord, true)
		return fmt.Errorf("index update failed: %w", err)
	}

	return nil
}

// Delete deletes a record by primary key
func (t *Table) Delete(key Record) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	keyStr := t.generatePrimaryKey(key)
	if keyStr == "" {
		return false, fmt.Errorf("invalid primary key")
	}

	record, exists := t.Data[keyStr]
	if !exists {
		return false, nil
	}

	// Update indexes (remove)
	if err := t.updateIndexes(record, false); err != nil {
		return false, fmt.Errorf("index update failed: %w", err)
	}

	delete(t.Data, keyStr)
	return true, nil
}

// Scan performs a table scan
func (t *Table) Scan(scanner *Scanner) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Convert table data to records for scanning
	var records []Record
	for _, record := range t.Data {
		records = append(records, *record)
	}

	// Set records in scanner
	scanner.SetRecords(records)
	return nil
}

// Helper Methods

// generatePrimaryKey generates a string key from primary key values
func (t *Table) generatePrimaryKey(record Record) string {
	if len(record.Cols) == 0 || len(record.Vals) == 0 {
		return ""
	}

	// Find primary key values
	var keyParts []string
	for i := 0; i < t.Def.PKeys && i < len(record.Cols); i++ {
		colName := t.Def.Cols[i]
		value := record.Get(colName)
		if value == nil {
			return ""
		}
		keyParts = append(keyParts, valueToString(*value))
	}

	// Join with separator
	key := ""
	for i, part := range keyParts {
		if i > 0 {
			key += "|"
		}
		key += part
	}
	return key
}

// copyRecord creates a deep copy of a record
func (t *Table) copyRecord(record Record) Record {
	recordCopy := Record{
		Cols: make([]string, len(record.Cols)),
		Vals: make([]Value, len(record.Vals)),
	}

	copy(recordCopy.Cols, record.Cols)
	for i, val := range record.Vals {
		recordCopy.Vals[i] = Value{
			Type: val.Type,
			I64:  val.I64,
			Str:  make([]byte, len(val.Str)),
		}
		copy(recordCopy.Vals[i].Str, val.Str)
	}

	return recordCopy
}

// updateIndexes updates all indexes for a record
func (t *Table) updateIndexes(record *Record, add bool) error {
	for _, index := range t.Indexes {
		if add {
			if err := index.Add(record); err != nil {
				return err
			}
		} else {
			if err := index.Remove(record); err != nil {
				return err
			}
		}
	}
	return nil
}

// valueToString converts a Value to string representation
func valueToString(value Value) string {
	switch value.Type {
	case TYPE_INT64:
		return fmt.Sprintf("%d", value.I64)
	case TYPE_BYTES:
		return string(value.Str)
	default:
		return ""
	}
}

// GetTableInfo returns information about the table
func (t *Table) GetTableInfo() *TableInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	info := &TableInfo{
		Name:        t.Def.Name,
		Columns:     make([]ColumnInfo, len(t.Def.Cols)),
		PrimaryKeys: t.Def.PKeys,
		RecordCount: len(t.Data),
		IndexCount:  len(t.Indexes),
	}

	for i, col := range t.Def.Cols {
		info.Columns[i] = ColumnInfo{
			Name:         col,
			Type:         int(t.Def.Types[i]),
			IsPrimaryKey: i < t.Def.PKeys,
		}
	}

	return info
}

// GetRecordCount returns the number of records in the table
func (t *Table) GetRecordCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Data)
}

// Clear removes all records from the table
func (t *Table) Clear() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Clear all indexes
	for _, index := range t.Indexes {
		index.Clear()
	}

	// Clear data
	t.Data = make(map[string]*Record)
	return nil
}

// ValidateTableIntegrity checks table integrity
func (t *Table) ValidateTableIntegrity() error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check that all records have valid primary keys
	for key, record := range t.Data {
		if err := validateRecord(*record, t.Def); err != nil {
			return fmt.Errorf("invalid record with key %s: %w", key, err)
		}
	}

	// Validate indexes
	for name, index := range t.Indexes {
		if err := index.Validate(); err != nil {
			return fmt.Errorf("index %s validation failed: %w", name, err)
		}
	}

	return nil
}
