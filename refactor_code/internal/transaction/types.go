package transaction

import (
	"fmt"
	"govetachun/go-mini-db/refactor_code/internal/database"
	"strings"
)

// Re-export types
type Record = database.Record
type Value = database.Value
type Scanner = database.Scanner
type TableDef = database.TableDef

// DBTX represents a database transaction
type DBTX struct {
	db DB
	// Transaction state
	active bool
	// For simple in-memory implementation
	tables  map[string]*TableDef
	records map[string]map[string]*Record // table -> key -> record
}

// DB represents a database interface
type DB interface {
	GetTableDef(name string) *TableDef
	ListTables() ([]string, error)
}

// TableNew creates a new table
func (tx *DBTX) TableNew(def *TableDef) error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}

	if def == nil {
		return fmt.Errorf("table definition cannot be nil")
	}

	// Validate table definition
	if err := tx.validateTableDef(def); err != nil {
		return fmt.Errorf("invalid table definition: %w", err)
	}

	// Check if table already exists
	if _, exists := tx.tables[def.Name]; exists {
		return fmt.Errorf("table %s already exists", def.Name)
	}

	// Create a copy of the definition
	tableDef := &TableDef{
		Name:  def.Name,
		Cols:  make([]string, len(def.Cols)),
		Types: make([]uint32, len(def.Types)),
		PKeys: def.PKeys,
	}

	copy(tableDef.Cols, def.Cols)
	copy(tableDef.Types, def.Types)

	// Add to transaction tables
	tx.tables[def.Name] = tableDef

	// Initialize records map for this table
	tx.records[def.Name] = make(map[string]*Record)

	return nil
}

// Scan performs a table scan
func (tx *DBTX) Scan(table string, scanner *Scanner) error {
	// Implementation would go here
	return nil
}

// Delete deletes a record
func (tx *DBTX) Delete(table string, key Record) (bool, error) {
	if !tx.active {
		return false, fmt.Errorf("transaction not active")
	}

	tableRecords, exists := tx.records[table]
	if !exists {
		return false, fmt.Errorf("table %s does not exist", table)
	}

	// Generate key string from primary key columns
	keyStr := tx.generateKeyString(table, key)
	if keyStr == "" {
		return false, fmt.Errorf("invalid primary key")
	}

	// Check if record exists
	if _, exists := tableRecords[keyStr]; !exists {
		return false, nil
	}

	// Delete the record
	delete(tableRecords, keyStr)
	return true, nil
}

// Insert inserts a record
func (tx *DBTX) Insert(table string, record Record) error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}

	tableDef, exists := tx.tables[table]
	if !exists {
		return fmt.Errorf("table %s does not exist", table)
	}

	// Validate record against table schema
	if err := tx.validateRecord(record, tableDef); err != nil {
		return fmt.Errorf("record validation failed: %w", err)
	}

	// Generate key string from primary key columns
	keyStr := tx.generateKeyString(table, record)
	if keyStr == "" {
		return fmt.Errorf("failed to generate primary key")
	}

	// Check if record already exists
	tableRecords := tx.records[table]
	if _, exists := tableRecords[keyStr]; exists {
		return fmt.Errorf("record with primary key already exists")
	}

	// Create a copy of the record
	recordCopy := tx.copyRecord(record)
	tableRecords[keyStr] = &recordCopy

	return nil
}

// Get retrieves a record
func (tx *DBTX) Get(table string, key Record) (*Record, error) {
	if !tx.active {
		return nil, fmt.Errorf("transaction not active")
	}

	tableRecords, exists := tx.records[table]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", table)
	}

	// Generate key string from primary key columns
	keyStr := tx.generateKeyString(table, key)
	if keyStr == "" {
		return nil, fmt.Errorf("invalid primary key")
	}

	// Check if record exists
	record, exists := tableRecords[keyStr]
	if !exists {
		return nil, nil
	}

	// Return a copy
	result := tx.copyRecord(*record)
	return &result, nil
}

// Update updates a record
func (tx *DBTX) Update(table string, key Record, record Record) error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}

	tableDef, exists := tx.tables[table]
	if !exists {
		return fmt.Errorf("table %s does not exist", table)
	}

	// Validate new record against table schema
	if err := tx.validateRecord(record, tableDef); err != nil {
		return fmt.Errorf("record validation failed: %w", err)
	}

	// Generate key string from primary key columns
	keyStr := tx.generateKeyString(table, key)
	if keyStr == "" {
		return fmt.Errorf("invalid primary key")
	}

	// Check if record exists
	tableRecords := tx.records[table]
	if _, exists := tableRecords[keyStr]; !exists {
		return fmt.Errorf("record not found")
	}

	// Create a copy of the new record
	recordCopy := tx.copyRecord(record)
	tableRecords[keyStr] = &recordCopy

	return nil
}

// DropTable drops a table
func (tx *DBTX) DropTable(tableName string) error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}

	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Check if table exists
	if _, exists := tx.tables[tableName]; !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Remove table definition and all records
	delete(tx.tables, tableName)
	delete(tx.records, tableName)

	return nil
}

// AlterTable alters a table
func (tx *DBTX) AlterTable(tableName string, newDef *TableDef) error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}

	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if newDef == nil {
		return fmt.Errorf("new table definition cannot be nil")
	}

	// Check if table exists
	if _, exists := tx.tables[tableName]; !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate new table definition
	if err := tx.validateTableDef(newDef); err != nil {
		return fmt.Errorf("invalid table definition: %w", err)
	}

	// Preserve the original name
	newDef.Name = tableName
	tx.tables[tableName] = newDef

	return nil
}

// CreateIndex creates an index
func (tx *DBTX) CreateIndex(indexName string, tableName string, columnNames []string) error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}

	if indexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if len(columnNames) == 0 {
		return fmt.Errorf("index must have at least one column")
	}

	// Check if table exists
	tableDef, exists := tx.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate column names exist in table
	for _, colName := range columnNames {
		found := false
		for _, tableCol := range tableDef.Cols {
			if tableCol == colName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column %s does not exist in table %s", colName, tableName)
		}
	}

	// For now, just return success (index creation would be implemented in a full system)
	return nil
}

// DropIndex drops an index
func (tx *DBTX) DropIndex(indexName string) error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}

	if indexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	// For now, just return success (index dropping would be implemented in a full system)
	return nil
}

// TruncateTable truncates a table
func (tx *DBTX) TruncateTable(tableName string) error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}

	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Check if table exists
	if _, exists := tx.tables[tableName]; !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Clear all records but keep table definition
	tx.records[tableName] = make(map[string]*Record)

	return nil
}

// RenameTable renames a table
func (tx *DBTX) RenameTable(oldName string, newName string) error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}

	if oldName == "" {
		return fmt.Errorf("old table name cannot be empty")
	}

	if newName == "" {
		return fmt.Errorf("new table name cannot be empty")
	}

	if oldName == newName {
		return nil // No-op
	}

	// Check if old table exists
	tableDef, exists := tx.tables[oldName]
	if !exists {
		return fmt.Errorf("table %s does not exist", oldName)
	}

	// Check if new table name already exists
	if _, exists := tx.tables[newName]; exists {
		return fmt.Errorf("table %s already exists", newName)
	}

	// Get records from old table
	oldRecords := tx.records[oldName]

	// Create new table definition with new name
	newDef := &TableDef{
		Name:  newName,
		Cols:  tableDef.Cols,
		Types: tableDef.Types,
		PKeys: tableDef.PKeys,
	}

	// Add new table and remove old one
	tx.tables[newName] = newDef
	tx.records[newName] = oldRecords
	delete(tx.tables, oldName)
	delete(tx.records, oldName)

	return nil
}

// GetDB returns the underlying database
func (tx *DBTX) GetDB() DB {
	return tx.db
}

// NewDBTX creates a new database transaction
func NewDBTX(db DB) *DBTX {
	return &DBTX{
		db:      db,
		active:  false,
		tables:  make(map[string]*TableDef),
		records: make(map[string]map[string]*Record),
	}
}

// Begin starts the transaction
func (tx *DBTX) Begin() error {
	if tx.active {
		return fmt.Errorf("transaction already active")
	}
	tx.active = true
	return nil
}

// Commit commits the transaction
func (tx *DBTX) Commit() error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}
	tx.active = false
	return nil
}

// Abort aborts the transaction
func (tx *DBTX) Abort() error {
	if !tx.active {
		return fmt.Errorf("transaction not active")
	}
	tx.active = false
	// Clear transaction state
	tx.tables = make(map[string]*TableDef)
	tx.records = make(map[string]map[string]*Record)
	return nil
}

// Helper Methods

// generateKeyString generates a string key from primary key values
func (tx *DBTX) generateKeyString(table string, record Record) string {
	tableDef, exists := tx.tables[table]
	if !exists {
		return ""
	}

	if len(record.Cols) == 0 || len(record.Vals) == 0 {
		return ""
	}

	// Find primary key values
	var keyParts []string
	for i := 0; i < tableDef.PKeys && i < len(record.Cols); i++ {
		colName := tableDef.Cols[i]
		value := record.Get(colName)
		if value == nil {
			return ""
		}
		keyParts = append(keyParts, tx.valueToString(*value))
	}

	// Join with separator
	return strings.Join(keyParts, "|")
}

// valueToString converts a Value to string representation
func (tx *DBTX) valueToString(value Value) string {
	switch value.Type {
	case database.TYPE_INT64:
		return fmt.Sprintf("%d", value.I64)
	case database.TYPE_BYTES:
		return string(value.Str)
	default:
		return ""
	}
}

// copyRecord creates a deep copy of a record
func (tx *DBTX) copyRecord(record Record) Record {
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

// validateRecord validates a record against table schema
func (tx *DBTX) validateRecord(record Record, tdef *TableDef) error {
	// Check column count
	if len(record.Cols) != len(record.Vals) {
		return fmt.Errorf("column count mismatch in record")
	}

	// Check if all required columns are present
	for _, colName := range tdef.Cols {
		found := false
		for _, recordCol := range record.Cols {
			if recordCol == colName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("missing required column: %s", colName)
		}
	}

	// Validate data types
	for _, colName := range record.Cols {
		// Find column in table definition
		var colType uint32
		for j, tdefCol := range tdef.Cols {
			if tdefCol == colName {
				colType = tdef.Types[j]
				break
			}
		}

		// Validate type
		value := record.Get(colName)
		if value != nil && value.Type != colType {
			return fmt.Errorf("type mismatch for column %s: expected %d, got %d",
				colName, colType, value.Type)
		}
	}

	return nil
}

// validateTableDef validates a table definition
func (tx *DBTX) validateTableDef(def *TableDef) error {
	if def.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if len(def.Cols) == 0 {
		return fmt.Errorf("table must have at least one column")
	}

	if len(def.Cols) != len(def.Types) {
		return fmt.Errorf("column count mismatch: %d columns, %d types",
			len(def.Cols), len(def.Types))
	}

	if def.PKeys <= 0 {
		return fmt.Errorf("table must have at least one primary key")
	}

	if def.PKeys > len(def.Cols) {
		return fmt.Errorf("primary key count (%d) cannot exceed column count (%d)",
			def.PKeys, len(def.Cols))
	}

	// Check for duplicate column names
	colSet := make(map[string]bool)
	for _, col := range def.Cols {
		if colSet[col] {
			return fmt.Errorf("duplicate column name: %s", col)
		}
		colSet[col] = true
	}

	// Validate column names
	for _, col := range def.Cols {
		if col == "" {
			return fmt.Errorf("column name cannot be empty")
		}
		if !tx.isValidIdentifier(col) {
			return fmt.Errorf("invalid column name: %s", col)
		}
	}

	// Validate data types
	for i, colType := range def.Types {
		if colType != database.TYPE_INT64 && colType != database.TYPE_BYTES {
			return fmt.Errorf("invalid data type for column %s: %d", def.Cols[i], colType)
		}
	}

	return nil
}

// isValidIdentifier checks if a string is a valid identifier
func (tx *DBTX) isValidIdentifier(name string) bool {
	if len(name) == 0 {
		return false
	}

	// First character must be letter or underscore
	if !tx.isLetter(name[0]) && name[0] != '_' {
		return false
	}

	// Remaining characters must be letter, digit, or underscore
	for i := 1; i < len(name); i++ {
		if !tx.isLetter(name[i]) && !tx.isDigit(name[i]) && name[i] != '_' {
			return false
		}
	}

	return true
}

// isLetter checks if a byte is a letter
func (tx *DBTX) isLetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// isDigit checks if a byte is a digit
func (tx *DBTX) isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}
