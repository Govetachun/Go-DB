package database

import (
	"fmt"
	"sync"
)

// SimpleDB represents a simple in-memory database implementation
type SimpleDB struct {
	tables map[string]*TableDef
	mu     sync.RWMutex
}

// NewSimpleDB creates a new simple database instance
func NewSimpleDB() *SimpleDB {
	return &SimpleDB{
		tables: make(map[string]*TableDef),
	}
}

// GetTableDef returns a table definition by name
func (db *SimpleDB) GetTableDef(name string) *TableDef {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.tables[name]
}

// ListTables returns a list of all table names
func (db *SimpleDB) ListTables() ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tables := make([]string, 0, len(db.tables))
	for name := range db.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

// CreateTable creates a new table
func (db *SimpleDB) CreateTable(def *TableDef) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if def == nil {
		return fmt.Errorf("table definition cannot be nil")
	}

	if def.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if _, exists := db.tables[def.Name]; exists {
		return fmt.Errorf("table %s already exists", def.Name)
	}

	// Validate table definition
	if err := validateTableDef(def); err != nil {
		return fmt.Errorf("invalid table definition: %w", err)
	}

	db.tables[def.Name] = def
	return nil
}

// DropTable drops a table
func (db *SimpleDB) DropTable(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	delete(db.tables, name)
	return nil
}

// AlterTable alters a table definition
func (db *SimpleDB) AlterTable(name string, newDef *TableDef) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	if newDef == nil {
		return fmt.Errorf("new table definition cannot be nil")
	}

	// Validate new table definition
	if err := validateTableDef(newDef); err != nil {
		return fmt.Errorf("invalid table definition: %w", err)
	}

	// Preserve the original name
	newDef.Name = name
	db.tables[name] = newDef
	return nil
}

// RenameTable renames a table
func (db *SimpleDB) RenameTable(oldName, newName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[oldName]; !exists {
		return fmt.Errorf("table %s does not exist", oldName)
	}

	if _, exists := db.tables[newName]; exists {
		return fmt.Errorf("table %s already exists", newName)
	}

	if oldName == newName {
		return nil // No-op
	}

	// Get the table definition
	tdef := db.tables[oldName]

	// Create new table definition with new name
	newDef := &TableDef{
		Name:  newName,
		Cols:  tdef.Cols,
		Types: tdef.Types,
		PKeys: tdef.PKeys,
	}

	// Add new table and remove old one
	db.tables[newName] = newDef
	delete(db.tables, oldName)

	return nil
}

// validateTableDef validates a table definition
func validateTableDef(def *TableDef) error {
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
		if !isValidIdentifier(col) {
			return fmt.Errorf("invalid column name: %s", col)
		}
	}

	// Validate data types
	for i, colType := range def.Types {
		if colType != TYPE_INT64 && colType != TYPE_BYTES {
			return fmt.Errorf("invalid data type for column %s: %d", def.Cols[i], colType)
		}
	}

	return nil
}

// isValidIdentifier checks if a string is a valid identifier
func isValidIdentifier(name string) bool {
	if len(name) == 0 {
		return false
	}

	// First character must be letter or underscore
	if !isLetter(name[0]) && name[0] != '_' {
		return false
	}

	// Remaining characters must be letter, digit, or underscore
	for i := 1; i < len(name); i++ {
		if !isLetter(name[i]) && !isDigit(name[i]) && name[i] != '_' {
			return false
		}
	}

	return true
}

// isLetter checks if a byte is a letter
func isLetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// isDigit checks if a byte is a digit
func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

// validateRecord validates a record against table schema
func validateRecord(record Record, tdef *TableDef) error {
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
