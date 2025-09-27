package database

import (
	"fmt"
	"sync"
)

// SchemaManager handles schema operations
type SchemaManager struct {
	tables map[string]*TableDef
	mu     sync.RWMutex
}

// NewSchemaManager creates a new schema manager
func NewSchemaManager() *SchemaManager {
	return &SchemaManager{
		tables: make(map[string]*TableDef),
	}
}

// CreateTable creates a new table schema
func (sm *SchemaManager) CreateTable(def *TableDef) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if def == nil {
		return fmt.Errorf("table definition cannot be nil")
	}

	if def.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if _, exists := sm.tables[def.Name]; exists {
		return fmt.Errorf("table %s already exists", def.Name)
	}

	// Validate table definition
	if err := validateTableDef(def); err != nil {
		return fmt.Errorf("invalid table definition: %w", err)
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

	sm.tables[def.Name] = tableDef
	return nil
}

// DropTable drops a table schema
func (sm *SchemaManager) DropTable(name string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.tables[name]; !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	delete(sm.tables, name)
	return nil
}

// GetTableDef returns a table definition by name
func (sm *SchemaManager) GetTableDef(name string) *TableDef {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	def, exists := sm.tables[name]
	if !exists {
		return nil
	}

	// Return a copy
	tableDef := &TableDef{
		Name:  def.Name,
		Cols:  make([]string, len(def.Cols)),
		Types: make([]uint32, len(def.Types)),
		PKeys: def.PKeys,
	}

	copy(tableDef.Cols, def.Cols)
	copy(tableDef.Types, def.Types)

	return tableDef
}

// ListTables returns all table names
func (sm *SchemaManager) ListTables() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	tables := make([]string, 0, len(sm.tables))
	for name := range sm.tables {
		tables = append(tables, name)
	}
	return tables
}

// AlterTable alters a table schema
func (sm *SchemaManager) AlterTable(name string, newDef *TableDef) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if table exists
	oldDef, exists := sm.tables[name]
	if !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	if newDef == nil {
		return fmt.Errorf("new table definition cannot be nil")
	}

	// Validate new table definition
	if err := validateTableDef(newDef); err != nil {
		return fmt.Errorf("invalid table definition: %w", err)
	}

	// Ensure the name matches
	if newDef.Name != name {
		return fmt.Errorf("table name mismatch: expected %s, got %s", name, newDef.Name)
	}

	// Validate schema compatibility
	if err := sm.validateSchemaCompatibility(oldDef, newDef); err != nil {
		return fmt.Errorf("schema compatibility check failed: %w", err)
	}

	// Create a copy of the new definition
	tableDef := &TableDef{
		Name:  newDef.Name,
		Cols:  make([]string, len(newDef.Cols)),
		Types: make([]uint32, len(newDef.Types)),
		PKeys: newDef.PKeys,
	}

	copy(tableDef.Cols, newDef.Cols)
	copy(tableDef.Types, newDef.Types)

	sm.tables[name] = tableDef
	return nil
}

// RenameTable renames a table
func (sm *SchemaManager) RenameTable(oldName, newName string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if oldName == newName {
		return nil // No-op
	}

	// Check if old table exists
	def, exists := sm.tables[oldName]
	if !exists {
		return fmt.Errorf("table %s does not exist", oldName)
	}

	// Check if new table name already exists
	if _, exists := sm.tables[newName]; exists {
		return fmt.Errorf("table %s already exists", newName)
	}

	// Validate new table name
	if newName == "" {
		return fmt.Errorf("new table name cannot be empty")
	}

	if !isValidIdentifier(newName) {
		return fmt.Errorf("invalid table name: %s", newName)
	}

	// Create new definition with new name
	newDef := &TableDef{
		Name:  newName,
		Cols:  make([]string, len(def.Cols)),
		Types: make([]uint32, len(def.Types)),
		PKeys: def.PKeys,
	}

	copy(newDef.Cols, def.Cols)
	copy(newDef.Types, def.Types)

	// Add new table and remove old one
	sm.tables[newName] = newDef
	delete(sm.tables, oldName)

	return nil
}

// AddColumn adds a column to a table
func (sm *SchemaManager) AddColumn(tableName string, columnName string, columnType uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if table exists
	def, exists := sm.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Check if column already exists
	for _, col := range def.Cols {
		if col == columnName {
			return fmt.Errorf("column %s already exists in table %s", columnName, tableName)
		}
	}

	// Validate column name
	if columnName == "" {
		return fmt.Errorf("column name cannot be empty")
	}

	if !isValidIdentifier(columnName) {
		return fmt.Errorf("invalid column name: %s", columnName)
	}

	// Validate column type
	if columnType != TYPE_INT64 && columnType != TYPE_BYTES {
		return fmt.Errorf("invalid data type: %d", columnType)
	}

	// Create new table definition with added column
	newDef := &TableDef{
		Name:  def.Name,
		Cols:  append(def.Cols, columnName),
		Types: append(def.Types, columnType),
		PKeys: def.PKeys, // Primary keys remain the same
	}

	sm.tables[tableName] = newDef
	return nil
}

// DropColumn drops a column from a table
func (sm *SchemaManager) DropColumn(tableName string, columnName string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if table exists
	def, exists := sm.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Find column index
	colIndex := -1
	for i, col := range def.Cols {
		if col == columnName {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return fmt.Errorf("column %s does not exist in table %s", columnName, tableName)
	}

	// Check if column is part of primary key
	if colIndex < def.PKeys {
		return fmt.Errorf("cannot drop primary key column %s", columnName)
	}

	// Create new table definition without the column
	newCols := make([]string, 0, len(def.Cols)-1)
	newTypes := make([]uint32, 0, len(def.Types)-1)

	for i, col := range def.Cols {
		if i != colIndex {
			newCols = append(newCols, col)
			newTypes = append(newTypes, def.Types[i])
		}
	}

	newDef := &TableDef{
		Name:  def.Name,
		Cols:  newCols,
		Types: newTypes,
		PKeys: def.PKeys,
	}

	sm.tables[tableName] = newDef
	return nil
}

// ModifyColumn modifies a column type
func (sm *SchemaManager) ModifyColumn(tableName string, columnName string, newType uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if table exists
	def, exists := sm.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Find column index
	colIndex := -1
	for i, col := range def.Cols {
		if col == columnName {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return fmt.Errorf("column %s does not exist in table %s", columnName, tableName)
	}

	// Validate new type
	if newType != TYPE_INT64 && newType != TYPE_BYTES {
		return fmt.Errorf("invalid data type: %d", newType)
	}

	// Create new table definition with modified column type
	newTypes := make([]uint32, len(def.Types))
	copy(newTypes, def.Types)
	newTypes[colIndex] = newType

	newDef := &TableDef{
		Name:  def.Name,
		Cols:  def.Cols,
		Types: newTypes,
		PKeys: def.PKeys,
	}

	sm.tables[tableName] = newDef
	return nil
}

// RenameColumn renames a column
func (sm *SchemaManager) RenameColumn(tableName string, oldName string, newName string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if table exists
	def, exists := sm.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Find old column index
	oldIndex := -1
	for i, col := range def.Cols {
		if col == oldName {
			oldIndex = i
			break
		}
	}

	if oldIndex == -1 {
		return fmt.Errorf("column %s does not exist in table %s", oldName, tableName)
	}

	// Check if new column name already exists
	for _, col := range def.Cols {
		if col == newName {
			return fmt.Errorf("column %s already exists in table %s", newName, tableName)
		}
	}

	// Validate new column name
	if newName == "" {
		return fmt.Errorf("new column name cannot be empty")
	}

	if !isValidIdentifier(newName) {
		return fmt.Errorf("invalid column name: %s", newName)
	}

	// Create new table definition with renamed column
	newCols := make([]string, len(def.Cols))
	copy(newCols, def.Cols)
	newCols[oldIndex] = newName

	newDef := &TableDef{
		Name:  def.Name,
		Cols:  newCols,
		Types: def.Types,
		PKeys: def.PKeys,
	}

	sm.tables[tableName] = newDef
	return nil
}

// GetSchemaInfo returns information about the schema
func (sm *SchemaManager) GetSchemaInfo() *SchemaInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	info := &SchemaInfo{
		TableCount: len(sm.tables),
		Tables:     make([]TableInfo, 0, len(sm.tables)),
	}

	for _, def := range sm.tables {
		tableInfo := TableInfo{
			Name:        def.Name,
			Columns:     make([]ColumnInfo, len(def.Cols)),
			PrimaryKeys: def.PKeys,
		}

		for i, col := range def.Cols {
			tableInfo.Columns[i] = ColumnInfo{
				Name:         col,
				Type:         int(def.Types[i]),
				IsPrimaryKey: i < def.PKeys,
			}
		}

		info.Tables = append(info.Tables, tableInfo)
	}

	return info
}

// ValidateSchema validates the entire schema
func (sm *SchemaManager) ValidateSchema() error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for name, def := range sm.tables {
		if err := validateTableDef(def); err != nil {
			return fmt.Errorf("table %s validation failed: %w", name, err)
		}
	}

	return nil
}

// Helper Methods

// validateSchemaCompatibility validates compatibility between old and new schemas
func (sm *SchemaManager) validateSchemaCompatibility(oldDef, newDef *TableDef) error {
	// Check that primary key columns haven't changed
	if oldDef.PKeys != newDef.PKeys {
		return fmt.Errorf("primary key count changed from %d to %d", oldDef.PKeys, newDef.PKeys)
	}

	// Check that primary key column names haven't changed
	for i := 0; i < oldDef.PKeys; i++ {
		if i >= len(newDef.Cols) || oldDef.Cols[i] != newDef.Cols[i] {
			return fmt.Errorf("primary key column %d changed from %s to %s",
				i, oldDef.Cols[i], newDef.Cols[i])
		}
	}

	// Check that primary key column types haven't changed
	for i := 0; i < oldDef.PKeys; i++ {
		if oldDef.Types[i] != newDef.Types[i] {
			return fmt.Errorf("primary key column %s type changed from %d to %d",
				oldDef.Cols[i], oldDef.Types[i], newDef.Types[i])
		}
	}

	return nil
}

// SchemaInfo represents schema metadata
type SchemaInfo struct {
	TableCount int
	Tables     []TableInfo
}

// ExportSchema exports the schema to a portable format
func (sm *SchemaManager) ExportSchema() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// In a real implementation, this would serialize to JSON or another format
	// For now, we'll return a simple representation
	schema := make(map[string]*TableDef)
	for name, def := range sm.tables {
		schema[name] = def
	}

	// This is a placeholder - in a real implementation, you'd use JSON or similar
	return []byte(fmt.Sprintf("%+v", schema)), nil
}

// ImportSchema imports a schema from a portable format
func (sm *SchemaManager) ImportSchema(data []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// This is a placeholder - in a real implementation, you'd parse JSON or similar
	// For now, we'll just validate that the data is not empty
	if len(data) == 0 {
		return fmt.Errorf("empty schema data")
	}

	// In a real implementation, you would:
	// 1. Parse the data format (JSON, YAML, etc.)
	// 2. Validate the schema
	// 3. Replace the current schema

	return fmt.Errorf("schema import not implemented")
}

// CloneSchema creates a copy of the current schema
func (sm *SchemaManager) CloneSchema() *SchemaManager {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	newSM := NewSchemaManager()
	for name, def := range sm.tables {
		newDef := &TableDef{
			Name:  def.Name,
			Cols:  make([]string, len(def.Cols)),
			Types: make([]uint32, len(def.Types)),
			PKeys: def.PKeys,
		}

		copy(newDef.Cols, def.Cols)
		copy(newDef.Types, def.Types)

		newSM.tables[name] = newDef
	}

	return newSM
}
