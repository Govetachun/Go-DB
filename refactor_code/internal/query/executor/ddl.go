package executor

import (
	"fmt"
)

// ExecuteCreateTable executes a CREATE TABLE statement
func ExecuteCreateTable(req *QLCreateTable, tx DBTX) error {
	// Convert query.TableDef to database.TableDef
	def := &TableDef{
		Name:  req.Def.Name,
		Cols:  req.Def.Cols,
		Types: req.Def.Types,
		PKeys: req.Def.PKeys,
	}

	// Validate table definition
	if err := validateTableDef(def); err != nil {
		return fmt.Errorf("table definition validation failed: %w", err)
	}

	// Check if table already exists
	existing := tx.GetDB().GetTableDef(req.Def.Name)
	if existing != nil {
		return fmt.Errorf("table %s already exists", req.Def.Name)
	}

	// Create the table
	err := tx.TableNew(def)
	if err != nil {
		return fmt.Errorf("table creation failed: %w", err)
	}

	return nil
}

// ExecuteCreateTableIfNotExists executes CREATE TABLE IF NOT EXISTS
func ExecuteCreateTableIfNotExists(req *QLCreateTable, tx DBTX) error {
	// Check if table already exists
	existing := tx.GetDB().GetTableDef(req.Def.Name)
	if existing != nil {
		// Table exists, do nothing
		return nil
	}

	// Create the table
	return ExecuteCreateTable(req, tx)
}

// ExecuteDropTable executes a DROP TABLE statement
func ExecuteDropTable(tableName string, tx DBTX) error {
	// Check if table exists
	existing := tx.GetDB().GetTableDef(tableName)
	if existing == nil {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Drop the table
	err := tx.DropTable(tableName)
	if err != nil {
		return fmt.Errorf("table drop failed: %w", err)
	}

	return nil
}

// ExecuteDropTableIfExists executes DROP TABLE IF EXISTS
func ExecuteDropTableIfExists(tableName string, tx DBTX) error {
	// Check if table exists
	existing := tx.GetDB().GetTableDef(tableName)
	if existing == nil {
		// Table doesn't exist, do nothing
		return nil
	}

	// Drop the table
	return ExecuteDropTable(tableName, tx)
}

// ExecuteAlterTableAddColumn executes ALTER TABLE ADD COLUMN
func ExecuteAlterTableAddColumn(tableName string, columnName string, columnType uint32, tx DBTX) error {
	// Get existing table definition
	tdef := tx.GetDB().GetTableDef(tableName)
	if tdef == nil {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Check if column already exists
	for _, col := range tdef.Cols {
		if col == columnName {
			return fmt.Errorf("column %s already exists in table %s", columnName, tableName)
		}
	}

	// Create new table definition with added column
	newDef := &TableDef{
		Name:  tdef.Name,
		Cols:  append(tdef.Cols, columnName),
		Types: append(tdef.Types, columnType),
		PKeys: tdef.PKeys, // Primary keys remain the same
	}

	// Alter the table
	err := tx.AlterTable(tableName, newDef)
	if err != nil {
		return fmt.Errorf("alter table failed: %w", err)
	}

	return nil
}

// ExecuteAlterTableDropColumn executes ALTER TABLE DROP COLUMN
func ExecuteAlterTableDropColumn(tableName string, columnName string, tx DBTX) error {
	// Get existing table definition
	tdef := tx.GetDB().GetTableDef(tableName)
	if tdef == nil {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Find column index
	colIndex := -1
	for i, col := range tdef.Cols {
		if col == columnName {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return fmt.Errorf("column %s does not exist in table %s", columnName, tableName)
	}

	// Check if column is part of primary key
	if colIndex < tdef.PKeys {
		return fmt.Errorf("cannot drop primary key column %s", columnName)
	}

	// Create new table definition without the column
	newCols := make([]string, 0, len(tdef.Cols)-1)
	newTypes := make([]uint32, 0, len(tdef.Types)-1)

	for i, col := range tdef.Cols {
		if i != colIndex {
			newCols = append(newCols, col)
			newTypes = append(newTypes, tdef.Types[i])
		}
	}

	newDef := &TableDef{
		Name:  tdef.Name,
		Cols:  newCols,
		Types: newTypes,
		PKeys: tdef.PKeys,
	}

	// Alter the table
	err := tx.AlterTable(tableName, newDef)
	if err != nil {
		return fmt.Errorf("alter table failed: %w", err)
	}

	return nil
}

// ExecuteAlterTableModifyColumn executes ALTER TABLE MODIFY COLUMN
func ExecuteAlterTableModifyColumn(tableName string, columnName string, newType uint32, tx DBTX) error {
	// Get existing table definition
	tdef := tx.GetDB().GetTableDef(tableName)
	if tdef == nil {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Find column index
	colIndex := -1
	for i, col := range tdef.Cols {
		if col == columnName {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return fmt.Errorf("column %s does not exist in table %s", columnName, tableName)
	}

	// Create new table definition with modified column type
	newTypes := make([]uint32, len(tdef.Types))
	copy(newTypes, tdef.Types)
	newTypes[colIndex] = newType

	newDef := &TableDef{
		Name:  tdef.Name,
		Cols:  tdef.Cols,
		Types: newTypes,
		PKeys: tdef.PKeys,
	}

	// Alter the table
	err := tx.AlterTable(tableName, newDef)
	if err != nil {
		return fmt.Errorf("alter table failed: %w", err)
	}

	return nil
}

// ExecuteCreateIndex executes CREATE INDEX statement
func ExecuteCreateIndex(indexName string, tableName string, columnNames []string, tx DBTX) error {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(tableName)
	if tdef == nil {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate column names
	for _, colName := range columnNames {
		found := false
		for _, tdefCol := range tdef.Cols {
			if tdefCol == colName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column %s does not exist in table %s", colName, tableName)
		}
	}

	// Create the index
	err := tx.CreateIndex(indexName, tableName, columnNames)
	if err != nil {
		return fmt.Errorf("index creation failed: %w", err)
	}

	return nil
}

// ExecuteDropIndex executes DROP INDEX statement
func ExecuteDropIndex(indexName string, tx DBTX) error {
	// Drop the index
	err := tx.DropIndex(indexName)
	if err != nil {
		return fmt.Errorf("index drop failed: %w", err)
	}

	return nil
}

// ExecuteTruncateTable executes TRUNCATE TABLE statement
func ExecuteTruncateTable(tableName string, tx DBTX) error {
	// Check if table exists
	existing := tx.GetDB().GetTableDef(tableName)
	if existing == nil {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Truncate the table
	err := tx.TruncateTable(tableName)
	if err != nil {
		return fmt.Errorf("table truncate failed: %w", err)
	}

	return nil
}

// ExecuteRenameTable executes RENAME TABLE statement
func ExecuteRenameTable(oldName string, newName string, tx DBTX) error {
	// Check if old table exists
	existing := tx.GetDB().GetTableDef(oldName)
	if existing == nil {
		return fmt.Errorf("table %s does not exist", oldName)
	}

	// Check if new table name already exists
	newExisting := tx.GetDB().GetTableDef(newName)
	if newExisting != nil {
		return fmt.Errorf("table %s already exists", newName)
	}

	// Rename the table
	err := tx.RenameTable(oldName, newName)
	if err != nil {
		return fmt.Errorf("table rename failed: %w", err)
	}

	return nil
}

// ExecuteRenameColumn executes RENAME COLUMN statement
func ExecuteRenameColumn(tableName string, oldName string, newName string, tx DBTX) error {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(tableName)
	if tdef == nil {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Find old column index
	oldIndex := -1
	for i, col := range tdef.Cols {
		if col == oldName {
			oldIndex = i
			break
		}
	}

	if oldIndex == -1 {
		return fmt.Errorf("column %s does not exist in table %s", oldName, tableName)
	}

	// Check if new column name already exists
	for _, col := range tdef.Cols {
		if col == newName {
			return fmt.Errorf("column %s already exists in table %s", newName, tableName)
		}
	}

	// Create new table definition with renamed column
	newCols := make([]string, len(tdef.Cols))
	copy(newCols, tdef.Cols)
	newCols[oldIndex] = newName

	newDef := &TableDef{
		Name:  tdef.Name,
		Cols:  newCols,
		Types: tdef.Types,
		PKeys: tdef.PKeys,
	}

	// Alter the table
	err := tx.AlterTable(tableName, newDef)
	if err != nil {
		return fmt.Errorf("alter table failed: %w", err)
	}

	return nil
}

// validateTableDef validates a table definition
func validateTableDef(def *TableDef) error {
	// Check table name
	if def.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Check column count
	if len(def.Cols) == 0 {
		return fmt.Errorf("table must have at least one column")
	}

	if len(def.Cols) != len(def.Types) {
		return fmt.Errorf("column count mismatch: %d columns, %d types",
			len(def.Cols), len(def.Types))
	}

	// Check primary key count
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

// GetTableInfo returns information about a table
func GetTableInfo(tableName string, tx DBTX) (*TableInfo, error) {
	tdef := tx.GetDB().GetTableDef(tableName)
	if tdef == nil {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	info := &TableInfo{
		Name:        tdef.Name,
		Columns:     make([]ColumnInfo, len(tdef.Cols)),
		PrimaryKeys: tdef.PKeys,
	}

	for i, col := range tdef.Cols {
		info.Columns[i] = ColumnInfo{
			Name:         col,
			Type:         int(tdef.Types[i]),
			IsPrimaryKey: i < tdef.PKeys,
		}
	}

	return info, nil
}

// TableInfo represents table metadata
type TableInfo struct {
	Name        string
	Columns     []ColumnInfo
	PrimaryKeys int
}

// ColumnInfo represents column metadata
type ColumnInfo struct {
	Name         string
	Type         int
	IsPrimaryKey bool
}

// ListTables returns a list of all table names
func ListTables(tx DBTX) ([]string, error) {
	return tx.GetDB().ListTables()
}
