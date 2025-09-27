package executor

import (
	"fmt"
)

// ExecuteInsert executes an INSERT statement
func ExecuteInsert(req *QLInsert, tx DBTX) (uint64, error) {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	// Validate column count
	if len(req.Names) > 0 && len(req.Names) != len(req.Values[0]) {
		return 0, fmt.Errorf("column count mismatch: expected %d, got %d",
			len(req.Names), len(req.Values[0]))
	}

	var insertedCount uint64

	// Process each value set
	for _, valueRow := range req.Values {
		// Create record for insertion
		record := Record{}

		if len(req.Names) > 0 {
			// Insert with specified columns
			record.Cols = req.Names
			record.Vals = make([]Value, len(valueRow))

			// Evaluate each value expression
			for i, expr := range valueRow {
				ctx := QLEvalContex{}
				qlEval(&ctx, expr)
				if ctx.err != nil {
					return 0, fmt.Errorf("value evaluation failed: %w", ctx.err)
				}
				record.Vals[i] = ctx.out
			}
		} else {
			// Insert with all columns (in table order)
			record.Cols = tdef.Cols
			record.Vals = make([]Value, len(tdef.Cols))

			// Evaluate each value expression
			for i, expr := range valueRow {
				if i >= len(tdef.Cols) {
					return 0, fmt.Errorf("too many values: expected %d, got %d",
						len(tdef.Cols), len(valueRow))
				}

				ctx := QLEvalContex{}
				qlEval(&ctx, expr)
				if ctx.err != nil {
					return 0, fmt.Errorf("value evaluation failed: %w", ctx.err)
				}
				record.Vals[i] = ctx.out
			}
		}

		// Validate record against table schema
		if err := validateRecord(record, tdef); err != nil {
			return 0, fmt.Errorf("record validation failed: %w", err)
		}

		// Insert the record
		err := tx.Insert(req.Table, record)
		if err != nil {
			return 0, fmt.Errorf("insert failed: %w", err)
		}

		insertedCount++
	}

	return insertedCount, nil
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

// InsertMode represents different insert modes
type InsertMode int

const (
	InsertModeInsertOnly InsertMode = iota // Only insert new records
	InsertModeUpdateOnly                   // Only update existing records
	InsertModeUpsert                       // Insert or update
)

// ExecuteInsertWithMode executes INSERT with specific mode
func ExecuteInsertWithMode(req *QLInsert, tx DBTX, mode InsertMode) (uint64, error) {
	switch mode {
	case InsertModeInsertOnly:
		return executeInsertOnly(req, tx)
	case InsertModeUpdateOnly:
		return executeUpdateOnly(req, tx)
	case InsertModeUpsert:
		return executeUpsert(req, tx)
	default:
		return 0, fmt.Errorf("unknown insert mode: %d", mode)
	}
}

// executeInsertOnly inserts only if record doesn't exist
func executeInsertOnly(req *QLInsert, tx DBTX) (uint64, error) {
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	var insertedCount uint64

	for _, valueRow := range req.Values {
		record := buildRecord(valueRow, req.Names, tdef)
		if record == nil {
			return 0, fmt.Errorf("failed to build record")
		}

		// Check if record already exists
		key := buildPrimaryKey(*record, tdef)
		existing, err := tx.Get(req.Table, key)
		if err != nil {
			return 0, fmt.Errorf("check existing record failed: %w", err)
		}

		if existing != nil {
			// Record exists, skip insertion
			continue
		}

		// Insert the record
		err = tx.Insert(req.Table, *record)
		if err != nil {
			return 0, fmt.Errorf("insert failed: %w", err)
		}

		insertedCount++
	}

	return insertedCount, nil
}

// executeUpdateOnly updates only if record exists
func executeUpdateOnly(req *QLInsert, tx DBTX) (uint64, error) {
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	var updatedCount uint64

	for _, valueRow := range req.Values {
		record := buildRecord(valueRow, req.Names, tdef)
		if record == nil {
			return 0, fmt.Errorf("failed to build record")
		}

		// Check if record exists
		key := buildPrimaryKey(*record, tdef)
		existing, err := tx.Get(req.Table, key)
		if err != nil {
			return 0, fmt.Errorf("check existing record failed: %w", err)
		}

		if existing == nil {
			// Record doesn't exist, skip update
			continue
		}

		// Update the record
		err = tx.Update(req.Table, key, *record)
		if err != nil {
			return 0, fmt.Errorf("update failed: %w", err)
		}

		updatedCount++
	}

	return updatedCount, nil
}

// executeUpsert inserts or updates record
func executeUpsert(req *QLInsert, tx DBTX) (uint64, error) {
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	var upsertedCount uint64

	for _, valueRow := range req.Values {
		record := buildRecord(valueRow, req.Names, tdef)
		if record == nil {
			return 0, fmt.Errorf("failed to build record")
		}

		// Check if record exists
		key := buildPrimaryKey(*record, tdef)
		existing, err := tx.Get(req.Table, key)
		if err != nil {
			return 0, fmt.Errorf("check existing record failed: %w", err)
		}

		if existing == nil {
			// Insert new record
			err = tx.Insert(req.Table, *record)
		} else {
			// Update existing record
			err = tx.Update(req.Table, key, *record)
		}

		if err != nil {
			return 0, fmt.Errorf("upsert failed: %w", err)
		}

		upsertedCount++
	}

	return upsertedCount, nil
}

// buildRecord constructs a record from value expressions
func buildRecord(valueRow []QLNode, colNames []string, tdef *TableDef) *Record {
	record := &Record{}

	if len(colNames) > 0 {
		// Use specified column names
		record.Cols = colNames
		record.Vals = make([]Value, len(valueRow))

		for i, expr := range valueRow {
			ctx := QLEvalContex{}
			qlEval(&ctx, expr)
			if ctx.err != nil {
				return nil
			}
			record.Vals[i] = ctx.out
		}
	} else {
		// Use all table columns in order
		record.Cols = tdef.Cols
		record.Vals = make([]Value, len(valueRow))

		for i, expr := range valueRow {
			ctx := QLEvalContex{}
			qlEval(&ctx, expr)
			if ctx.err != nil {
				return nil
			}
			record.Vals[i] = ctx.out
		}
	}

	return record
}

// buildPrimaryKey constructs primary key from record
func buildPrimaryKey(record Record, tdef *TableDef) Record {
	key := Record{}
	key.Cols = tdef.Cols[:tdef.PKeys]
	key.Vals = make([]Value, tdef.PKeys)

	for i := 0; i < tdef.PKeys; i++ {
		colName := tdef.Cols[i]
		value := record.Get(colName)
		if value == nil {
			// This shouldn't happen if record is valid
			return Record{}
		}
		key.Vals[i] = *value
	}

	return key
}
