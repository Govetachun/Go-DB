package executor

import (
	"fmt"
)

// ExecuteDelete executes a DELETE statement
func ExecuteDelete(req *QLDelete, tx DBTX) (uint64, error) {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	// Execute scan to find records to delete
	var out []Record
	records, err := qlScan(&req.QLScan, tx, out)
	if err != nil {
		return 0, fmt.Errorf("scan failed: %w", err)
	}

	var deletedCount uint64

	// Process each matching record
	for _, record := range records {
		// Build primary key for this record
		key := buildPrimaryKey(record, tdef)

		// Delete the record
		deleted, err := tx.Delete(req.Table, key)
		if err != nil {
			return 0, fmt.Errorf("delete failed: %w", err)
		}

		if !deleted {
			return 0, fmt.Errorf("delete operation failed for record")
		}

		deletedCount++
	}

	return deletedCount, nil
}

// ExecuteDeleteWithCondition executes DELETE with additional conditions
func ExecuteDeleteWithCondition(req *QLDelete, tx DBTX, condition QLNode) (uint64, error) {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	// Execute scan to find records to delete
	var out []Record
	records, err := qlScan(&req.QLScan, tx, out)
	if err != nil {
		return 0, fmt.Errorf("scan failed: %w", err)
	}

	var deletedCount uint64

	// Process each matching record
	for _, record := range records {
		// Apply additional condition if provided
		if condition.Value.Type != 0 {
			ctx := QLEvalContex{env: record}
			qlEval(&ctx, condition)
			if ctx.err != nil {
				return 0, fmt.Errorf("condition evaluation failed: %w", ctx.err)
			}
			if ctx.out.Type != TYPE_INT64 {
				return 0, fmt.Errorf("condition must be boolean type")
			}
			if ctx.out.I64 == 0 {
				// Condition not met, skip this record
				continue
			}
		}

		// Build primary key for this record
		key := buildPrimaryKey(record, tdef)

		// Delete the record
		deleted, err := tx.Delete(req.Table, key)
		if err != nil {
			return 0, fmt.Errorf("delete failed: %w", err)
		}

		if !deleted {
			return 0, fmt.Errorf("delete operation failed for record")
		}

		deletedCount++
	}

	return deletedCount, nil
}

// ExecuteDeleteSingle deletes a single record by primary key
func ExecuteDeleteSingle(req *QLDelete, tx DBTX, key Record) error {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return fmt.Errorf("table %s not found", req.Table)
	}

	// Validate key structure
	if len(key.Cols) != tdef.PKeys {
		return fmt.Errorf("primary key length mismatch: expected %d, got %d",
			tdef.PKeys, len(key.Cols))
	}

	// Validate key column names
	for i, colName := range key.Cols {
		if colName != tdef.Cols[i] {
			return fmt.Errorf("primary key column mismatch at position %d: expected %s, got %s",
				i, tdef.Cols[i], colName)
		}
	}

	// Delete the record
	deleted, err := tx.Delete(req.Table, key)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	if !deleted {
		return fmt.Errorf("record not found or delete operation failed")
	}

	return nil
}

// ExecuteDeleteBatch deletes multiple records in a batch
func ExecuteDeleteBatch(req *QLDelete, tx DBTX, keys []Record) (uint64, error) {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	var deletedCount uint64

	// Process each key
	for _, key := range keys {
		// Validate key structure
		if len(key.Cols) != tdef.PKeys {
			return 0, fmt.Errorf("primary key length mismatch: expected %d, got %d",
				tdef.PKeys, len(key.Cols))
		}

		// Delete the record
		deleted, err := tx.Delete(req.Table, key)
		if err != nil {
			return 0, fmt.Errorf("delete failed: %w", err)
		}

		if deleted {
			deletedCount++
		}
	}

	return deletedCount, nil
}

// ExecuteDeleteByRange deletes records within a key range
func ExecuteDeleteByRange(req *QLDelete, tx DBTX, startKey, endKey Record) (uint64, error) {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	// Create scanner for range scan
	sc := Scanner{}
	sc.Key1 = startKey
	sc.Key2 = endKey
	sc.Cmp1 = CMP_GE
	sc.Cmp2 = CMP_LE

	// Execute range scan
	err := tx.Scan(req.Table, &sc)
	if err != nil {
		return 0, fmt.Errorf("range scan failed: %w", err)
	}

	var deletedCount uint64

	// Process each record in range
	for sc.Valid() {
		var record Record
		sc.Deref(&record)

		// Build primary key for this record
		key := buildPrimaryKey(record, tdef)

		// Delete the record
		deleted, err := tx.Delete(req.Table, key)
		if err != nil {
			return 0, fmt.Errorf("delete failed: %w", err)
		}

		if deleted {
			deletedCount++
		}

		sc.Next()
	}

	return deletedCount, nil
}

// ExecuteDeleteAll deletes all records from a table
func ExecuteDeleteAll(req *QLDelete, tx DBTX) (uint64, error) {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	// Create scanner for full table scan
	sc := Scanner{}
	sc.Cmp1 = CMP_GE
	sc.Cmp2 = CMP_LE

	// Execute full table scan
	err := tx.Scan(req.Table, &sc)
	if err != nil {
		return 0, fmt.Errorf("table scan failed: %w", err)
	}

	var deletedCount uint64

	// Process each record
	for sc.Valid() {
		var record Record
		sc.Deref(&record)

		// Build primary key for this record
		key := buildPrimaryKey(record, tdef)

		// Delete the record
		deleted, err := tx.Delete(req.Table, key)
		if err != nil {
			return 0, fmt.Errorf("delete failed: %w", err)
		}

		if deleted {
			deletedCount++
		}

		sc.Next()
	}

	return deletedCount, nil
}

// ValidateDeleteRequest validates a DELETE request before execution
func ValidateDeleteRequest(req *QLDelete, tx DBTX) error {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return fmt.Errorf("table %s not found", req.Table)
	}

	// Validate scan configuration
	if req.Key1.Value.Type != 0 {
		// Validate key1 if provided
		if err := validateScanKey(req.Key1, tdef); err != nil {
			return fmt.Errorf("key1 validation failed: %w", err)
		}
	}

	if req.Key2.Value.Type != 0 {
		// Validate key2 if provided
		if err := validateScanKey(req.Key2, tdef); err != nil {
			return fmt.Errorf("key2 validation failed: %w", err)
		}
	}

	// Validate filter if provided
	if req.Filter.Value.Type != 0 {
		if err := validateFilter(req.Filter, tdef); err != nil {
			return fmt.Errorf("filter validation failed: %w", err)
		}
	}

	return nil
}

// validateScanKey validates a scan key expression
func validateScanKey(key QLNode, tdef *TableDef) error {
	// Basic validation - in a full implementation, this would validate
	// that the key expression references valid columns and has correct types
	return nil
}

// validateFilter validates a filter expression
func validateFilter(filter QLNode, tdef *TableDef) error {
	// Basic validation - in a full implementation, this would validate
	// that the filter expression references valid columns and returns boolean
	return nil
}

// GetDeletePreview shows what records would be deleted without actually deleting
func GetDeletePreview(req *QLDelete, tx DBTX) ([]Record, error) {
	// Execute scan to find records that would be deleted
	var out []Record
	records, err := qlScan(&req.QLScan, tx, out)
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	return records, nil
}

// CountRecordsToDelete counts how many records would be deleted
func CountRecordsToDelete(req *QLDelete, tx DBTX) (uint64, error) {
	records, err := GetDeletePreview(req, tx)
	if err != nil {
		return 0, err
	}

	return uint64(len(records)), nil
}
