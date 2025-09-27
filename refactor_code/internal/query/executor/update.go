package executor

import (
	"fmt"
)

// ExecuteUpdate executes an UPDATE statement
func ExecuteUpdate(req *QLUpdate, tx DBTX) (uint64, error) {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	// Validate SET clause
	if len(req.Names) != len(req.Values) {
		return 0, fmt.Errorf("column count mismatch in SET clause: %d columns, %d values",
			len(req.Names), len(req.Values))
	}

	// Execute scan to find records to update
	var out []Record
	records, err := qlScan(&req.QLScan, tx, out)
	if err != nil {
		return 0, fmt.Errorf("scan failed: %w", err)
	}

	var updatedCount uint64

	// Process each matching record
	for _, record := range records {
		// Build primary key for this record
		key := buildPrimaryKey(record, tdef)

		// Create updated record
		updatedRecord := record // Start with original record

		// Apply SET clause updates
		for i, colName := range req.Names {
			// Evaluate the new value
			ctx := QLEvalContex{env: record}
			qlEval(&ctx, req.Values[i])
			if ctx.err != nil {
				return 0, fmt.Errorf("value evaluation failed for column %s: %w", colName, ctx.err)
			}

			// Update the column value in the record
			for j, recordCol := range updatedRecord.Cols {
				if recordCol == colName {
					updatedRecord.Vals[j] = ctx.out
					break
				}
			}
		}

		// Validate updated record
		if err := validateRecord(updatedRecord, tdef); err != nil {
			return 0, fmt.Errorf("updated record validation failed: %w", err)
		}

		// Perform the update
		err := tx.Update(req.Table, key, updatedRecord)
		if err != nil {
			return 0, fmt.Errorf("update failed: %w", err)
		}

		updatedCount++
	}

	return updatedCount, nil
}

// ExecuteUpdateWithCondition executes UPDATE with additional conditions
func ExecuteUpdateWithCondition(req *QLUpdate, tx DBTX, condition QLNode) (uint64, error) {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	// Validate SET clause
	if len(req.Names) != len(req.Values) {
		return 0, fmt.Errorf("column count mismatch in SET clause: %d columns, %d values",
			len(req.Names), len(req.Values))
	}

	// Execute scan to find records to update
	var out []Record
	records, err := qlScan(&req.QLScan, tx, out)
	if err != nil {
		return 0, fmt.Errorf("scan failed: %w", err)
	}

	var updatedCount uint64

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

		// Create updated record
		updatedRecord := record // Start with original record

		// Apply SET clause updates
		for i, colName := range req.Names {
			// Evaluate the new value
			ctx := QLEvalContex{env: record}
			qlEval(&ctx, req.Values[i])
			if ctx.err != nil {
				return 0, fmt.Errorf("value evaluation failed for column %s: %w", colName, ctx.err)
			}

			// Update the column value in the record
			for j, recordCol := range updatedRecord.Cols {
				if recordCol == colName {
					updatedRecord.Vals[j] = ctx.out
					break
				}
			}
		}

		// Validate updated record
		if err := validateRecord(updatedRecord, tdef); err != nil {
			return 0, fmt.Errorf("updated record validation failed: %w", err)
		}

		// Perform the update
		err := tx.Update(req.Table, key, updatedRecord)
		if err != nil {
			return 0, fmt.Errorf("update failed: %w", err)
		}

		updatedCount++
	}

	return updatedCount, nil
}

// ExecuteUpdateSingle updates a single record by primary key
func ExecuteUpdateSingle(req *QLUpdate, tx DBTX, key Record) error {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return fmt.Errorf("table %s not found", req.Table)
	}

	// Get the existing record
	record, err := tx.Get(req.Table, key)
	if err != nil {
		return fmt.Errorf("get record failed: %w", err)
	}
	if record == nil {
		return fmt.Errorf("record not found")
	}

	// Validate SET clause
	if len(req.Names) != len(req.Values) {
		return fmt.Errorf("column count mismatch in SET clause: %d columns, %d values",
			len(req.Names), len(req.Values))
	}

	// Create updated record
	updatedRecord := *record // Start with original record

	// Apply SET clause updates
	for i, colName := range req.Names {
		// Evaluate the new value
		ctx := QLEvalContex{env: *record}
		qlEval(&ctx, req.Values[i])
		if ctx.err != nil {
			return fmt.Errorf("value evaluation failed for column %s: %w", colName, ctx.err)
		}

		// Update the column value in the record
		for j, recordCol := range updatedRecord.Cols {
			if recordCol == colName {
				updatedRecord.Vals[j] = ctx.out
				break
			}
		}
	}

	// Validate updated record
	if err := validateRecord(updatedRecord, tdef); err != nil {
		return fmt.Errorf("updated record validation failed: %w", err)
	}

	// Perform the update
	err = tx.Update(req.Table, key, updatedRecord)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	return nil
}

// ExecuteUpdateBatch updates multiple records in a batch
func ExecuteUpdateBatch(req *QLUpdate, tx DBTX, keys []Record) (uint64, error) {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return 0, fmt.Errorf("table %s not found", req.Table)
	}

	// Validate SET clause
	if len(req.Names) != len(req.Values) {
		return 0, fmt.Errorf("column count mismatch in SET clause: %d columns, %d values",
			len(req.Names), len(req.Values))
	}

	var updatedCount uint64

	// Process each key
	for _, key := range keys {
		// Get the existing record
		record, err := tx.Get(req.Table, key)
		if err != nil {
			return 0, fmt.Errorf("get record failed: %w", err)
		}
		if record == nil {
			// Record not found, skip
			continue
		}

		// Create updated record
		updatedRecord := *record // Start with original record

		// Apply SET clause updates
		for i, colName := range req.Names {
			// Evaluate the new value
			ctx := QLEvalContex{env: *record}
			qlEval(&ctx, req.Values[i])
			if ctx.err != nil {
				return 0, fmt.Errorf("value evaluation failed for column %s: %w", colName, ctx.err)
			}

			// Update the column value in the record
			for j, recordCol := range updatedRecord.Cols {
				if recordCol == colName {
					updatedRecord.Vals[j] = ctx.out
					break
				}
			}
		}

		// Validate updated record
		if err := validateRecord(updatedRecord, tdef); err != nil {
			return 0, fmt.Errorf("updated record validation failed: %w", err)
		}

		// Perform the update
		err = tx.Update(req.Table, key, updatedRecord)
		if err != nil {
			return 0, fmt.Errorf("update failed: %w", err)
		}

		updatedCount++
	}

	return updatedCount, nil
}

// ValidateUpdateRequest validates an UPDATE request before execution
func ValidateUpdateRequest(req *QLUpdate, tx DBTX) error {
	// Get table definition
	tdef := tx.GetDB().GetTableDef(req.Table)
	if tdef == nil {
		return fmt.Errorf("table %s not found", req.Table)
	}

	// Validate SET clause
	if len(req.Names) == 0 {
		return fmt.Errorf("SET clause cannot be empty")
	}

	if len(req.Names) != len(req.Values) {
		return fmt.Errorf("column count mismatch in SET clause: %d columns, %d values",
			len(req.Names), len(req.Values))
	}

	// Validate column names
	for _, colName := range req.Names {
		found := false
		for _, tdefCol := range tdef.Cols {
			if tdefCol == colName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column %s not found in table %s", colName, req.Table)
		}
	}

	// Validate that primary key columns are not being updated
	for _, colName := range req.Names {
		for i := 0; i < tdef.PKeys; i++ {
			if tdef.Cols[i] == colName {
				return fmt.Errorf("cannot update primary key column: %s", colName)
			}
		}
	}

	return nil
}
