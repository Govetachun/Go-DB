package relationaldb

import "fmt"

// add a row to the table
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])

	// Check if record exists to get old value for index maintenance
	var oldVal []byte
	if mode == MODE_UPDATE_ONLY || mode == MODE_UPSERT {
		if existingVal, exists := db.kv.Get(key); exists {
			oldVal = existingVal
		}
	}

	added, err := db.kv.Update(key, val, mode)
	if err != nil || !added || len(tdef.Indexes) == 0 {
		return added, err
	}

	// maintain indexes
	if added && oldVal != nil {
		// Delete old index entries
		oldValues := make([]Value, len(tdef.Cols))
		copy(oldValues, values)
		decodeValues(oldVal, oldValues[tdef.PKeys:])
		indexOp(db, tdef, Record{Cols: tdef.Cols, Vals: oldValues}, INDEX_DEL)
	}
	if added {
		// Add new index entries
		indexOp(db, tdef, rec, INDEX_ADD)
	}
	return added, nil
}

// add a record
func (db *DB) Set(table string, rec Record, mode int) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode)
}
func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY)
}
func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY)
}
func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT)
}

// delete a record by its primary key
func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])

	// Get the old value for index maintenance
	var oldVal []byte
	if existingVal, exists := db.kv.Get(key); exists {
		oldVal = existingVal
	}

	deleted := db.kv.Delete(key)
	if !deleted || len(tdef.Indexes) == 0 {
		return deleted, nil
	}

	// maintain indexes
	if deleted && oldVal != nil {
		// Delete index entries for the old record
		oldValues := make([]Value, len(tdef.Cols))
		copy(oldValues, values)
		decodeValues(oldVal, oldValues[tdef.PKeys:])
		indexOp(db, tdef, Record{Cols: tdef.Cols, Vals: oldValues}, INDEX_DEL)
	}
	return true, nil
}
func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}
