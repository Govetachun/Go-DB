package relationaldb

import "fmt"

func (rec *Record) AddStr(key string, val []byte) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_BYTES, Str: val})
	return rec
}

func (rec *Record) AddInt64(key string, val int64) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_INT64, I64: val})
	return rec
}

func (rec *Record) Get(key string) *Value {
	for i, col := range rec.Cols {
		if col == key {
			return &rec.Vals[i]
		}
	}
	return nil
}

func tableDefCheck(tdef *TableDef) error {
	if tdef.Name == "" {
		return fmt.Errorf("table name is required")
	}
	if len(tdef.Types) == 0 {
		return fmt.Errorf("table must have at least one column")
	}
	if len(tdef.Types) != len(tdef.Cols) {
		return fmt.Errorf("types and columns count mismatch")
	}
	if tdef.PKeys <= 0 || tdef.PKeys > len(tdef.Cols) {
		return fmt.Errorf("invalid primary key count")
	}
	for i, index := range tdef.Indexes {
		index, err := checkIndexKeys(tdef, index)
		if err != nil {
		return err
		}
		tdef.Indexes[i] = index
	}
	return nil
}
