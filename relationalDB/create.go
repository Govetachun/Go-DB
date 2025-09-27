package relationaldb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"govetachun/go-mini-db/utils"
)

func (db *DB) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}
	// check the existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	utils.Assert(err == nil, "err == nil")
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}
	// allocate a new prefix
	utils.Assert(tdef.Prefix == 0, "tdef.Prefix == 0")
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	utils.Assert(err == nil, "err == nil")
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		utils.Assert(tdef.Prefix > TABLE_PREFIX_MIN, "tdef.Prefix > TABLE_PREFIX_MIN")
	} else {
		meta.AddStr("val", make([]byte, 4))
	}
	// update the next prefix
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
	_, err = dbUpdate(db, TDEF_META, *meta, 0)
	if err != nil {
		return err
	}
	// allocate new prefixes
	for i := range tdef.Indexes {
		prefix := tdef.Prefix + 1 + uint32(i)
		tdef.IndexPrefixes = append(tdef.IndexPrefixes, prefix)
	}
	// update the next prefix
	ntree := 1 + uint32(len(tdef.Indexes))
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+ntree)
	_, err = dbUpdate(db, TDEF_META, *meta, 0)
	if err != nil {
		return err
	}
	// store the definition
	val, err := json.Marshal(tdef)
	utils.Assert(err == nil, "err == nil")
	table.AddStr("def", val)
	_, err = dbUpdate(db, TDEF_TABLE, *table, 0)
	return err
}
