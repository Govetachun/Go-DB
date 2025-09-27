package relationaldb

import "govetachun/go-mini-db/utils"

// maintain indexes after a record is added or removed
func indexOp(db *DB, tdef *TableDef, rec Record, op int) {
	key := make([]byte, 0, 256)
	irec := make([]Value, len(tdef.Cols))
	for i, index := range tdef.Indexes {
		// the indexed key
		for j, c := range index {
			irec[j] = *rec.Get(c)
		}
		// update the KV store
		key = encodeKey(key[:0], tdef.IndexPrefixes[i], irec[:len(index)])
		var done bool
		var err error
		switch op {
		case INDEX_ADD:
			done, err = db.kv.Update(key, nil, MODE_INSERT_ONLY)
		case INDEX_DEL:
			done, err = db.kv.Delete(key), nil
		default:
			panic("what?")
		}
		utils.Assert(err == nil, "err == nil") // XXX: will fix this in later chapters
		utils.Assert(done, "done")
	}
}
