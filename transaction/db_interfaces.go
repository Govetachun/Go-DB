package transaction

import (
	concurrentreaderwriter "govetachun/go-mini-db/concurrent-reader-writer"
	relationaldb "govetachun/go-mini-db/relationalDB"
)

type DBTX struct {
	kv      KVTX
	db      *relationaldb.DB
	kvStore *concurrentreaderwriter.KV
}

func (db *DBTX) Begin(tx *DBTX) {
	tx.db = db.db
	tx.kvStore = db.kvStore
	Begin(db.kvStore, &tx.kv)
}

func (db *DBTX) Commit(tx *DBTX) error {
	return Commit(db.kvStore, &tx.kv)
}

func (db *DBTX) Abort(tx *DBTX) {
	Abort(db.kvStore, &tx.kv)
}
func (tx *DBTX) TableNew(tdef *relationaldb.TableDef) error {
	return tx.db.TableNew(tdef)
}
func (tx *DBTX) Get(table string, rec *relationaldb.Record) (bool, error) {
	return tx.db.Get(table, rec)
}
func (tx *DBTX) Set(table string, rec relationaldb.Record, mode int) (bool, error) {
	return tx.db.Set(table, rec, mode)
}
func (tx *DBTX) Delete(table string, rec relationaldb.Record) (bool, error) {
	return tx.db.Delete(table, rec)
}
func (tx *DBTX) Scan(table string, req *relationaldb.Scanner) error {
	return tx.db.Scan(table, req)
}

// GetDB returns the underlying database (exported for query execution use)
func (tx *DBTX) GetDB() *relationaldb.DB {
	return tx.db
}
