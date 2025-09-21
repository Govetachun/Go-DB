package relationaldb

func (req *InsertReq) InsertEx() {
	// Check if key already exists
	_, exists := req.tree.Get(req.Key)

	switch req.Mode {
	case MODE_UPSERT:
		// Insert or replace
		req.tree.Insert(req.Key, req.Val)
		req.Added = !exists
	case MODE_UPDATE_ONLY:
		// Only update existing keys
		if exists {
			req.tree.Insert(req.Key, req.Val)
			req.Added = false
		} else {
			req.Added = false
		}
	case MODE_INSERT_ONLY:
		// Only add new keys
		if !exists {
			req.tree.Insert(req.Key, req.Val)
			req.Added = true
		} else {
			req.Added = false
		}
	}
}

func (db *DB) Update(key []byte, val []byte, mode int) (bool, error) {
	req := &InsertReq{
		tree: db.kv,
		Key:  key,
		Val:  val,
		Mode: mode,
	}
	req.InsertEx()
	return req.Added, nil
}
