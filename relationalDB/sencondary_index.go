package relationaldb

import "govetachun/go-mini-db/utils"

func checkIndexKeys(tdef *TableDef, index []string) ([]string, error) {
	icols := map[string]bool{}
	for _, c := range index {
		// check the index columns
		// omitted...
		icols[c] = true
	}
	// add the primary key to the index
	for _, c := range tdef.Cols[:tdef.PKeys] {
		if !icols[c] {
			index = append(index, c)
		}
	}
	utils.Assert(len(index) < len(tdef.Cols), "len(index) < len(tdef.Cols)")
	return index, nil
}
func colIndex(tdef *TableDef, col string) int {
	for i, c := range tdef.Cols {
		if c == col {
			return i
		}
	}
	return -1
}
