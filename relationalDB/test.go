package relationaldb

import "fmt"

// Step 1: Select an Index

// We’ll also implement range queries using a prefix of an index. For example, we can do x
// < a AND a < y on the index [a, b, c], which contains the prefix [a]. Selecting an index
// is simply matching columns by the input prefix. The primary key is considered before
// secondary indexes

func findIndex(tdef *TableDef, keys []string) (int, error) {
	pk := tdef.Cols[:tdef.PKeys]
	if isPrefix(pk, keys) {
		// use the primary key.
		// also works for full table scans without a key.
		return -1, nil
	}
	// find a suitable index
	winner := -2
	for i, index := range tdef.Indexes {
		if !isPrefix(index, keys) {
			continue
		}
		if winner == -2 || len(index) < len(tdef.Indexes[winner]) {
			winner = i
		}
	}
	if winner == -2 {
		return -2, fmt.Errorf("no index found")
	}
	return winner, nil
}
func isPrefix(long []string, short []string) bool {
	if len(long) < len(short) {
		return false
	}
	for i, c := range short {
		if long[i] != c {
			return false
		}
	}
	return true
}

// Step 2: Encode Index Prefix
// We may have to encode extra columns if the input key uses a prefix of an index instead of
// the full index. For example, for a query v1 < a with the index [a, b], we cannot use [v1]
// < key as the underlying B-tree query, because any key [v1, v2] satisfies [v1] < [v1, v2]
// while violating v1 < a.
// Instead, we can use [v1, MAX] < key in this case where the MAX is the maximum possible
// value for column b. Below is the function for encoding a partial query key with additional
// columns

// The range key can be a prefix of the index key,
// we may have to encode missing columns to make the comparison work.
func encodeKeyPartial(
	out []byte, prefix uint32, values []Value,
	tdef *TableDef, keys []string, cmp int,
) []byte {
	out = encodeKey(out, prefix, values)
	// Encode the missing columns as either minimum or maximum values,
	// depending on the comparison operator.
	// 1. The empty string is lower than all possible value encodings,
	// thus we don't need to add anything for CMP_LT and CMP_GE.
	// 2. The maximum encodings are all 0xff bytes.
	max := cmp == CMP_GT || cmp == CMP_LE
loop:
	for i := len(values); max && i < len(keys); i++ {
		switch tdef.Types[colIndex(tdef, keys[i])] {
		case TYPE_BYTES:
			out = append(out, 0xff)
			break loop // stops here since no string encoding starts with 0xff
		case TYPE_INT64:
			out = append(out, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
		default:
			panic("what?")
		}
	}
	return out
}
// For the int64 type, the maximum value is encoded as all 0xff bytes. The problem is that
// there is no maximum value for strings. What we can do is use the "\xff" as the encoding
// of the “pseudo maximum string value”, and change the normal string encoding to not
// startk with the "\xff".
// The first byte of a string is escaped by the "\xfe" byte if it’s "\xff" or "\xfe". Thus all
// string encodings are lower than "\xff".


// Step 3: Fetch Rows via Indexes
// The index key contains all primary key columns so that we can find the full row. The
// Scanner type is now aware of the selected index


