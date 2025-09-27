package database

// TableDef represents a table definition
type TableDef struct {
	Name  string
	Cols  []string
	Types []uint32
	PKeys int // number of primary key columns
}

// Record represents a database record
type Record struct {
	Cols []string
	Vals []Value
}

// Value represents a database value
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// Data types
const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// Get retrieves a value from the record by column name
func (r *Record) Get(col string) *Value {
	for i, c := range r.Cols {
		if c == col {
			return &r.Vals[i]
		}
	}
	return nil
}

// Comparison operators
const (
	CMP_GE = 1 // >=
	CMP_GT = 2 // >
	CMP_LT = 3 // <
	CMP_LE = 4 // <=
	CMP_EQ = 5 // =
	CMP_NE = 6 // !=
)

// Scanner represents a table scanner for queries
type Scanner struct {
	// Key range for scanning
	Key1, Key2 Record
	Cmp1, Cmp2 int

	// Internal state for iteration
	currentRecord *Record
	isValid       bool
	position      int
	totalRecords  int
}

// Valid checks if scanner is in valid state
func (sc *Scanner) Valid() bool {
	return sc.isValid && sc.position < sc.totalRecords
}

// Next advances the scanner
func (sc *Scanner) Next() {
	sc.position++
	if sc.position >= sc.totalRecords {
		sc.isValid = false
	}
}

// Deref gets current record from scanner
func (sc *Scanner) Deref(rec *Record) {
	if sc.currentRecord != nil {
		*rec = *sc.currentRecord
	}
}

// SetRecords sets the records to scan (for testing/simulation)
func (sc *Scanner) SetRecords(records []Record) {
	sc.totalRecords = len(records)
	sc.position = 0
	sc.isValid = len(records) > 0
	if len(records) > 0 {
		sc.currentRecord = &records[0]
	}
}

// TableInfo represents table metadata
type TableInfo struct {
	Name        string
	Columns     []ColumnInfo
	PrimaryKeys int
	RecordCount int
	IndexCount  int
}

// ColumnInfo represents column metadata
type ColumnInfo struct {
	Name         string
	Type         int
	IsPrimaryKey bool
}
