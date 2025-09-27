package database

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// IndexManager handles index operations
type IndexManager struct {
	indexes map[string]*Index
	mu      sync.RWMutex
}

// Index represents a database index
type Index struct {
	Name        string
	TableName   string
	Columns     []string
	ColumnTypes []uint32
	Data        map[string][]*Record // Index key -> Records mapping
	mu          sync.RWMutex
}

// IndexEntry represents an entry in an index
type IndexEntry struct {
	Key     string
	Records []*Record
}

// NewIndexManager creates a new index manager
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*Index),
	}
}

// CreateIndex creates a new index
func (im *IndexManager) CreateIndex(name string, tableName string, columns []string, columnTypes []uint32) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if name == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	if len(columns) == 0 {
		return fmt.Errorf("index must have at least one column")
	}

	if len(columns) != len(columnTypes) {
		return fmt.Errorf("column count mismatch: %d columns, %d types", len(columns), len(columnTypes))
	}

	if _, exists := im.indexes[name]; exists {
		return fmt.Errorf("index %s already exists", name)
	}

	// Validate column names
	for _, col := range columns {
		if col == "" {
			return fmt.Errorf("column name cannot be empty")
		}
		if !isValidIdentifier(col) {
			return fmt.Errorf("invalid column name: %s", col)
		}
	}

	// Validate column types
	for i, colType := range columnTypes {
		if colType != TYPE_INT64 && colType != TYPE_BYTES {
			return fmt.Errorf("invalid data type for column %s: %d", columns[i], colType)
		}
	}

	// Create new index
	index := &Index{
		Name:        name,
		TableName:   tableName,
		Columns:     make([]string, len(columns)),
		ColumnTypes: make([]uint32, len(columnTypes)),
		Data:        make(map[string][]*Record),
	}

	copy(index.Columns, columns)
	copy(index.ColumnTypes, columnTypes)

	im.indexes[name] = index
	return nil
}

// DropIndex drops an index
func (im *IndexManager) DropIndex(name string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, exists := im.indexes[name]; !exists {
		return fmt.Errorf("index %s does not exist", name)
	}

	delete(im.indexes, name)
	return nil
}

// GetIndex returns an index by name
func (im *IndexManager) GetIndex(name string) *Index {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.indexes[name]
}

// ListIndexes returns all index names
func (im *IndexManager) ListIndexes() []string {
	im.mu.RLock()
	defer im.mu.RUnlock()

	indexes := make([]string, 0, len(im.indexes))
	for name := range im.indexes {
		indexes = append(indexes, name)
	}
	return indexes
}

// ListIndexesForTable returns all indexes for a specific table
func (im *IndexManager) ListIndexesForTable(tableName string) []string {
	im.mu.RLock()
	defer im.mu.RUnlock()

	var indexes []string
	for name, index := range im.indexes {
		if index.TableName == tableName {
			indexes = append(indexes, name)
		}
	}
	return indexes
}

// Index Operations

// Add adds a record to the index
func (idx *Index) Add(record *Record) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	key, err := idx.generateIndexKey(record)
	if err != nil {
		return fmt.Errorf("failed to generate index key: %w", err)
	}

	// Check if record already exists in this index key
	records := idx.Data[key]
	for _, existingRecord := range records {
		if idx.recordsEqual(existingRecord, record) {
			// Record already exists, no need to add
			return nil
		}
	}

	// Add record to index
	idx.Data[key] = append(records, record)
	return nil
}

// Remove removes a record from the index
func (idx *Index) Remove(record *Record) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	key, err := idx.generateIndexKey(record)
	if err != nil {
		return fmt.Errorf("failed to generate index key: %w", err)
	}

	records := idx.Data[key]
	for i, existingRecord := range records {
		if idx.recordsEqual(existingRecord, record) {
			// Remove record from slice
			idx.Data[key] = append(records[:i], records[i+1:]...)

			// If no records left for this key, remove the key
			if len(idx.Data[key]) == 0 {
				delete(idx.Data, key)
			}
			return nil
		}
	}

	// Record not found in index
	return nil
}

// Lookup looks up records by index key
func (idx *Index) Lookup(key Record) ([]*Record, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	indexKey, err := idx.generateIndexKey(&key)
	if err != nil {
		return nil, fmt.Errorf("failed to generate index key: %w", err)
	}

	records := idx.Data[indexKey]
	if len(records) == 0 {
		return nil, nil
	}

	// Return copies of records
	result := make([]*Record, len(records))
	for i, record := range records {
		copy := idx.copyRecord(*record)
		result[i] = &copy
	}

	return result, nil
}

// RangeLookup performs a range lookup on the index
func (idx *Index) RangeLookup(startKey, endKey Record) ([]*Record, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []*Record

	// Get all index keys and sort them
	var keys []string
	for key := range idx.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Find range
	startKeyStr, err := idx.generateIndexKey(&startKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate start key: %w", err)
	}

	endKeyStr, err := idx.generateIndexKey(&endKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate end key: %w", err)
	}

	// Find records in range
	for _, key := range keys {
		if key >= startKeyStr && key <= endKeyStr {
			records := idx.Data[key]
			for _, record := range records {
				copy := idx.copyRecord(*record)
				result = append(result, &copy)
			}
		}
	}

	return result, nil
}

// Clear clears all data from the index
func (idx *Index) Clear() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.Data = make(map[string][]*Record)
	return nil
}

// Validate validates the index integrity
func (idx *Index) Validate() error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Check that all index entries have valid keys
	for key, records := range idx.Data {
		if key == "" {
			return fmt.Errorf("empty index key found")
		}

		for _, record := range records {
			if record == nil {
				return fmt.Errorf("nil record found in index")
			}

			// Validate that record has all required columns
			for _, colName := range idx.Columns {
				if record.Get(colName) == nil {
					return fmt.Errorf("record missing required column %s", colName)
				}
			}
		}
	}

	return nil
}

// GetIndexInfo returns information about the index
func (idx *Index) GetIndexInfo() *IndexInfo {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	info := &IndexInfo{
		Name:        idx.Name,
		TableName:   idx.TableName,
		Columns:     make([]string, len(idx.Columns)),
		ColumnTypes: make([]uint32, len(idx.ColumnTypes)),
		EntryCount:  len(idx.Data),
	}

	copy(info.Columns, idx.Columns)
	copy(info.ColumnTypes, idx.ColumnTypes)

	// Count total records
	totalRecords := 0
	for _, records := range idx.Data {
		totalRecords += len(records)
	}
	info.RecordCount = totalRecords

	return info
}

// Helper Methods

// generateIndexKey generates a string key from index column values
func (idx *Index) generateIndexKey(record *Record) (string, error) {
	if record == nil {
		return "", fmt.Errorf("record cannot be nil")
	}

	var keyParts []string
	for i, colName := range idx.Columns {
		value := record.Get(colName)
		if value == nil {
			return "", fmt.Errorf("missing column %s", colName)
		}

		// Validate type
		if value.Type != idx.ColumnTypes[i] {
			return "", fmt.Errorf("type mismatch for column %s: expected %d, got %d",
				colName, idx.ColumnTypes[i], value.Type)
		}

		keyParts = append(keyParts, valueToString(*value))
	}

	// Join with separator
	return strings.Join(keyParts, "|"), nil
}

// recordsEqual checks if two records are equal
func (idx *Index) recordsEqual(r1, r2 *Record) bool {
	if r1 == nil || r2 == nil {
		return r1 == r2
	}

	// Compare primary key values
	for i := 0; i < len(idx.Columns) && i < len(r1.Cols) && i < len(r2.Cols); i++ {
		colName := idx.Columns[i]
		v1 := r1.Get(colName)
		v2 := r2.Get(colName)

		if v1 == nil || v2 == nil {
			return v1 == v2
		}

		if !valuesEqual(*v1, *v2) {
			return false
		}
	}

	return true
}

// copyRecord creates a deep copy of a record
func (idx *Index) copyRecord(record Record) Record {
	recordCopy := Record{
		Cols: make([]string, len(record.Cols)),
		Vals: make([]Value, len(record.Vals)),
	}

	copy(recordCopy.Cols, record.Cols)
	for i, val := range record.Vals {
		recordCopy.Vals[i] = Value{
			Type: val.Type,
			I64:  val.I64,
			Str:  make([]byte, len(val.Str)),
		}
		copy(recordCopy.Vals[i].Str, val.Str)
	}

	return recordCopy
}

// valuesEqual checks if two values are equal
func valuesEqual(v1, v2 Value) bool {
	if v1.Type != v2.Type {
		return false
	}

	switch v1.Type {
	case TYPE_INT64:
		return v1.I64 == v2.I64
	case TYPE_BYTES:
		return string(v1.Str) == string(v2.Str)
	default:
		return false
	}
}

// IndexInfo represents index metadata
type IndexInfo struct {
	Name        string
	TableName   string
	Columns     []string
	ColumnTypes []uint32
	EntryCount  int
	RecordCount int
}

// GetIndexStatistics returns statistics about the index
func (idx *Index) GetIndexStatistics() *IndexStatistics {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := &IndexStatistics{
		Name:           idx.Name,
		TableName:      idx.TableName,
		TotalKeys:      len(idx.Data),
		TotalRecords:   0,
		AverageRecords: 0,
		MaxRecords:     0,
		MinRecords:     0,
	}

	if len(idx.Data) == 0 {
		return stats
	}

	// Calculate statistics
	for _, records := range idx.Data {
		count := len(records)
		stats.TotalRecords += count

		if count > stats.MaxRecords {
			stats.MaxRecords = count
		}
		if stats.MinRecords == 0 || count < stats.MinRecords {
			stats.MinRecords = count
		}
	}

	stats.AverageRecords = float64(stats.TotalRecords) / float64(stats.TotalKeys)
	return stats
}

// IndexStatistics represents index performance statistics
type IndexStatistics struct {
	Name           string
	TableName      string
	TotalKeys      int
	TotalRecords   int
	AverageRecords float64
	MaxRecords     int
	MinRecords     int
}
