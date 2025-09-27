package concurrency

import (
	"fmt"
	"sync"
	"time"
)

// TransactionID represents a unique transaction identifier
type TransactionID uint64

// MVCCManager manages multi-version concurrency control
type MVCCManager struct {
	mu           sync.RWMutex
	versions     map[string]map[string]*Version // table -> key -> version
	transactions map[TransactionID]*Transaction
	nextVersion  uint64
	nextTxID     TransactionID
}

// Version represents a version of data
type Version struct {
	Version     uint64
	Transaction TransactionID
	Data        interface{}
	Timestamp   time.Time
	IsDeleted   bool
	Next        *Version // Linked list of versions
}

// Transaction represents an MVCC transaction
type Transaction struct {
	ID        TransactionID
	StartTime time.Time
	ReadTime  time.Time
	Status    TransactionStatus
	ReadSet   map[string]map[string]bool        // table -> key -> read
	WriteSet  map[string]map[string]interface{} // table -> key -> data
	mu        sync.RWMutex
}

// TransactionStatus represents the status of an MVCC transaction
type TransactionStatus int

const (
	StatusActive TransactionStatus = iota
	StatusCommitted
	StatusAborted
)

// NewMVCCManager creates a new MVCC manager
func NewMVCCManager() *MVCCManager {
	return &MVCCManager{
		versions:     make(map[string]map[string]*Version),
		transactions: make(map[TransactionID]*Transaction),
		nextVersion:  1,
		nextTxID:     1,
	}
}

// BeginTransaction starts a new MVCC transaction
func (mvcc *MVCCManager) BeginTransaction() *Transaction {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	txID := mvcc.nextTxID
	mvcc.nextTxID++

	transaction := &Transaction{
		ID:        txID,
		StartTime: time.Now(),
		ReadTime:  time.Now(),
		Status:    StatusActive,
		ReadSet:   make(map[string]map[string]bool),
		WriteSet:  make(map[string]map[string]interface{}),
	}

	mvcc.transactions[txID] = transaction
	return transaction
}

// Read reads data from a specific version
func (mvcc *MVCCManager) Read(tx *Transaction, table string, key string) (interface{}, error) {
	if tx.Status != StatusActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check write set first (read your own writes)
	if tx.WriteSet[table] != nil {
		if data, exists := tx.WriteSet[table][key]; exists {
			// Record in read set
			if tx.ReadSet[table] == nil {
				tx.ReadSet[table] = make(map[string]bool)
			}
			tx.ReadSet[table][key] = true
			return data, nil
		}
	}

	// Find the appropriate version to read
	version, err := mvcc.findReadVersion(tx, table, key)
	if err != nil {
		return nil, err
	}

	if version == nil || version.IsDeleted {
		return nil, nil // Not found or deleted
	}

	// Record in read set
	if tx.ReadSet[table] == nil {
		tx.ReadSet[table] = make(map[string]bool)
	}
	tx.ReadSet[table][key] = true

	return version.Data, nil
}

// Write writes data to a new version
func (mvcc *MVCCManager) Write(tx *Transaction, table string, key string, data interface{}) error {
	if tx.Status != StatusActive {
		return fmt.Errorf("transaction is not active")
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Add to write set
	if tx.WriteSet[table] == nil {
		tx.WriteSet[table] = make(map[string]interface{})
	}
	tx.WriteSet[table][key] = data

	return nil
}

// Delete marks data as deleted
func (mvcc *MVCCManager) Delete(tx *Transaction, table string, key string) error {
	if tx.Status != StatusActive {
		return fmt.Errorf("transaction is not active")
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Add to write set with nil data (indicates deletion)
	if tx.WriteSet[table] == nil {
		tx.WriteSet[table] = make(map[string]interface{})
	}
	tx.WriteSet[table][key] = nil

	return nil
}

// CommitTransaction commits an MVCC transaction
func (mvcc *MVCCManager) CommitTransaction(tx *Transaction) error {
	if tx.Status != StatusActive {
		return fmt.Errorf("transaction is not active")
	}

	// Validate transaction
	if err := mvcc.validateTransaction(tx); err != nil {
		tx.Status = StatusAborted
		return fmt.Errorf("transaction validation failed: %w", err)
	}

	// Apply writes
	if err := mvcc.applyWrites(tx); err != nil {
		tx.Status = StatusAborted
		return fmt.Errorf("failed to apply writes: %w", err)
	}

	tx.Status = StatusCommitted
	return nil
}

// AbortTransaction aborts an MVCC transaction
func (mvcc *MVCCManager) AbortTransaction(tx *Transaction) {
	if tx.Status != StatusActive {
		return
	}

	tx.Status = StatusAborted
}

// Helper Methods

// findReadVersion finds the appropriate version to read for a transaction
func (mvcc *MVCCManager) findReadVersion(tx *Transaction, table string, key string) (*Version, error) {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	tableVersions, exists := mvcc.versions[table]
	if !exists {
		return nil, nil
	}

	// Find the latest version that was committed before the transaction's read time
	var latestVersion *Version
	for _, version := range tableVersions {
		if version.Transaction == tx.ID {
			continue // Skip our own uncommitted writes
		}

		if version.Timestamp.Before(tx.ReadTime) || version.Timestamp.Equal(tx.ReadTime) {
			if latestVersion == nil || version.Version > latestVersion.Version {
				latestVersion = version
			}
		}
	}

	return latestVersion, nil
}

// validateTransaction validates an MVCC transaction
func (mvcc *MVCCManager) validateTransaction(tx *Transaction) error {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	// Check for conflicts with other transactions
	for table, readKeys := range tx.ReadSet {
		for key := range readKeys {
			// Check if any other transaction has written to this key since we read it
			if err := mvcc.checkWriteConflict(tx, table, key); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkWriteConflict checks for write conflicts
func (mvcc *MVCCManager) checkWriteConflict(tx *Transaction, table string, key string) error {
	tableVersions, exists := mvcc.versions[table]
	if !exists {
		return nil
	}

	// Check if any version was created after our read time
	if version, exists := tableVersions[key]; exists {
		if version.Transaction == tx.ID {
			return nil // Skip our own writes
		}

		if version.Timestamp.After(tx.ReadTime) {
			return fmt.Errorf("write conflict: key %s in table %s was modified after transaction read time", key, table)
		}
	}

	return nil
}

// applyWrites applies the write set to create new versions
func (mvcc *MVCCManager) applyWrites(tx *Transaction) error {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	for table, writes := range tx.WriteSet {
		if mvcc.versions[table] == nil {
			mvcc.versions[table] = make(map[string]*Version)
		}

		for key, data := range writes {
			version := &Version{
				Version:     mvcc.nextVersion,
				Transaction: tx.ID,
				Data:        data,
				Timestamp:   time.Now(),
				IsDeleted:   (data == nil),
			}

			mvcc.versions[table][key] = version
			mvcc.nextVersion++
		}
	}

	return nil
}

// GetVersion returns a specific version
func (mvcc *MVCCManager) GetVersion(table string, key string) *Version {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	if tableVersions, exists := mvcc.versions[table]; exists {
		return tableVersions[key]
	}
	return nil
}

// GetLatestVersion returns the latest version for a key
func (mvcc *MVCCManager) GetLatestVersion(table string, key string) *Version {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	if tableVersions, exists := mvcc.versions[table]; exists {
		if version, exists := tableVersions[key]; exists {
			return version
		}
	}
	return nil
}

// ListVersions returns all versions for a table
func (mvcc *MVCCManager) ListVersions(table string) map[string]*Version {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	if tableVersions, exists := mvcc.versions[table]; exists {
		// Return a copy
		result := make(map[string]*Version)
		for key, version := range tableVersions {
			result[key] = version
		}
		return result
	}
	return nil
}

// GetTransaction returns a transaction by ID
func (mvcc *MVCCManager) GetTransaction(txID TransactionID) *Transaction {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()
	return mvcc.transactions[txID]
}

// ListTransactions returns all active transactions
func (mvcc *MVCCManager) ListTransactions() []*Transaction {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	var active []*Transaction
	for _, tx := range mvcc.transactions {
		if tx.Status == StatusActive {
			active = append(active, tx)
		}
	}
	return active
}

// CleanupVersions removes old versions
func (mvcc *MVCCManager) CleanupVersions(maxAge time.Duration) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for table, versions := range mvcc.versions {
		for key, version := range versions {
			if version.Timestamp.Before(cutoff) && version.IsDeleted {
				delete(versions, key)
			}
		}
		if len(versions) == 0 {
			delete(mvcc.versions, table)
		}
	}
}

// GetMVCCInfo returns information about the MVCC state
func (mvcc *MVCCManager) GetMVCCInfo() *MVCCInfo {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	info := &MVCCInfo{
		NextVersion:        mvcc.nextVersion,
		NextTxID:           mvcc.nextTxID,
		TableCount:         len(mvcc.versions),
		TransactionCount:   len(mvcc.transactions),
		ActiveTransactions: 0,
		VersionCount:       0,
	}

	// Count active transactions and versions
	for _, tx := range mvcc.transactions {
		if tx.Status == StatusActive {
			info.ActiveTransactions++
		}
	}

	for _, versions := range mvcc.versions {
		info.VersionCount += len(versions)
	}

	return info
}

// MVCCInfo represents MVCC state information
type MVCCInfo struct {
	NextVersion        uint64
	NextTxID           TransactionID
	TableCount         int
	TransactionCount   int
	ActiveTransactions int
	VersionCount       int
}

// VersionHistory represents the history of versions for a key
type VersionHistory struct {
	Table    string
	Key      string
	Versions []*Version
}

// GetVersionHistory returns the version history for a key
func (mvcc *MVCCManager) GetVersionHistory(table string, key string) *VersionHistory {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	history := &VersionHistory{
		Table:    table,
		Key:      key,
		Versions: make([]*Version, 0),
	}

	if tableVersions, exists := mvcc.versions[table]; exists {
		if version, exists := tableVersions[key]; exists {
			// Follow the linked list of versions
			current := version
			for current != nil {
				history.Versions = append(history.Versions, current)
				current = current.Next
			}
		}
	}

	return history
}

// Snapshot represents a point-in-time snapshot
type Snapshot struct {
	Timestamp time.Time
	Versions  map[string]map[string]*Version
}

// CreateSnapshot creates a point-in-time snapshot
func (mvcc *MVCCManager) CreateSnapshot() *Snapshot {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	snapshot := &Snapshot{
		Timestamp: time.Now(),
		Versions:  make(map[string]map[string]*Version),
	}

	// Copy all versions
	for table, versions := range mvcc.versions {
		snapshot.Versions[table] = make(map[string]*Version)
		for key, version := range versions {
			snapshot.Versions[table][key] = version
		}
	}

	return snapshot
}

// ReadFromSnapshot reads data from a snapshot
func (snapshot *Snapshot) ReadFromSnapshot(table string, key string) interface{} {
	if tableVersions, exists := snapshot.Versions[table]; exists {
		if version, exists := tableVersions[key]; exists {
			if version.IsDeleted {
				return nil
			}
			return version.Data
		}
	}
	return nil
}
