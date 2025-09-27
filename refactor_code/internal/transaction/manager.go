package transaction

import (
	"fmt"
	"sync"
	"time"
)

// TransactionManager manages database transactions
type TransactionManager struct {
	transactions map[TransactionID]*Transaction
	nextID       TransactionID
	mu           sync.RWMutex
	db           DB
}

// TransactionID represents a unique transaction identifier
type TransactionID uint64

// Transaction represents a database transaction
type Transaction struct {
	ID        TransactionID
	StartTime time.Time
	Isolation IsolationLevel
	Status    TransactionStatus
	ReadSet   map[string]map[string]bool    // table -> key -> exists
	WriteSet  map[string]map[string]*Record // table -> key -> record
	Locks     map[string]bool               // table -> locked
	mu        sync.RWMutex
}

// TransactionStatus represents the state of a transaction
type TransactionStatus int

const (
	StatusActive TransactionStatus = iota
	StatusCommitted
	StatusAborted
	StatusPrepared
)

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(db DB) *TransactionManager {
	return &TransactionManager{
		transactions: make(map[TransactionID]*Transaction),
		nextID:       1,
		db:           db,
	}
}

// BeginTransaction starts a new transaction
func (tm *TransactionManager) BeginTransaction(isolation IsolationLevel) (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txID := tm.nextID
	tm.nextID++

	transaction := &Transaction{
		ID:        txID,
		StartTime: time.Now(),
		Isolation: isolation,
		Status:    StatusActive,
		ReadSet:   make(map[string]map[string]bool),
		WriteSet:  make(map[string]map[string]*Record),
		Locks:     make(map[string]bool),
	}

	tm.transactions[txID] = transaction
	return transaction, nil
}

// CommitTransaction commits a transaction
func (tm *TransactionManager) CommitTransaction(tx *Transaction) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != StatusActive {
		return fmt.Errorf("transaction is not active")
	}

	// Validate transaction
	if err := tm.validateTransaction(tx); err != nil {
		tx.Status = StatusAborted
		return fmt.Errorf("transaction validation failed: %w", err)
	}

	// Apply writes
	if err := tm.applyWrites(tx); err != nil {
		tx.Status = StatusAborted
		return fmt.Errorf("failed to apply writes: %w", err)
	}

	// Release locks
	tm.releaseLocks(tx)

	tx.Status = StatusCommitted
	return nil
}

// AbortTransaction aborts a transaction
func (tm *TransactionManager) AbortTransaction(tx *Transaction) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != StatusActive {
		return fmt.Errorf("transaction is not active")
	}

	// Release locks
	tm.releaseLocks(tx)

	// Clear write set
	tx.WriteSet = make(map[string]map[string]*Record)

	tx.Status = StatusAborted
	return nil
}

// GetTransaction retrieves a transaction by ID
func (tm *TransactionManager) GetTransaction(txID TransactionID) *Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.transactions[txID]
}

// ListTransactions returns all active transactions
func (tm *TransactionManager) ListTransactions() []*Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var active []*Transaction
	for _, tx := range tm.transactions {
		if tx.Status == StatusActive {
			active = append(active, tx)
		}
	}
	return active
}

// CleanupTransactions removes completed transactions
func (tm *TransactionManager) CleanupTransactions() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	cutoff := time.Now().Add(-24 * time.Hour) // Keep transactions for 24 hours
	for txID, tx := range tm.transactions {
		if tx.Status != StatusActive && tx.StartTime.Before(cutoff) {
			delete(tm.transactions, txID)
		}
	}
}

// Transaction Operations

// Read performs a read operation within a transaction
func (tx *Transaction) Read(table string, key string) (*Record, error) {
	if tx.Status != StatusActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check write set first (read your own writes)
	if tx.WriteSet[table] != nil {
		if record, exists := tx.WriteSet[table][key]; exists {
			// Add to read set
			if tx.ReadSet[table] == nil {
				tx.ReadSet[table] = make(map[string]bool)
			}
			tx.ReadSet[table][key] = true
			return record, nil
		}
	}

	// Read from database
	// This would be implemented with actual database access
	// For now, return nil to indicate record not found
	record := &Record{} // Placeholder

	// Add to read set
	if tx.ReadSet[table] == nil {
		tx.ReadSet[table] = make(map[string]bool)
	}
	tx.ReadSet[table][key] = (record != nil)

	return record, nil
}

// Write performs a write operation within a transaction
func (tx *Transaction) Write(table string, key string, record *Record) error {
	if tx.Status != StatusActive {
		return fmt.Errorf("transaction is not active")
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Add to write set
	if tx.WriteSet[table] == nil {
		tx.WriteSet[table] = make(map[string]*Record)
	}
	tx.WriteSet[table][key] = record

	return nil
}

// Delete performs a delete operation within a transaction
func (tx *Transaction) Delete(table string, key string) error {
	if tx.Status != StatusActive {
		return fmt.Errorf("transaction is not active")
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Add to write set with nil value (indicates deletion)
	if tx.WriteSet[table] == nil {
		tx.WriteSet[table] = make(map[string]*Record)
	}
	tx.WriteSet[table][key] = nil

	return nil
}

// AcquireLock acquires a lock for a table
func (tx *Transaction) AcquireLock(table string) error {
	if tx.Status != StatusActive {
		return fmt.Errorf("transaction is not active")
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if already locked
	if tx.Locks[table] {
		return nil
	}

	// In a real implementation, this would use a lock manager
	// For now, just mark as locked
	tx.Locks[table] = true
	return nil
}

// ReleaseLock releases a lock for a table
func (tx *Transaction) ReleaseLock(table string) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	delete(tx.Locks, table)
}

// Helper Methods

// validateTransaction validates a transaction before commit
func (tm *TransactionManager) validateTransaction(tx *Transaction) error {
	// Check for conflicts based on isolation level
	switch tx.Isolation {
	case IsolationReadUncommitted:
		// No validation needed
	case IsolationReadCommitted:
		// Check for write-write conflicts
		return tm.validateWriteWriteConflicts(tx)
	case IsolationRepeatableRead:
		// Check for read-write and write-write conflicts
		if err := tm.validateReadWriteConflicts(tx); err != nil {
			return err
		}
		return tm.validateWriteWriteConflicts(tx)
	case IsolationSerializable:
		// Check for all types of conflicts
		if err := tm.validateReadWriteConflicts(tx); err != nil {
			return err
		}
		return tm.validateWriteWriteConflicts(tx)
	default:
		return fmt.Errorf("unknown isolation level: %d", tx.Isolation)
	}

	return nil
}

// validateReadWriteConflicts checks for read-write conflicts
func (tm *TransactionManager) validateReadWriteConflicts(tx *Transaction) error {
	// In a real implementation, this would check if any data
	// read by this transaction has been modified by other transactions
	// For now, return nil (no conflicts)
	return nil
}

// validateWriteWriteConflicts checks for write-write conflicts
func (tm *TransactionManager) validateWriteWriteConflicts(tx *Transaction) error {
	// In a real implementation, this would check if any data
	// written by this transaction has been modified by other transactions
	// For now, return nil (no conflicts)
	return nil
}

// applyWrites applies the write set to the database
func (tm *TransactionManager) applyWrites(tx *Transaction) error {
	for _, writes := range tx.WriteSet {
		for _, record := range writes {
			if record == nil {
				// Delete operation
				// In a real implementation, this would delete from the database
				continue
			}
			// Insert/Update operation
			// In a real implementation, this would write to the database
		}
	}
	return nil
}

// releaseLocks releases all locks held by a transaction
func (tm *TransactionManager) releaseLocks(tx *Transaction) {
	for table := range tx.Locks {
		tx.ReleaseLock(table)
	}
}

// GetTransactionInfo returns information about a transaction
func (tx *Transaction) GetTransactionInfo() *TransactionInfo {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	info := &TransactionInfo{
		ID:         tx.ID,
		StartTime:  tx.StartTime,
		Isolation:  tx.Isolation,
		Status:     tx.Status,
		ReadCount:  0,
		WriteCount: 0,
		LockCount:  len(tx.Locks),
	}

	// Count reads and writes
	for _, reads := range tx.ReadSet {
		info.ReadCount += len(reads)
	}
	for _, writes := range tx.WriteSet {
		info.WriteCount += len(writes)
	}

	return info
}

// TransactionInfo represents transaction metadata
type TransactionInfo struct {
	ID         TransactionID
	StartTime  time.Time
	Isolation  IsolationLevel
	Status     TransactionStatus
	ReadCount  int
	WriteCount int
	LockCount  int
}

// IsActive checks if the transaction is active
func (tx *Transaction) IsActive() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.Status == StatusActive
}

// GetAge returns the age of the transaction
func (tx *Transaction) GetAge() time.Duration {
	return time.Since(tx.StartTime)
}

// GetReadSetSize returns the number of items in the read set
func (tx *Transaction) GetReadSetSize() int {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	total := 0
	for _, reads := range tx.ReadSet {
		total += len(reads)
	}
	return total
}

// GetWriteSetSize returns the number of items in the write set
func (tx *Transaction) GetWriteSetSize() int {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	total := 0
	for _, writes := range tx.WriteSet {
		total += len(writes)
	}
	return total
}
