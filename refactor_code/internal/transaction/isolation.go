package transaction

import (
	"fmt"
	"sync"
)

// IsolationLevel represents the isolation level of a transaction
type IsolationLevel int

const (
	IsolationReadUncommitted IsolationLevel = iota
	IsolationReadCommitted
	IsolationRepeatableRead
	IsolationSerializable
)

// String returns the string representation of an isolation level
func (il IsolationLevel) String() string {
	switch il {
	case IsolationReadUncommitted:
		return "READ UNCOMMITTED"
	case IsolationReadCommitted:
		return "READ COMMITTED"
	case IsolationRepeatableRead:
		return "REPEATABLE READ"
	case IsolationSerializable:
		return "SERIALIZABLE"
	default:
		return "UNKNOWN"
	}
}

// IsolationManager manages isolation levels and conflict detection
type IsolationManager struct {
	mu            sync.RWMutex
	versionMap    map[string]uint64                   // table -> version
	readVersions  map[TransactionID]map[string]uint64 // tx -> table -> version
	writeVersions map[TransactionID]map[string]uint64 // tx -> table -> version
}

// NewIsolationManager creates a new isolation manager
func NewIsolationManager() *IsolationManager {
	return &IsolationManager{
		versionMap:    make(map[string]uint64),
		readVersions:  make(map[TransactionID]map[string]uint64),
		writeVersions: make(map[TransactionID]map[string]uint64),
	}
}

// BeginTransaction initializes isolation tracking for a transaction
func (im *IsolationManager) BeginTransaction(txID TransactionID, isolation IsolationLevel) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	// Initialize version tracking for the transaction
	im.readVersions[txID] = make(map[string]uint64)
	im.writeVersions[txID] = make(map[string]uint64)

	return nil
}

// EndTransaction cleans up isolation tracking for a transaction
func (im *IsolationManager) EndTransaction(txID TransactionID) {
	im.mu.Lock()
	defer im.mu.Unlock()

	delete(im.readVersions, txID)
	delete(im.writeVersions, txID)
}

// ReadOperation handles a read operation with isolation
func (im *IsolationManager) ReadOperation(txID TransactionID, table string, isolation IsolationLevel) (uint64, error) {
	im.mu.Lock()
	defer im.mu.Unlock()

	currentVersion := im.versionMap[table]

	// Record the read version for the transaction
	if im.readVersions[txID] == nil {
		im.readVersions[txID] = make(map[string]uint64)
	}
	im.readVersions[txID][table] = currentVersion

	// Check for conflicts based on isolation level
	switch isolation {
	case IsolationReadUncommitted:
		// No conflict checking
		return currentVersion, nil
	case IsolationReadCommitted:
		// No conflict checking for reads
		return currentVersion, nil
	case IsolationRepeatableRead:
		// Check if we've read this table before with a different version
		if prevVersion, exists := im.readVersions[txID][table]; exists && prevVersion != currentVersion {
			return 0, fmt.Errorf("repeatable read violation: table %s version changed from %d to %d",
				table, prevVersion, currentVersion)
		}
		return currentVersion, nil
	case IsolationSerializable:
		// Same as repeatable read for reads
		if prevVersion, exists := im.readVersions[txID][table]; exists && prevVersion != currentVersion {
			return 0, fmt.Errorf("serializable read violation: table %s version changed from %d to %d",
				table, prevVersion, currentVersion)
		}
		return currentVersion, nil
	default:
		return 0, fmt.Errorf("unknown isolation level: %d", isolation)
	}
}

// WriteOperation handles a write operation with isolation
func (im *IsolationManager) WriteOperation(txID TransactionID, table string, isolation IsolationLevel) (uint64, error) {
	im.mu.Lock()
	defer im.mu.Unlock()

	currentVersion := im.versionMap[table]

	// Record the write version for the transaction
	if im.writeVersions[txID] == nil {
		im.writeVersions[txID] = make(map[string]uint64)
	}
	im.writeVersions[txID][table] = currentVersion

	// Check for conflicts based on isolation level
	switch isolation {
	case IsolationReadUncommitted:
		// No conflict checking
		return currentVersion, nil
	case IsolationReadCommitted:
		// Check for write-write conflicts
		return im.checkWriteWriteConflicts(txID, table, currentVersion)
	case IsolationRepeatableRead:
		// Check for read-write and write-write conflicts
		if _, err := im.checkReadWriteConflicts(txID, table, currentVersion); err != nil {
			return 0, err
		}
		return im.checkWriteWriteConflicts(txID, table, currentVersion)
	case IsolationSerializable:
		// Check for all types of conflicts
		if _, err := im.checkReadWriteConflicts(txID, table, currentVersion); err != nil {
			return 0, err
		}
		return im.checkWriteWriteConflicts(txID, table, currentVersion)
	default:
		return 0, fmt.Errorf("unknown isolation level: %d", isolation)
	}
}

// CommitTransaction commits a transaction and updates versions
func (im *IsolationManager) CommitTransaction(txID TransactionID) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	// Update versions for all tables written by this transaction
	if writeVersions, exists := im.writeVersions[txID]; exists {
		for table := range writeVersions {
			im.versionMap[table]++
		}
	}

	// Clean up transaction tracking
	delete(im.readVersions, txID)
	delete(im.writeVersions, txID)

	return nil
}

// AbortTransaction aborts a transaction without updating versions
func (im *IsolationManager) AbortTransaction(txID TransactionID) {
	im.mu.Lock()
	defer im.mu.Unlock()

	// Clean up transaction tracking without updating versions
	delete(im.readVersions, txID)
	delete(im.writeVersions, txID)
}

// Helper Methods

// checkReadWriteConflicts checks for read-write conflicts
func (im *IsolationManager) checkReadWriteConflicts(txID TransactionID, table string, currentVersion uint64) (uint64, error) {
	// Check if any other transaction has written to this table since we read it
	for otherTxID, readVersions := range im.readVersions {
		if otherTxID == txID {
			continue
		}

		if readVersion, exists := readVersions[table]; exists {
			if readVersion < currentVersion {
				return 0, fmt.Errorf("read-write conflict: transaction %d read table %s at version %d, but current version is %d",
					otherTxID, table, readVersion, currentVersion)
			}
		}
	}
	return currentVersion, nil
}

// checkWriteWriteConflicts checks for write-write conflicts
func (im *IsolationManager) checkWriteWriteConflicts(txID TransactionID, table string, currentVersion uint64) (uint64, error) {
	// Check if any other transaction has written to this table
	for otherTxID, writeVersions := range im.writeVersions {
		if otherTxID == txID {
			continue
		}

		if writeVersion, exists := writeVersions[table]; exists {
			if writeVersion >= currentVersion {
				return 0, fmt.Errorf("write-write conflict: transaction %d wrote to table %s at version %d, but current version is %d",
					otherTxID, table, writeVersion, currentVersion)
			}
		}
	}
	return currentVersion, nil
}

// GetTableVersion returns the current version of a table
func (im *IsolationManager) GetTableVersion(table string) uint64 {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.versionMap[table]
}

// GetTransactionReadVersion returns the version at which a transaction read a table
func (im *IsolationManager) GetTransactionReadVersion(txID TransactionID, table string) (uint64, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	if readVersions, exists := im.readVersions[txID]; exists {
		if version, exists := readVersions[table]; exists {
			return version, true
		}
	}
	return 0, false
}

// GetTransactionWriteVersion returns the version at which a transaction wrote to a table
func (im *IsolationManager) GetTransactionWriteVersion(txID TransactionID, table string) (uint64, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	if writeVersions, exists := im.writeVersions[txID]; exists {
		if version, exists := writeVersions[table]; exists {
			return version, true
		}
	}
	return 0, false
}

// GetIsolationInfo returns information about isolation state
func (im *IsolationManager) GetIsolationInfo() *IsolationInfo {
	im.mu.RLock()
	defer im.mu.RUnlock()

	info := &IsolationInfo{
		TableCount:        len(im.versionMap),
		ActiveReads:       len(im.readVersions),
		ActiveWrites:      len(im.writeVersions),
		TableVersions:     make(map[string]uint64),
		TransactionReads:  make(map[TransactionID]map[string]uint64),
		TransactionWrites: make(map[TransactionID]map[string]uint64),
	}

	// Copy table versions
	for table, version := range im.versionMap {
		info.TableVersions[table] = version
	}

	// Copy transaction read versions
	for txID, readVersions := range im.readVersions {
		info.TransactionReads[txID] = make(map[string]uint64)
		for table, version := range readVersions {
			info.TransactionReads[txID][table] = version
		}
	}

	// Copy transaction write versions
	for txID, writeVersions := range im.writeVersions {
		info.TransactionWrites[txID] = make(map[string]uint64)
		for table, version := range writeVersions {
			info.TransactionWrites[txID][table] = version
		}
	}

	return info
}

// IsolationInfo represents isolation state information
type IsolationInfo struct {
	TableCount        int
	ActiveReads       int
	ActiveWrites      int
	TableVersions     map[string]uint64
	TransactionReads  map[TransactionID]map[string]uint64
	TransactionWrites map[TransactionID]map[string]uint64
}

// ValidateIsolationLevel validates an isolation level
func ValidateIsolationLevel(level IsolationLevel) error {
	switch level {
	case IsolationReadUncommitted, IsolationReadCommitted,
		IsolationRepeatableRead, IsolationSerializable:
		return nil
	default:
		return fmt.Errorf("invalid isolation level: %d", level)
	}
}

// GetIsolationLevelFromString converts a string to an isolation level
func GetIsolationLevelFromString(s string) (IsolationLevel, error) {
	switch s {
	case "READ UNCOMMITTED", "read uncommitted":
		return IsolationReadUncommitted, nil
	case "READ COMMITTED", "read committed":
		return IsolationReadCommitted, nil
	case "REPEATABLE READ", "repeatable read":
		return IsolationRepeatableRead, nil
	case "SERIALIZABLE", "serializable":
		return IsolationSerializable, nil
	default:
		return 0, fmt.Errorf("unknown isolation level: %s", s)
	}
}

// CompareIsolationLevels compares two isolation levels
func CompareIsolationLevels(level1, level2 IsolationLevel) int {
	if level1 < level2 {
		return -1
	} else if level1 > level2 {
		return 1
	}
	return 0
}

// IsStrongerIsolation checks if one isolation level is stronger than another
func IsStrongerIsolation(level1, level2 IsolationLevel) bool {
	return level1 > level2
}

// GetIsolationLevelDescription returns a description of an isolation level
func GetIsolationLevelDescription(level IsolationLevel) string {
	switch level {
	case IsolationReadUncommitted:
		return "Allows dirty reads, non-repeatable reads, and phantom reads"
	case IsolationReadCommitted:
		return "Prevents dirty reads, but allows non-repeatable reads and phantom reads"
	case IsolationRepeatableRead:
		return "Prevents dirty reads and non-repeatable reads, but allows phantom reads"
	case IsolationSerializable:
		return "Prevents dirty reads, non-repeatable reads, and phantom reads"
	default:
		return "Unknown isolation level"
	}
}
