package concurrency

import (
	"fmt"
	"sync"
	"time"
)

// RWMutex represents a reader-writer lock with additional features
type RWMutex struct {
	mu           sync.RWMutex
	readers      int
	writers      int
	readWaiters  int
	writeWaiters int
	readCond     *sync.Cond
	writeCond    *sync.Cond
	stats        *LockStats
}

// LockStats represents statistics for a lock
type LockStats struct {
	ReadAcquisitions  int64
	WriteAcquisitions int64
	ReadWaits         int64
	WriteWaits        int64
	ReadWaitTime      time.Duration
	WriteWaitTime     time.Duration
	mu                sync.Mutex
}

// NewRWMutex creates a new reader-writer lock
func NewRWMutex() *RWMutex {
	rw := &RWMutex{
		stats: &LockStats{},
	}
	rw.readCond = sync.NewCond(&rw.mu)
	rw.writeCond = sync.NewCond(&rw.mu)
	return rw
}

// RLock acquires a read lock
func (rw *RWMutex) RLock() {
	start := time.Now()

	rw.mu.Lock()
	defer rw.mu.Unlock()

	// Wait if there are writers or write waiters
	for rw.writers > 0 || rw.writeWaiters > 0 {
		rw.readWaiters++
		rw.stats.mu.Lock()
		rw.stats.ReadWaits++
		rw.stats.mu.Unlock()
		rw.readCond.Wait()
		rw.readWaiters--
	}

	rw.readers++
	rw.stats.mu.Lock()
	rw.stats.ReadAcquisitions++
	rw.stats.ReadWaitTime += time.Since(start)
	rw.stats.mu.Unlock()
}

// RUnlock releases a read lock
func (rw *RWMutex) RUnlock() {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	rw.readers--
	if rw.readers == 0 && rw.writeWaiters > 0 {
		rw.writeCond.Signal()
	}
}

// Lock acquires a write lock
func (rw *RWMutex) Lock() {
	start := time.Now()

	rw.mu.Lock()
	defer rw.mu.Unlock()

	// Wait if there are readers or writers
	for rw.readers > 0 || rw.writers > 0 {
		rw.writeWaiters++
		rw.stats.mu.Lock()
		rw.stats.WriteWaits++
		rw.stats.mu.Unlock()
		rw.writeCond.Wait()
		rw.writeWaiters--
	}

	rw.writers++
	rw.stats.mu.Lock()
	rw.stats.WriteAcquisitions++
	rw.stats.WriteWaitTime += time.Since(start)
	rw.stats.mu.Unlock()
}

// Unlock releases a write lock
func (rw *RWMutex) Unlock() {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	rw.writers--
	if rw.writeWaiters > 0 {
		rw.writeCond.Signal()
	} else if rw.readWaiters > 0 {
		rw.readCond.Broadcast()
	}
}

// TryRLock attempts to acquire a read lock without blocking
func (rw *RWMutex) TryRLock() bool {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.writers > 0 || rw.writeWaiters > 0 {
		return false
	}

	rw.readers++
	rw.stats.mu.Lock()
	rw.stats.ReadAcquisitions++
	rw.stats.mu.Unlock()
	return true
}

// TryLock attempts to acquire a write lock without blocking
func (rw *RWMutex) TryLock() bool {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.readers > 0 || rw.writers > 0 {
		return false
	}

	rw.writers++
	rw.stats.mu.Lock()
	rw.stats.WriteAcquisitions++
	rw.stats.mu.Unlock()
	return true
}

// GetStats returns lock statistics
func (rw *RWMutex) GetStats() *LockStats {
	rw.stats.mu.Lock()
	defer rw.stats.mu.Unlock()

	// Return a copy
	return &LockStats{
		ReadAcquisitions:  rw.stats.ReadAcquisitions,
		WriteAcquisitions: rw.stats.WriteAcquisitions,
		ReadWaits:         rw.stats.ReadWaits,
		WriteWaits:        rw.stats.WriteWaits,
		ReadWaitTime:      rw.stats.ReadWaitTime,
		WriteWaitTime:     rw.stats.WriteWaitTime,
	}
}

// ResetStats resets lock statistics
func (rw *RWMutex) ResetStats() {
	rw.stats.mu.Lock()
	defer rw.stats.mu.Unlock()

	rw.stats.ReadAcquisitions = 0
	rw.stats.WriteAcquisitions = 0
	rw.stats.ReadWaits = 0
	rw.stats.WriteWaits = 0
	rw.stats.ReadWaitTime = 0
	rw.stats.WriteWaitTime = 0
}

// GetState returns the current state of the lock
func (rw *RWMutex) GetState() *LockState {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	return &LockState{
		Readers:      rw.readers,
		Writers:      rw.writers,
		ReadWaiters:  rw.readWaiters,
		WriteWaiters: rw.writeWaiters,
	}
}

// LockState represents the current state of a lock
type LockState struct {
	Readers      int
	Writers      int
	ReadWaiters  int
	WriteWaiters int
}

// String returns a string representation of the lock state
func (ls *LockState) String() string {
	return fmt.Sprintf("R:%d W:%d RW:%d WW:%d",
		ls.Readers, ls.Writers, ls.ReadWaiters, ls.WriteWaiters)
}

// LockManager manages multiple locks
type LockManager struct {
	locks map[string]*RWMutex
	mu    sync.RWMutex
}

// NewLockManager creates a new lock manager
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]*RWMutex),
	}
}

// GetLock returns a lock for the given key
func (lm *LockManager) GetLock(key string) *RWMutex {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lock, exists := lm.locks[key]; exists {
		return lock
	}

	lock := NewRWMutex()
	lm.locks[key] = lock
	return lock
}

// RemoveLock removes a lock from the manager
func (lm *LockManager) RemoveLock(key string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	delete(lm.locks, key)
}

// ListLocks returns all lock keys
func (lm *LockManager) ListLocks() []string {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	keys := make([]string, 0, len(lm.locks))
	for key := range lm.locks {
		keys = append(keys, key)
	}
	return keys
}

// GetLockStats returns statistics for all locks
func (lm *LockManager) GetLockStats() map[string]*LockStats {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	stats := make(map[string]*LockStats)
	for key, lock := range lm.locks {
		stats[key] = lock.GetStats()
	}
	return stats
}

// GetLockStates returns the state of all locks
func (lm *LockManager) GetLockStates() map[string]*LockState {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	states := make(map[string]*LockState)
	for key, lock := range lm.locks {
		states[key] = lock.GetState()
	}
	return states
}

// ResetAllStats resets statistics for all locks
func (lm *LockManager) ResetAllStats() {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	for _, lock := range lm.locks {
		lock.ResetStats()
	}
}

// LockInfo represents information about a lock
type LockInfo struct {
	Key          string
	State        *LockState
	Stats        *LockStats
	LastAccessed time.Time
}

// GetLockInfo returns detailed information about a lock
func (lm *LockManager) GetLockInfo(key string) *LockInfo {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	lock, exists := lm.locks[key]
	if !exists {
		return nil
	}

	return &LockInfo{
		Key:          key,
		State:        lock.GetState(),
		Stats:        lock.GetStats(),
		LastAccessed: time.Now(),
	}
}

// GetAllLockInfo returns detailed information about all locks
func (lm *LockManager) GetAllLockInfo() map[string]*LockInfo {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	info := make(map[string]*LockInfo)
	for key, lock := range lm.locks {
		info[key] = &LockInfo{
			Key:          key,
			State:        lock.GetState(),
			Stats:        lock.GetStats(),
			LastAccessed: time.Now(),
		}
	}
	return info
}

// CleanupUnusedLocks removes locks that haven't been used recently
func (lm *LockManager) CleanupUnusedLocks(maxAge time.Duration) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for key, lock := range lm.locks {
		state := lock.GetState()
		// If lock is not in use and hasn't been accessed recently, remove it
		if state.Readers == 0 && state.Writers == 0 &&
			state.ReadWaiters == 0 && state.WriteWaiters == 0 {
			delete(lm.locks, key)
		}
	}
}

// LockTimeout represents a timeout for lock operations
type LockTimeout struct {
	Duration time.Duration
	Timer    *time.Timer
}

// NewLockTimeout creates a new lock timeout
func NewLockTimeout(duration time.Duration) *LockTimeout {
	return &LockTimeout{
		Duration: duration,
		Timer:    time.NewTimer(duration),
	}
}

// WaitForLock waits for a lock with a timeout
func (rw *RWMutex) WaitForLock(timeout time.Duration, isWrite bool) bool {
	timeoutChan := time.After(timeout)
	acquired := make(chan bool, 1)

	go func() {
		if isWrite {
			rw.Lock()
		} else {
			rw.RLock()
		}
		acquired <- true
	}()

	select {
	case <-acquired:
		return true
	case <-timeoutChan:
		return false
	}
}

// WaitForRLock waits for a read lock with a timeout
func (rw *RWMutex) WaitForRLock(timeout time.Duration) bool {
	return rw.WaitForLock(timeout, false)
}

// WaitForWriteLock waits for a write lock with a timeout
func (rw *RWMutex) WaitForWriteLock(timeout time.Duration) bool {
	return rw.WaitForLock(timeout, true)
}
