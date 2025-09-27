package errors

import "fmt"

// DatabaseError represents a database-specific error
type DatabaseError struct {
	Code    int
	Message string
	Cause   error
}

func (e DatabaseError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("database error %d: %s (caused by: %v)", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("database error %d: %s", e.Code, e.Message)
}

// Error codes
const (
	ErrCodeUnknown          = 0
	ErrCodeParseError       = 1000
	ErrCodeExecError        = 2000
	ErrCodeStorageError     = 3000
	ErrCodeConcurrencyError = 4000
)

// NewDatabaseError creates a new database error
func NewDatabaseError(code int, message string, cause error) error {
	return DatabaseError{Code: code, Message: message, Cause: cause}
}

// NewParseError creates a parse error
func NewParseError(message string, cause error) error {
	return NewDatabaseError(ErrCodeParseError, message, cause)
}

// NewExecError creates an execution error
func NewExecError(message string, cause error) error {
	return NewDatabaseError(ErrCodeExecError, message, cause)
}

// NewStorageError creates a storage error
func NewStorageError(message string, cause error) error {
	return NewDatabaseError(ErrCodeStorageError, message, cause)
}

// NewConcurrencyError creates a concurrency error
func NewConcurrencyError(message string, cause error) error {
	return NewDatabaseError(ErrCodeConcurrencyError, message, cause)
}

