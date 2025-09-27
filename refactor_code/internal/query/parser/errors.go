package parser

import "fmt"

// ParseError represents a parsing error
type ParseError struct {
	Message string
	Pos     int
}

func (e ParseError) Error() string {
	return fmt.Sprintf("parse error at position %d: %s", e.Pos, e.Message)
}

// NewParseError creates a new parse error
func NewParseError(message string, pos int) error {
	return ParseError{Message: message, Pos: pos}
}
