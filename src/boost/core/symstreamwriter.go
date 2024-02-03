package core

// Package defines the interfaces

import (
	"time"
)

// Symbol Stream Writer provides the interface for encoding and writing the
// symbol table elements into a stream.
type SymStreamWriter interface {

	// Write the InitSymTable instruction with the specified version and
	// attribute values.
	WriteInitInstruction(
		version uint16,
		attributeValues []string,
		completionFn WriteCompletionFn) error

	// Write the UpdateSymTable instruction with the specified version,
	// sequence number and attribute values.
	WriteUpdateInstruction(
		version uint16,
		sequenceNum uint32,
		attributeValues []string,
		completionFn WriteCompletionFn) error

	// Write the AddAttribute instruction with the specified version,
	// sequence number, attribute name, encoding type and index values.
	WriteAttributeInstruction(
		version uint16,
		sequenceNum uint32,
		atributeName string,
		encodingType AttributeEncoding,
		indexValues []uint64,
		completionFn WriteCompletionFn) error

	// Write the EndSymTable instruction with the specified version and
	// sequence number.
	WriteEndInstruction(
		version uint16,
		sequenceNum uint32,
		completionFc WriteCompletionFn) error

	// Wait for all pending write operations to complete or until the specified
	// timeout is reached. If timeout is 0, wait indefinitely wait for all
	// pending writes to complete.
	Wait(timeout time.Duration) error
}
