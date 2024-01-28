package core

import (
	"time"

	xtime "github.com/m3db/m3/src/x/time"
)

type TableInstruction int

const (
	// Initiatize the table
	InitSymTable TableInstruction = iota

	// Update the table
	UpdateSymTable

	// Add a new attribute to the table
	AddAttribute

	// End Dictionary
	EndSymTable

	// NOP
	NOPInstruction
)

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

type SymStreamReader interface {

	// Seek to first entry between the specified start and end time in the
	// underlying stream.
	Seek(startTime xtime.UnixNano, endTime xtime.UnixNano) error

	// Read the next instruction from the stream and return the version,
	// sequence number and the instruction. If End of stream is reached,
	// returns NOPInstruction. Otherwise, return error.
	Next() (uint16, uint32, TableInstruction, error)

	// Read InitSymTable instruction parameter from the current position in the
	// stream. If the current instruction is not InitSymTable, return error.
	ReadInitInstruction() ([]string, error)

	// Read UpdateSymTable instruction parameter from the current position in
	// the stream. If the current instruction is not UpdateSymTable, return
	// error.
	ReadUpdateInstruction() ([]string, error)

	// Read AddAttribute instruction parameter from the current position in
	// the stream. If the current instruction is not AddAttribute, return
	// error.
	ReadAttributeInstruction() (string, AttributeEncoding, []uint64, error)

	// Read the EndSymTable instruction from the stream at the current
	// location of the underlying stream. Return error if the instruction
	// could not be read.
	ReadEndInstruction() (string, []uint64, error)
}
