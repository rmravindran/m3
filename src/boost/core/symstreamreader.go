package core

import (
	xtime "github.com/m3db/m3/src/x/time"
)

// A Symbol Table Stream Reader provides the interface to read the symbol
// table operations encoded in the stream.
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
