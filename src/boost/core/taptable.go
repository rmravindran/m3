package core

import (
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type TAPWriteCompletionFn func(err error)

// Defines the Transactional & Analytical Processing (TAP) table interface
// for the M3DB database. The table allows multiple columns to be presented in
// a single table for TAP operations
type TAPTable interface {

	// Name of the table
	Name() string

	// Namespace of the table
	Namespace() ident.ID

	// Write a float64 value into the table having the specified attributes
	// and timestamp.
	WriteTagged(
		id ident.ID,
		attributes ident.TagIterator,
		time xtime.UnixNano,
		value float64,
		uint xtime.Unit,
		completionFn TAPWriteCompletionFn)
}
