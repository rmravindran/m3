package core

import (
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// Defines a series family to be used for Transactional & Analytical Processing
// use case. A series family is a collection of series that shares certain
// attributes.
type SeriesFamily interface {

	// Name of the table
	Name() string

	// Namespace of the table
	Namespace() ident.ID

	// Write a float64 value into the table having the specified attributes.
	Write(
		id ident.ID,
		attributes ident.TagIterator,
		time xtime.UnixNano,
		value float64,
		unit xtime.Unit,
		completionFn WriteCompletionFn) error

	// Write a float64 value into the table having the specified tags and
	// attributes.
	WriteTagged(
		id ident.ID,
		tags ident.TagIterator,
		attributes ident.TagIterator,
		time xtime.UnixNano,
		value float64,
		unit xtime.Unit,
		completionFn WriteCompletionFn) error
}
