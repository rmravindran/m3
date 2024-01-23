package client

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/boost/core"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// Implements the TAPTable interface
type M3DBTAPTable struct {
	// Name of the table
	name string

	// Namespace of the table
	namespace ident.ID

	// Current version of the table
	version uint16

	// Underlying Boost Session
	session *BoostSession

	// Distribution factor
	distributionFactor uint32

	// Next distribution index.
	nextDistributionIndex atomic.Uint32

	// Dictionary Limit
	dictionaryLimit uint32

	// Max concurrent writes
	maxConcurrentWrites uint32

	// Write control mutex
	writeControlMutex sync.Mutex

	// Atomic write counter for pending writes
	pendingWrites atomic.Uint32
}

// NewM3DBTAPTable creates a new M3DBTAPTable
func NewM3DBTAPTable(
	name string,
	namespace ident.ID,
	version uint16,
	session *BoostSession,
	distributionFactor uint32,
	dictionaryLimit uint32,
	maxConcurrentWrites uint32,
) *M3DBTAPTable {
	ret := &M3DBTAPTable{
		name:                  name,
		namespace:             namespace,
		version:               version,
		session:               session,
		distributionFactor:    distributionFactor,
		nextDistributionIndex: atomic.Uint32{},
		dictionaryLimit:       dictionaryLimit,
		maxConcurrentWrites:   maxConcurrentWrites,
		writeControlMutex:     sync.Mutex{},
		pendingWrites:         atomic.Uint32{},
	}
	ret.pendingWrites.Store(0)
	ret.nextDistributionIndex.Store(0)

	return ret
}

// Name returns the name of the table
func (t *M3DBTAPTable) Name() string {
	return t.name
}

// Namespace returns the namespace of the table
func (t *M3DBTAPTable) Namespace() ident.ID {
	return t.namespace
}

// WriteTagged writes a float64 value into the table having the specified attributes
// and timestamp.
func (t *M3DBTAPTable) WriteTagged(
	id ident.ID,
	attributes ident.TagIterator,
	timestamp xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	completionFn core.TAPWriteCompletionFn) {

	// First check and wait if we have too many pending writes
	if t.pendingWrites.Load() >= t.maxConcurrentWrites {
		for t.pendingWrites.Load() >= t.maxConcurrentWrites {
			// Sleep for 100 microseconds
			time.Sleep(100 * time.Microsecond)
		}

		t.pendingWrites.Store(0)
	}

	// Find the id from the distribution factor
	nextDistributionIndex := t.nextDistributionIndex.Load() % t.distributionFactor
	prefix := fmt.Sprintf("m3_dist_%d_", nextDistributionIndex)

	// Find the modified id
	id = ident.StringID(prefix + id.String())

	t.session.WriteValueWithTaggedAttributes(
		t.namespace,
		id,
		nil,
		attributes,
		timestamp,
		value,
		unit,
		completionFn)
}
