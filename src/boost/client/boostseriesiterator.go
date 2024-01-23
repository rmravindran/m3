package client

import (
	"encoding/binary"

	"github.com/m3db/m3/src/boost/core"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type SymTableFetchFunction func(
	namespaceId ident.ID,
	symTableName string,
	version uint16,
	timeBegin xtime.UnixNano,
	timeEnd xtime.UnixNano) (*core.SymTable, error)

type BoostSeriesIterator struct {
	seriesIter      encoding.SeriesIterator
	symTableFetchFn SymTableFetchFunction
	symTable        *core.SymTable
	startTime       xtime.UnixNano
	endTime         xtime.UnixNano
	annotation      ts.Annotation
	attributeIter   ident.TagIterator
}

// NewBoostSeriesIterator returns a new series iterator
func NewBoostSeriesIterator(
	seriesIter encoding.SeriesIterator,
	symTableFetchFn SymTableFetchFunction,
	startTime xtime.UnixNano,
	endTime xtime.UnixNano) *BoostSeriesIterator {
	return &BoostSeriesIterator{
		seriesIter:      seriesIter,
		symTableFetchFn: symTableFetchFn,
		symTable:        nil,
		startTime:       startTime,
		endTime:         endTime,
		annotation:      nil,
		attributeIter:   nil,
	}
}

// Moves to the next item
func (bsi *BoostSeriesIterator) Next() bool {
	bsi.attributeIter = nil
	bsi.annotation = nil
	return bsi.seriesIter.Next()
}

// Returns the current item. Users should not hold onto the returned
// values as it may get invalidated when the Next is called.
func (bsi *BoostSeriesIterator) Current() (
	ts.Datapoint, xtime.Unit, ts.Annotation) {
	dp, t, annotation := bsi.seriesIter.Current()
	bsi.annotation = annotation
	return dp, t, nil
}

// Err returns any errors encountered
func (bsi *BoostSeriesIterator) Err() error {
	return bsi.seriesIter.Err()
}

// Close closes the iterator
func (bsi *BoostSeriesIterator) Close() {
	bsi.annotation = nil
	bsi.attributeIter = nil
	bsi.seriesIter.Close()
}

// ID returns the ID of the series
func (bsi *BoostSeriesIterator) ID() ident.ID {
	return bsi.seriesIter.ID()
}

// Namespace returns the namespace of the series
func (bsi *BoostSeriesIterator) Namespace() ident.ID {
	return bsi.seriesIter.Namespace()
}

// Tags returns the tags of the series
func (bsi *BoostSeriesIterator) Tags() ident.TagIterator {
	return bsi.seriesIter.Tags()
}

// Attributes returns the attributes of the series
func (bsi *BoostSeriesIterator) Attributes() ident.TagIterator {
	if bsi.attributeIter != nil {
		return bsi.attributeIter
	}

	// First 2 bytes the version of the symtable
	version := binary.LittleEndian.Uint16(bsi.annotation)
	if (bsi.symTable == nil) || (bsi.symTable.Version() != version) {
		symTableName := "m3_symboltable_" + bsi.ID().String()
		symTable, err := bsi.symTableFetchFn(
			bsi.Namespace(),
			symTableName,
			version,
			bsi.startTime,
			bsi.endTime)
		if err != nil {
			return nil
		}
		bsi.symTable = symTable
	}

	indexedHeaderSz := int(binary.LittleEndian.Uint16(bsi.annotation[2:]))
	indexedHeader := make([]int, indexedHeaderSz)
	tmp := bsi.annotation[4:]
	for i := range indexedHeader {
		indexedHeader[i] = int(binary.LittleEndian.Uint32(tmp[i*4:]))
	}

	attributeMap := bsi.symTable.GetAttributesFromIndexedHeader(indexedHeader)
	attrTags := make([]ident.Tag, len(attributeMap))
	ndx := 0
	for name, value := range attributeMap {
		attrTags[ndx] = ident.Tag{Name: ident.StringID(name), Value: ident.StringID(value)}
		ndx++
	}
	bsi.attributeIter = ident.NewTagsIterator(ident.NewTags(attrTags...))
	return bsi.attributeIter
}
