package core

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// Implements the SymStreamWriter to write the symbol table to an m3db time series
type M3DBSymStreamWriter struct {
	namespace     ident.ID
	streamId      ident.ID
	session       client.Session
	encodingSpace []byte
	pendingWrites atomic.Int32
}

func NewM3DBSymStreamWriter(
	namespace ident.ID,
	streamId ident.ID,
	session client.Session) *M3DBSymStreamWriter {
	// At most 16k worth of instruction info could written into a
	// single point. TODO: This needs to be in-syn with the m3db limits
	// on the size of annotations
	ret := &M3DBSymStreamWriter{
		namespace:     namespace,
		streamId:      streamId,
		session:       session,
		encodingSpace: make([]byte, 16*1024),
		pendingWrites: atomic.Int32{},
	}
	ret.pendingWrites.Store(0)
	return ret
}

func (su *M3DBSymStreamWriter) WriteInitInstruction(
	version uint16,
	attributeValues []string,
	completionFn WriteCompletionFn) error {
	// Write the dictionary instruction param such that the parseDictionaryInstruction
	// function can parse it

	// Write the header (version, flags, etc)
	// Note that Init instruction's sequence number is always 1
	ndx := su.encodeHeader(su.encodingSpace, version, InitSymTable, 1)
	if ndx <= 0 {
		return errors.New("unable to write instruction header to the stream")
	}

	// Write the values
	sz := su.encodeAttributeValues(su.encodingSpace[ndx:], attributeValues)
	if sz <= 0 {
		return errors.New("unable to write instruction params to the stream")
	}
	ndx += sz

	// Copy the data from the encodedSpace to the encodedCopy
	encodedCopy := make([]byte, ndx)
	copy(encodedCopy, su.encodingSpace[:ndx])

	// Timestamp it here (instead of the goroutine) to capturne the intended
	// chronological order of the instructions
	t := xtime.Now()

	go func(t xtime.UnixNano, encodedData []byte) {
		su.pendingWrites.Add(1)
		err := su.session.Write(
			su.namespace,
			su.streamId,
			t,
			0,
			xtime.Millisecond,
			encodedData)
		su.pendingWrites.Add(-1)
		if completionFn != nil {
			completionFn(err)
		}
	}(t, encodedCopy)

	// TODO: Updates stats

	return nil
}

func (su *M3DBSymStreamWriter) WriteUpdateInstruction(
	version uint16,
	sequenceNum uint32,
	attributeValues []string,
	completionFn WriteCompletionFn) error {
	// Write the dictionary instruction param such that the parseDictionaryInstruction
	// function can parse it

	// Encode the header (version, flags, etc) to the encodingSpace
	ndx := su.encodeHeader(su.encodingSpace, version, UpdateSymTable, sequenceNum)
	if ndx <= 0 {
		return errors.New("unable to write instruction header to the stream")
	}

	// Write the values into the encodingSpace
	sz := su.encodeAttributeValues(su.encodingSpace[ndx:], attributeValues)
	if sz <= 0 {
		return errors.New("unable to write instruction to the stream")
	}
	ndx += sz

	// Copy the data from the encodedSpace to the encodedCopy
	encodedCopy := make([]byte, ndx)
	copy(encodedCopy, su.encodingSpace[:ndx])

	// Timestamp it here (instead of the goroutine) to capturne the intended
	// chronological order of the instructions
	t := xtime.Now()

	go func(t xtime.UnixNano, encodedData []byte) {
		su.pendingWrites.Add(1)
		err := su.session.Write(
			su.namespace,
			su.streamId,
			t,
			0,
			xtime.Millisecond,
			encodedData)
		su.pendingWrites.Add(-1)
		if completionFn != nil {
			completionFn(err)
		}
	}(t, encodedCopy)

	// TODO: Updates stats

	return nil
}

func (su *M3DBSymStreamWriter) WriteAttributeInstruction(
	version uint16,
	sequenceNum uint32,
	attributeName string,
	encodingType AttributeEncoding,
	indexValues []uint64,
	completionFn WriteCompletionFn) error {
	// Write the attribute table instruction param such that the parseAddAttributeInstruction
	// function can parse it

	// Write the header (version, flags, etc)
	ndx := su.encodeHeader(su.encodingSpace, version, AddAttribute, sequenceNum)
	if ndx <= 0 {
		return errors.New("unable to write instruction header to the stream")
	}

	// Write the AddAttribute instruction parameters
	binary.LittleEndian.PutUint16(su.encodingSpace[ndx:], uint16(len(attributeName)))
	ndx += 2
	copy(su.encodingSpace[ndx:], []byte(attributeName))
	ndx += len(attributeName)
	binary.LittleEndian.PutUint16(su.encodingSpace[ndx:], uint16(encodingType))
	ndx += 2
	binary.LittleEndian.PutUint32(su.encodingSpace[ndx:], uint32(len(indexValues)))
	ndx += 4
	for _, v := range indexValues {
		binary.LittleEndian.PutUint64(su.encodingSpace[ndx:], uint64(v))
		ndx += 8
	}

	// Copy the data from the encodedSpace to the encodedCopy
	encodedCopy := make([]byte, ndx)
	copy(encodedCopy, su.encodingSpace[:ndx])

	// Timestamp it here (instead of the goroutine) to capturne the intended
	// chronological order of the instructions
	t := xtime.Now()

	go func(t xtime.UnixNano, encodedData []byte) {
		su.pendingWrites.Add(1)
		su.session.Write(
			su.namespace,
			su.streamId,
			t,
			0,
			xtime.Millisecond,
			encodedData)
		su.pendingWrites.Add(-1)
		if completionFn != nil {
			completionFn(nil)
		}
	}(t, encodedCopy)

	// TODO: Updates stats

	return nil
}

func (su *M3DBSymStreamWriter) WriteEndInstruction(
	version uint16,
	sequenceNum uint32,
	completionFn WriteCompletionFn) error {

	// Write the header (version, flags, etc)
	ndx := su.encodeHeader(su.encodingSpace, version, EndSymTable, sequenceNum)
	if ndx <= 0 {
		return errors.New("unable to write instruction header to the stream")
	}

	// Copy the data from the encodedSpace to the encodedCopy
	encodedCopy := make([]byte, ndx)
	copy(encodedCopy, su.encodingSpace[:ndx])

	// Timestamp it here (instead of the goroutine) to capturne the intended
	// chronological order of the instructions
	t := xtime.Now()

	go func(t xtime.UnixNano, encodedData []byte) {
		su.pendingWrites.Add(1)
		su.session.Write(
			su.namespace,
			su.streamId,
			t,
			0,
			xtime.Millisecond,
			encodedData)
		su.pendingWrites.Add(-1)
		if completionFn != nil {
			completionFn(nil)
		}
	}(t, encodedCopy)

	// TODO: Updates stats

	return nil
}

// Wait for all pending write operations to complete or until the specified
// timeout is reached. If timeout is 0, wait indefinitely until all pending
// writes are completed.
func (su *M3DBSymStreamWriter) Wait(timeout time.Duration) error {
	// Wait for all pending writes to complete or timeout to occur
	totalUs := 0

	for {
		if su.pendingWrites.Load() == 0 {
			break
		}
		time.Sleep(100 * time.Microsecond)
		totalUs += 100
		if (timeout > 0) && (totalUs > int(timeout/time.Microsecond)) {
			return errors.New("timeout waiting for pending writes to complete")
		}
	}

	return nil
}

func (su *M3DBSymStreamWriter) encodeAttributeValues(
	dst []byte,
	attributeValues []string) int {

	// Write the attribute values
	sz := 0
	binary.LittleEndian.PutUint32(dst[sz:], uint32(len(attributeValues)))
	sz += 4
	for _, v := range attributeValues {
		binary.LittleEndian.PutUint16(dst[sz:], uint16(len(v)))
		sz += 2
		copy(dst[sz:], []byte(v))
		sz += len(v)
	}

	return sz
}

// Write the header (version, flags, etc) to the stream
func (su *M3DBSymStreamWriter) encodeHeader(
	dst []byte,
	version uint16,
	instruction TableInstruction,
	sequenceNum uint32) int {

	// Write the flags (version and instruction)
	sz := 0
	var flags uint32 = (uint32(instruction) & 0xFF) | (uint32(version) << 16)
	binary.LittleEndian.PutUint32(dst[sz:], flags)
	sz += 4

	// Write the sequence number
	binary.LittleEndian.PutUint32(dst[sz:], sequenceNum)
	sz += 4

	return sz
}
