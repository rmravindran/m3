package core

import (
	"encoding/binary"
	"errors"

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
}

func NewM3DBSymStreamWriter(namespace ident.ID, streamId ident.ID, session client.Session) *M3DBSymStreamWriter {
	// At most 16k worth of instruction info could written into a
	// single point. TODO: This needs to be in-syn with the m3db limits
	// on the size of annotations
	return &M3DBSymStreamWriter{
		namespace:     namespace,
		streamId:      streamId,
		session:       session,
		encodingSpace: make([]byte, 16*1024)}
}

func (su *M3DBSymStreamWriter) WriteInitInstruction(
	version uint16,
	attributeValues []string) error {
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

	// Write to the m3db time series
	t := xtime.Now()
	err := su.session.Write(
		su.namespace,
		su.streamId,
		t,
		0,
		xtime.Millisecond,
		su.encodingSpace[:ndx])

	// TODO: Updates stats

	return err
}

func (su *M3DBSymStreamWriter) WriteUpdateInstruction(
	version uint16,
	sequenceNum uint32,
	attributeValues []string) error {
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

	// Write to the m3db series
	t := xtime.Now()
	err := su.session.Write(
		su.namespace,
		su.streamId,
		t,
		0,
		xtime.Millisecond,
		su.encodingSpace[:ndx])

	// TODO: Updates stats

	return err
}

func (su *M3DBSymStreamWriter) WriteAttributeInstruction(
	version uint16,
	sequenceNum uint32,
	attributeName string, encodingType AttributeEncoding, indexValues []uint64) error {
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

	// TODO Write the m3db client session
	// Write the m3db client session
	t := xtime.Now()
	err := su.session.Write(
		su.namespace,
		su.streamId,
		t,
		0,
		xtime.Millisecond,
		su.encodingSpace[:ndx])

	// TODO: Updates stats

	return err
}

func (su *M3DBSymStreamWriter) WriteEndInstruction(
	version uint16,
	sequenceNum uint32) error {
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
