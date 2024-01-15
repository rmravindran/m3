package core

import (
	"encoding/binary"
	"errors"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// Implements the SymStreamReader to read the symbol table to an m3db time series
type M3DBSymStreamReader struct {
	namespace     ident.ID
	streamId      ident.ID
	session       client.Session
	seriesIt      encoding.SeriesIterator
	itInstruction TableInstruction
	itSeqNum      uint32
	itRaw         []byte
}

func NewM3DBSymStreamReader(namespace ident.ID, streamId ident.ID, session client.Session) *M3DBSymStreamReader {
	// At most 16k worth of instruction info could read into the
	// temporary space. TODO: This needs to be in-syn with the m3db limits
	// on the size of annotations
	return &M3DBSymStreamReader{
		namespace:     namespace,
		streamId:      streamId,
		session:       session,
		seriesIt:      nil,
		itInstruction: NOPInstruction,
		itSeqNum:      0,
		itRaw:         nil,
	}
}

// Seek to first entry between the specified start and end time in the
// underlying stream.
func (sr *M3DBSymStreamReader) Seek(startTime xtime.UnixNano, endTime xtime.UnixNano) error {

	// Fetch the time series
	seriesIter, err := sr.session.Fetch(
		sr.namespace, sr.streamId, startTime, endTime)
	if err != nil {
		return errors.New("unable to fetch the symbol table stream")
	}

	sr.seriesIt = seriesIter
	sr.itSeqNum = 0

	return nil
}

// Read the next instruction from the stream and return the version,
// sequence number and the instruction. If End of stream is reached,
// returns NOPInstruction. Otherwise, return error.
func (sr *M3DBSymStreamReader) Next() (uint16, uint32, TableInstruction, error) {

	if !sr.seriesIt.Next() {
		return 0, 0, NOPInstruction, nil
	}

	_, _, raw := sr.seriesIt.Current()
	if len(raw) < 8 {
		return 0, 0, NOPInstruction, errors.New("invalid symbol table data")
	}

	version, instruction, seqNum, err := sr.decodeHeader(raw)
	if err != nil {
		return 0, 0, NOPInstruction, err
	}

	sr.itInstruction = instruction
	sr.itSeqNum = seqNum
	sr.itRaw = raw

	return version, seqNum, instruction, nil
}

// Read InitSymTable instruction parameter from the current position in the
// stream. If the current instruction is not InitSymTable, return error.
func (sr *M3DBSymStreamReader) ReadInitInstruction() ([]string, error) {

	if sr.itRaw == nil || sr.itInstruction != InitSymTable {
		return nil, errors.New("stream not seeked to a InitSymTable instruction")
	}

	// Ok now we got we want. Parse the instruction params
	attributeValues, err := sr.decodeDictionaryInstructionParams(sr.itRaw[8:])
	if err != nil {
		return nil, err
	}

	return attributeValues, nil
}

// Read UpdateSymTable instruction parameter from the current position in
// the stream. If the current instruction is not UpdateSymTable, return
// error.
func (sr *M3DBSymStreamReader) ReadUpdateInstruction() ([]string, error) {

	if sr.itRaw == nil || sr.itInstruction != UpdateSymTable {
		return nil, errors.New("stream not seeked to a UpdateSymTable instruction")
	}

	// Ok now we got we want. Parse the instruction params
	attributeValues, err := sr.decodeDictionaryInstructionParams(sr.itRaw[8:])
	if err != nil {
		return nil, err
	}

	return attributeValues, nil
}

// Read AddAttribute instruction parameter from the current position in
// the stream. If the current instruction is not AddAttribute, return
// error.
func (sr *M3DBSymStreamReader) ReadAttributeInstruction() (string, AttributeEncoding, []uint64, error) {

	if sr.itRaw == nil || sr.itInstruction != AddAttribute {
		return "", 0, nil, errors.New("stream not seeked to a AddAttribute instruction")
	}

	// Ok now we got we want. Parse the instruction params
	attrName, encodingType, indexValues, err := sr.decodeAddAttributeInstructionParams(sr.itRaw[8:])
	if err != nil {
		return "", 0, nil, err
	}

	return attrName, encodingType, indexValues, nil
}

// Decode the instruction header from the stream
func (sr *M3DBSymStreamReader) decodeHeader(raw []byte) (uint16, TableInstruction, uint32, error) {

	flags := binary.LittleEndian.Uint32(raw)
	// Decode version and instruction from flags
	version := uint16(flags >> 16) // Upper 16 bits
	instruction := flags & 0xFF    // Lower 8 bits
	// Decode the sequence number
	sequenceNum := binary.LittleEndian.Uint32(raw[4:])

	if instruction >= uint32(NOPInstruction) {
		return 0, 0, 0, errors.New("invalid instruction")
	}

	return version, TableInstruction(instruction), sequenceNum, nil
}

// Decode the dictionary update instruction params from the stream
func (sr *M3DBSymStreamReader) decodeDictionaryInstructionParams(raw []byte) ([]string, error) {
	// Decode the number of values
	numValues := binary.LittleEndian.Uint32(raw)
	raw = raw[4:]
	values := make([]string, numValues)
	for i := 0; i < int(numValues); i++ {
		// Decode the length of the value
		valueLen := binary.LittleEndian.Uint16(raw)
		raw = raw[2:]
		// Decode the value
		values[i] = string(raw[:valueLen])
		raw = raw[valueLen:]
	}

	return values, nil
}

// Decode the add attribute instruction params from the stream
func (sr *M3DBSymStreamReader) decodeAddAttributeInstructionParams(raw []byte) (string, AttributeEncoding, []uint64, error) {
	// Decode the length of the attribute name
	attrNameLen := binary.LittleEndian.Uint16(raw)
	raw = raw[2:]
	// Decode the attribute name
	attrName := string(raw[:attrNameLen])
	raw = raw[attrNameLen:]
	// Decode the encoding type
	encodingType := AttributeEncoding(binary.LittleEndian.Uint16(raw))
	raw = raw[2:]
	// Decode the number of values
	numValues := binary.LittleEndian.Uint32(raw)
	raw = raw[4:]
	values := make([]uint64, numValues)
	for i := 0; i < int(numValues); i++ {
		// Decode the value
		values[i] = binary.LittleEndian.Uint64(raw)
		raw = raw[8:]
	}

	return attrName, encodingType, values, nil
}
