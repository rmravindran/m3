package core

import (
	"errors"
	"time"
)

// Supported attribute encodings
type AttributeEncoding int

const (
	// Values are encoded as raw unsigned values
	UnsignedNumericValue AttributeEncoding = iota

	// Dictionary encoding. AttributeTable will index into to a dictionary.
	DictionaryEncodedValue
)

// All symbols part of an attribute are indexed seperately.
type AttributeTable struct {
	name                   string
	encodingType           AttributeEncoding
	encodedValues          []uint64
	encodedValuesFromIndex map[uint64]uint64
}

// Symbol Table implementation
type SymTable struct {

	// Name of the symbol table. Usually the seriesId with a prefix
	// such as "m3_symboltable_"
	name string

	// Current version of the symbol table
	version uint16

	// Instruction sequence number reprensents a unique state identifier
	instructionSeqNum uint32

	// Is the table finalized. Once finalized, no more updates can be made
	finalized bool

	// Dictionary encoded entries of the attributes
	dictToString map[uint64]string
	dictToIndex  map[string]uint64

	// Header, aka the column index
	header map[string]int

	// Attribute Tables
	attributeTable map[string]*AttributeTable

	// Stream writer
	streamWriter SymStreamWriter
}

//--------------
//- CONSTRUCTORS
//--------------

func NewSymTable(name string, version uint16, streamWriter SymStreamWriter) *SymTable {
	return &SymTable{
		name:              name,
		version:           version,
		instructionSeqNum: 0,
		finalized:         false,
		dictToString:      make(map[uint64]string),
		dictToIndex:       make(map[string]uint64),
		header:            make(map[string]int),
		attributeTable:    make(map[string]*AttributeTable),
		streamWriter:      streamWriter,
	}
}

//--------------
//- ACCESSORS
//--------------

// Returns the name of the symbol table
func (sym *SymTable) Name() string {
	return sym.name
}

// Return the version of the symbol table
func (sym *SymTable) Version() uint16 {
	return sym.version
}

// Returns the number of symbols in the symbol table
func (sym *SymTable) NumSymbols() int {
	return len(sym.dictToString)
}

// Returns the number of attributes in the symbol table
func (sym *SymTable) NumAttributes() int {
	return len(sym.attributeTable)
}

// Returns true if the given attribute value already exists in the symbol table
func (sym *SymTable) AttributeValueExists(value string) bool {
	_, ok := sym.dictToIndex[value]
	return ok
}

// Find the index of the given attribute value. If the attribute having the
// specified name or the given value doesn't exist, return -1
func (sym *SymTable) FindAttributeIndex(name string, value string) int {
	if _, ok := sym.attributeTable[name]; !ok {
		return -1
	}

	dictIndex, ok := sym.dictToIndex[value]
	if !ok {
		return -1
	}

	if val, ok := sym.attributeTable[name].encodedValuesFromIndex[dictIndex]; ok {
		return int(val)
	}

	return -1
}

// Return the index header for the given set of attributes. If input refers
// to an attribute that does exist or a value that is not in the symbol table,
// the corresponding index is set to -1 and will return false
func (sym *SymTable) GetIndexedHeader(attributes map[string]string) ([]int, bool) {
	header := make([]int, len(sym.header))

	if len(sym.header) == 0 {
		return header, true
	}

	hasMissing := false
	for name, i := range sym.header {
		header[i] = -1
		val, ok := attributes[name]
		if ok {
			header[i] = sym.FindAttributeIndex(name, val)
			if header[i] == -1 {
				hasMissing = true
			}
		} else {
			hasMissing = true
		}
	}

	return header, hasMissing
}

// Return the attribute name and value map for the given indexed header
func (sym *SymTable) GetAttributesFromIndexedHeader(header []int) map[string]string {
	attributes := make(map[string]string)
	for name, i := range sym.header {
		if i < len(header) {
			if header[i] != -1 {
				attributes[name] = sym.FindAttributeValue(name, uint64(header[i]))
			}
		}
	}
	return attributes
}

// Find the attribute value for the given index. If the attribute having the
// specified name doesn't exist or the index is out of bounds, return an empty
func (sym *SymTable) FindAttributeValue(name string, index uint64) string {
	if _, ok := sym.attributeTable[name]; !ok {
		return ""
	}

	if index >= uint64(len(sym.attributeTable[name].encodedValues)) {
		return ""
	}

	dictIndex := sym.attributeTable[name].encodedValues[index]
	value, ok := sym.dictToString[dictIndex]
	if !ok {
		return ""
	}

	return value
}

// Return true if the specified other symbol table is the same as this one
// Two symbol tables are the same if they encode the same symbols and attribute
// values
func (sym *SymTable) IsSame(other *SymTable) bool {
	if len(sym.dictToString) != len(other.dictToString) {
		return false
	}

	for k, v := range sym.dictToString {
		if other.dictToString[k] != v {
			return false
		}
	}

	if len(sym.attributeTable) != len(other.attributeTable) {
		return false
	}

	for k, v := range sym.attributeTable {
		otherV, ok := other.attributeTable[k]
		if !ok {
			return false
		}

		if v.encodingType != otherV.encodingType {
			return false
		}

		if len(v.encodedValues) != len(otherV.encodedValues) {
			return false
		}

		for i, val := range v.encodedValues {
			if val != otherV.encodedValues[i] {
				return false
			}
		}
	}

	return true
}

//-----------
//- MODIFIERS
//-----------

// Update the dictionary with the given attribute values. If the attribute
// values already exists in the dictionary, this is an error
func (sym *SymTable) UpdateDictionary(
	attributeValues []string,
	writeCompleteFn WriteCompletionFn) error {
	if len(attributeValues) == 0 {
		return errors.New("attribute values are empty")
	}

	indexValue := uint64(len(sym.dictToString))
	for _, attributeValue := range attributeValues {

		if _, ok := sym.dictToString[indexValue]; ok {
			// This should, in theory never happen. The only this could happend
			// is someone modified the dicToString or dictToIndex through
			// another means
			return errors.New("index value already exists in symbol table")
		}

		if _, ok := sym.dictToIndex[attributeValue]; ok {
			return errors.New("attribute name already exists in symbol table")
		}

		sym.dictToString[indexValue] = attributeValue
		sym.dictToIndex[attributeValue] = indexValue
		indexValue++
	}

	// Update the stream if the table is not finalized and we have a stream
	// writer attached to this symtable
	if !sym.finalized && sym.streamWriter != nil {
		var err error = nil
		if sym.instructionSeqNum == 0 {
			err = sym.streamWriter.WriteInitInstruction(
				sym.version,
				attributeValues,
				writeCompleteFn)
		} else {
			err = sym.streamWriter.WriteUpdateInstruction(
				sym.version,
				sym.instructionSeqNum+1,
				attributeValues,
				writeCompleteFn)
		}
		if err != nil {
			return err
		}

		// Update the sequence number
		sym.instructionSeqNum++
	}

	return nil
}

// Inserts the given attribute value into the attribute having the specified
// name. If the attribute name doesn't exist, a new attribute is created in the
// SymTable. If the value already exists, this is a NOP
func (sym *SymTable) InsertAttributeValue(
	name string,
	value string,
	writeCompleteFn WriteCompletionFn) error {

	if _, ok := sym.dictToIndex[value]; !ok {
		id := uint64(len(sym.dictToIndex))
		sym.dictToIndex[value] = id
		sym.dictToString[id] = value
	}

	if _, ok := sym.attributeTable[name]; !ok {
		sym.attributeTable[name] = &AttributeTable{
			name:                   name,
			encodingType:           DictionaryEncodedValue,
			encodedValues:          make([]uint64, 0, 10),
			encodedValuesFromIndex: make(map[uint64]uint64),
		}
		sym.header[name] = len(sym.header)
	}

	// If the value is not already part of the attribute, make it
	id := sym.dictToIndex[value]
	if _, ok := sym.attributeTable[name].encodedValuesFromIndex[id]; !ok {
		sym.attributeTable[name].encodedValues = append(sym.attributeTable[name].encodedValues, id)
		sym.attributeTable[name].encodedValuesFromIndex[id] = uint64(len(sym.attributeTable[name].encodedValuesFromIndex))
	}

	// Update the stream
	return sym.updateStreamWithAttributeInstructionParam(
		name,
		[]uint64{id},
		writeCompleteFn)
}

// Insert the attribute values denoted by the given attribute indices into the
// attribute having the specified name. If the attribute indices are not valid
// (i.e. they don't exist in the symbol table), this is an error.
func (sym *SymTable) InsertAttributeIndices(
	name string,
	indices []uint64,
	writeCompleteFn WriteCompletionFn) error {
	if _, ok := sym.attributeTable[name]; !ok {
		sym.attributeTable[name] = &AttributeTable{
			name:                   name,
			encodingType:           DictionaryEncodedValue,
			encodedValues:          make([]uint64, 0, 10),
			encodedValuesFromIndex: make(map[uint64]uint64),
		}
		sym.header[name] = len(sym.header)
	}

	// Do a sanity check to make sure the indices are valid	before touching
	// the symbol table
	for _, index := range indices {
		if _, ok := sym.dictToString[index]; !ok {
			return errors.New("index value doesn't exist in symbol table")
		}
	}

	// Update the foward and reverse mapping
	for _, index := range indices {
		sym.attributeTable[name].encodedValues = append(sym.attributeTable[name].encodedValues, index)
		sym.attributeTable[name].encodedValuesFromIndex[index] = uint64(len(sym.attributeTable[name].encodedValuesFromIndex))
	}

	return sym.updateStreamWithAttributeInstructionParam(
		name,
		indices,
		writeCompleteFn)
}

// Finalize the symbol table. Once finalized, no more updates can be made
func (sym *SymTable) Finalize() {
	sym.finalized = true
	// TODO: Write the End instruction to the stream
}

//----------------
//- PUBLIC METHODS
//----------------

// Wait for all pending write operations to complete or until the specified
// timeout is reached. If timeout is 0, wait indefinitely wait for all
// pending writes to complete.
func (sym *SymTable) Wait(timeout time.Duration) {
	if sym.streamWriter != nil {
		sym.streamWriter.Wait(timeout)
	}
}

//-----------------
//- PRIVATE METHODS
//-----------------

// Update the stream if the table is not finalized and we have a stream
// writer attached to this symtable
func (sym *SymTable) updateStreamWithAttributeInstructionParam(
	name string,
	indices []uint64,
	writeCompleteFn WriteCompletionFn) error {

	if sym.streamWriter != nil && !sym.finalized {
		err := sym.streamWriter.WriteAttributeInstruction(
			sym.version,
			sym.instructionSeqNum+1,
			name,
			DictionaryEncodedValue,
			indices,
			writeCompleteFn)
		if err != nil {
			return err
		}

		// Update the sequence number
		sym.instructionSeqNum++
	}

	return nil
}
