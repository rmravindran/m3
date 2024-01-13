package core

import (
	"errors"
)

type AttributeEncoding int

const (
	// Values are encoded as raw unsigned values
	UnsignedNumericValue AttributeEncoding = iota

	// Map values to indices in a dictionary
	DictionaryEncodedValue
)

type AttributeTable struct {
	name                   string
	encodingType           AttributeEncoding
	encodedValues          []uint64
	encodedValuesFromIndex map[uint64]uint64
}

type SymTable struct {
	name string

	// Dictionary encoded entries of the attributes
	dictToString map[uint64]string
	dictToIndex  map[string]uint64

	// Header, aka the column index
	header map[string]int

	// Attribute Tables
	attributeTable map[string]*AttributeTable
}

func NewSymTable(name string) *SymTable {
	return &SymTable{
		name:           name,
		dictToString:   make(map[uint64]string),
		dictToIndex:    make(map[string]uint64),
		header:         make(map[string]int),
		attributeTable: make(map[string]*AttributeTable),
	}
}

// Returns the name of the symbol table
func (sym *SymTable) Name() string {
	return sym.name
}

// Returns the number of symbols in the symbol table
func (sym *SymTable) NumSymbols() int {
	return len(sym.dictToString)
}

// Update the dictionary with the given indices and attribute values. If the
// index already exists in the dictionary, or the attribute name already exists
// in the dictionary, this is an error
func (sym *SymTable) UpdateDictionary(indices []uint64, attributeValues []string) error {
	if len(indices) == 0 || len(indices) != len(attributeValues) {
		return errors.New("indices and values must of the same size")
	}

	for i, indexValue := range indices {
		attributeValue := attributeValues[i]

		if _, ok := sym.dictToString[indexValue]; ok {
			return errors.New("index value already exists in symbol table")
		}

		if _, ok := sym.dictToIndex[attributeValue]; ok {
			return errors.New("attribute name already exists in symbol table")
		}

		sym.dictToString[indexValue] = attributeValue
		sym.dictToIndex[attributeValue] = indexValue
	}

	return nil
}

// Returns true if the given attribute value already exists in the symbol table
func (sym *SymTable) AttributeValueExists(value string) bool {
	_, ok := sym.dictToIndex[value]
	return ok
}

// Inserts the given attribute value into the attribute having the specified
// name. If the attribute name doesn't exist, a new attribute is created in the
// SymTable. If the value already exists, this is a NOP
func (sym *SymTable) InsertAttributeValue(name string, value string) error {

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

	return nil
}

// Insert the attribute values denoted by the given attribute indices into the
// attribute having the specified name. If the attribute indices are not valid
// (i.e. they don't exist in the symbol table), this is an error.
func (sym *SymTable) InsertAttributeIndices(name string, indices []uint64) error {
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

	return nil
}

// Find the index of the given attribute value. If the attribute value doesn't
// exist, returns -1
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

// Find the attribute value for the given index. If the index doesn't exist,
// returns an empty string
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

// Return the index header for the given set of attributes
func (sym *SymTable) GetIndexedHeader(attributes map[string]string) []int {
	header := make([]int, len(sym.header))

	for name, i := range sym.header {
		header[i] = -1
		val, ok := attributes[name]
		if ok {
			header[i] = sym.FindAttributeIndex(name, val)
		}
	}

	return header
}
