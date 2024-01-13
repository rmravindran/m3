package client

import (
	"encoding/binary"
	"errors"

	"github.com/m3db/m3/src/boost/core"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type TableInstruction int

const (
	// Initiatize the table
	InitSymTable TableInstruction = iota

	// Update the table
	UpdateSymTable

	// Add a new attribute to the table
	AddAttribute

	// End Dictionary
	EndSymTable
)

type AttributeInstructionParams struct {
	attributeName string
	encodingType  core.AttributeEncoding
	indexValues   []uint64
}

type DictionaryInstructionParams struct {
	dictValues []string
}

type Instruction struct {
	instruction                TableInstruction
	attributeInstructionParam  *AttributeInstructionParams
	dictionaryInstructionParam *DictionaryInstructionParams
}

type SymTableBuilder struct {
	streamPrefix string
}

func NewSymTableBuilder() *SymTableBuilder {
	return &SymTableBuilder{streamPrefix: "m3_metrics_symtable"}
}

// Scans the m3db and builds the SymTable
func (stb *SymTableBuilder) BuildSymTable(
	session client.Session,
	namespaceId ident.ID,
	name string,
	tagsIt ident.TagIterator,
	version uint64,
	timeBegin xtime.UnixNano,
	timeEnd xtime.UnixNano) (*core.SymTable, error) {

	seriesID := ident.StringID(stb.streamName(name, tagsIt))
	seriesIter, err := session.Fetch(namespaceId, seriesID, timeBegin, timeEnd)
	if err != nil {
		return nil, errors.New("unable to fetch the symbol table stream")
	}

	for seriesIter.Next() {
		_, _, raw := seriesIter.Current()
		if len(raw) < 16 {
			return nil, errors.New("invalid symbol table data")
		}
		flags := binary.LittleEndian.Uint64(raw)
		v := binary.LittleEndian.Uint64(raw[8:])
		instruction := flags & 0xFFFFFFFF
		if instruction != uint64(InitSymTable) || version != v {
			continue
		}

		// We found the Init entry matching the requested version. Build the
		// symbol table until we find the END entry
		symTable := core.NewSymTable(name)
		doRestart := false
		for seriesIter.Next() {
			_, _, raw := seriesIter.Current()
			if len(raw) < 16 {
				return nil, errors.New("invalid symbol table data")
			}
			flags := binary.LittleEndian.Uint64(raw)
			v := binary.LittleEndian.Uint64(raw[8:])
			instruction := flags & 0xFFFFFFFF
			switch instruction {
			case uint64(InitSymTable):
				// The symtable we read was not complete. We need to restart
				// the build of symtable from THIS point onwards. This usually
				// happens if the symtable was not fully written to the stream
				if v == version {
					// Restart
					symTable = core.NewSymTable(name)
					break
				} else {
					// Something really bad happened
					return nil, errors.New("symbol table with version " + string(v) + " found when expecting version " + string(version))
				}
			case uint64(UpdateSymTable):
				instrParams, err := stb.parseDictionaryInstructionParams(raw[16:])
				if err != nil {
					// Restart
					doRestart = true
					break
				}
				indices := make([]uint64, len(instrParams.dictValues))
				baseIndex := uint64(symTable.NumSymbols())
				for i := 0; i < len(instrParams.dictValues); i++ {
					indices[i] = uint64(baseIndex)
					baseIndex++
				}
				err = symTable.UpdateDictionary(indices, instrParams.dictValues)
				if err != nil {
					// Restart
					doRestart = true
					break
				}

			case uint64(AddAttribute):
				instrParams, err := stb.parseAddAttributeInstructionParams(raw[16:])
				if err != nil {
					// Restart
					doRestart = true
					break
				}
				err = symTable.InsertAttributeIndices(instrParams.attributeName, instrParams.indexValues)
				if err != nil {
					// Restart
					doRestart = true
					break
				}

			case uint64(EndSymTable):
				// Done
				return symTable, nil
			}

			if doRestart {
				// We need to restart the build of symtable from the NEXT point
				// onwards, but start when we find the next Init entry with
				// the required version
				doRestart = false
				break
			}
		}
	}

	return nil, nil
}

// Return the fully qualified stream name for the given symtable name
func (stb *SymTableBuilder) streamName(symTableName string, tagsIt ident.TagIterator) string {
	seriesName := stb.streamPrefix + "_" + symTableName
	for tagsIt.Next() {
		tag := tagsIt.Current()
		seriesName += "," + tag.Name.String() + "=" + tag.Value.String()
	}
	return seriesName
}

// Parse and return the DictionaryInstructionParams from the given raw bytes
func (stb *SymTableBuilder) parseDictionaryInstructionParams(raw []byte) (*DictionaryInstructionParams, error) {
	if len(raw) < 4 {
		return nil, errors.New("invalid symbol table data")
	}
	numValues := binary.LittleEndian.Uint32(raw)
	ndx := 4
	dictValues := make([]string, numValues)
	for i := 0; i < int(numValues); i++ {
		// Read the size of the string, at most 16k character values
		size := int(binary.LittleEndian.Uint16(raw[int(ndx):]))
		ndx += 2
		dictValues[i] = string(raw[ndx : ndx+size])
		ndx += size
	}
	return &DictionaryInstructionParams{dictValues: dictValues}, nil
}

// Parse and return the AddAttributeInstructionParams from the given raw bytes
func (stb *SymTableBuilder) parseAddAttributeInstructionParams(raw []byte) (*AttributeInstructionParams, error) {
	if len(raw) < 2 {
		return nil, errors.New("invalid symbol table data")
	}
	ndx := 0

	instruction := &AttributeInstructionParams{}
	sz := int(binary.LittleEndian.Uint16(raw[ndx:]))
	ndx += 2
	instruction.attributeName = string(raw[ndx : ndx+sz])
	ndx += sz
	instruction.encodingType = core.AttributeEncoding(binary.LittleEndian.Uint16(raw[ndx:]))
	ndx += 2

	numValues := binary.LittleEndian.Uint32(raw[ndx:])
	ndx += 4
	instruction.indexValues = make([]uint64, numValues)
	for i := 0; i < int(numValues); i, ndx = i+1, ndx+8 {
		instruction.indexValues[i] = binary.LittleEndian.Uint64(raw[ndx:])
	}
	return instruction, nil
}
