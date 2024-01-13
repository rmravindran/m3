package core

import (
	"encoding/binary"

	"github.com/m3db/m3/src/boost/core"
	"github.com/m3db/m3/src/dbnode/client"
)

type StreamUpdater struct {
	streamPrefix string
	session      *client.Session
	tmpSpace     []byte
}

func NewStreamUpdater(streamPrefix string, session *client.Session) *StreamUpdater {
	return &StreamUpdater{
		streamPrefix: streamPrefix,
		session:      session,
		tmpSpace:     make([]byte, 16*1024)}
}

func (su *StreamUpdater) WriteInitInstruction(attributeValues []string) error {
	// Write the dictionary instruction param such that the parseDictionaryInstruction
	// function can parse it
	ndx := 0
	binary.LittleEndian.PutUint32(su.tmpSpace, uint32(len(attributeValues)))
	ndx += 4
	for _, v := range attributeValues {
		binary.LittleEndian.PutUint16(su.tmpSpace[ndx:], uint16(len(v)))
		ndx += 2
		copy(su.tmpSpace[ndx:], []byte(v))
		ndx += len(v)
	}

	return nil
}

func (su *StreamUpdater) WriteAttributeInstruction(
	attributeName string, encodingType core.AttributeEncoding, indexValues []uint64) error {
	// Write the attribute table instruction param such that the parseAddAttributeInstruction
	// function can parse it
	ndx := 0
	binary.LittleEndian.PutUint16(su.tmpSpace, uint16(len(attributeName)))
	ndx += 2
	copy(su.tmpSpace[ndx:], []byte(attributeName))
	ndx += len(attributeName)
	binary.LittleEndian.PutUint16(su.tmpSpace[ndx:], uint16(encodingType))
	ndx += 2
	binary.LittleEndian.PutUint32(su.tmpSpace[ndx:], uint32(len(indexValues)))
	ndx += 4
	for _, v := range indexValues {
		binary.LittleEndian.PutUint64(su.tmpSpace[ndx:], uint64(v))
		ndx += 8
	}

	return nil
}
