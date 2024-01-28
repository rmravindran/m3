package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSymTableUpdateDictionary(t *testing.T) {

	symTable := NewSymTable("test", 1, nil)
	require.NotNil(t, symTable)

	attributeValues := []string{"a", "b", "c", "d", "e"}

	err := symTable.UpdateDictionary(attributeValues, nil)
	require.NoError(t, err)

	// Test that the dictionary is updated correctly
	require.True(t, symTable.AttributeValueExists("a"))
	require.True(t, symTable.AttributeValueExists("b"))
	require.True(t, symTable.AttributeValueExists("c"))
	require.True(t, symTable.AttributeValueExists("d"))
	require.True(t, symTable.AttributeValueExists("e"))

	// Test something that doesn't exist
	require.False(t, symTable.AttributeValueExists("f"))

	// Adding something that already exists should fail
	err = symTable.UpdateDictionary([]string{"a"}, nil)
	require.Error(t, err)
}

func TestSymTableAttributes(t *testing.T) {
	symTable := NewSymTable("test", 1, nil)
	require.NotNil(t, symTable)

	attributeValues := []string{"a", "b", "c", "d", "e"}
	err := symTable.UpdateDictionary(attributeValues, nil)
	require.NoError(t, err)

	// Create an attribute
	symTable.InsertAttributeValue("host", "a", nil)
	require.Equal(t, 0, symTable.FindAttributeIndex("host", "a"))
	// value "b" hasn't yet mapped to the host attribute
	require.Equal(t, -1, symTable.FindAttributeIndex("host", "b"))
	// now map it. Observe that the index should be continuous from the previous
	// index
	symTable.InsertAttributeValue("host", "b", nil)
	require.Equal(t, 1, symTable.FindAttributeIndex("host", "b"))
	// and verify that previous index hasn't changed.
	require.Equal(t, 0, symTable.FindAttributeIndex("host", "a"))

	// Find the attribute value for the given index
	require.Equal(t, "a", symTable.FindAttributeValue("host", 0))
	require.Equal(t, "b", symTable.FindAttributeValue("host", 1))
	// Asking for a non-existent index should return an empty string
	require.Equal(t, "", symTable.FindAttributeValue("host", 2))
	require.Equal(t, "", symTable.FindAttributeValue("host", 3))
	require.Equal(t, "", symTable.FindAttributeValue("host", 4))

	// Get the indexed header. It should have just 1 entry since we only
	// have 1 attribute
	indexedHeader, hasMissing := symTable.GetIndexedHeader(map[string]string{"host": "a"})
	require.Equal(t, 1, len(indexedHeader))
	require.Equal(t, 0, indexedHeader[0])
	require.False(t, hasMissing)
	// Asking for a non-existent attribute should return a header with -1
	indexedHeader, hasMissing = symTable.GetIndexedHeader(map[string]string{"crap": "a"})
	require.Equal(t, 1, len(indexedHeader))
	require.Equal(t, -1, indexedHeader[0])
	require.True(t, hasMissing)
	// Asking for a non-existent value should return a header with -1
	indexedHeader, hasMissing = symTable.GetIndexedHeader(map[string]string{"host": "crap"})
	require.Equal(t, 1, len(indexedHeader))
	require.Equal(t, -1, indexedHeader[0])
	require.True(t, hasMissing)
}

func TestSymTableMultipleAttributesSharingIndex(t *testing.T) {
	symTable := NewSymTable("test", 1, nil)
	require.NotNil(t, symTable)

	// host, src, dst all share the same universe of values

	attributeValues := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	err := symTable.UpdateDictionary(attributeValues, nil)
	require.NoError(t, err)

	// Add all attribute values as host attribute values
	for ndx, v := range attributeValues {
		symTable.InsertAttributeValue("host", v, nil)
		require.Equal(t, ndx, symTable.FindAttributeIndex("host", v))
	}

	// Add some of the attribute values as src attribute values
	// Note that each attribute has its own index space starting at 0.
	for ndx, v := range attributeValues[0:5] {
		symTable.InsertAttributeValue("src", v, nil)
		require.Equal(t, ndx, symTable.FindAttributeIndex("src", v))
	}

	// Add remaining attribute values as dst attribute values
	for ndx, v := range attributeValues[5:] {
		symTable.InsertAttributeValue("dst", v, nil)
		require.Equal(t, ndx, symTable.FindAttributeIndex("dst", v))
	}

	// Check the attribute values for the host attribute
	for ndx, v := range attributeValues {
		require.Equal(t, v, symTable.FindAttributeValue("host", uint64(ndx)))
	}
	// Check the attribute values for the src attribute
	for ndx, v := range attributeValues[0:5] {
		require.Equal(t, v, symTable.FindAttributeValue("src", uint64(ndx)))
	}
	// Check the attribute values for the dst attribute
	for ndx, v := range attributeValues[5:] {
		require.Equal(t, v, symTable.FindAttributeValue("dst", uint64(ndx)))
	}

	// Check the indexed data, it should have 3 entries all 0 since we are
	// asking for the first value encoded in the insert operation for each
	// attribute
	indexedHeader, hasMissing := symTable.GetIndexedHeader(
		map[string]string{
			"host": "a",
			"src":  "a",
			"dst":  "f",
		})
	require.Equal(t, 3, len(indexedHeader))
	require.Equal(t, 0, indexedHeader[0])
	require.Equal(t, 0, indexedHeader[0])
	require.Equal(t, 0, indexedHeader[0])
	require.False(t, hasMissing)
}

func TestSymTableSame(t *testing.T) {
	symTable := NewSymTable("test", 1, nil)
	otherTable := NewSymTable("test2", 2, nil)
	require.NotNil(t, symTable)
	require.NotNil(t, otherTable)

	// host, src, dst all share the same universe of values

	attributeValues := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	err := symTable.UpdateDictionary(attributeValues, nil)
	require.NoError(t, err)
	err = otherTable.UpdateDictionary(attributeValues, nil)
	require.NoError(t, err)

	// Add all attribute values as host attribute values
	for _, v := range attributeValues {
		symTable.InsertAttributeValue("host", v, nil)
		otherTable.InsertAttributeValue("host", v, nil)
	}
	require.True(t, symTable.IsSame(otherTable))

	// Missing dictionary entry is not the same
	symTable.InsertAttributeValue("host", "crap", nil)
	require.False(t, symTable.IsSame(otherTable))

	// Now it should be same
	otherTable.InsertAttributeValue("host", "crap", nil)
	require.True(t, symTable.IsSame(otherTable))

	// Add some of the attribute values as src attribute values
	for _, v := range attributeValues[0:5] {
		symTable.InsertAttributeValue("src", v, nil)
	}

	// Attribute doesn't exist int the other table
	require.False(t, symTable.IsSame(otherTable))

	// Attribute exists but doesn't all the values
	for _, v := range attributeValues[0:4] {
		otherTable.InsertAttributeValue("src", v, nil)
	}
	require.False(t, symTable.IsSame(otherTable))

	// Now all values exists
	otherTable.InsertAttributeValue("src", attributeValues[4], nil)
	require.True(t, symTable.IsSame(otherTable))

	// Attribute exist but not applied in the same order. This implies
	// that the two streams that built up the the symtable was not streamed
	// in the same order
	symTable.InsertAttributeValue("dst", attributeValues[0], nil)
	symTable.InsertAttributeValue("dst", attributeValues[1], nil)
	otherTable.InsertAttributeValue("dst", attributeValues[1], nil)
	otherTable.InsertAttributeValue("dst", attributeValues[0], nil)
	require.False(t, symTable.IsSame(otherTable))
}
