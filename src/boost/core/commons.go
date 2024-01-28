package core

import (
	"fmt"

	"github.com/m3db/m3/src/x/ident"
)

type WriteCompletionFn func(err error)

// Resolves the name of the symbol table stream for the given series id
type SymbolTableStreamNameResolver func(qualifiedSeriesId ident.ID) string

func DefaultSymbolTableStreamNameResolver(qualifiedSeriesId ident.ID) string {
	return GetSymbolTableName(qualifiedSeriesId.String())
}

func SingleStreamSymbolTableStreamNameResolver(qualifiedSeriesId ident.ID) string {
	// Remove the prefix "m3_data_#####_"
	return "m3_symboltable_" + qualifiedSeriesId.String()[14:]
}

// Return the fully qualified data series name from the series id
func GetQualifiedSeriesName(id ident.ID, distributionId uint16) string {
	return fmt.Sprintf("m3_data_%05d_", distributionId)
}

// Return the series name from the qualified name
func GetSeriesName(qualifiedSeriesName string) string {
	return qualifiedSeriesName[8:]
}

// Return the symbol table name from the qualified series name
func GetSymbolTableName(qualifiedSeriesName string) string {
	return "m3_symboltable_" + GetSeriesName(qualifiedSeriesName)
}
