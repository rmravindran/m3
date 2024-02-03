package core

import (
	"fmt"
	"strings"

	"github.com/m3db/m3/src/x/ident"
)

const (
	c_SymbolTablePrefix string = "m3_symboltable_"
	//c_SFPrefix          string = "m3_seriesfamily_"
	c_DataSeriesPrefix string = "m3_data"
)

// A fully qualified series name is the name of the series with the
// distribution id:
// Example: domain::seriesfamily::data00001::cpu_utilization where
// "cpu_utilization" is th series name and "00001" is the shard id, "domain" is
// the domain and "seriesfamily" is the series family

type WriteCompletionFn func(err error)

// Resolves the name of the symbol table stream for the given series id
type SymbolTableStreamNameResolver func(qualifiedSeriesId ident.ID) string

func SingleStreamSymbolTableStreamNameResolver(qualifiedSeriesId ident.ID) string {
	// Remove the prefix "m3_data_#####_"
	return c_SymbolTablePrefix + qualifiedSeriesId.String()[14:]
}

// Return the fully qualified data series name from the series id
func GetQualifiedSeriesName(domain string, sf string, distributionId uint16, id ident.ID) string {
	return fmt.Sprintf("%s::%s::%s%05d::%s", domain, sf, c_DataSeriesPrefix, distributionId, id.String())
}

// Return the series name from the qualified name
func GetSeriesName(qualifiedSeriesName string) string {
	parts := strings.SplitN(qualifiedSeriesName, "::", 4)
	if parts == nil || len(parts) < 4 {
		return ""
	}
	return parts[3]
}

// Return the qualifiers from the qualified series name
func GetQualifiers(qualifiedSeriesName string) (string, string, int, string) {
	parts := strings.SplitN(qualifiedSeriesName, "::", 4)
	if parts == nil || len(parts) < 4 {
		return "", "", 0, ""
	}
	shardId := 0

	// Extract the shard id from the 3rd part of the qualified series name
	fmt.Sscanf(parts[2], c_DataSeriesPrefix+"%05d", &shardId)

	return parts[0], parts[1], shardId, parts[3]
}

// Return the symbol table name from the qualified series name
func GetSymbolTableName(qualifiedSeriesName string) string {
	domain, seriesFamily, _, seriesName := GetQualifiers(qualifiedSeriesName)
	symTableName := fmt.Sprintf("%s::%s::symboltable", domain, seriesFamily)

	// If the series family is "naf", then the series is not part of family and
	// the symbol table is specific for the series.
	if seriesFamily == "naf" {
		symTableName = symTableName + "::" + seriesName
	}

	return symTableName
}
