// Copyright (c) 2023 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"context"
	"log"
	"os"
	"time"

	boostclient "github.com/m3db/m3/src/boost/client"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	yaml "gopkg.in/yaml.v2"
)

const (
	namespace = "metrics_0_30m"
)

var (
	namespaceID = ident.StringID(namespace)
)

type config struct {
	Client client.Configuration `yaml:"client"`
}

var configFile = "config.yaml"

func main() {

	cfgBytes, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("unable to read config file: %s, err: %v", configFile, err)
	}

	cfg := &config{}
	if err := yaml.UnmarshalStrict(cfgBytes, cfg); err != nil {
		log.Fatalf("unable to parse YAML: %v", err)
	}

	client, err := cfg.Client.NewClient(client.ConfigurationParameters{})
	if err != nil {
		log.Fatalf("unable to create new M3DB client: %v", err)
	}

	session, err := client.NewSession()
	if err != nil {
		log.Fatalf("unable to create new M3DB session: %v", err)
	}
	defer session.Close()

	writeAttributeStream(session)
	//runTaggedExample(session)
}

func writeAttributeStream(session client.Session) {

	// First we create an instance of BoostSession which retains the fcuntion
	// of a regular session but also has the ability to read and write
	// attributes in a series.

	boostSession := boostclient.NewBoostSession(session, 1000)

	log.Printf("------ run tagged example ------")
	var (
		seriesID = ident.StringID("__name__=\"cpu_user_util\",region=\"us-east-1\",service=\"myservice1\"")
		tags     = []ident.Tag{
			{Name: ident.StringID("region"), Value: ident.StringID("us-east-1")},
			{Name: ident.StringID("service"), Value: ident.StringID("myservice1")},
		}
		tagsIter = ident.NewTagsIterator(ident.NewTags(tags...))

		attributes = []ident.Tag{
			{Name: ident.StringID("host"), Value: ident.StringID("host-000001")},
		}
		attrIter = ident.NewTagsIterator(ident.NewTags(attributes...))
	)

	// Write a ts value with tags and attributes
	timestamp := xtime.Now()
	value := 42.0
	err := boostSession.WriteValueWithTaggedAttributes(
		namespaceID,
		seriesID,
		tagsIter,
		attrIter,
		timestamp,
		value,
		xtime.Millisecond)
	if err != nil {
		log.Fatalf("error writing series %s, err: %v", seriesID.String(), err)
	}

	end := xtime.Now()
	start := end.Add(-time.Minute)

	// Now lets read the series back out
	seriesIter, err := boostSession.FetchValueWithTaggedAttribute(
		namespaceID,
		seriesID,
		start,
		end)
	if err != nil {
		log.Fatalf("error fetching data for untagged series: %v", err)
	}
	for seriesIter.Next() {
		dp, _, _ := seriesIter.Current()
		log.Printf("Series Value %s: %v", dp.TimestampNanos.String(), dp.Value)

		// Lets also print out the tags and attributes
		tags := seriesIter.Tags()
		for tags.Next() {
			tag := tags.Current()
			log.Printf("Tag %s=%s", tag.Name.String(), tag.Value.String())
		}

		attributes := seriesIter.Attributes()
		for attributes.Next() {
			attribute := attributes.Current()
			log.Printf("Attribute %s=%s", attribute.Name.String(), attribute.Value.String())
		}
	}
	if err := seriesIter.Err(); err != nil {
		log.Fatalf("error in series iterator: %v", err)
	}

	/*
		// Now read the data back out using a different session
		seriesIter, err = boostSession.FetchValueWithTaggedAttributes(
			namespaceID,
			seriesID,
			start,
			end)

		// Test read the symtable
		symTableName := "m3_symboltable_" + seriesID.String()
		symTableSeriesId := ident.StringID(symTableName)

		symTableReader := boostcore.NewM3DBSymStreamReader(
			namespaceID,
			symTableSeriesId,
			session)
		err = symTableReader.Seek(start, end)
		if err != nil {
			log.Fatalf("error seeking symtable: %v", err)
		}

		version, instruction, seqNum, err := symTableReader.Next()
		if err != nil {
			log.Fatalf("error reading symtable: %v", err)
		}
		log.Printf("SymTable Version: %d, Instruction: %d, SeqNum: %d", version, instruction, seqNum)
	*/
}

// runTaggedExample demonstrates how to write "tagged" (indexed) metrics data
// and then read it back out again by either:
//
//  1. Querying for a set of time series using an inverted index query
//  2. Querying for a specific time series by its ID directly
func runTaggedExample(session client.Session) {
	log.Printf("------ run tagged example ------")
	var (
		seriesID = ident.StringID("{__name__=\"network_in\",host=\"host-01\",region=\"us-east-1\"}")
		tags     = []ident.Tag{
			{Name: ident.StringID("host"), Value: ident.StringID("host01")},
			{Name: ident.StringID("region"), Value: ident.StringID("us-east-1")},
		}
		tagsIter = ident.NewTagsIterator(ident.NewTags(tags...))
	)
	// Write a tagged series ID using millisecond precision.
	timestamp := xtime.Now()
	value := 42.0
	err := session.WriteTagged(namespaceID, seriesID, tagsIter,
		timestamp, value, xtime.Millisecond, nil)
	if err != nil {
		log.Fatalf("error writing series %s, err: %v", seriesID.String(), err)
	}

	// 1. Fetch data for the tagged seriesID using a query (only data written
	// within the last minute).
	end := xtime.Now()
	start := end.Add(-time.Minute)

	// Use regexp to filter on a single tag, use idx.NewConjunctionQuery to
	// to search on multiple tags, etc.
	reQuery, err := idx.NewRegexpQuery([]byte("host"), []byte("host[0-9]+"))
	if err != nil {
		log.Fatalf("error in creating query: %v", err)
	}

	deadline, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))

	resultsIter, _, err := session.FetchTagged(deadline, namespaceID, index.Query{Query: reQuery},
		index.QueryOptions{StartInclusive: start, EndExclusive: end})
	if err != nil {
		log.Fatalf("error fetching data for tagged series: %v", err)
	}
	for _, seriesIter := range resultsIter.Iters() {
		log.Printf("series: %s", seriesIter.ID().String())
		tags := seriesIter.Tags()
		for tags.Next() {
			tag := tags.Current()
			log.Printf("%s=%s", tag.Name.String(), tag.Value.String())
		}
		if err := tags.Err(); err != nil {
			log.Fatalf("error in tag iterator: %v", err)
		}
		for seriesIter.Next() {
			dp, _, _ := seriesIter.Current()
			log.Printf("%s: %v", dp.TimestampNanos.String(), dp.Value)
		}
		if err := seriesIter.Err(); err != nil {
			log.Fatalf("error in series iterator: %v", err)
		}
	}
	cancelFunc()

	// 2. Fetch data for the series ID directly, skips the inverted index.
	seriesIter, err := session.Fetch(namespaceID, seriesID, start, end)
	if err != nil {
		log.Fatalf("error fetching data for untagged series: %v", err)
	}
	for seriesIter.Next() {
		dp, _, _ := seriesIter.Current()
		log.Printf("%s: %v", dp.TimestampNanos.String(), dp.Value)
	}
	if err := seriesIter.Err(); err != nil {
		log.Fatalf("error in series iterator: %v", err)
	}
}
