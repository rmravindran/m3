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
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	boostclient "github.com/m3db/m3/src/boost/client"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	yaml "gopkg.in/yaml.v2"

	"github.com/montanaflynn/stats"
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

	fmt.Printf("gomaxprocs %d\n", runtime.GOMAXPROCS(0))

	opt := client.Options()

	opt = opt.SetWriteBatchSize(16).
		SetHostQueueOpsFlushSize(16).
		SetAsyncWriteMaxConcurrency(8096).
		//SetWriteOpPoolSize(8096).
		SetHostQueueOpsArrayPoolSize(128).
		//SetUseV2BatchAPIs(true).
		SetHostQueueOpsFlushInterval(100 * time.Microsecond)
	chOpt := opt.ChannelOptions()
	chOpt.DefaultConnectionOptions.SendBufferSize = 8 * 1024 * 1024
	chOpt.DefaultConnectionOptions.RecvBufferSize = 8 * 1024 * 1024

	log.Printf("AsyncWriteMaxCuncurrency: %d", client.Options().AsyncWriteMaxConcurrency())
	log.Printf("WriteOpPoolSize: %d", client.Options().WriteOpPoolSize())
	log.Printf("HostQueueOpsArrayPoolSize: %d", client.Options().HostQueueOpsArrayPoolSize())
	log.Printf("WriteBatchSize: %d", client.Options().WriteBatchSize())
	log.Printf("HostQueueOpsFlushSize: %d", client.Options().HostQueueOpsFlushSize())
	log.Printf("HostQueueOpsFlushInterval: %s", client.Options().HostQueueOpsFlushInterval().String())

	session, err := client.NewSessionWithOptions(opt)
	if err != nil {
		log.Fatalf("unable to create new M3DB session: %v", err)
	}

	defer session.Close()

	log.Printf("------ run tagged attribute example ------")

	f, err := os.Create("cpu_profile.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	runtime.SetCPUProfileRate(100)
	defer pprof.StopCPUProfile()

	// First write and read a tagged value with attribute.
	//seriesID, start, end := writeAndReadTaggedValueWithAttribute(session)

	// Now read the data using a different session (forcing the symbol table
	// to be fetched from the database)
	//readTaggedValueWithAttribute(session, seriesID, start, end)

	// Test writing a large number of attributes
	writeAndReadLargeData(session, 10000)
	//time.Sleep(250 * time.Millisecond)
	//writeAndReadLargeDataWithoutAttributes(session, 10000)
}

var numWrites atomic.Uint64

func completionFn(err error) {
	numWrites.Add(1)
}

func writeAndReadTaggedValueWithAttribute(session client.Session) (ident.ID, xtime.UnixNano, xtime.UnixNano) {

	// First we create an instance of BoostSession which retains the fcuntion
	// of a regular session but also has the ability to read and write
	// attributes in a series.

	boostSession := boostclient.NewBoostSession(session, 1000)

	start := xtime.Now()

	log.Printf("------ write to db ------")
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
		xtime.Millisecond,
		completionFn)
	if err != nil {
		log.Fatalf("error writing series %s, err: %v", seriesID.String(), err)
	}

	end := xtime.Now()
	start = start.Add(-time.Millisecond * 5)

	log.Printf("------ read from db ------")

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

	return seriesID, start, end
}

func readTaggedValueWithAttribute(
	session client.Session,
	seriesID ident.ID,
	start xtime.UnixNano,
	end xtime.UnixNano) {

	// First we create an instance of BoostSession which retains the function
	// of a regular session but also has the ability to read and write
	// attributes in a series.

	boostSession := boostclient.NewBoostSession(session, 1000)

	log.Printf("------ read with new session instance ------")

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
}

func timer(name string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", name, time.Since(start))
	}
}

func writeAndReadLargeDataWithoutAttributes(session client.Session, count int) {

	// Create session with large number of data without attribute values

	start := xtime.Now()
	var (
		seriesID = ident.StringID("__name__=\"cpu_user_util\",region=\"us-east-1\",service=\"myservice1\"")
		tags     = []ident.Tag{
			{Name: ident.StringID("region"), Value: ident.StringID("us-east-1")},
			{Name: ident.StringID("service"), Value: ident.StringID("myservice1")},
		}
		tagsIter = ident.NewTagsIterator(ident.NewTags(tags...))
	)

	func() {
		defer timer("write-large-series-without-attributes")()
		log.Printf("------ write large data (no attributes) to db ------")
		writtenValue := 1.0
		// Same series, but with 1000000 attributes
		tookTimes := make([]int, count)
		for i := 0; i < count; i++ {
			//seriesName := "__name__=\"cpu_user_util\",region=\"us-east-1\",service=\"myservice1\""
			//hostName := seriesName + fmt.Sprintf("host-%07d", i%10)
			//seriesID = ident.StringID(hostName)
			clockStart := xtime.Now()
			// Write a ts value with tags and attributes
			timestamp := xtime.Now()
			err := session.WriteTagged(
				namespaceID,
				seriesID,
				tagsIter,
				timestamp,
				writtenValue,
				xtime.Nanosecond,
				nil)
			if err != nil {
				log.Fatalf("error writing series %s, err: %v", seriesID.String(), err)
			}
			writtenValue++
			clockStop := xtime.Now()
			dur := clockStop - clockStart
			tookTimes[i] = dur.ToTime().Nanosecond() / 1000
		}
		d := stats.LoadRawData(tookTimes)
		//a, _ := stats.Min(d)
		//fmt.Println("min time:", a)
		pcts := []float64{50, 90, 99, 99.9}
		desc, _ := stats.Describe(d, false, &pcts)
		fmt.Println(desc.String(2))
	}()
	end := xtime.Now()
	return

	func() {
		defer timer("read-large-series-without-attributes")()
		log.Printf("------ read large data (no attributes) to db ------")
		expVal := 1.0
		// Now lets read the series back out
		seriesIter, err := session.Fetch(
			namespaceID,
			seriesID,
			start.Add(-time.Millisecond*5), // Adjust by 5 milliseconds
			end)
		if err != nil {
			log.Fatalf("error fetching data for untagged series: %v", err)
		}
		for seriesIter.Next() {
			dp, _, _ := seriesIter.Current()
			if dp.Value != expVal {
				log.Fatalf("unexpected value: found %v but expected %v", dp.Value, expVal)
			}
			expVal++
			//log.Printf("Series Value %s: %v", dp.TimestampNanos.String(), dp.Value)

			// Lets also print out the tags and attributes
			tags := seriesIter.Tags()
			for tags.Next() {
				tag := tags.Current()
				log.Printf("Tag %s=%s", tag.Name.String(), tag.Value.String())
			}
		}

		if err := seriesIter.Err(); err != nil {
			log.Fatalf("error in series iterator: %v", err)
		}
	}()
}

func writeAndReadLargeData(session client.Session, count int) {

	// Create session with large number of attribute values

	boostSession := boostclient.NewBoostSession(session, 1000)

	var (
		seriesID = ident.StringID("__name__=\"cpu_user_util\",region=\"us-east-1\",service=\"myservice1\"")
		tags     = []ident.Tag{
			{Name: ident.StringID("region"), Value: ident.StringID("us-east-1")},
			{Name: ident.StringID("service"), Value: ident.StringID("myservice1")},
		}
		tagsIter = ident.NewTagsIterator(ident.NewTags(tags...))
	)

	// Write with fresh dictionary
	numWrites = atomic.Uint64{}
	numWrites.Store(0)
	writeDataWithAttributes(boostSession, seriesID, tagsIter, count)
	time.Sleep(250 * time.Millisecond)

	// Write with reuse of the dictionary
	log.Printf("Write with reuse of the dictionary")
	numWrites.Store(0)
	start, end := writeDataWithAttributes(boostSession, seriesID, tagsIter, count)
	time.Sleep(5000 * time.Millisecond)

	readDataWithAttribute(boostSession, seriesID, start, end)
}

func writeDataWithAttributes(boostSession *boostclient.BoostSession,
	seriesID ident.ID,
	tagsIter ident.TagIterator,
	count int) (xtime.UnixNano, xtime.UnixNano) {
	defer timer("write-large-series-with-attributes")()
	log.Printf("------ write large data (with attributes) to db ------")
	start := xtime.Now()
	writtenValue := 1.0

	// Same series, but with 1000000 attributes
	for i := 0; i < count; i++ {
		hostName := fmt.Sprintf("host-%07d", i)
		attributes := []ident.Tag{
			{Name: ident.StringID("host"), Value: ident.StringID(hostName)},
		}
		attrIter := ident.NewTagsIterator(ident.NewTags(attributes...))

		// Write a ts value with tags and attributes
		timestamp := xtime.Now()
		err := boostSession.WriteValueWithTaggedAttributes(
			namespaceID,
			seriesID,
			tagsIter,
			attrIter,
			timestamp,
			writtenValue,
			xtime.Nanosecond,
			completionFn)
		if err != nil {
			log.Fatalf("error writing series %s, err: %v", seriesID.String(), err)
		}
		writtenValue++
	}
	end := xtime.Now()

	// Wait until numWrites reaches count
	for {
		if numWrites.Load() == uint64(count) {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	return start, end
}

func readDataWithAttribute(boostSession *boostclient.BoostSession,
	seriesID ident.ID,
	start xtime.UnixNano,
	end xtime.UnixNano) {
	defer timer("read-large-series-with-attributes")()
	log.Printf("------ read large data (with attributes) to db ------")
	expVal := 1.0
	// Now lets read the series back out
	seriesIter, err := boostSession.FetchValueWithTaggedAttribute(
		namespaceID,
		seriesID,
		start.Add(-time.Millisecond*5), // Adjust by 5 milliseconds
		end)
	if err != nil {
		log.Fatalf("error fetching data for untagged series: %v", err)
	}
	ndx := 0
	for seriesIter.Next() {
		dp, _, _ := seriesIter.Current()
		if dp.Value != expVal {
			log.Fatalf("unexpected value: found %v but expected %v", dp.Value, expVal)
		}
		expVal++
		//log.Printf("Series Value %s: %v", dp.TimestampNanos.String(), dp.Value)

		// Lets also print out the tags and attributes
		tags := seriesIter.Tags()
		for tags.Next() {
			tag := tags.Current()
			log.Printf("Tag %s=%s", tag.Name.String(), tag.Value.String())
		}

		attributes := seriesIter.Attributes()
		if attributes.Next() {
			attribute := attributes.Current()
			hostName := fmt.Sprintf("host-%07d", ndx)
			if (attribute.Name.String() != "host") || (attribute.Value.String() != hostName) {
				log.Fatalf("unexpected attribute: found %s=%s but expected host=%s", attribute.Name.String(), attribute.Value.String(), hostName)
			}
		}
		ndx++
	}

	if err := seriesIter.Err(); err != nil {
		log.Fatalf("error in series iterator: %v", err)
	}

}
