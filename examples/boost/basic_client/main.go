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
	"time"

	boostclient "github.com/m3db/m3/src/boost/client"
	"github.com/m3db/m3/src/dbnode/client"
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

	fmt.Printf("gomaxprocs %d\n", runtime.GOMAXPROCS(0))

	opt := client.Options()

	opt = opt.SetWriteBatchSize(256).
		SetHostQueueOpsFlushSize(256).
		SetAsyncWriteMaxConcurrency(8096).
		//SetWriteOpPoolSize(8096).
		SetHostQueueOpsArrayPoolSize(128).
		//SetUseV2BatchAPIs(true).
		SetHostQueueOpsFlushInterval(100 * time.Microsecond)
	chOpt := opt.ChannelOptions()
	chOpt.DefaultConnectionOptions.SendBufferSize = 4 * 1024 * 1024
	chOpt.DefaultConnectionOptions.RecvBufferSize = 4 * 1024 * 1024

	log.Printf("AsyncWriteMaxCuncurrency: %d", opt.AsyncWriteMaxConcurrency())
	log.Printf("WriteOpPoolSize: %d", opt.WriteOpPoolSize())
	log.Printf("HostQueueOpsArrayPoolSize: %d", opt.HostQueueOpsArrayPoolSize())
	log.Printf("WriteBatchSize: %d", opt.WriteBatchSize())
	log.Printf("HostQueueOpsFlushSize: %d", opt.HostQueueOpsFlushSize())
	log.Printf("HostQueueOpsFlushInterval: %s", opt.HostQueueOpsFlushInterval().String())
	log.Printf("SendBufferSize: %d", opt.ChannelOptions().DefaultConnectionOptions.SendBufferSize)
	log.Printf("RecvBufferSize: %d", opt.ChannelOptions().DefaultConnectionOptions.RecvBufferSize)

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

	// Write and read large table data
	writeAndReadLargeTableData(session, 100000)
}

func writeAndReadLargeTableData(session client.Session, count int) {

	// Create session with large number of attribute values
	var maxConcurrentWrites uint32 = 512

	boostSession := boostclient.NewBoostSession(
		session,
		1000,
		maxConcurrentWrites)

	// Create new M3DBTapTable
	sf := boostclient.NewM3DBSeriesFamily(
		"myAppSF",
		namespaceID,
		1,
		boostSession,
		64,
		100000000,
		maxConcurrentWrites)

	var (
		seriesID = ident.StringID("__name__=\"cpu_user_util\",region=\"us-east-1\",service=\"myservice1\"")
		tags     = []ident.Tag{
			{Name: ident.StringID("region"), Value: ident.StringID("us-east-1")},
			{Name: ident.StringID("service"), Value: ident.StringID("myservice1")},
		}
		tagsIter = ident.NewTagsIterator(ident.NewTags(tags...))
	)

	// Write with fresh dictionary
	writeSF(sf, seriesID, tagsIter, count)

	// Write with reuse of the dictionary
	log.Printf("Write with reuse of the dictionary")
	start, end := writeSF(sf, seriesID, tagsIter, count)
	time.Sleep(5000 * time.Millisecond)

	// Now read back and check the values are in order.
	readSF(sf, seriesID, start, end)
}

func writeSF(sf *boostclient.M3DBSeriesFamily,
	seriesID ident.ID,
	tagsIter ident.TagIterator,
	count int) (xtime.UnixNano, xtime.UnixNano) {
	defer timer("write-large-series-m3db-table")()
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
		err := sf.WriteTagged(
			seriesID,
			tagsIter,
			attrIter,
			timestamp,
			writtenValue,
			xtime.Nanosecond,
			nil)
		if err != nil {
			log.Fatalf("error writing series %s, err: %v", seriesID.String(), err)
		}
		writtenValue++
	}
	errWait := sf.Wait(5 * time.Second)
	if errWait != nil {
		log.Fatalf("error waiting for pending writes to complete: %v", errWait)
	}
	end := xtime.Now()

	return start, end
}

func readSF(sf *boostclient.M3DBSeriesFamily,
	seriesID ident.ID,
	start xtime.UnixNano,
	end xtime.UnixNano) {
	defer timer("read-large-series-with-attributes")()
	log.Printf("------ read large data (with attributes) to db ------")
	expVal := 1.0
	// Now lets read the series back out
	seriesIter, err := sf.Fetch(
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

func timer(name string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", name, time.Since(start))
	}
}
