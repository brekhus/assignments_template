package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	inF, err := os.Open(inFile)
	checkError(err)
	b, err := ioutil.ReadAll(inF)
	checkError(err)
	var writers []*os.File
	for i := 0; i < nReduce; i++ {
		outF, err := os.Create(reduceName(jobName, mapTaskNumber, i))
		fmt.Printf("create: %s\n", reduceName(jobName, mapTaskNumber, i))
		checkError(err)
		writers = append(writers, outF)
	}
	defer func() {
		for _, writer := range writers {
			writer.Close()
		}
	}()
	var encoders []*json.Encoder
	for _, writer := range writers {
		encoder := json.NewEncoder(writer)
		encoders = append(encoders, encoder)
	}

	for _, kv := range mapF(inFile, string(b)) {
		slot := ihash(kv.Key) % uint32(nReduce)
		encoder := encoders[slot]
		encoder.Encode(kv)
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
