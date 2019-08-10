package mapreduce

import (
	"encoding/json"
	"io"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	inF, err := os.Open(reduceName(jobName, nMap, reduceTaskNumber))
	checkError(err)
	defer inF.Close()

	outF, err := os.Create(mergeName(jobName, reduceTaskNumber))
	checkError(err)
	defer outF.Close()

	decoder := json.NewDecoder(inF)
	keys := make(map[string][]string)
	for {
		var kv KeyValue
		err := decoder.Decode(&kv)
		if err != nil {
			if err != io.EOF {
				checkError(err)
			}
			break
		}
		keys[kv.Key] = append(keys[kv.Key], kv.Value)
	}

	encoder := json.NewEncoder(outF)
	for k, v := range keys {
		value := reduceF(k, v)
		encoder.Encode(KeyValue{Key: k, Value: value})
	}
}
