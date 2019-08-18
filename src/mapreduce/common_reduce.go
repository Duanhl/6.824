package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	kvMap := make(map[string][]string)
	for mapTask := 0; mapTask < nMap; mapTask++ {
		file, err := os.Open(reduceName(jobName, mapTask, reduceTask))
		if err != nil {
			log.Fatal("reduce open intermediate file error:", err)
		}
		defer file.Close()

		dec := *json.NewDecoder(file)
		for {
			kv := KeyValue{"", ""}
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	ofile, err := os.Create(outFile)
	if err != nil {
		log.Fatal("reduce open out file error:", err)
	}
	defer ofile.Close()
	enc := json.NewEncoder(ofile)

	// partI需要按键的数值进行排序
	//var keys []int
	//for k, _ := range kvMap {
	//	aint, _ := strconv.Atoi(k)
	//	keys = append(keys, aint)
	//}
	//sort.Ints(keys)

	var keys []string
	for k, _ := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		//kstr := strconv.Itoa(k)
		kstr := k
		enc.Encode(&KeyValue{kstr, reduceF(kstr, kvMap[kstr])})
	}
}
