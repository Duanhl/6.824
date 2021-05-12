package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (bk ByKey) Less(i, j int) bool { return bk[i].Key < bk[j].Key }

func (bk ByKey) Swap(i, j int) { bk[i], bk[j] = bk[j], bk[i] }

func (bk ByKey) Len() int { return len(bk) }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		stop := false
		for !stop {
			requireArgs := &RequireTaskArgs{}
			requireReply := &RequireTaskReply{}

			if call("Coordinator.RequireTask", &requireArgs, &requireReply) {
				switch requireReply.Type {
				case Mapper:
					Dprintf("worker get map task from master: %s", requireReply.Files[0])

					outf := doMapf(requireReply.Files[0], requireReply.NReduce, mapf)

					commitArgs := &CommitTaskArgs{
						Type:  Mapper,
						Index: requireReply.Index,
						Files: outf,
					}
					commitReply := &CommitTaskReply{}
					if !call("Coordinator.CommitTask", &commitArgs, &commitReply) {
						stop = true
					}
					break
				case Reducer:
					Dprintf("worker get reduce task from master: %v", requireReply.Index)

					doReducef(requireReply.Files, requireReply.Index, reducef)

					commitArgs := &CommitTaskArgs{
						Type:  Reducer,
						Index: requireReply.Index,
					}
					commitReply := &CommitTaskReply{}
					if !call("Coordinator.CommitTask", &commitArgs, &commitReply) {
						stop = true
					}
					break
				case None:
					time.Sleep(50 * time.Millisecond)
					break
				}
			} else {
				stop = true
			}
		}
		wg.Done()
	}()

	wg.Wait()
}

func doMapf(f string, nReduce int, mapf func(string, string) []KeyValue) []string {

	// read from input file
	file, err := os.Open(f)
	if err != nil {
		log.Fatalf("open file %s failed, error: %v", file, err)
	}
	c, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("read %s failed, error: %v", file, err)
	}

	// do map function
	kvs := mapf(f, string(c))

	// create map output temp file
	var tmpfs []*os.File
	var encs []*json.Encoder
	for i := 0; i < nReduce; i++ {
		tmpf, err := ioutil.TempFile("/home/work/logs/mr", "mr-map-tmp-")
		if err != nil {
			log.Fatalf("create tmpFile failed")
		}
		tmpfs = append(tmpfs, tmpf)
		encs = append(encs, json.NewEncoder(tmpf))
	}

	// write result to temp file
	sort.Sort(ByKey(kvs))
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[i].Key == kvs[j].Key {
			j++
		}

		n := ihash(kvs[i].Key) % nReduce
		for k := i; k < j; k++ {
			err := encs[n].Encode(&kvs[k])
			if err != nil {
				log.Fatal("encode kv failed")
			}
		}
		i = j
	}

	// close temp file
	var rst []string
	for _, tmpf := range tmpfs {
		rst = append(rst, tmpf.Name())
		tmpf.Close()
	}
	return rst
}

func doReducef(files []string, taskIndex int, reducef func(string, []string) string) {
	output, err := ioutil.TempFile("/home/work/logs/mr", "mr-reduce-tmp-")
	if err != nil {
		log.Fatal("create reduce temp file failed")
	}

	var intermediate []KeyValue
	for _, fn := range files {
		f, err := os.Open(fn)
		if err != nil {
			log.Fatalf("open map temp file failed: %v", fn)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}

	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		s := reducef(intermediate[i].Key, values)
		fmt.Fprintf(output, "%v %v\n", intermediate[i].Key, s)

		i = j
	}

	output.Close()
	os.Rename(output.Name(), "mr-out-"+strconv.Itoa(taskIndex))
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
