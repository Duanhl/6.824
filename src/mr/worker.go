package mr

import (
	"fmt"
	"os"
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
					Dprintf("worker get map task from master: %s", requireReply.Filename)
					kvs := mapf(requireReply.Filename, requireReply.Content)

					commitArgs := &CommitTaskArgs{
						Type:  Mapper,
						Index: requireReply.Index,
						KVS:   kvs,
					}
					commitReply := &CommitTaskReply{}
					if !call("Coordinator.CommitTask", &commitArgs, &commitReply) {
						stop = true
					}
					break
				case Reducer:
					Dprintf("worker get reduce task from master: %v", requireReply.Index)
					oname := "mr-out-" + strconv.Itoa(requireReply.Index)
					ofile, _ := os.Create(oname)

					for _, kvl := range requireReply.KVL {
						output := reducef(kvl.Key, kvl.Values)
						fmt.Fprintf(ofile, "%v %v\n", kvl.Key, output)
					}

					ofile.Close()

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
