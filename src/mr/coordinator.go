package mr

import (
	"io/ioutil"
	"log"
	"sort"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	maps    []MapProgress
	reduces []ReduceProgress

	state uint32
}

const ProgressTimeOut = 10

const (
	MapPhase uint32 = iota
	ReducePhase
	Stop
)

type ProgressState uint8

const (
	Waiting ProgressState = iota
	Processing
	Done
)

type ReduceProgress struct {
	state     ProgressState
	startTime time.Time
	kv        []KVList
}

type MapProgress struct {
	state     ProgressState
	startTime time.Time
	file      string
	content   string
	kv        []KeyValue
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequireTask(args *RequireTaskArgs, reply *RequireTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case MapPhase:
		if index := c.findAvailableMapProgress(); index != -1 {
			c.maps[index].state = Processing
			c.maps[index].startTime = time.Now()

			reply.Type = Mapper
			reply.Filename = c.maps[index].file
			reply.Content = c.maps[index].content
			reply.Index = index
		} else {
			reply.Type = None
		}
		break
	case ReducePhase:
		if index := c.findAvaiableReduceProgress(); index != -1 {
			c.reduces[index].state = Processing
			c.reduces[index].startTime = time.Now()

			reply.Type = Reducer
			reply.KVL = c.reduces[index].kv
			reply.Index = index
		} else {
			reply.Type = None
		}
		break
	case Stop:
		reply.Type = None
		return MasterStopError
	}
	return nil
}

func (c *Coordinator) findAvailableMapProgress() int {
	for i := 0; i < len(c.maps); i++ {
		p := c.maps[i]
		if p.state == Waiting || (p.state == Processing && time.Since(p.startTime).Seconds() > ProgressTimeOut) {
			return i
		}
	}
	return -1
}

func (c *Coordinator) findAvaiableReduceProgress() int {
	for i := 0; i < len(c.reduces); i++ {
		p := c.reduces[i]
		if p.state == Waiting || (p.state == Processing && time.Since(p.startTime).Seconds() > ProgressTimeOut) {
			return i
		}
	}
	return -1
}

func (c *Coordinator) CommitTask(args *CommitTaskArgs, reply *CommitTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case Mapper:
		Dprintf("submit map task: %v", c.maps[args.Index].file)
		if c.state == MapPhase && c.maps[args.Index].state == Processing {
			c.maps[args.Index].state = Done
			c.maps[args.Index].kv = args.KVS

			if c.checkMapProgress() {
				c.becomeReducePhase()
			}
		}
		reply.Ok = true
		break
	case Reducer:
		Dprintf("submit reduce task: %v", args.Index)
		if c.state == ReducePhase && c.reduces[args.Index].state == Processing {
			c.reduces[args.Index].state = Done

			if c.checkReduceProgress() {
				c.doneJob()
			}
		}
		reply.Ok = true
		break
	}

	if c.state == Stop {
		return MasterStopError
	}
	return nil
}

func (c *Coordinator) checkMapProgress() bool {
	for _, p := range c.maps {
		if p.state != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) becomeReducePhase() {

	intermediateKV := []KeyValue{}
	for _, p := range c.maps {
		intermediateKV = append(intermediateKV, p.kv...)
	}
	sort.Sort(ByKey(intermediateKV))

	i := 0
	for i < len(intermediateKV) {
		j := i + 1
		for j < len(intermediateKV) && intermediateKV[i].Key == intermediateKV[j].Key {
			j++
		}
		KVL := KVList{
			Key:    intermediateKV[i].Key,
			Values: []string{},
		}
		for k := i; k < j; k++ {
			KVL.Values = append(KVL.Values, intermediateKV[k].Value)
		}
		n := ihash(KVL.Key) % len(c.reduces)
		c.reduces[n].kv = append(c.reduces[n].kv, KVL)
		i = j
	}
	c.state = ReducePhase
	Dprintf("master change to reduce phase...")
}

func (c *Coordinator) checkReduceProgress() bool {
	for _, p := range c.reduces {
		if p.state != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) doneJob() {
	c.state = Stop
	Dprintf("master stop...")
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	ret = c.state == Stop
	c.mu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.maps = make([]MapProgress, len(files))
	for i := range c.maps {
		f, err := os.Open(files[i])
		if err != nil {
			log.Fatalf("cannot open %v", files[i])
		}
		content, err := ioutil.ReadAll(f)
		if err != nil {
			log.Fatalf("cannot read %v", files[i])
		}
		f.Close()

		c.maps[i] = MapProgress{
			state:     Waiting,
			startTime: time.Now(),
			file:      files[i],
			content:   string(content),
			kv:        []KeyValue{},
		}
	}

	c.reduces = make([]ReduceProgress, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduces[i] = ReduceProgress{
			state:     Waiting,
			startTime: time.Now(),
			kv:        []KVList{},
		}
	}
	c.state = MapPhase
	Dprintf("master start server... nreduces: %v", nReduce)

	c.server()
	return &c
}
