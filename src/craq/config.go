package craq

import (
	"errors"
	"labrpc"
	"math/rand"
	"strings"
	"sync"
)

type ZKServer struct {
	id       int64
	cateHead *TreeNode
	lock     sync.Mutex
}

func NewZK() *ZKServer {
	return &ZKServer{
		id: rand.Int63(),
		cateHead: &TreeNode{
			parent: nil,
			child:  make([]*TreeNode, 0),
			status: Status{},
		},
	}
}

func (zk *ZKServer) Get(path string) (*Status, error) {
	if path[0] != '/' {
		return nil, errors.New("path should start with character '/' ")
	}
	levels := strings.Split(path, "/")
	parent := zk.cateHead
	for _, level := range levels {
		for _, child := range parent.child {
			if child.status.path == level {
				parent = child
				break
			}
		}
	}
	return nil, nil
}

func (zk *ZKServer) Put(path string, data []byte) (*Status, error) {
	return nil, nil
}

func (zk *ZKServer) Delete(path string) (*Status, error) {
	return nil, nil
}

func (zk *ZKServer) Child(path string) ([]string, error) {
	return nil, nil
}

func (zk *ZKServer) AddListener(path string, listen func()) {
}

type EventType int8

const (
	CHANGE EventType = 0
)

type Event struct {
}

type Listener struct {
	client *labrpc.ClientEnd
}

type TreeNode struct {
	parent *TreeNode
	child  []*TreeNode
	status Status
}

type Status struct {
	client     string
	path       string
	createTime int64
	updateTime int64
	data       []byte
	version    int64
}
