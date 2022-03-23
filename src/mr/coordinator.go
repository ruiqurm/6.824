package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	RemainList  []string
	FileStatus  map[int]int
	TaskMapping map[int]string // ID -> filename
	nReduce     int
	id_counter  int
	status      int
}

// Your code here -- RPC handlers for the worker to call.

type EmptyRequest struct{}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	reply.ID = c.id_counter
	c.id_counter++
	return nil
}

func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	switch c.status {
	case 0:
		{
			reply.Type = 0
		}
	case 1:
		{
			// map阶段
			if len(c.RemainList) != 0 {
				reply.Type = 1
				reply.NReduce = c.nReduce
				reply.Filename = c.RemainList[0]
				c.RemainList = c.RemainList[1:]
				c.TaskMapping[args.ID] = reply.Filename
			} else {
				reply.Type = 0
			}
		}
	case 2:
		{
			// reduce阶段
			reply.Type = 0

		}

	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	c.RemainList = files
	c.id_counter = 0
	c.nReduce = nReduce
	c.FileStatus = make(map[int]int)
	c.TaskMapping = make(map[int]string)
	c.status = 1
	c.server()
	return &c
}
