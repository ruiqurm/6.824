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
	Id_counter  int
}

// Your code here -- RPC handlers for the worker to call.
type TaskReply struct {
	TaskType string // map or reduce
	FileName string
	ID       int
}
type EmptyRequest struct{}

func (c *Coordinator) GetTask(args *EmptyRequest, reply *TaskReply) error {
	if len(c.RemainList) == 0 {
		reply.TaskType = "done"
	} else {
		reply.TaskType = "map"
		reply.FileName = c.RemainList[0]
		c.RemainList = c.RemainList[1:]
		c.TaskMapping[c.Id_counter] = reply.FileName
		reply.ID = c.Id_counter
		c.Id_counter++
	}
	return nil
}

//
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
	c.Id_counter = 0
	c.FileStatus = make(map[int]int)
	c.TaskMapping = make(map[int]string)
	c.server()
	return &c
}
