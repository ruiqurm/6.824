package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	Files       []string
	RemainJobs  chan int
	RunningTask map[string]int
	nReduce     int
	nJob        int
	finished    int
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
			select {
			case job := <-c.RemainJobs:
				{
					// 有job可以运行
					reply.Id = job
					reply.Type = 1
					reply.NReduce = c.nReduce
					reply.Filename = c.Files[job]
				}
			default:
				{
					reply.Type = 0
				}

			}
		}
	case 2:
		{
			// reduce阶段
			select {
			case job := <-c.RemainJobs:
				{
					reply.Type = 2
					reply.NReduce = c.nJob
					reply.Id = job
				}
			default:
				{
					reply.Type = 0
				}
			}
		}

	}
	return nil
}
func (c *Coordinator) DoneWork(args *DoneWorkArgs, reply *DoneWorkReply) error {
	switch c.status {
	case 0:
		{
			reply.Done = true
		}
	case 1:
		{
			reply.Done = false
			c.finished += 1 // add lock
			if c.finished == c.nJob {
				fmt.Println("map done")
				for i := 0; i < c.nReduce; i++ {
					c.RemainJobs <- i
				}
				c.finished = 0
				c.status = 2
			}
		}
	case 2:
		{
			reply.Done = false
			c.finished += 1
			if c.finished == c.nReduce {
				c.status = 0

			}
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

	// Your code here.
	return c.status == 0

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.RemainJobs = make(chan int, max(nReduce, len(files)))
	for i := range files {
		c.RemainJobs <- i
	}
	c.id_counter = 0
	c.finished = 0
	c.nReduce = nReduce
	c.nJob = len(files)
	c.status = 1
	c.server()
	return &c
}
func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
