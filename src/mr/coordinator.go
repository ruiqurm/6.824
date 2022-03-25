package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	Job       int
	StartTime int64
}
type Coordinator struct {
	// Your definitions here.
	Files        []string // imutable
	nReduce      int32    // imutable
	nJob         int32    // imutable
	RemainJobs   chan int
	RunningTask  sync.Map //
	JobCond      *sync.Cond
	JobCondMutex *sync.Mutex
	finishedLock *sync.Mutex
	finished     int32 // count for finished jobs
	status       atomic.Value
}

// Your code here -- RPC handlers for the worker to call.

type EmptyRequest struct{}

// func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
// 	// reply.ID = int(c.id_counter)
// 	// atomic.AddInt32(&c.id_counter, 1)
// 	reply.ID = 0
// 	return nil
// }

func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	for {
		status := c.status.Load()
		switch status {
		case 1:
			{
				// map阶段
				select {
				case job := <-c.RemainJobs:
					{
						// 有job可以运行
						reply.Id = job
						reply.Type = 1
						reply.NReduce = int(c.nReduce)
						reply.Filename = c.Files[job]
						now := time.Now().UnixMilli()
						reply.Token = fmt.Sprintf("%v-%v", now, job)
						c.RunningTask.Store(reply.Token, Task{job, now})
						return nil
					}
				default:
					{
						fmt.Println("wait")
						c.JobCondMutex.Lock()
						c.JobCond.Wait()
						c.JobCondMutex.Unlock()
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
						reply.NReduce = int(c.nJob)
						reply.Id = job
						now := time.Now().UnixMilli()
						reply.Token = fmt.Sprintf("%v-%v", now, job)
						c.RunningTask.Store(reply.Token, Task{job, now})
						return nil
					}
				default:
					{
						c.JobCondMutex.Lock()
						c.JobCond.Wait()
						c.JobCondMutex.Unlock()
					}
				}
			}
		default:
			{
				reply.Type = 0
				reply.Token = ""
				return nil
			}

		}
	}
}
func (c *Coordinator) DoneWork(args *DoneWorkArgs, reply *DoneWorkReply) error {
	_, found := c.RunningTask.Load(args.Token)
	if !found {
		fmt.Println("token not found", args.ID, args.Token)
		reply.Done = false
		return nil
	}
	status := c.status.Load()
	switch status {
	case 0:
		{
			reply.Done = true
		}
	case 1:
		{
			reply.Done = false
			c.RunningTask.Delete(args.Token)
			fmt.Println("Done map", args.ID)
			c.finishedLock.Lock()
			c.finished += 1
			defer c.finishedLock.Unlock()
			if c.finished == c.nJob {
				fmt.Println("map done")
				c.status.Store(3)
				for i := 0; i < int(c.nReduce); i++ {
					c.RemainJobs <- i
				}
				c.JobCond.Broadcast()
				c.finished = 0
				c.status.Store(2)
			}
		}
	case 2:
		{
			reply.Done = false
			c.RunningTask.Delete(args.Token)
			fmt.Println("Done reduce", args.ID)
			c.finishedLock.Lock()
			c.finished += 1
			defer c.finishedLock.Unlock()
			if c.finished == c.nReduce {
				fmt.Println("reduce done")
				c.JobCond.Broadcast()
				c.status.Store(0)
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
	// check and kill zombie jobs
	now := time.Now().UnixMilli()
	c.RunningTask.Range(func(key, value interface{}) bool {
		timestamp := value.(Task).StartTime
		if now-timestamp > 10000 {
			fmt.Println("kill zombie job", value.(Task).Job)
			c.RunningTask.Delete(key)
			c.RemainJobs <- value.(Task).Job
			c.JobCond.Broadcast()
		}
		return true
	})
	status := c.status.Load()
	return status == 0

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// tmp, err := os.MkdirTemp("", "example")
	// if err != nil {
	// 	panic(err)
	// }
	// defer os.RemoveAll(tmp)
	c.Files = files
	c.RemainJobs = make(chan int, max(nReduce, len(files)))
	for i := range files {
		c.RemainJobs <- i
	}

	c.JobCondMutex = &sync.Mutex{}
	c.JobCond = sync.NewCond(c.JobCondMutex)
	c.finishedLock = &sync.Mutex{}
	c.finished = 0
	c.nReduce = int32(nReduce)
	c.nJob = int32(len(files))
	c.status.Store(1)
	c.server()
	return &c
}
func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
