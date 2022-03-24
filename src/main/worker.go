package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"plugin"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	// 首先向master注册
	id := Register()

	stop := false
	for !stop {
		jid := CallForWork(id)
		if jid != -1 {
			stop = ReplyWork(jid)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func Register() int {
	// register the worker with the coordinator
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.Register", &args, &reply)
	if !ok {
		fmt.Println("register failed!")
		os.Exit(1)
	}
	return reply.ID
}

// 获取一个map或者reduce
func CallForWork(id int) int {
	args := GetWorkArgs{}
	reply := GetWorkReply{}
	args.ID = id
	ok := call("Coordinator.GetWork", &args, &reply)
	if !ok {
		fmt.Println("call failed!")
		os.Exit(1)
	}
	if reply.Type == 0 {
		// go to sleep
		fmt.Println("sleep")
		time.Sleep(time.Duration(1) * time.Second)
		return -1
	}
	if reply.Type == 1 {
		plug, err := plugin.Open("wc.so")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		map_function, err := plug.Lookup("Map")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		filename := reply.Filename
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		var encoders []*json.Encoder
		var files []*os.File
		for i := 0; i < reply.NReduce; i++ {
			file, _ := os.Create(fmt.Sprintf("mr-%d-%d", reply.Id, i))
			encoders = append(encoders, json.NewEncoder(file))
		}
		// save file in format: mg-job-reduce.txt
		for _, obj := range map_function.(func(string, string) []KeyValue)(filename, string(content)) {
			key := obj.Key
			encoders[ihash(key)%reply.NReduce].Encode(&obj)
		}
		for _, file := range files {
			file.Close()
		}
		return reply.Id
	} else if reply.Type == 2 {
		plug, err := plugin.Open("wc.so")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		reduce, err := plug.Lookup("Reduce")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		var dict map[string][]string
		for i := 0; i < reply.NReduce; i++ {
			file, _ := os.Open(fmt.Sprintf("mr-%d-%d", i, reply.Id))
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				} else {
					dict[kv.Key] = make([]string, 0)
					dict[kv.Key] = append(dict[kv.Key], kv.Value)
				}
			}
			file.Close()
		}
		var result []KeyValue
		for key, values := range dict {
			var kv KeyValue
			kv.Key = key
			kv.Value = reduce.(func(string, []string) string)(key, values)
		}
		file, _ := os.Create(fmt.Sprintf("mr-%d.txt", reply.Id))
		encoder := json.NewEncoder(file)
		for kv := range result {
			encoder.Encode(&kv)
		}
		file.Close()
		return reply.Id
	}
	return -1
}

func ReplyWork(filename int) bool {
	args := DoneWorkArgs{}
	reply := DoneWorkReply{}
	args.Id = filename
	ok := call("Coordinator.DoneWork", &args, &reply)
	if !ok {
		fmt.Println("call failed!")
		os.Exit(1)
	}
	return reply.Done
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
