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
	"sort"
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

var map_function, reduce_function plugin.Symbol

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	plug, err := plugin.Open("wc.so")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	map_function, err = plug.Lookup("Map")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	reduce_function, err = plug.Lookup("Reduce")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	// 首先向master注册
	id := Register()

	stop := false
	for !stop {
		jid := CallForWork(id)
		if jid >= 0 {
			stop = ReplyWork(jid)
		} else if jid == -1 {
			stop = true
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
		return -2
	}
	if reply.Type == 1 {
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
		// var files []*os.File
		for i := 0; i < reply.NReduce; i++ {
			file, err := ioutil.TempFile("tmp", "tmpfile")
			if err != nil {
				panic(err)
			}
			encoders = append(encoders, json.NewEncoder(file))
			defer os.Rename(file.Name(), "tmp/"+fmt.Sprintf("mg-%v-%v.txt", id, i))
			defer file.Close()
		}
		// save file in format: mg-job-reduce.txt
		for _, obj := range map_function.(func(string, string) []KeyValue)(filename, string(content)) {
			key := obj.Key
			encoders[ihash(key)%reply.NReduce].Encode(&obj)
		}
		return reply.Id
	} else if reply.Type == 2 {
		dict := make(map[string][]string)
		for i := 0; i < reply.NReduce; i++ {
			file, _ := os.Open(fmt.Sprintf("tmp/mg-%d-%d.txt", i, reply.Id))
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				} else {
					dict[kv.Key] = append(dict[kv.Key], kv.Value)
				}
			}
			file.Close()
		}
		var result []KeyValue
		for key, values := range dict {
			var kv KeyValue
			kv.Key = key
			kv.Value = reduce_function.(func(string, []string) string)(key, values)
			result = append(result, kv)
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].Value > result[j].Value
		})
		file, _ := os.Create(fmt.Sprintf("mr-out-%d.txt", reply.Id))
		defer file.Close()
		for _, kv := range result {
			fmt.Fprint(file, kv.Key+"\t"+kv.Value+"\n")
		}
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
