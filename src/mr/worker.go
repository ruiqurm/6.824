package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// var map_function, reduce_function plugin.Symbol

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// load wc.so plugin
	// plug, err := plugin.Open("/home/ruiqurm/lab/6.824/src/mrapps/wc.so")
	// if err != nil {
	// 	panic(err)
	// }
	// map_function, err = plug.Lookup("Map")
	// if err != nil {
	// 	panic(err)
	// }
	// reduce_function, err = plug.Lookup("Reduce")
	// if err != nil {
	// 	panic(err)
	// }
	// 首先向master注册
	// id := Register()

	stop := false
	for !stop {
		if !CallForWork(mapf, reducef) {
			break
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
func CallForWork(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	args := GetWorkArgs{}
	reply := GetWorkReply{}
	ok := call("Coordinator.GetWork", &args, &reply)
	if !ok {
		fmt.Println("can not connect with server;close worker")
		os.Exit(0)
	}
	switch reply.Type {
	case 1:
		{
			// read file from server
			filename := reply.Filename
			file, err := os.Open(filename)
			check_or_panic(err)
			content, err := io.ReadAll(file)
			check_or_panic(err)
			// save middle file as json list
			var encoders []*json.Encoder
			for i := 0; i < reply.NReduce; i++ {
				// create temp file to avoid conflict
				file, err := os.CreateTemp("", "tmpfile")
				check_or_panic(err)
				encoders = append(encoders, json.NewEncoder(file))
				// save file in format: mg-job-reduce.txt
				defer os.Rename(file.Name(), fmt.Sprintf("mg-%v-%v.txt", reply.Id, i))
				defer file.Close()
			}
			// map_function.(func(string, string) []KeyValue)(filename, string(content))
			for _, obj := range mapf(filename, string(content)) {
				key := obj.Key
				encoders[ihash(key)%reply.NReduce].Encode(&obj)
			}

		}
	case 2:
		{
			dict := make(map[string][]string)
			for i := 0; i < reply.NReduce; i++ {
				file, err := os.Open(fmt.Sprintf("mg-%d-%d.txt", i, reply.Id))
				check_or_panic(err)
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
				defer os.Remove(file.Name())
			}
			var result []KeyValue
			for key, values := range dict {
				var kv KeyValue
				kv.Key = key
				kv.Value = reducef(key, values)
				result = append(result, kv)
			}
			sort.Slice(result, func(i, j int) bool {
				return result[i].Value > result[j].Value
			})
			file, _ := os.Create(fmt.Sprintf("mr-out-%d.txt", reply.Id))
			defer file.Close()
			for _, kv := range result {
				fmt.Fprint(file, kv.Key+" "+kv.Value+"\n")
			}
		}
	default:
		{
			return false
		}
	}
	return !ReplyWork(reply.Id, reply.Token)
}

func ReplyWork(id int, token string) bool {
	args := DoneWorkArgs{}
	reply := DoneWorkReply{}
	args.ID = id
	args.Token = token
	ok := call("Coordinator.DoneWork", &args, &reply)
	if !ok {
		fmt.Println("can not connect with server;close worker")
		os.Exit(0)
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

func check_or_panic(err error) {
	if err != nil {
		panic(err)
	}
}
