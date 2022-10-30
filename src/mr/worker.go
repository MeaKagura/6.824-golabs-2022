package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	working := true
	for working {
		reply := CallGetTask()
		switch reply.Task.Type {
		case MapTask:
			//fmt.Printf("executing map task %v... \n", reply.Task.Id)
			executeMapTask(mapf, &reply)
			//fmt.Printf("map task %v is completed. \n", reply.Task.Id)
			CallCompleteTask(&TaskArgs{Task: reply.Task})
		case ReduceTask:
			//fmt.Printf("executing reduce task %v... \n", reply.Task.Id)
			executeReduceTask(reducef, &reply)
			//fmt.Printf("reduce task %v is completed. \n", reply.Task.Id)
			CallCompleteTask(&TaskArgs{Task: reply.Task})
		case WaitingTask:
			//fmt.Printf("waiting for a task... \n")
			time.Sleep(time.Second)
			continue
		default:
			//fmt.Printf("there is no task to get, this worker will exit. \n")
			working = false
		}
	}
}

// CallGetTask make an RPC call to the coordinator to request a map task.
func CallGetTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		//fmt.Printf("CallGetTask succeeded.\n")
	} else {
		//fmt.Printf("CallGetTask failed!\n")
	}
	return reply
}

// CallCompleteTask make an RPC call to the coordinator to complete a map task.
func CallCompleteTask(args *TaskArgs) {
	reply := TaskReply{}
	ok := call("Coordinator.AcceptTask", &args, &reply)
	if ok {
		//fmt.Printf("CallCompleteTask succeeded.\n")
	} else {
		//fmt.Printf("CallCompleteTask failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func executeMapTask(mapf func(string, string) []KeyValue, reply *TaskReply) {
	// open input file
	inputFilename := reply.Task.InputFilenames[0]
	file, err := os.Open(inputFilename)
	if err != nil {
		log.Fatalf("cannot open %v", inputFilename)
	}
	// read input file
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFilename)
	}
	file.Close()
	// group intermediate list of key/value pairs by reduceNumber.
	kva := mapf(inputFilename, string(content))
	sort.Sort(ByKey(kva))
	intermediate := make([][]KeyValue, reply.ReduceNum)
	for i, j := 0, 0; i < len(kva); i = j {
		reduceNumber := ihash(kva[i].Key) % reply.ReduceNum
		kv := KeyValue{Key: kva[i].Key, Value: kva[i].Value}
		for j < len(kva) && kva[j].Key == kva[i].Key {
			intermediate[reduceNumber] = append(intermediate[reduceNumber], kv)
			j++
		}
	}
	// store key/value pairs with same reduceNumber to the same intermediate file.
	for i := 0; i < reply.ReduceNum; i++ {
		intermediateFilename := "mr-tmp-" +
			strconv.Itoa(reply.Task.Id) +
			"-" +
			strconv.Itoa(i)
		intermediateFile, err := os.Create(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot create %v", intermediateFilename)
		}
		enc := json.NewEncoder(intermediateFile)
		for _, kv := range intermediate[i] {
			enc.Encode(kv)
		}
		intermediateFile.Close()
	}
}

func executeReduceTask(reducef func(string, []string) string, reply *TaskReply) {
	// open intermediate files
	intermediateFilenames := reply.Task.InputFilenames

	// collect all key/value paris of files with the same reduce number.
	var kva []KeyValue
	for i := 0; i < len(intermediateFilenames); i++ {
		intermediateFile, err := os.Open(intermediateFilenames[i])
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFilenames[i])
		}
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		intermediateFile.Close()
	}
	// sort key/value paris to realize classification
	sort.Sort(ByKey(kva))
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
	for i, j := 0, 0; i < len(kva); i = j {
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
	}
	os.Rename(tempFile.Name(), "mr-out-"+strconv.Itoa(reply.Task.Id))
}
