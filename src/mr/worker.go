package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	task := GetTask()

	for ; task != nil; task = GetTask() {
		switch task.State {
		case MapTask:
			{
				fmt.Println("get map task")
				fmt.Println(task)
				performMap(task, mapf)
				Finished(task)
			}

		case ReduceTask:
			{
				fmt.Println("get reduce task")
				fmt.Println(task)
				performReduce(task, reducef)
				Finished(task)
			}

		case WaitTask:
			{
				fmt.Println("pending, sleep 300ms")
				time.Sleep(300 * time.Millisecond)
			}

		case EndTask:
			{
				os.Exit(0)
			}
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func performMap(task *TaskInfo, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()

	kva := mapf(task.FileName, string(content))

	//Create temporary files and encoders for each file
	tmpFiles := []*os.File{}
	tmpFilenames := []string{}
	encoders := []*json.Encoder{}
	for r := 0; r < task.NReduce; r++ {
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("cannot create tmp file")
		}
		encoder := json.NewEncoder(tmpFile)

		tmpFiles = append(tmpFiles, tmpFile)
		tmpFilenames = append(tmpFilenames, tmpFile.Name())
		encoders = append(encoders, encoder)
	}

	// write output keys to tmp file using the ihash function
	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce
		encoders[r].Encode(&kv)
	}
	for _, f := range tmpFiles {
		f.Close()
	}

	// rename tmp files
	for r := 0; r < task.NReduce; r++ {
		fname := fmt.Sprintf("mr-%d-%d", task.TaskIndex, r)
		os.Rename(tmpFilenames[r], fname)
	}
}

func performReduce(task *TaskInfo, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for m := 1; m <= task.NMap; m++ {
		filename := fmt.Sprintf("mr-%d-%d", m, task.TaskIndex)
		// fmt.Println(filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("cannot open tmpFile")
	}

	for key_begin := 0; key_begin < len(kva); {
		key_end := key_begin + 1
		for key_end < len(kva) && kva[key_end].Key == kva[key_begin].Key {
			key_end++
		}
		values := []string{}
		for k := key_begin; k < key_end; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[key_begin].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", kva[key_begin].Key, output)
		key_begin = key_end
	}

	outFile := fmt.Sprintf("mr-out-%d", task.TaskIndex)
	os.Rename(tmpFile.Name(), outFile)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func GetTask() *TaskInfo {
	args := ExampleArgs{}
	task := TaskInfo{}
	call("Coordinator.DispatchTask", &args, &task)
	return &task
}

func Finished(task *TaskInfo) {
	reply := ExampleArgs{}
	call("Coordinator.JoinTask", task, &reply)
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
