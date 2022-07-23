package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {


	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	oname := "mr-"

	failedCount := 0
	// ask coordinator for a task
	for failedCount < 3 {
		task := AskTask()
		if (task.Type == 0) {
			failedCount++
			continue
		}

		if (task.Type == 1) {
			filename := "../main" + task.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			// combine
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				os.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
		}
	}

}

func getWorkerId() string {
	// read machine id
	filename := "/etc/machine-id"
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	machineIdBytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	machineId := string(machineIdBytes)
	
	// worker id = machine_id + process_id
	return fmt.Sprintf("%s%d", machineId, os.Getpid())
}

func AskTask() AskTaskReply {
	args := AskTaskArgs{}

	args.WorkerId = getWorkerId()

	reply := AskTaskReply{}

	ok := call("Coordinator.Task", args, &reply)
    if ok {
        fmt.Printf("reply.Filename %v\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
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
	args.X = 1

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
