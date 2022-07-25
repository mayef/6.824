package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task := AskTask()
		if task.Type == 0 {
			time.Sleep(time.Second * 3)
		}

		// failedCount := 0``
		// ask coordinator for a task
		// for failedCount < 3 {
		// task := AskTask()
		// if task.Type == 0 {
		// 	failedCount++
		// 	continue
		// }

		if task.Type == MAP {
			mapFn(&task, mapf, reducef)
		}

		if task.Type == REDUCE {
			reduceFn(&task, mapf, reducef)
		}
	}
}

func mapFn(task *AskTaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	namePrefix := "mr-" + fmt.Sprint(task.Index) + "-"
	filename := task.FileLocation
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	rel, err := filepath.Rel("/home/zzz/mit/6.824/src/main/mr-tmp", filename)
	if err != nil {
		fmt.Println(err.Error())
		log.Fatalf("cannot transfer %v", filename)
	}
	kva := mapf(rel, string(content))

	sort.Sort(ByKey(kva))

	// combine
	/* i := 0
	combinedKva := make(map[string]string)
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
		combinedKva[kva[i].Key] = output

		i = j
	}

	// write to nReduce files
	encMap := make(map[int]*json.Encoder)
	for k := range combinedKva {
		rIndex := ihash(k) % task.NReduce
		filename := namePrefix + fmt.Sprint(rIndex)
		file, err := os.OpenFile("/home/zzz/mit/6.824/src/mr/"+filename, os.O_CREATE|os.O_RDWR, 0755)
		if err != nil {
			fmt.Println(err.Error())
			break
		}
		encMap[rIndex] = json.NewEncoder(file)
		defer file.Close()
	}
	for k, v := range combinedKva {
		rIndex := ihash(k) % task.NReduce
		err := encMap[rIndex].Encode(KeyValue{Key: k, Value: v})
		if err != nil {
			fmt.Println(err.Error())
			break
		}
	}*/

	encMap := make(map[int]*json.Encoder)
	fileLocationsMap := make(map[int]string)
	for _, kv := range kva {
		rIndex := ihash(kv.Key) % task.NReduce
		filename := "/home/zzz/mit/6.824/src/mr/intermediate/" + namePrefix + fmt.Sprint(rIndex)
		_, err := os.Stat(filename)
		if errors.Is(err, os.ErrNotExist) {
			file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0755)
			if err != nil {
				fmt.Println(err.Error())
				break
			}
			encMap[rIndex] = json.NewEncoder(file)
			fileLocationsMap[rIndex] = filename
			defer file.Close()
		}
	}
	for _, kv := range kva {
		rIndex := ihash(kv.Key) % task.NReduce
		err := encMap[rIndex].Encode(kv)
		if err != nil {
			fmt.Println(err.Error())
			break
		}
	}

	// fileLocations := []string{}
	// for i := 0; i < len(fileLocationsMap); i++ {
	// 	fileLocations = append(fileLocations, fileLocationsMap[i])
	// }
	ReplyTask(task.Type, task.Index, &fileLocationsMap)
}

func reduceFn(task *AskTaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	locations := task.FileLocations
	kva := []KeyValue{}
	for _, v := range locations {
		filename := v
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := "mr-out-" + fmt.Sprint(task.Index)
	ofile, _ := os.Create(oname)
	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-0.
	//
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	ReplyTask(task.Type, task.Index, nil)
}

func getWorkerId() WorkerId {
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
	return WorkerId(fmt.Sprintf("%s%d", machineId, os.Getpid()))
}

func AskTask() AskTaskReply {
	args := AskTaskArgs{}

	args.WorkerId = getWorkerId()

	reply := AskTaskReply{}

	ok := call("Coordinator.AskTask", args, &reply)
	if ok {
		// fmt.Printf("reply.Filename %v\n", reply.FileLocation)
	} else {
		fmt.Printf("call failed!\n")
		panic("cannot connect to coordinator")
	}
	return reply
}

func ReplyTask(taskType TaskType, index int, fileLocations *map[int]string) ReplyTaskReply {
	args := ReplyTaskArgs{WorkerId: getWorkerId(), Type: taskType}
	args.Index = index
	if taskType == MAP {
		args.FileLocations = *fileLocations
	}

	reply := ReplyTaskReply{}

	ok := call("Coordinator.ReplyTask", args, &reply)
	if ok {
		// fmt.Printf("reply.Status %v, %s\n", reply.Success, reply.Error)
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
	// reply.Y = 1919810 // cannot set a non-default value, cause when the server sets
	// the value to the default value, the client will not get the value and keep the non-default value that is setted before calling.

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", args, &reply)
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
