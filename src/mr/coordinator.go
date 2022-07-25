package mr

import (
	"fmt"
	"math"

	// "github.com/larytet-go/accumulator"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Stage Stage
	cmnMu sync.Mutex

	Tasks        []Task
	Idle         chan Task
	Inprogress   map[string]Task
	inprogressMu sync.Mutex
	// InprogressCount *accumulator.Accumulator

	IntermediateFiles [][]string // [nReduce][files]
	filesMu           sync.Mutex
}

type Task struct {
	// common
	Type TaskType
	// Status   TaskStatus
	WorkerId WorkerId

	// map
	Index        int
	FileLocation string
	NReduce      int
	// reduce
	FileLocations []string
	// map
}

func (c *Coordinator) getStage() Stage {
	var stage Stage
	c.cmnMu.Lock()
	stage = c.Stage
	c.cmnMu.Unlock()
	return stage
}

func (c *Coordinator) setStage(stg Stage) {
	c.cmnMu.Lock()
	c.Stage = stg
	c.cmnMu.Unlock()
}

func (c *Coordinator) IntermediateFilesStore(fileLocations map[int]string) {
	c.filesMu.Lock()
	for k, v := range fileLocations {
		c.IntermediateFiles[k] = append(c.IntermediateFiles[k], v)
	}
	// c.IntermediateFiles[index] = append(c.IntermediateFiles[index], fileLocations...)
	c.filesMu.Unlock()
}

func (c *Coordinator) InprogressLenth() int {
	length := 0
	c.inprogressMu.Lock()
	length = len(c.Inprogress)
	c.inprogressMu.Unlock()
	return length
}

func (c *Coordinator) InprogressLoadAndDelete(key string) (task Task, loaded bool) {
	var t Task
	var l bool
	c.inprogressMu.Lock()
	v, ok := c.Inprogress[key]
	t = v
	l = ok
	if ok {
		delete(c.Inprogress, key)
	}
	c.inprogressMu.Unlock()
	return t, l
}

func (c *Coordinator) InprogressStore(key string, value Task) {
	c.inprogressMu.Lock()
	c.Inprogress[key] = value
	c.inprogressMu.Unlock()
}

// func (c *Coordinator) task

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	workerId := args.WorkerId
	// c.TaskMutex.Lock()
	// for i, v := range c.Tasks {
	// 	if (v.Status == IDLE) {
	// 		reply.Type = MAP
	// 		reply.index = i
	// 		reply.FileLocation = v.FileLocation
	// 		reply.NReduce = v.NReduce

	// 		v.WorkerId = workerId
	// 		v.Status = IN_PROGRESS
	// 		break
	// 	}
	// }
	// c.TaskMutex.Unlock()

	if len(c.Idle) == 0 {
		return nil
	}
	task := <-c.Idle
	task.WorkerId = workerId
	c.InprogressStore(string(workerId), task)

	reply.Index = task.Index
	if task.Type == MAP {
		reply.Type = MAP
		reply.FileLocation = task.FileLocation
		reply.NReduce = task.NReduce
	}
	if task.Type == REDUCE {
		reply.Type = REDUCE
		reply.FileLocations = task.FileLocations
	}

	go func() {
		time.Sleep(time.Second * 15)
		t, loaded := c.InprogressLoadAndDelete(string(workerId))
		if loaded {
			fmt.Println("AskTask: FAILED, " + t.FileLocation + ", " + string(t.WorkerId))
			t.WorkerId = ""
			c.Idle <- t
		}
	}()

	return nil
}

func (c *Coordinator) ReplyTask(args *ReplyTaskArgs, reply *ReplyTaskReply) error {
	workerId := args.WorkerId
	if args.Type == MAP {
		c.IntermediateFilesStore(args.FileLocations)
	}
	_, loaded := c.InprogressLoadAndDelete(string(workerId))

	if loaded {
		reply.Success = true
	} else {
		reply.Success = false
		reply.Error = "mayef"
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	// reply.Y = args.X + 114513
	reply.Y = 0
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
	if len(c.Idle) == 0 && c.InprogressLenth() == 0 && c.getStage() == RDCSTG {
		ret = true
		os.RemoveAll("/home/zzz/mit/6.824/src/mr/intermediate")
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{Stage: MPSTG, Idle: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))), Inprogress: make(map[string]Task), filesMu: sync.Mutex{}, inprogressMu: sync.Mutex{}, cmnMu: sync.Mutex{}, IntermediateFiles: make([][]string, nReduce)}
	os.Mkdir("/home/zzz/mit/6.824/src/mr/intermediate", 0755)

	// Your code here.
	for i, v := range files {
		path, error := filepath.Abs(v)
		if error != nil {
			continue
		}

		task := Task{
			Type:         MAP,
			FileLocation: path,

			NReduce: nReduce,
			Index:   i,
		}
		c.Tasks = append(c.Tasks, task)
		c.Idle <- task
	}

	go func() {
		for {
			if len(c.Idle) == 0 && c.InprogressLenth() == 0 && c.getStage() == Stage(MAP) {
				fmt.Println("mayef map done")
				c.setStage(RDCSTG)
				for i, v := range c.IntermediateFiles {
					task := Task{
						Type:          REDUCE,
						FileLocations: v,
						Index:         i,
					}
					c.Tasks = append(c.Tasks, task)
					c.Idle <- task
				}

				fmt.Println("mayef reduce prepare done")

				break
			}
			time.Sleep(time.Second * 5)
		}
	}()

	c.server()
	return &c
}
