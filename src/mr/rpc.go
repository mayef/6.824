package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// ask task
type AskTaskArgs struct {
	WorkerId WorkerId
}

type AskTaskReply struct {
	Type          TaskType // 任务类型, 1: map, 2: reduce
	Index         int      // map tasks index, for naming intermediate files
	FileLocation  string   // map字段
	NReduce       int      // map field
	FileLocations []string // reduce字段
}

type ReplyTaskArgs struct {
	WorkerId      WorkerId
	Type          TaskType
	Index         int
	FileLocations map[int]string
}

type ReplyTaskReply struct {
	Success bool
	Error   string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
