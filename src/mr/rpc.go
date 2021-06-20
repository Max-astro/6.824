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

// // heap
// type TaskHeap []*TaskInfo

// func (h TaskHeap) Len() int           { return len(h) }
// func (h TaskHeap) Less(i, j int) bool { return h[i].Start.Before(h[j].Start) }
// func (h TaskHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
// func (h *TaskHeap) Pop() interface{} {
// 	n := len(*h)
// 	top := (*h)[n-1]
// 	*h = (*h)[:n-1]
// 	return top
// }
// func (h *TaskHeap) Push(task interface{}) { *h = append(*h, task.(*TaskInfo)) }
// ----------------------------------------------------------------

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
