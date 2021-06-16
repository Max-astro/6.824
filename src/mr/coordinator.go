package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
   MapTask    = 0
   ReduceTask = 1
   WaitTask   = 2
   EndTask    = 3
)

type TaskInfo struct {
   // Declared in consts above
   State int
   TaskIndex int
   FileName  string
   nReduce int
}

// Helper data structure
type TaskStatQueue struct {
	taskArray []TaskInfo
	mutex     sync.Mutex
}

func (queue *TaskStatQueue) lock() {
	queue.mutex.Lock()
}

func (queue *TaskStatQueue) unlock() {
	queue.mutex.Unlock()
}

func (queue *TaskStatQueue) Size() int {
	return len(queue.taskArray)
}

func (queue *TaskStatQueue) Pop() *TaskInfo {
	queue.lock()
	arrayLength := len(queue.taskArray)
	if arrayLength == 0 {
		queue.unlock()
		return nil
	}
	ret := queue.taskArray[arrayLength-1]
	queue.taskArray = queue.taskArray[:arrayLength-1]
	queue.unlock()
	return &ret
}

func (queue *TaskStatQueue) Push(taskStat *TaskInfo) {
	queue.lock()
	if taskStat == nil {
		queue.unlock()
		return
	}
	queue.taskArray = append(queue.taskArray, *taskStat)
	queue.unlock()
}

func (queue *TaskStatQueue) Remove(filename string, state int) *TaskInfo {
	queue.lock()
	defer queue.unlock()
	for i, task := range queue.taskArray {
		if task.FileName == filename && task.State == state {
			queue.taskArray = append(queue.taskArray[:i], queue.taskArray[i+1:]...)
			return &task
		}
	}
	return nil
}

//------------------ helper End ------------------

type Coordinator struct {
	// Your definitions here.
	Mut sync.Mutex
	JobCnt int
	nReduce int
	taskNotify chan TaskInfo
	undoMapTasks TaskStatQueue
	undoReduceTasks TaskStatQueue
	runningTasks TaskStatQueue
	IsDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) DispatchTask(args *ExampleArgs, reply *TaskInfo) error {
	c.Mut.Lock()
	defer c.Mut.Unlock()

	var task *TaskInfo
	if task = c.undoMapTasks.Pop(); task != nil {
		c.JobCnt += 1

		runningTask := TaskInfo{ReduceTask, c.JobCnt, task.FileName, c.nReduce}
		c.runningTasks.Push(&runningTask)
	} else if task = c.undoReduceTasks.Pop(); task != nil {
		c.runningTasks.Push(task)
	} else if c.IsDone {
		task = &TaskInfo{EndTask, -1, "", c.nReduce}
	} else {
		task = &TaskInfo{WaitTask, -1, "", c.nReduce}
	}
	*reply = *task

	return nil
}

func (c *Coordinator) JoinTask(args *TaskInfo, reply *ExampleArgs) error {
	switch args.State {
		case MapTask: {
			reduceTask := c.runningTasks.Remove(args.FileName, args.State)
			reduceTask.State = ReduceTask
			c.undoReduceTasks.Push(reduceTask)
		}

		case ReduceTask: {
			c.runningTasks.Remove(args.FileName, args.State)
			if c.runningTasks.Size() == 0 {
				c.IsDone = true
			}
		}

		default:
			panic("Task Join failed")
	}
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
	return c.IsDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator {
		JobCnt:0,
		taskNotify: make(chan TaskInfo),
		IsDone: false,
		nReduce: nReduce,
	}

	// Your code here.
	for _, filename := range files {
		c.JobCnt += 1
		c.undoMapTasks.Push(&TaskInfo{MapTask, c.JobCnt, filename, nReduce})

		// file, err := os.Open(filename)
		// if err != nil {
		// 	log.Fatalf("cannot open %v", filename)
		// }
		// content, err := ioutil.ReadAll(file)
		// if err != nil {
		// 	log.Fatalf("cannot read %v", filename)
		// }
		// fmt.Println(len(content))
		// file.Close()
		// kva := mapf(filename, string(content))
		// intermediate = append(intermediate, kva...)
	}

	c.server()
	return &c
}
