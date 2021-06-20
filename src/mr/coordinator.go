package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MapTask    = 0
	ReduceTask = 1
	WaitTask   = 2
	EndTask    = 3
)

type TaskInfo struct {
	// Declared in consts above
	State     int
	TaskIndex int
	NReduce   int
	NMap      int
	FileName  string
	Start     time.Time
}

func (t *TaskInfo) String() string {
	var stat string
	if t.State == MapTask {
		stat = "Map"
	} else if t.State == ReduceTask {
		stat = "Reduce"
	} else if t.State == WaitTask {
		stat = "Wating"
	} else {
		stat = "EndTask"
	}
	return fmt.Sprintf("%v %v %v %v %v\n", stat, t.TaskIndex, t.NReduce, t.NMap, t.FileName)
}

// Helper data structure
type TaskStatQueue struct {
	TaskArray []TaskInfo
	mutex     sync.Mutex
}

func (queue *TaskStatQueue) Size() int {
	return len(queue.TaskArray)
}

func (queue *TaskStatQueue) Pop() *TaskInfo {
	queue.mutex.Lock()
	arrayLength := len(queue.TaskArray)
	if arrayLength == 0 {
		queue.mutex.Unlock()
		return nil
	}
	ret := queue.TaskArray[arrayLength-1]
	queue.TaskArray = queue.TaskArray[:arrayLength-1]
	queue.mutex.Unlock()
	return &ret
}

func (queue *TaskStatQueue) Push(taskStat *TaskInfo) {
	queue.mutex.Lock()
	if taskStat == nil {
		queue.mutex.Unlock()
		return
	}
	queue.TaskArray = append(queue.TaskArray, *taskStat)
	queue.mutex.Unlock()
}

func (queue *TaskStatQueue) Remove(filename string) *TaskInfo {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()
	for i, task := range queue.TaskArray {
		if task.FileName == filename {
			queue.TaskArray = append(queue.TaskArray[:i], queue.TaskArray[i+1:]...)
			return &task
		}
	}
	return nil
}

//------------------ helper End ------------------

type Coordinator struct {
	// Your definitions here.
	Mut                sync.Mutex
	JobCnt             int
	nReduce            int
	nMap               int
	taskNotify         chan TaskInfo
	mapTaskQue         TaskStatQueue
	reduceTaskQue      TaskStatQueue
	runningMapTasks    map[string]*TaskInfo
	runningReduceTasks map[int]*TaskInfo
	MapFinished        bool
	IsDone             bool
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

	fmt.Printf("###QueCheck: inqueue: %v, %v; running: %v %v\n", c.mapTaskQue.Size(), c.reduceTaskQue.Size(), len(c.runningMapTasks), len(c.runningReduceTasks))
	var task *TaskInfo

	if !c.MapFinished {
		if task = c.mapTaskQue.Pop(); task != nil {
			task.Start = time.Now()
			c.runningMapTasks[task.FileName] = task
			// fmt.Printf("##Map## running map: %v %v\n", len(c.runningMapTasks), len(c.runningReduceTasks))
			// } else if c.IsMapFinished() {
			// 	task = c.reduceTaskQue.Pop()
			// 	if task == nil {
			// 		panic("Dispatcher: Both map&reduce task queue are empty!")
			// 	}
		} else {
			task = &TaskInfo{WaitTask, -1, c.nReduce, c.nMap, "", time.Time{}}
		}
	} else if task = c.reduceTaskQue.Pop(); task != nil {
		task.Start = time.Now()
		c.runningReduceTasks[task.TaskIndex] = task
		fmt.Printf("##Reduce## running map: %v %v\n", len(c.runningMapTasks), len(c.runningReduceTasks))
		fmt.Println(task)
	} else if c.IsDone {
		task = &TaskInfo{EndTask, -1, c.nReduce, c.nMap, "", time.Time{}}
	} else {
		task = &TaskInfo{WaitTask, -1, c.nReduce, c.nMap, "", time.Time{}}
	}

	*reply = *task
	fmt.Println("dispatching task:", reply)
	// update IsDone flag
	c.IsAllDone()

	return nil
}

// no need to lock because caller will lock the mutex
func (c *Coordinator) IsMapFinished() bool {
	if c.mapTaskQue.Size() == 0 && len(c.runningMapTasks) == 0 {
		c.MapFinished = true
		return true
	}
	for filename, task := range c.runningMapTasks {
		gap := time.Now().Second() - task.Start.Second()
		fmt.Printf("IsMapFinished: gap = %ds\n", gap)
		if gap > 10 {
			delete(c.runningMapTasks, filename)
			c.mapTaskQue.Push(task)
		}
	}
	return false
}

// no need to lock too
func (c *Coordinator) IsAllDone() bool {
	if c.IsMapFinished() && c.reduceTaskQue.Size() == 0 && len(c.runningReduceTasks) == 0 {
		c.IsDone = true
		return true
	}
	for taskIdx, task := range c.runningReduceTasks {
		gap := time.Now().Second() - task.Start.Second()
		fmt.Printf("IsAllDone: gap = %ds\n", gap)
		if gap > 10 {
			delete(c.runningReduceTasks, taskIdx)
			c.reduceTaskQue.Push(task)
		}
	}
	return false
}

func (c *Coordinator) JoinTask(args *TaskInfo, reply *ExampleArgs) error {
	c.Mut.Lock()
	defer c.Mut.Unlock()

	fmt.Println("Joining task: ", args)

	switch args.State {
	case MapTask:
		{
			reduceTask, ok := c.runningMapTasks[args.FileName]
			if !ok {
				msg := fmt.Sprintf("Join faild %v", args)
				panic(msg)
			}
			delete(c.runningMapTasks, args.FileName)
			reduceTask.State = ReduceTask
			// all map tasks are finished, then create reduce tasks
			if c.IsMapFinished() && c.reduceTaskQue.Size() == 0 {
				c.createReduceTasks()
			}
		}

	case ReduceTask:
		{
			_, ok := c.runningReduceTasks[args.TaskIndex]
			if !ok {
				msg := fmt.Sprintf("Join faild %v\nrunningReduceTasks: %v\n", args, len(c.runningReduceTasks))
				panic(msg)
			}

			delete(c.runningReduceTasks, args.TaskIndex)
		}

	default:
		panic("Task Join failed")
	}
	return nil
}

func (c *Coordinator) createReduceTasks() {
	for idx := 0; idx < c.nReduce; idx++ {
		task := TaskInfo{ReduceTask, idx, c.nReduce, c.nMap, "", time.Time{}}
		fmt.Println(task)
		c.reduceTaskQue.Push(&task)
	}
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
	c.Mut.Lock()
	defer c.Mut.Unlock()
	return c.IsDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JobCnt:             0,
		taskNotify:         make(chan TaskInfo),
		nReduce:            nReduce,
		nMap:               len(files),
		runningMapTasks:    make(map[string]*TaskInfo),
		runningReduceTasks: make(map[int]*TaskInfo),
		IsDone:             false,
		MapFinished:        false,
	}

	// Your code here.
	for _, filename := range files {
		c.JobCnt += 1
		task := TaskInfo{MapTask, c.JobCnt, nReduce, len(files), filename, time.Time{}}
		fmt.Println(task)
		c.mapTaskQue.Push(&task)
	}

	// for t := c.mapTaskQue.Pop(); t != nil; t = c.mapTaskQue.Pop() {
	// 	fmt.Println(t)
	// }

	c.server()
	return &c
}
