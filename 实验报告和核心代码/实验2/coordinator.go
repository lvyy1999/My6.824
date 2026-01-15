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
	TaskTimeout     = 10
	CheckInterval   = 2
	InvalidWorkerID = 0
	MinimumWorkerID = 1
)

// Coordinator : the data struct of master
type Coordinator struct {
	// Your definitions here.
	mMutex              sync.Mutex
	mNReduce            int
	mMapTasks           []TaskInfo
	mReduceTasks        []TaskInfo
	mNextWorkerID       int
	mNUncompletedMap    int
	mNUncompletedReduce int
}

// AskForTaskHandler : handle the AskForTask request from worker
func (c *Coordinator) AskForTaskHandler(args *AskForTaskArgs, reply *AskForTaskReply) error {
	c.mMutex.Lock()
	defer c.mMutex.Unlock()
	// if the workerID is invalid, assign a new workerID
	workerID := args.WorkerID
	if workerID == 0 {
		workerID = c.mNextWorkerID
		c.mNextWorkerID++
	}
	reply.WorkerID = workerID

	// choose an available task to assign
	var task *TaskInfo
	if c.mNUncompletedMap > 0 { // if there are some map tasks uncompleted, can only assign map task
		for i := range c.mMapTasks {
			mapTask := &c.mMapTasks[i]
			if mapTask.TaskState == TaskStateIdle {
				task = mapTask
				break
			}
		}
	} else if c.mNUncompletedReduce > 0 { // after all map tasks completed, can assign reduce task
		for i := range c.mReduceTasks {
			reduceTask := &c.mReduceTasks[i]
			if reduceTask.TaskState == TaskStateIdle {
				task = reduceTask
				break
			}
		}
	}

	if task == nil {
		// if no available task can do, return TASK_TYPE_NONE
		reply.TaskType = TaskTypeNone
	} else {
		// pass the available task info to reply
		reply.TaskID = task.TaskID
		reply.NReduce = c.mNReduce
		reply.TaskType = task.TaskType
		reply.InputFile = task.InputFile
		reply.IntermediateFiles = task.IntermediateFiles
		// update task state
		task.WorkerID = workerID
		task.StartTime = time.Now().Unix()
		task.TaskState = TaskStateInProgress
	}

	return nil
}

// SubmitTaskHandler : handle the SubmitTask request from worker
func (c *Coordinator) SubmitTaskHandler(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	c.mMutex.Lock()
	defer c.mMutex.Unlock()
	// choose the tasks list by args.TaskType
	var tasks *[]TaskInfo
	if args.TaskType == TaskTypeMap {
		tasks = &c.mMapTasks
	} else if args.TaskType == TaskTypeReduce {
		tasks = &c.mReduceTasks
	} else {
		fmt.Println("unknown task type")
		return nil
	}

	// check the taskID, and choose the corresponding task
	var task *TaskInfo
	taskID := args.TaskID
	if taskID < 0 || taskID >= len(*tasks) {
		fmt.Println("task id out of range")
		return nil
	} else {
		task = &(*tasks)[taskID]
	}

	// check the workerID with recent allocated workerID
	// because the task maybe reallocated to another worker when timeout
	if args.WorkerID != (*task).WorkerID {
		fmt.Println("workerID not match")
		return nil
	}

	// check the task state, can only transfer to completed from in progress
	if (*task).TaskState != TaskStateInProgress {
		fmt.Println("task state not match")
		return nil
	}

	// pass all check, update task and master state, return result to worker
	reply.TaskState = TaskStateCompleted
	(*task).TaskState = TaskStateCompleted
	if args.TaskType == TaskTypeMap {
		c.mNUncompletedMap--
		// if map completed, store intermediate files to reduce task info
		for reduceTaskID, file := range args.IntermediateFiles {
			c.mReduceTasks[reduceTaskID].IntermediateFiles = append(c.mReduceTasks[reduceTaskID].IntermediateFiles, file)
		}
	} else {
		c.mNUncompletedReduce--
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// Done : return true when all tasks completed
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mMutex.Lock()
	ret := c.mNUncompletedReduce == 0
	c.mMutex.Unlock()
	return ret
}

// CheckTaskTimeout : check if any task not completed in specified time
// if a task is timeout, reset its state, make it available for another worker
func (c *Coordinator) CheckTaskTimeout() {
	for !c.Done() {
		// get current time
		current := time.Now().Unix()

		c.mMutex.Lock()
		// check map tasks
		for idx := range c.mMapTasks {
			task := &c.mMapTasks[idx]
			if task.TaskState == TaskStateInProgress && current-task.StartTime >= TaskTimeout {
				fmt.Printf("map task %d timeout, workerID = %d\n", task.TaskID, task.WorkerID)
				task.WorkerID = 0
				task.StartTime = 0
				task.TaskState = TaskStateIdle
			}
		}
		// check reduce tasks
		for idx := range c.mReduceTasks {
			task := &c.mReduceTasks[idx]
			if task.TaskState == TaskStateInProgress && current-task.StartTime >= TaskTimeout {
				fmt.Printf("reduce task %d timeout, workerID = %d\n", task.TaskID, task.WorkerID)
				task.WorkerID = 0
				task.StartTime = 0
				task.TaskState = TaskStateIdle
			}
		}
		c.mMutex.Unlock()

		// wait a little time to check again
		time.Sleep(time.Second * time.Duration(CheckInterval))
	}
}

// MakeCoordinator : create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// init master params
	c := Coordinator{
		mNReduce:            nReduce,
		mNextWorkerID:       MinimumWorkerID,
		mNUncompletedMap:    len(files),
		mNUncompletedReduce: nReduce,
	}

	// init map tasks
	for i, file := range files {
		c.mMapTasks = append(c.mMapTasks, TaskInfo{
			TaskID:            i,
			WorkerID:          InvalidWorkerID,
			TaskType:          TaskTypeMap,
			InputFile:         file,
			TaskState:         TaskStateIdle,
			IntermediateFiles: nil,
		})
	}

	// init reduce tasks
	for i := 0; i < nReduce; i++ {
		c.mReduceTasks = append(c.mReduceTasks, TaskInfo{
			TaskID:            i,
			WorkerID:          InvalidWorkerID,
			TaskType:          TaskTypeReduce,
			TaskState:         TaskStateIdle,
			IntermediateFiles: nil,
		})
	}

	// run a thread to check task timeout
	go c.CheckTaskTimeout()
	// run a rpc server to listen request from worker
	c.server()
	return &c
}
