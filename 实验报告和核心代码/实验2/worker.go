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
	"strconv"
	"time"
)

const (
	AskForTaskInterval    = 1
	AskForTaskFailedLimit = 1
)

// the type of task
const (
	TaskTypeNone = iota
	TaskTypeMap
	TaskTypeReduce
)

// the state of task
const (
	TaskStateIdle = iota
	TaskStateInProgress
	TaskStateCompleted
)

// ByKey : for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// TaskInfo : the info struct of task
type TaskInfo struct {
	TaskID            int
	WorkerID          int
	TaskType          int
	TaskState         int
	StartTime         int64
	InputFile         string
	OutputFile        string
	IntermediateFiles []string
}

// KeyValue : Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// DoMapTask : call mapf and notify master after completed
// mapf : map func defined by client
// filename : input file name, pass it to mapf
// taskID : the ID of this map task, return it to master
// workerID : the ID of this worker, return it to master
// nReduce : the number of reduce workers, will generate nReduce intermediate files
func DoMapTask(mapf func(string, string) []KeyValue, filename string, taskID, workerID, nReduce int) {
	// read file and pass it to Map
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		fmt.Println("cannot close file, err:", err)
	}
	// call the map func from the client
	kva := mapf(filename, string(content))

	// write intermediate data into files from memory
	intermediateFiles := make(map[int]string)
	for i := 0; i < nReduce; i++ {
		// mr-X-Y, where X is the Map task number, and Y is the reduce task number
		outfileName := "mr-" + strconv.Itoa(taskID) + strconv.Itoa(i)
		// create temp intermediate file to store intermediate data
		outfile, err := ioutil.TempFile("", outfileName)
		if err != nil {
			log.Fatalf("cannot create temp %v", outfileName)
		}
		// defer to close the temp file
		// can't delete it in this task, because the reduce worker need to read it later
		defer outfile.Close()
		// record th intermediate files' name, and return them to master later
		intermediateFiles[i] = outfile.Name()
		// encode json and write into corresponding intermediate file
		enc := json.NewEncoder(outfile)
		for _, kv := range kva {
			// use ihash(key) % NReduce to choose the reduce task number
			if ihash(kv.Key)%nReduce == i {
				if err := enc.Encode(&kv); err != nil {
					fmt.Println("encode json error:", err)
				}
			}
		}
	}

	// notify the master that ths map task has completed
	args := SubmitTaskArgs{
		TaskID:            taskID,
		WorkerID:          workerID,
		TaskType:          TaskTypeMap,
		IntermediateFiles: intermediateFiles,
	}
	reply := SubmitTaskReply{}
	ok := call("Coordinator.SubmitTaskHandler", &args, &reply)
	if !ok {
		fmt.Println("call Coordinator.SubmitTaskHandler failed!")
	}
}

// DoReduceTask : call reducef and notify master after completed
// reducef : reduce func defined by client
// intermediateFiles : intermediate files' name, read intermediate data from them
// taskID : the ID of this map task, return it to master
// workerID : the ID of this worker, return it to master
func DoReduceTask(reducef func(string, []string) string, intermediateFiles []string, taskID, workerID int) {
	// read each intermediate files, read intermediate data from the json
	intermediate := []KeyValue{}
	for _, filename := range intermediateFiles {
		// open the intermediate file
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()

		// read json from file and decode
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// sort the intermediate key-value pairs by key
	sort.Sort(ByKey(intermediate))

	// To ensure that nobody observes partially written files in the presence of crashes,
	// create a temporary file firstly and atomically rename it when it is completely written.
	outfileName := "mr-out-" + strconv.Itoa(taskID)
	outfile, err := ioutil.TempFile("", outfileName)
	if err != nil {
		log.Fatalf("cannot create temp file %v", outfileName)
	}
	defer outfile.Close()

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-Y.
	for i, j := 0, 1; i < len(intermediate); i, j = j, j+1 {
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// call the reduce func from client
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err = fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("cannot write to file %v", outfileName)
		}
	}

	// rename the temp file
	err = os.Rename(outfile.Name(), outfileName)
	if err != nil {
		log.Fatalf("cannot rename temp file %v", outfileName)
	}

	// notify the master that ths reduce task has completed
	args := SubmitTaskArgs{
		TaskID:   taskID,
		WorkerID: workerID,
		TaskType: TaskTypeReduce,
	}
	reply := SubmitTaskReply{}
	ok := call("Coordinator.SubmitTaskHandler", &args, &reply)
	if !ok {
		fmt.Println("call Coordinator.SubmitTaskHandler failed!")
	} else if reply.TaskState != TaskStateCompleted {
		fmt.Println("not submit successfully, maybe timeout!")
	} else {
		// can only clean the intermediate files after completing the reduce task and submit to master successfully,
		// because the master may reassign the task to another worker if this worker is timeout, so that these files may be read again.
		for _, filename := range intermediateFiles {
			err = os.Remove(filename)
			if err != nil {
				fmt.Println("remove temp file error:", err)
			}
		}
	}
}

// Worker : main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	workerID := 0
	failedCount := 0
	// loop to ask for a task from master and do it,
	// exit when call master failed
	for {
		// call rpc method to ask for a task
		args := AskForTaskArgs{}
		args.WorkerID = workerID
		reply := AskForTaskReply{}
		ok := call("Coordinator.AskForTaskHandler", &args, &reply)
		if !ok {
			fmt.Println("call Coordinator.AskForTaskHandler failed!")
			// if call failed three times, maybe master has finished, worker can exit
			if failedCount++; failedCount >= AskForTaskFailedLimit {
				fmt.Println("program will exit!")
				return
			}
		} else {
			// call successfully, restart counting failure times
			failedCount = 0

			// be assigned a workerID by master
			if workerID == 0 {
				workerID = reply.WorkerID
			}

			// do task according to reply.TaskType
			switch reply.TaskType {
			case TaskTypeNone:
				// no task to do
			case TaskTypeMap:
				DoMapTask(mapf, reply.InputFile, reply.TaskID, reply.WorkerID, reply.NReduce)
			case TaskTypeReduce:
				DoReduceTask(reducef, reply.IntermediateFiles, reply.TaskID, reply.WorkerID)
			}
		}

		// wait a little time to ask again
		time.Sleep(time.Second * time.Duration(AskForTaskInterval))
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
