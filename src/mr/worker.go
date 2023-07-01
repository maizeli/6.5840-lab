package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"time"
)

// Map functions return a slice of KeyValue.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := &GetTaskArgs{}
		reply := &GetTaskReply{}
		ok := call("Coordinator.GetTask", args, reply)
		if !ok {
			continue
		}
		if reply.Done {
			break
		}
		if reply.Task != nil {
			fmt.Printf("reply.Task=%v\n", Marshal(reply.Task))
			err := StartTask(reply.Task, reply.NReduce, mapf, reducef)
			if err != nil {
				fmt.Printf("Worker running fail, err=%v\n", err)
			}
			args := &UpdateTaskStatusArgs{
				TaskId:     reply.Task.ID,
				TaskStatus: TaskStatusDone,
				TaskType:   reply.Task.Type,
			}
			reply := &UpdateTaskStatusReply{}
			ok = call("Coordinator.UpdateTaskStatus", args, reply)
			if !ok {
				continue
			}
		}
		time.Sleep(time.Millisecond)
	}
}

func StartTask(task *Task, nReduce int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {
	switch task.Type {
	case TaskTypeMap:
		return StartMapTask(task, nReduce, mapf)
	case TaskTypeReduce:
		return StartReduceTask(task, nReduce, reducef)
	}

	return nil
}

func StartMapTask(task *Task, nReduce int, mapf func(string, string) []KeyValue) error {
	fmt.Printf("MapTask[%v] begin...\n", task.ID)
	file, err := os.Open(task.FileName)
	if err != nil {
		return fmt.Errorf("StartMapTask fail, err=%w", err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("StartMapTask fail, err=%w", err)
	}
	file.Close()
	kva := mapf(task.FileName, string(content))
	files := []*os.File{}
	for i := 0; i < int(nReduce); i++ {
		file, err := os.CreateTemp("", "*")
		if err != nil {
			return fmt.Errorf("StartMapTask fail, err=%w", err)
		}
		files = append(files, file)
	}
	for _, kv := range kva {
		idx := ihash(kv.Key) % int(nReduce)
		enc := json.NewEncoder(files[idx])
		err := enc.Encode(&kv)
		if err != nil {
			return fmt.Errorf("StartMapTask: json.Encode fail, err=%w", err)
		}
	}
	for i, file := range files {
		if err := os.Rename(file.Name(), fmt.Sprintf("mr-%v-%v", task.ID, i)); err != nil {
			return fmt.Errorf("StartMapTask fail, err=%w", err)
		}
	}
	fmt.Printf("MapTask[%v] Success!!!\n", task.ID)

	return nil
}

func StartReduceTask(task *Task, nReduce int, reducef func(string, []string) string) error {
	fmt.Printf("call StartReduceTask\n")
	entries, err := os.ReadDir("./")
	if err != nil {
		return fmt.Errorf("StartReduceTask fail, err=%w", err)
	}
	str := fmt.Sprintf(`^mr-\w+-%v$`, task.ID)
	pattern := regexp.MustCompile(str)
	key := ""
	values := []string{}
	for _, entry := range entries {
		if pattern.MatchString(entry.Name()) {
			file, err := os.Open(entry.Name())
			// fmt.Println("fileName=", entry.Name())
			if err != nil {
				return fmt.Errorf("StartReduceTask fail, err=%w", err)
			}

			decoder := json.NewDecoder(file)
			for {
				var kv KeyValue
				err := decoder.Decode(&kv)
				if err == io.EOF {
					break
				}
				if err != nil {
					fmt.Println("StartReduceTask: decoder.Decode fail, err=", err)
					break
				}
				// fmt.Println("k=", kv.Key, "v=", kv.Value)
				key = kv.Key
				values = append(values, kv.Value)
			}
		}
	}
	result := reducef(key, values)
	file, err := os.CreateTemp("", "*")
	if err != nil {
		return fmt.Errorf("StartReduceTask fail, err=%w", err)
	}
	path := file.Name()
	_, err = file.Write([]byte(fmt.Sprintf("%v %v\n", key, result)))
	if err != nil {
		return fmt.Errorf("StartReduceTask fail, err=%w", err)
	}
	_ = file.Close()
	if err = os.Rename(path, fmt.Sprintf("mr-out-%v", task.ID)); err != nil {
		return fmt.Errorf("StartReduceTask fail, err=%w", err)
	}

	fmt.Printf("call StartReduceTask success\n")
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":12345")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("call: dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	fmt.Println("call ", rpcname, "fail, req=", Marshal(args), "resp=", Marshal(reply), "err=", err)
	if err == nil {
		return true
	}

	fmt.Println("call ", rpcname, "fail, req=", Marshal(args), "resp=", Marshal(reply), "err=", err)
	return false
}
