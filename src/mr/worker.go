package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
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
			fmt.Printf("call Coorinator.GetTask fail")
			continue
		}
		if reply.Done {
			break
		}
		if reply.Task != nil {
			err := StartTask(reply.Task, reply.NReduce, mapf, reducef)
			if err != nil {
				fmt.Printf("Worker running fail, err=%v\n", err)
			}
			args := &UpdateTaskStatusArgs{}
			reply := &UpdateTaskStatusReply{}
			ok = call("Coordinator.UpdateTaskStatus", args, reply)
			if !ok {
				continue
			}
		}
	}
}

func StartTask(task *Task, nReduce int32, mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {
	switch task.Type {
	case TaskTypeMap:
		return StartMapTask(task, nReduce, mapf)
	case TaskTypeReduce:
		return StartReduceTask(task, nReduce, reducef)
	}

	return nil
}

func StartMapTask(task *Task, nReduce int32, mapf func(string, string) []KeyValue) error {
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
		_, err := files[idx].WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
		if err != nil {
			return fmt.Errorf("StartMapTask fail, err=%w", err)
		}
	}
	for i, file := range files {
		if err := os.Rename(file.Name(), fmt.Sprintf("mr-%v-%v", task.ID, i)); err != nil {
			return fmt.Errorf("StartMapTask fail, err=%w", err)
		}
	}
	fmt.Printf("StartMapTask Success!!!\n")

	return nil
}

func StartReduceTask(task *Task, nReduce int32, reducef func(string, []string) string) error {
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
			if err != nil {
				return fmt.Errorf("StartReduceTask fail, err=%w", err)
			}
			decoder := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := decoder.Decode(&kv); err != nil {
					break
				}
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
