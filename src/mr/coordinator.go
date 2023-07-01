package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var Logger *log.Logger

func init() {
	file, err := os.Create(fmt.Sprintf("log-%v.txt", time.Now().Unix()))
	if err != nil {
		panic(err)
	}
	Logger = log.New(file, "", log.Lshortfile|log.Ldate|log.Lmicroseconds)
}

type TaskStatus int32

const (
	TaskStatusNotStart TaskStatus = 1
	TaskStatusRunning  TaskStatus = 2
	TaskStatusDone     TaskStatus = 3
	TaskStatusFail     TaskStatus = 4
)

type TaskType int32

const (
	TaskTypeMap    TaskType = 1
	TaskTypeReduce TaskType = 2
)

type Task struct {
	Status   TaskStatus
	ID       int32
	FileName string
	Type     TaskType
}

type Coordinator struct {
	MapTasks    []*Task
	ReduceTasks []*Task
	Lock        sync.Mutex
	NReduce     int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	checkDone := func(tasks []*Task) bool {
		for _, task := range tasks {
			if task.Status == TaskStatusRunning || task.Status == TaskStatusFail || task.Status == TaskStatusNotStart {
				return false
			}
		}
		return true
	}
	c.Lock.Lock()
	defer func() {
		if reply != nil {
			reply.Done = checkDone(c.ReduceTasks)
		}
		Logger.Println("GetTaskReply=", Marshal(reply.Task))
		c.Lock.Unlock()
		// 超时设置为失败
		go func() {
			if reply == nil || reply.Task == nil {
				return
			}
			timer := time.NewTimer(time.Second * 10)
			defer timer.Stop()
			select {
			case _ = <-timer.C:
				c.Lock.Lock()
				defer c.Lock.Unlock()
				if reply.Task.Status == TaskStatusRunning {
					Logger.Println("set timeout success")
					reply.Task.Status = TaskStatusFail
				}
			}
		}()
	}()
	// get map task
	for _, task := range c.MapTasks {
		if task.Status == TaskStatusNotStart || task.Status == TaskStatusFail {
			task.Status = TaskStatusRunning
			reply.Task = task
			reply.NReduce = c.NReduce
			return nil
		}
	}

	// 此时map存在失败或者运行中的任务
	if done := checkDone(c.MapTasks); !done {
		Logger.Println("MapTaskHasRunning!!!")
		return nil
	}
	// map has done, get reduce task
	for _, task := range c.ReduceTasks {
		if task.Status == TaskStatusNotStart || task.Status == TaskStatusFail {
			reply.Task = task
			task.Status = TaskStatusRunning
			reply.NReduce = c.NReduce
			return nil
		}
	}

	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	if args.TaskType == TaskTypeMap {
		return c.updateMapTaskStatus(args.TaskId, args.TaskStatus)
	}

	return c.updateReduceStatus(args.TaskId, args.TaskStatus)
}

func (c *Coordinator) updateMapTaskStatus(id int32, status TaskStatus) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	idx := -1
	for i, task := range c.MapTasks {
		if task.ID == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("not found map task, task id=%v", id)
	}
	Logger.Printf("MapTask[%v] %v->%v\n", id, c.MapTasks[idx].Status, status)
	c.MapTasks[idx].Status = status

	return nil
}

func (c *Coordinator) updateReduceStatus(id int32, status TaskStatus) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	idx := -1
	for i, task := range c.ReduceTasks {
		if task.ID == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("not found map task, task id=%v", id)
	}
	Logger.Printf("Reduce[%v] %v->%v", id, c.ReduceTasks[idx].Status, status)
	c.ReduceTasks[idx].Status = status

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		Logger.Printf("rpc.Register fail, err=%w\n", err)
		panic(err)
	}
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":12345")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	Logger.Printf("server.Listen success\n")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	for _, task := range c.ReduceTasks {
		if task.Status != TaskStatusDone {
			ret = false
			break
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Lock:    sync.Mutex{},
		NReduce: nReduce,
	}

	for i, fileName := range files {
		c.MapTasks = append(c.MapTasks, &Task{
			Status:   TaskStatusNotStart,
			ID:       int32(i),
			FileName: fileName,
			Type:     TaskTypeMap,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &Task{
			Status: TaskStatusNotStart,
			ID:     int32(i),
			Type:   TaskTypeReduce,
		})
	}

	Logger.Println(Marshal(c))
	c.server()
	return &c
}

func Marshal(i interface{}) string {
	bytes, _ := json.Marshal(i)
	return string(bytes)
}
