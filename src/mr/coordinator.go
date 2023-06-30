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

type TaskStatus int32

const (
	TaskStatusNotStart TaskStatus = iota + 1
	TaskStatusRunning
	TaskStatusDone
	TaskStatusFail
)

type TaskType int32

const (
	TaskTypeMap = iota + 1
	TaskTypeReduce
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
	fmt.Printf("[GetTask] args=%v", *args)
	c.Lock.Lock()
	defer func() {
		c.Lock.Unlock()
		fmt.Printf("[GetTask] reply=%v", *reply)
	}()
	// get map task
	for _, task := range c.MapTasks {
		if task.Status == TaskStatusNotStart || task.Status == TaskStatusFail {
			reply.Task = task
			task.Status = TaskStatusRunning
			reply.Done = false
			return nil
		}
	}
	// map has done, get reduce task
	for _, task := range c.ReduceTasks {
		if task.Status == TaskStatusNotStart || task.Status == TaskStatusFail {
			reply.Done = false
			reply.Task = task
			task.Status = TaskStatusRunning
			return nil
		}
	}
	checkDone := func(tasks []*Task) bool {
		for _, task := range tasks {
			if task.Status == TaskStatusRunning || task.Status == TaskStatusFail || task.Status == TaskStatusNotStart {
				return false
			}
		}
		return true
	}

	reply.Done = (checkDone(c.ReduceTasks) || checkDone(c.MapTasks))

	// 超时设置为失败
	go func() {
		timer := time.NewTimer(time.Second * 10)
		defer timer.Stop()
		select {
		case _ = <-timer.C:
			c.Lock.Lock()
			defer c.Lock.Unlock()
			if reply.Task.Status == TaskStatusRunning {
				reply.Task.Status = TaskStatusFail
			}
		}
	}()

	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	if args.TaskType == TaskTypeMap {
		return c.updateMapTaskStatus(args.TaskId, args.TaskStatus)
	}

	return c.updateMapTaskStatus(args.TaskId, args.TaskStatus)
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
	fmt.Printf("MapTask[%v] %v->%v", id, c.MapTasks[idx].Status, status)
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
	fmt.Printf("Reduce[%v] %v->%v", id, c.ReduceTasks[idx].Status, status)
	c.ReduceTasks[idx].Status = status

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
	c := Coordinator{}

	coo := &Coordinator{}
	for i, fileName := range files {
		coo.MapTasks = append(coo.MapTasks, &Task{
			Status:   TaskStatusNotStart,
			ID:       int32(i),
			FileName: fileName,
			Type:     TaskTypeMap,
		})
	}

	for i := 0; i < nReduce; i++ {
		coo.ReduceTasks = append(coo.ReduceTasks, &Task{
			Status: TaskStatusNotStart,
			ID:     int32(i),
			Type:   TaskTypeReduce,
		})
	}

	c.server()
	return &c
}
