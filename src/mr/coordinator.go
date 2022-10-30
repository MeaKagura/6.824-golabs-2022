package mr

import (
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type CoordinatorState int

const (
	MapPhase CoordinatorState = iota
	ReducePhase
	AllCompleted
)

type TaskState int

const (
	Working TaskState = iota
	Waiting
	Completed
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitingTask
)

type TaskInfo struct {
	Id             int
	Type           TaskType
	State          TaskState
	InputFilenames []string
}

type Coordinator struct {
	State          CoordinatorState
	TasksInfo      map[int]*TaskInfo
	MapTaskChan    chan *TaskInfo
	ReduceTaskChan chan *TaskInfo
	InputFilenames []string
	ReduceNum      int
	TaskId         int
}

// Your code here -- RPC handlers for the worker to call.

// GetTaskId assign a task id to recognize a specific task.
func (c *Coordinator) GetTaskId() int {
	id := c.TaskId
	c.TaskId++
	return id
}

// generateMapTasks generate all map tasks from input files.
func (c *Coordinator) generateMapTasks(inputFilenames []string) error {
	for i := 0; i < len(inputFilenames); i++ {
		task := TaskInfo{
			Id:             c.GetTaskId(),
			Type:           MapTask,
			State:          Waiting,
			InputFilenames: []string{inputFilenames[i]},
		}
		c.TasksInfo[task.Id] = &task
		c.MapTaskChan <- &task
		//fmt.Println("generate a map task:", &task)
	}
	return nil
}

// generateReduceTasks generate all reduce tasks from input files.
func (c *Coordinator) generateReduceTasks() error {
	for i := 0; i < c.ReduceNum; i++ {
		task := TaskInfo{
			Id:    c.GetTaskId(),
			Type:  ReduceTask,
			State: Waiting,
			InputFilenames: func(reduceNum int) []string {
				var inputFilenames []string
				path, _ := os.Getwd()
				files, _ := ioutil.ReadDir(path)
				for _, f := range files {
					if strings.HasPrefix(f.Name(), "mr-tmp") &&
						strings.HasSuffix(f.Name(), strconv.Itoa(reduceNum)) {
						inputFilenames = append(inputFilenames, f.Name())
					}
				}
				return inputFilenames
			}(i),
		}
		c.TasksInfo[task.Id] = &task
		c.ReduceTaskChan <- &task
		//fmt.Println("generate a reduce task:", &task)
	}
	return nil
}

// AssignTask assign an available task to a worker.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	// lock to avoid race among workers.
	mu.Lock()
	defer mu.Unlock()

	// check the CoordinatorState to assign a related task.
	reply.ReduceNum = c.ReduceNum
	switch c.State {
	case MapPhase:
		if len(c.MapTaskChan) > 0 {
			task := <-c.MapTaskChan
			task.State = Working
			reply.Task = &TaskInfo{
				Id:             task.Id,
				Type:           MapTask,
				State:          Working,
				InputFilenames: []string{task.InputFilenames[0]},
			}
			go c.checkCrash(task)
		} else {
			reply.Task = &TaskInfo{Type: WaitingTask}
		}
	case ReducePhase:
		if len(c.ReduceTaskChan) > 0 {
			task := <-c.ReduceTaskChan
			task.State = Working
			reply.Task = &TaskInfo{
				Id:             task.Id,
				Type:           ReduceTask,
				State:          Working,
				InputFilenames: task.InputFilenames,
			}
			go c.checkCrash(task)
		} else {
			reply.Task = &TaskInfo{Type: WaitingTask}
		}
	case AllCompleted:
		reply.Task = &TaskInfo{Type: ExitingTask}
	default:
		log.Fatal("CoordinatorState error.")
	}
	return nil
}

// AcceptTask accept a completed task from a worker.
func (c *Coordinator) AcceptTask(args *TaskArgs, reply *TaskReply) error {
	// lock to avoid race among workers.
	mu.Lock()
	defer mu.Unlock()

	// check the CoordinatorState to accept a related task.
	switch args.Task.Type {
	case MapTask:
		task, ok := c.TasksInfo[args.Task.Id]
		if !ok {
			log.Fatal("task id which the worker completed is undefined!")
		}
		if task.State == Working {
			task.State = Completed
			//fmt.Printf("map task %v is accepted. \n", task.Id)
			err := c.checkCoordinatorState()
			if err != nil {
				return err
			}
		} else {
			//fmt.Printf("reduce task %v is not accepted as its state is %v! \n", task.Id, task.State)
		}
	case ReduceTask:
		task, ok := c.TasksInfo[args.Task.Id]
		if !ok {
			log.Fatal("task id which the worker completed is undefined!")
		}
		if task.State == Working {
			task.State = Completed
			//fmt.Printf("reduce task %v is accepted. \n", task.Id)
			err := c.checkCoordinatorState()
			if err != nil {
				return err
			}
		} else {
			//fmt.Printf("reduce task %v is not accepted as its state is %v! \n", task.Id, task.State)
		}
	default:
		log.Fatal("task type which the worker completed is undefined!")
	}
	return nil
}

// checkCoordinatorState change the CoordinatorState if all tasks are completed.
func (c *Coordinator) checkCoordinatorState() error {
	mapTaskNum := 0
	reduceTaskNum := 0
	for i := 0; i < c.TaskId; i++ {
		if c.TasksInfo[i].Type == MapTask {
			mapTaskNum++
			if c.TasksInfo[i].State == Completed {
				mapTaskNum--
			}
		} else {
			reduceTaskNum++
			if c.TasksInfo[i].State == Completed {
				reduceTaskNum--
			}
		}
	}
	if c.State == MapPhase && mapTaskNum == 0 {
		c.State = ReducePhase
		c.TaskId = 0
		err := c.generateReduceTasks()
		if err != nil {
			return err
		}
	} else {
		if c.State == ReducePhase && reduceTaskNum == 0 {
			c.State = AllCompleted
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) checkCrash(task *TaskInfo) error {
	// wait 5 seconds for each worker.
	time.Sleep(5 * time.Second)

	// lock to avoid resetting state when executing AssignTask or AcceptTask.
	mu.Lock()
	defer mu.Unlock()

	// reset task state.
	if task.State != Completed {
		task.State = Waiting
		if task.Type == MapTask {
			//fmt.Printf("map task %v execution timeout, reset to waiting state. \n", task.Id)
			c.MapTaskChan <- task
		} else {
			//fmt.Printf("reduce task %v execution timeout, reset to waiting state. \n", task.Id)
			c.ReduceTaskChan <- task
		}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// lock to avoid returning true when executing AcceptTask
	mu.Lock()
	defer mu.Unlock()

	// return true when all tasks are completed.
	if c.State == AllCompleted {
		//fmt.Println("all tasks are completed.")
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:          MapPhase,
		TasksInfo:      make(map[int]*TaskInfo, len(files)+nReduce),
		MapTaskChan:    make(chan *TaskInfo, len(files)),
		ReduceTaskChan: make(chan *TaskInfo, nReduce),
		InputFilenames: files,
		ReduceNum:      nReduce,
		TaskId:         0,
	}
	err := c.generateMapTasks(files)
	if err != nil {
		log.Fatal("GenerateMapTasks error.")
		return &c
	}
	// listening for RPCs from workers
	c.server()
	return &c
}
