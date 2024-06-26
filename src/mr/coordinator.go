package mr

import (
	"errors"
	"go.uber.org/zap"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var logger *zap.SugaredLogger

func init() {
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logger1, err := config.Build()
	if err != nil {
		return
	}
	logger = logger1.Sugar()
}

const (
	MapPhase = iota
	ReducePhase
	FinishPhase
)

type Coordinator struct {
	// Your definitions here.
	Phase                 int
	mu                    sync.Mutex
	MapTask               []*Task
	ReduceTask            []*Task
	ReduceCnt             int
	AlloctedMapTaskMap    map[int]*Task
	AlloctedReduceTaskMap map[int]*Task
}

func CheckTaskFinished(taskMap map[int]*Task) bool {
	for _, task := range taskMap {
		if !task.Done {
			return false
		}
	}
	return true
}
func GetUnDone(taskMap map[int]*Task) map[int]*Task {
	res := map[int]*Task{}
	for _, task := range taskMap {
		if !task.Done {
			res[task.ID] = task
		}
	}
	return res
}

func (c *Coordinator) getNextTask() *Task {
	if c.Phase == MapPhase {
		if len(c.MapTask) > 0 {
			res := c.MapTask[0]
			c.AlloctedMapTaskMap[res.ID] = res
			c.MapTask = c.MapTask[1:]
			return res
		}
		// 所有的MapTask都分配了， 看看现在是不是全都执行完了
		if CheckTaskFinished(c.AlloctedMapTaskMap) {
			c.Phase = ReducePhase
			logger.Info("enter reduce")
			res := c.ReduceTask[0]
			c.ReduceTask = c.ReduceTask[1:]
			c.AlloctedReduceTaskMap[res.ID] = res
			return res
		}
		//x := GetUnDone(c.AlloctedMapTaskMap)
		//marshal, _ := json.Marshal(x)
		//logger.Infof("undo map task: %v, cnt: %v", string(marshal), len(x))
		return &Task{Type: WaitTask}
	} else if c.Phase == FinishPhase {
		return &Task{Type: FinishedTask}
	} else if c.Phase == ReducePhase {
		if len(c.ReduceTask) > 0 {
			res := c.ReduceTask[0]
			c.ReduceTask = c.ReduceTask[1:]
			c.AlloctedReduceTaskMap[res.ID] = res
			return res
		}
		if CheckTaskFinished(c.AlloctedReduceTaskMap) {
			logger.Info("enter finish")
			c.Phase = FinishPhase
			return &Task{Type: FinishedTask}
		}
		//x := GetUnDone(c.AlloctedReduceTaskMap)
		//marshal, _ := json.Marshal(x)
		//logger.Infof("undo reduce task: %v, cnt: %v", string(marshal), len(x))
		return &Task{Type: WaitTask}
	}
	logger.Panicf("feihua")
	return &Task{}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(req *AskTaskRequest, resp *AskTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task := c.getNextTask()
	t := time.Now()
	task.StartTime = &t
	resp.TaskID = task.ID
	resp.Type = task.Type
	resp.Files = task.Files
	resp.ReduceCnt = c.ReduceCnt
	logger.Debugf("phase: %v, alllocate task: %s", c.Phase, toJsonString(task))
	return nil
}

func (c *Coordinator) TellDone(req *TellDoneRequest, resp *TellDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.Type == MapTask {
		task, ok := c.AlloctedMapTaskMap[req.TaskID]
		if !ok {
			logger.Errorf("done error map task id: %v", req.TaskID)
			return errors.New("")
		}
		logger.Infof("done map task id: %v", req.TaskID)
		task.Done = true
	} else {
		task, ok := c.AlloctedReduceTaskMap[req.TaskID]
		if !ok {
			logger.Errorf("done error reduce task id: %v", task)
			return errors.New("")
		}
		logger.Infof("done reduce task id: %v", req.TaskID)
		task.Done = true
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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.Phase == FinishedTask
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Phase:                 MapPhase,
		MapTask:               MakeMapTask(files),
		ReduceTask:            MakeReduceTask(len(files), nReduce),
		AlloctedMapTaskMap:    map[int]*Task{},
		AlloctedReduceTaskMap: map[int]*Task{},
		ReduceCnt:             nReduce,
	}
	// Your code here.
	logger.Infof("Master Start")
	c.server()

	go c.dectorCrash()
	return &c
}

func (c *Coordinator) dectorCrash() {
	for {
		const t = 10
		logger.Info("dectorCrash")
		time.Sleep(2 * time.Second)
		c.mu.Lock()
		if c.Phase == FinishPhase { // 检查job 是否完成
			c.mu.Unlock()
			break
		}
		if c.Phase == MapPhase {
			for _, task := range GetUnDone(c.AlloctedMapTaskMap) {
				if task.StartTime != nil {
					duration := time.Since(*task.StartTime)
					if duration > t*time.Second {
						task.StartTime = nil
						c.MapTask = append(c.MapTask, task)
						logger.Warnf("map task timeout:%v", toJsonString(task))
					}
				}
			}
		} else if c.Phase == ReducePhase {
			for _, task := range GetUnDone(c.AlloctedReduceTaskMap) {
				if task.StartTime != nil {
					duration := time.Since(*task.StartTime)
					if duration > t*time.Second {
						task.StartTime = nil
						c.ReduceTask = append(c.ReduceTask, task)
						logger.Warnf("reduce task timeout:%v", toJsonString(task))
					}
				}
			}
		}
		c.mu.Unlock()
	}
}
