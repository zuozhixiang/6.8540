package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		id, Type, files, rCnt := AskTask()
		logger.Infof("ask taskId: %v, type: %v, files: %v", id, Type, files)
		if Type == FinishedTask {
			return
		}
		if Type == WaitTask {
			time.Sleep(1 * time.Second)
		}
		if Type == MapTask {
			err := doMapTask(id, files[0], mapf, rCnt)
			if err != nil {
				logger.Errorf("do map err: %v", err)
			}
		}
		if Type == ReduceTask {
			err := doReduceTask(id, files, reducef)
			if err != nil {
				logger.Errorf("do reduce err: %v", err)
			}
		}
	}
}

func doReduceTask(id int, files []string, reducef func(string, []string) string) error {
	// mr-0-y, mr-1-y, mr-2-y, mr-3-y
	logger.Infof("do reduce task: id[%v], files[%v]", id, files)
	intermediate := []KeyValue{}
	for _, filename := range files {
		kva := []KeyValue{}
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		content, err := io.ReadAll(file)
		if err != nil {
			return err
		}
		err = json.Unmarshal(content, &kva)
		if err != nil {
			return err
		}
		intermediate = append(intermediate, kva...)
	}
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	ofile, err := os.Create(fmt.Sprintf("mr-out-%v", id))
	if err != nil {
		return err
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	return TellDone(id, ReduceTask)
}

func doMapTask(id int, fileName string, mapf func(string, string) []KeyValue, rCnt int) error {
	// open file, then read content, call mapf(file, content)
	pwd, err := os.Getwd()
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		logger.Fatalf("can not open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		logger.Fatalf("cannot read %v", fileName)
	}

	kva := mapf(fileName, string(content))

	// key hash to reduce idx
	reduceKVA := make([][]KeyValue, rCnt)
	for _, kv := range kva {
		idx := ihash(kv.Key) % rCnt
		reduceKVA[idx] = append(reduceKVA[idx], kv)
	}

	for y, KVA := range reduceKVA {
		temp, err := os.CreateTemp(pwd, "123")

		if err != nil {
			return err
		}
		path := pwd + fmt.Sprintf("/mr-%v-%v", id, y)
		err = os.Rename(temp.Name(), path)
		if err != nil {
			return err
		}

		marshal, err := json.Marshal(KVA)
		if err != nil {
			return err
		}
		temp.Write(marshal)
		temp.Close()
	}
	return TellDone(id, MapTask)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

}

func AskTask() (id int, Type int, files []string, rCnt int) {
	req := AskTaskRequest{}
	resp := &AskTaskResponse{}
	flag := call("Coordinator.AskTask", req, resp)
	if flag {
		return resp.TaskID, resp.Type, resp.Files, resp.ReduceCnt
	} else {
		logger.Errorf("ask task fail")
	}
	return 0, WaitTask, nil, 0

}

func TellDone(id int, Type int) error {
	req := &TellDoneRequest{TaskID: id, Type: Type}
	resp := &TellDoneResponse{}
	flag := call("Coordinator.TellDone", req, resp)
	if !flag {
		return errors.New("call fail")
	}
	return nil
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
	logger.Errorf("%v fail, err: %v", rpcname, err)
	return false
}
