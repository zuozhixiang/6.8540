package kvraft

import (
	"fmt"
	"sync"
	"testing"
)

func TestLogger(t *testing.T) {
	debugf(GetMethod, 0, "zzx: %v", "123")
}

func TestMap(t *testing.T) {
	myMap := map[string]string{"zzx": "123"}
	myMap["xxx"] = myMap["xxx"] + "zzx"

	fmt.Println(myMap["xxx"])
}

func TestCondTiemout(t *testing.T) {
	var (
		mutex sync.Mutex
		cond  *sync.Cond
	)
	cond = sync.NewCond(&mutex)
	flag := false

	timeoutChan := make(chan bool, 1)
	go startTimeout(cond, timeoutChan)
	for !flag {
		select {
		case <-timeoutChan:
			flag = true
			fmt.Println("timeout")
		default:
			{
				mutex.Lock()
				cond.Wait()
				mutex.Unlock()
			}
		}
	}
}
