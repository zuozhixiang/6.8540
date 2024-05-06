package kvraft

import (
	"6.5840/labgob"
	"bytes"
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
		default:
			{
				mutex.Lock()
				cond.Wait()
				mutex.Unlock()
			}
		}
	}
}

func TestGob(t *testing.T) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	data := map[string]string{"zzx": "123", "xxx": "12344"}
	data1 := map[string]string{"zzx1": "123", "xxx1": "12344"}
	e.Encode(data)
	e.Encode(data1)
	x := w.Bytes()
	fmt.Println(string(x))

	r := bytes.NewBuffer(x)
	d := labgob.NewDecoder(r)
	var xx map[string]string
	d.Decode(&xx)

	var xx1 map[string]string
	d.Decode(&xx1)
	fmt.Println(xx, xx1)
}
