package shardkv

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestKey2Shard(t *testing.T) {
	shard := key2shard("2")
	fmt.Println(shard)
}

func F(i int, wg *sync.WaitGroup) {
	defer wg.Done()
	t := time.Duration(nrand() % 10000)
	time.Sleep(t * time.Millisecond)
	fmt.Printf("%v, %v\n", i, t)
}

func TestWaitG(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go F(i, &wg)
	}
	wg.Wait()
}
