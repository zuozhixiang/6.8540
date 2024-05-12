package shardkv

import (
	"fmt"
	"testing"
)

func TestKey2Shard(t *testing.T) {
	shard := key2shard("0")
	fmt.Println(shard)
}
