package shardctrler

import (
	"fmt"
	"testing"
)

func TestConfigJoin(t *testing.T) {
	config := Config{
		Num:    0,
		Shards: [10]int{0: 1, 3: 1, 4: 1, 5: 1, 8: 1, 1: 2, 2: 2, 6: 2, 7: 2, 9: 2},
		Groups: map[int][]string{1: {"1", "2", "3"}, 2: {"1", "2", "3"}},
	}
	// {0, 3,4,5,8}->gid1
	// {1,2,6,7,9}-> gid2

	// new join gid3, gid4
	req := JoinArgs{
		ID: 0,
		Servers: map[int][]string{
			3: []string{"1", "2", "3"},
			4: []string{"1", "2", "3"},
		},
	}
	newConf := ConstructAfterJoin(&config, req.Servers)
	fmt.Printf("before: %v\n", toJson(getGIDShards(config.Shards)))
	fmt.Printf("%+v\n%v\n", toJson(newConf), toJson(getGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}
}

func TestConfigJoin2(t *testing.T) {
	config := Config{
		Num:    0,
		Shards: [10]int{0: 1, 3: 1, 4: 1, 5: 1, 8: 1, 1: 2, 2: 2, 6: 2, 7: 2, 9: 2},
		Groups: map[int][]string{1: {"1", "2", "3"}, 2: {"1", "2", "3"}, 3: {"1"}},
	}
	// {0, 3,4}->gid1
	// {1,2,6}-> gid2
	// {5,8,7,9}->gid3
	x := []int{5, 8, 7, 9}
	for _, v := range x {
		config.Shards[v] = 3
	}

	// new join gid3, gid4
	req := JoinArgs{
		ID: 0,
		Servers: map[int][]string{
			4: []string{"1", "2", "3"},
		},
	}
	newConf := ConstructAfterJoin(&config, req.Servers)
	fmt.Printf("before: %v\n", toJson(getGIDShards(config.Shards)))
	fmt.Printf("%+v\n%v\n", toJson(newConf), toJson(getGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}
}

func TestJoinEmpty(t *testing.T) {
	config := Config{
		Num:    0,
		Shards: [10]int{},
		Groups: nil,
	}

	// new join gid4
	req := JoinArgs{
		ID: 0,
		Servers: map[int][]string{
			4: []string{"1", "2", "3"},
		},
	}
	newConf := ConstructAfterJoin(&config, req.Servers)
	fmt.Printf("before: %v\n", toJson(getGIDShards(config.Shards)))
	fmt.Printf("%+v\n%v\n", toJson(newConf), toJson(getGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}
}

func TestConfigLeave(t *testing.T) {
	config := Config{
		Num:    10,
		Shards: [10]int{},
		Groups: map[int][]string{1: {"1", "2", "3"}, 2: {"1", "2", "3"}, 3: {"1"}},
	}
	// {0,1,2}->1
	// {3,4,5}->2
	// {6,7,8,9}->3
	x := []int{0, 1, 2}
	for _, v := range x {
		config.Shards[v] = 1
	}
	x = []int{3, 4, 5}
	for _, v := range x {
		config.Shards[v] = 2
	}
	x = []int{6, 7, 8, 9}
	for _, v := range x {
		config.Shards[v] = 3
	}

	req := LeaveArgs{
		ID:   0,
		GIDs: []int{3},
	}
	newConf := constructAfterLeave(&config, req.GIDs)
	fmt.Printf("before: %v\n\n", toJson(getGIDShards(config.Shards)))
	fmt.Printf("%v\n, %v\n\n", toJson(newConf), toJson(getGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}

	req.GIDs = []int{2, 3}
	newConf = constructAfterLeave(&config, req.GIDs)
	fmt.Printf("%v\n, %v\n\n", toJson(newConf), toJson(getGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}

	req.GIDs = []int{1, 2, 3} // all leave
	newConf = constructAfterLeave(&config, req.GIDs)
	fmt.Printf("%v\n, %v\n\n", toJson(newConf), toJson(getGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}

	req.GIDs = []int{1}
	newConf = constructAfterLeave(&config, req.GIDs)
	fmt.Printf("%v\n, %v\n\n", toJson(newConf), toJson(getGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}
}

func TestMove(t *testing.T) {

}
