package shardctrler

import (
	"encoding/json"
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
	fmt.Printf("before: %v\n", toJson(GetGIDShards(config.Shards)))
	fmt.Printf("%+v\n%v\n", toJson(newConf), toJson(GetGIDShards(newConf.Shards)))
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
	fmt.Printf("before: %v\n", toJson(GetGIDShards(config.Shards)))
	fmt.Printf("%+v\n%v\n", toJson(newConf), toJson(GetGIDShards(newConf.Shards)))
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
	fmt.Printf("before: %v\n", toJson(GetGIDShards(config.Shards)))
	fmt.Printf("%+v\n%v\n", toJson(newConf), toJson(GetGIDShards(newConf.Shards)))
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
	fmt.Printf("before: %v\n\n", toJson(GetGIDShards(config.Shards)))
	fmt.Printf("%v\n, %v\n\n", toJson(newConf), toJson(GetGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}

	req.GIDs = []int{2, 3}
	newConf = constructAfterLeave(&config, req.GIDs)
	fmt.Printf("%v\n, %v\n\n", toJson(newConf), toJson(GetGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}

	req.GIDs = []int{1, 2, 3} // all leave
	newConf = constructAfterLeave(&config, req.GIDs)
	fmt.Printf("%v\n, %v\n\n", toJson(newConf), toJson(GetGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}

	req.GIDs = []int{1}
	newConf = constructAfterLeave(&config, req.GIDs)
	fmt.Printf("%v\n, %v\n\n", toJson(newConf), toJson(GetGIDShards(newConf.Shards)))
	if !CheckValid(newConf.Shards, newConf.Groups) {
		panic(t)
	}
}

func TestMove(t *testing.T) {

}

func TestConcurrent(t *testing.T) {
	data := `{"Num":11,"Shards":[1190,1140,1100,1170,160,1160,1120,1150,1180,190],"Groups":{"1100":["s100a"],"1120":["s120a"],"1130":["s130a"],"1140":["s140a"],"1150":["s150a"],"1160":["s160a"],"1170":["s170a"],"1180":["s180a"],"1190":["s190a"],"160":["s160b"],"190":["s190b"]}}`
	config := Config{}
	err := json.Unmarshal([]byte(data), &config)
	if err != nil {
		return
	}
	CheckValid(config.Shards, config.Groups)

	data = `{"Num":11,"Shards":[1160,160,1170,1150,1190,1130,1100,1110,1120,1180],"Groups":{"1100":["s100a"],"1110":["s110a"],"1120":["s120a"],"1130":["s130a"],"1150":["s150a"],"1160":["s160a"],"1170":["s170a"],"1180":["s180a"],"1190":["s190a"],"160":["s160b"],"170":["s170b"]}}`
	config = Config{}
	err = json.Unmarshal([]byte(data), &config)
	CheckValid(config.Shards, config.Groups)
	data = `{"Num":15,"Shards":[1100,110,1130,1160,1110,1180,1150,1170,100,1190],"Groups":{"100":["s100b"],"110":["s110b"],"1100":["s100a"],"1110":["s110a"],"1120":["s120a"],"1130":["s130a"],"1140":["s140a"],"1150":["s150a"],"1160":["s160a"],"1170":["s170a"],"1180":["s180a"],"1190":["s190a"],"160":["s160b"],"170":["s170b"],"180":["s180b"]}} 
`
	config = Config{}
	err = json.Unmarshal([]byte(data), &config)
	newconfig := constructAfterLeave(&config, []int{1100})
	if !CheckValid(newconfig.Shards, newconfig.Groups) {
		t.Fatalf("11")
	}

	data = `{"Num":19,"Shards":[1006,1008,1009,1003,2004,3001,3004,3007,2001,1002],"Groups":{"1000":["1000a","1000b","1000c"],"1001":["1001a","1001b","1001c"],"1002":["1002a","1002b","1002c"],"1003":["1003a","1003b","1003c"],"1004":["1004a","1004b","1004c"],"1005":["1005a","1005b","1005c"],"1006":["1006a","1006b","1006c"],"1007":["1007a","1007b","1007c"],"1008":["1008a","1008b","1008c"],"1009":["1009a","1009b","1009c"],"2001":["2001a"],"2002":["2002a"],"2004":["2004a"],"2005":["2005a"],"2007":["2007a"],"3001":["3001a"],"3002":["3002a"],"3004":["3004a"],"3005":["3005a"],"3007":["3007a"]}} 
 `
	config = Config{}
	err = json.Unmarshal([]byte(data), &config)
	newconfig = constructAfterLeave(&config, []int{1100})
	if !CheckValid(newconfig.Shards, newconfig.Groups) {
		t.Fatalf("11")
	}

}
