package shardctrler

import (
	"fmt"
	"sort"
)

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type JoinArgs struct {
	ID      int64
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	ID   int64
	GIDs []int
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	ID    int64
	Shard int
	GID   int
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	ID  int64
	Num int // desired config number
}

type QueryReply struct {
	Err    Err
	Config Config
}

func getSortedGID(groups map[int][]string) []int {
	res := make([]int, 0, len(groups))
	for gid, _ := range groups {
		res = append(res, gid)
	}
	sort.Ints(res)
	return res
}

func getGIDShards(shards [NShards]int) map[int][]int {
	res := map[int][]int{}
	for shard, gid := range shards {
		res[gid] = append(res[gid], shard)
	}
	return res
}

func getMod(num, mod int) (int, int) {
	avg := num / mod
	remainder := num % mod
	return avg, remainder
}

func CheckValid(shards [NShards]int, groups map[int][]string) bool {
	if len(groups) == 0 {
		for _, x := range shards {
			if x != 0 {
				return false
			}
		}
		return true
	}
	// check balance and  shard be repeatly allocated for more than one group
	allocatedMap := getGIDShards(shards)
	totSize := len(groups)
	gidSet := map[int]bool{}
	// check groups is same as shards
	for _, x := range shards {
		gidSet[x] = true
	}
	if len(gidSet) != totSize {
		return false
	}
	gids := []int{}
	for gid, _ := range gidSet {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	gidss := getSortedGID(groups)
	for i := 0; i < len(gids); i++ {
		if gidss[i] != gids[i] {
			return false
		}
	}
	// check balance
	sort.Slice(gidss, func(i, j int) bool {
		return len(allocatedMap[gidss[i]]) > len(allocatedMap[gidss[j]])
	})
	avg, remainder := getMod(NShards, totSize)
	for _, gid := range gidss {
		num := avg
		if remainder > 0 {
			num++
			remainder--
		}
		if num != len(allocatedMap[gid]) {
			return false
		}
	}
	return true
}

func ConstructAfterJoin(oldConfig *Config, newGroups map[int][]string) *Config {
	// keeping it as unmoved as possible.
	res := Config{
		Num:    oldConfig.Num + 1,
		Shards: [NShards]int{},
		Groups: nil,
	}
	for i := 0; i < NShards; i++ {
		res.Shards[i] = oldConfig.Shards[i]
	}
	newJoinGids := getSortedGID(newGroups)
	oldGids := getSortedGID(oldConfig.Groups)
	oldAllocate := getGIDShards(oldConfig.Shards)
	sort.Slice(oldGids, func(i, j int) bool {
		return len(oldAllocate[oldGids[i]]) > len(oldAllocate[oldGids[j]])
	}) // gid sort by the number of shard
	totGroups := map[int][]string{}
	for gid, srvs := range oldConfig.Groups {
		totGroups[gid] = srvs
	}
	for gid, srvs := range newGroups {
		totGroups[gid] = srvs
	}
	res.Groups = totGroups
	totSize := len(totGroups)
	avg, remainder := getMod(NShards, totSize)
	// need to reallocated shards
	needReAllocateShard := []int{}
	if len(oldGids) == 0 { // empty group
		for i := 0; i < NShards; i++ {
			needReAllocateShard = append(needReAllocateShard, i)
		}
	}
	for _, gid := range oldGids {
		num := avg
		shardSet := oldAllocate[gid]
		if remainder > 0 {
			remainder--
			num++
		}
		if num > len(shardSet) {
			msg := fmt.Sprintf("num is illegal, num:%v, len:%v", num, len(shardSet))
			panic(msg)
		} else if num < len(shardSet) {
			needReAllocateShard = append(needReAllocateShard, shardSet[num:]...)
		}
	}
	for _, gid := range newJoinGids {
		num := avg
		if remainder > 0 {
			remainder--
			num++
		}
		if num > len(needReAllocateShard) {
			panic(num)
		}
		newShards := needReAllocateShard[:num]

		for i := 0; i < num; i++ {
			res.Shards[newShards[i]] = gid
		}
		needReAllocateShard = needReAllocateShard[num:]
	}
	return &res
}

func constructAfterLeave(oldConfig *Config, gids []int) *Config {
	res := Config{
		Num:    oldConfig.Num + 1,
		Shards: [10]int{},
		Groups: nil,
	}
	for i := 0; i < len(oldConfig.Shards); i++ {
		res.Shards[i] = oldConfig.Shards[i]
	}
	oldAllocated := getGIDShards(oldConfig.Shards)
	totGroups := map[int][]string{}
	res.Groups = totGroups
	for gid, srvs := range oldConfig.Groups {
		totGroups[gid] = srvs
	}
	for _, gid := range gids {
		delete(totGroups, gid)
	}
	remainedGids := getSortedGID(totGroups)
	sort.Slice(remainedGids, func(i, j int) bool {
		return len(oldAllocated[remainedGids[i]]) < len(oldAllocated[remainedGids[j]])
	})
	totSize := len(totGroups)
	if totSize == 0 {
		for i := 0; i < len(oldConfig.Shards); i++ {
			res.Shards[i] = 0
		}
		return &res
	}
	avg, remainder := getMod(NShards, totSize)
	needReAllocateShard := []int{}
	for _, gid := range gids {
		needReAllocateShard = append(needReAllocateShard, oldAllocated[gid]...)
	}
	for _, gid := range remainedGids {
		num := avg
		if remainder > 0 {
			num++
			remainder--
		}
		cnt := num - len(oldAllocated[gid])
		if cnt < 0 {
			msg := fmt.Sprintf("%v < %v", num, len(oldAllocated[gid]))
			panic(msg)
		} else if cnt > 0 {
			newShards := needReAllocateShard[:cnt]
			for _, shard := range newShards {
				res.Shards[shard] = gid
			}
			needReAllocateShard = needReAllocateShard[cnt:]
		}
	}
	return &res
}

func constructAfterMove(oldConfig *Config, gid int, shard int) *Config {
	res := Config{
		Num:    oldConfig.Num + 1,
		Shards: oldConfig.Shards,
		Groups: oldConfig.Groups,
	}
	if shard < 0 || shard >= NShards {
		panic(shard)
	}
	if _, ok := oldConfig.Groups[gid]; !ok {
		panic(gid)
	}
	res.Shards[shard] = gid
	return &res
}
