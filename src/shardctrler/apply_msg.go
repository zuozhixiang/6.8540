package shardctrler

import (
	"6.5840/raft"
	"fmt"
)

func (sc *ShardCtrler) executeOp(op Op) {
	sc.executed[op.ID] = true
	switch op.Type {
	case QueryOp:
		args := op.Args.(QueryArgs)
		if args.Num == -1 || args.Num >= len(sc.configs) {
			args.Num = len(sc.configs) - 1
		}
		res := sc.configs[args.Num]
		sc.versionData[op.ID] = res
	case JoinOp:
		args := op.Args.(JoinArgs)
		lastConfig := sc.configs[len(sc.configs)-1]
		newGroups := args.Servers // new groups
		// rebalance shard
		newConfig := ConstructAfterJoin(&lastConfig, newGroups)
		sc.configs = append(sc.configs, *newConfig)
	case LeaveOp:
		args := op.Args.(LeaveArgs)
		gids := args.GIDs
		lastConfig := sc.configs[len(sc.configs)-1]
		// remove gids from config
		newConfig := constructAfterLeave(&lastConfig, gids)
		sc.configs = append(sc.configs, *newConfig)
	case MoveOp:
		args := op.Args.(MoveArgs)
		shard := args.Shard
		gid := args.GID
		lastConfig := sc.configs[len(sc.configs)-1]
		// move the shard to gid
		newConfig := constructAfterMove(&lastConfig, gid, shard)
		sc.configs = append(sc.configs, *newConfig)
	default:
		panic("illegal Op")
	}
}

func (sc *ShardCtrler) applyMsg() {
	for {
		if sc.killed() {
			return
		}
		msg := <-sc.applyCh
		needApplyMsg := []raft.ApplyMsg{msg}
		size := len(sc.applyCh)
		for i := 0; i < size; i++ {
			needApplyMsg = append(needApplyMsg, <-sc.applyCh)
		}
		sc.lock()
		lastApplied := sc.lastAppliedIndex
		for _, msg := range needApplyMsg {
			if msg.CommandValid {
				if msg.CommandIndex <= lastApplied {
					errmsg := fmt.Sprintf("[S%v], msg index: %v, lastApplied: %v, msg: %v", sc.me, msg.CommandIndex, lastApplied, msg)
					panic(errmsg)
				}
				lastApplied = msg.CommandIndex
				op := msg.Command.(Op)
				if sc.checkExecuted(op.ID) {
					continue
				}
				sc.executeOp(op)
				debugf(Apply, sc.me, "")
			} else {
				if msg.SnapshotIndex <= lastApplied {
					errmsg := fmt.Sprintf("[S%v], snapshot index: %v, lastApplied: %v, msg: %v", sc.me, msg.SnapshotIndex, lastApplied, raft.GetPrintMsg([]raft.ApplyMsg{msg}))
					panic(errmsg)
				}
				lastApplied = msg.SnapshotIndex
				sc.applySnapshot(msg.Snapshot)
				debugf(AppSnap, sc.me, "snapIndex: %v, snapTerm: %v", msg.SnapshotIndex, msg.SnapshotTerm)
			}
		}
		sc.lastAppliedIndex = lastApplied
		sc.cond.Broadcast()
		sc.unlock()
	}
}
