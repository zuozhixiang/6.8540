package shardctrler

import (
	"6.5840/raft"
	"fmt"
)

func (sc *ShardCtrler) executeOp(op Op) string {

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
			} else {
				if msg.SnapshotIndex <= lastApplied {
					errmsg := fmt.Sprintf("[S%v], snapshot index: %v, lastApplied: %v, msg: %v", sc.me, msg.SnapshotIndex, lastApplied, raft.GetPrintMsg([]raft.ApplyMsg{msg}))
					panic(errmsg)
				}
				lastApplied = msg.SnapshotIndex
				sc.applySnapshot(msg.Snapshot)
				// debugf(AppSnap, sc.me, "snapIndex: %v, snapTerm: %v", msg.SnapshotIndex, msg.SnapshotTerm)
			}
		}
		sc.lastAppliedIndex = lastApplied
		sc.cond.Broadcast()
		sc.unlock()
	}
}
