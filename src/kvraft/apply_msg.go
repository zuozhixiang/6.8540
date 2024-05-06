package kvraft

import (
	"6.5840/raft"
	"fmt"
)

func (kv *KVServer) executeOp(op Op) string {
	switch op.Type {
	case PutType:
		{
			kv.executed[op.ID] = true
			kv.data[op.Key] = op.Value
			return op.Value
		}
	case AppendType:
		{
			kv.executed[op.ID] = true
			kv.data[op.Key] = kv.data[op.Key] + op.Value
			return kv.data[op.Key]
		}
	case GetType:
		{
			kv.executed[op.ID] = true
			kv.versionData[op.ID] = kv.data[op.Key]
			return kv.data[op.Key]
		}
	case DeleteType:
		{
			delete(kv.executed, op.Key)
			delete(kv.versionData, op.Key)
		}
	}
	return ""
}

var opmap = map[OpType]string{
	GetType:    string(GetMethod),
	PutType:    string(PutMethod),
	AppendType: string(AppendMethod),
	DeleteType: "Delete",
}

func formatCmd(op Op) string {
	return fmt.Sprintf("[%v][%v][%v][%v]", op.ID, opmap[op.Type], op.Key, op.Value)
}

func (kv *KVServer) applyMsgForStateMachine() {

	for {
		if kv.killed() {
			return
		}
		msg := <-kv.applyCh
		needApplyMsg := []raft.ApplyMsg{msg}
		size := len(kv.applyCh)
		for i := 0; i < size; i++ {
			needApplyMsg = append(needApplyMsg, <-kv.applyCh)
		}
		kv.lock()
		lastApplied := kv.lastAppliedIndex
		for _, msg := range needApplyMsg {
			if msg.CommandValid {
				if msg.CommandIndex <= lastApplied {
					errmsg := fmt.Sprintf("[S%v], msg index: %v, lastApplied: %v, msg: %v", kv.me, msg.CommandIndex, lastApplied, msg)
					panic(errmsg)
				}
				lastApplied = msg.CommandIndex
				op := msg.Command.(Op)
				if kv.checkExecuted(op.ID) {
					continue
				}
				res := kv.executeOp(op)
				op.Value = res
				debugf(Apply, kv.me, "idx: %v, %v", msg.CommandIndex, formatCmd(op))
			} else {
				if msg.SnapshotIndex <= lastApplied {
					errmsg := fmt.Sprintf("[S%v], snapshot index: %v, lastApplied: %v, msg: %v", kv.me, msg.SnapshotIndex, lastApplied, raft.GetPrintMsg([]raft.ApplyMsg{msg}))
					panic(errmsg)
				}
				lastApplied = msg.SnapshotIndex
				kv.applySnapshot(msg.Snapshot)
				debugf(AppSnap, kv.me, "snapIndex: %v, snapTerm: %v", msg.SnapshotIndex, msg.SnapshotTerm)
			}
		}
		kv.lastAppliedIndex = lastApplied
		kv.cond.Broadcast()
		kv.unlock()
		//if len(kv.applyCh) == 0 {
		//	time.Sleep(10 * time.Millisecond)
		//}
	}

	for !kv.killed() {
		kv.lock()
		size := len(kv.applyCh)
		lastApplied := kv.lastAppliedIndex
		isNotify := false
		for i := 0; i < size; i++ {
			msg, ok := <-kv.applyCh
			if !ok {
				panic("chan close")
			}
			isNotify = true
			if msg.CommandValid {
				if msg.CommandIndex <= lastApplied {
					errmsg := fmt.Sprintf("[S%v], msg index: %v, lastApplied: %v, msg: %v", kv.me, msg.CommandIndex, lastApplied, msg)
					panic(errmsg)
					fmt.Println(errmsg)
					continue
				}
				lastApplied = msg.CommandIndex
				op := msg.Command.(Op)
				if kv.checkExecuted(op.ID) {
					continue
				}
				res := kv.executeOp(op)
				op.Value = res
				debugf(Apply, kv.me, "idx: %v, %v", msg.CommandIndex, formatCmd(op))
			} else {
				if msg.SnapshotIndex <= lastApplied {
					errmsg := fmt.Sprintf("[S%v], snapshot index: %v, lastApplied: %v, msg: %v", kv.me, msg.SnapshotIndex, lastApplied, raft.GetPrintMsg([]raft.ApplyMsg{msg}))
					panic(errmsg)
					continue
				}
				lastApplied = msg.SnapshotIndex
				kv.applySnapshot(msg.Snapshot)
				debugf(AppSnap, kv.me, "snapIndex: %v, snapTerm: %v", msg.SnapshotIndex, msg.SnapshotTerm)
			}
		}
		kv.lastAppliedIndex = lastApplied
		if isNotify {
			kv.cond.Broadcast()
		}
		kv.unlock()
	}
}
