package kvraft

import "fmt"

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
		//case DeleteType:
		//	{
		//		delete(kv.executed, op.Key)
		//	}
	}
	return ""
}

var opmap = map[OpType]string{
	GetType:    string(GetMethod),
	PutType:    string(PutMethod),
	AppendType: string(AppendMethod),
}

func formatCmd(op Op) string {
	return fmt.Sprintf("[%v][%v][%v][%v]", op.ID, opmap[op.Type], op.Key, op.Value)
}

func (kv *KVServer) applyMsgForStateMachine() {
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
				lastApplied = msg.CommandIndex
				op := msg.Command.(Op)
				if kv.checkExecuted(op.ID) {
					continue
				}
				res := kv.executeOp(op)
				op.Value = res
				debugf(Apply, kv.me, "%v", formatCmd(op))
			} else {
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
