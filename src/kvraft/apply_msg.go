package kvraft

func (kv *KVServer) executeOp(op Op) {
	switch op.Type {
	case PutType:
		{
			kv.data[op.Key] = op.Value
		}
	case AppendType:
		{
			kv.data[op.Key] = kv.data[op.Key] + op.Value
		}
	case GetType:
		{
			kv.versionData[op.ID] = kv.data[op.Key]
		}
	}
	kv.executed[op.ID] = true
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
				kv.executeOp(op)
				debugf(Apply, kv.me, "msg: %v", toJson(msg))
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
