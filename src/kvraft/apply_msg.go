package kvraft

func (kv *KVServer) applyMsgForStateMachine() {
	for !kv.killed() {
		kv.lock()
		size := len(kv.applyCh)
		isNotify := false
		for i := 0; i < size; i++ {
			msg, ok := <-kv.applyCh
			if !ok {
				break
			}
			isNotify = true
			if msg.CommandValid {
				op := msg.Command.(Op)
				switch op.Type {
				case PutType:
					{
						kv.executed[op.ID] = true
						kv.data[op.Key] = op.Value
					}
				case AppendType:
					{
						kv.executed[op.ID] = true
						kv.data[op.Key] = kv.data[op.Key] + op.Value
					}
				}
			}
		}
		kv.lastAppliedIndex += size
		if isNotify {
			kv.cond.Broadcast()
		}
		kv.unlock()
	}
}
