package shardctrler

func (sc *ShardCtrler) applyMsg() {
	for msg := range sc.applyCh {
		if sc.killed() {
			return
		}
		if msg.CommandValid {

		} else {

		}
	}
}
