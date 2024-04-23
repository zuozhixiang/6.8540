package kvraft

import "testing"

func TestLogger(t *testing.T) {
	debugf(GetMethod, 0, "zzx: %v", "123")
}
