package controller

import "testing"

// st() 必须对直接构造的 Controller 惰性初始化全部进程内对账状态，且多次调用
// 返回同一实例（New 之外的构造路径不得 panic）。
func TestReconcileStateLazyInit(t *testing.T) {
	c := &Controller{}
	st := c.st()
	if st == nil {
		t.Fatal("st() returned nil")
	}
	if st.nodeListFailures == nil || st.prefetchedRollouts == nil ||
		st.evacuatedNodes == nil || st.reportedOrphans == nil || st.machineLocks == nil {
		t.Fatalf("lazy state not fully initialized: %+v", st)
	}
	if c.st() != st {
		t.Fatal("st() must return the same state instance")
	}
	unlock := c.lockMachine("m1")
	unlock()
}
