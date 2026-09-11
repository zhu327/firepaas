package main

import (
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/agent/network/ebpf"
)

// TestFlowLogLimiter：deny flow 日志每秒上限 + 窗口滚动汇总；高 pps 拒绝流量
// 不得产生逐包日志（可用性防护）。
func TestFlowLogLimiter(t *testing.T) {
	var l flowLogLimiter
	t0 := time.Unix(1000, 0)
	for i := 0; i < flowDenyLogsPerSecond; i++ {
		if ok, _ := l.admit(t0); !ok {
			t.Fatalf("event %d within budget must be admitted", i)
		}
	}
	if ok, report := l.admit(t0); ok || report != 0 {
		t.Fatalf("over-budget event admitted: ok=%v report=%d", ok, report)
	}
	// 同秒内继续抑制（累计 2 条）。
	l.admit(t0)
	l.admit(t0)
	ok, report := l.admit(t0.Add(time.Second))
	if !ok || report != 3 {
		t.Fatalf("new window: ok=%v report=%d, want true/3", ok, report)
	}
	// 新窗口预算重置。
	for i := 0; i < flowDenyLogsPerSecond-1; i++ {
		if ok, _ := l.admit(t0.Add(time.Second)); !ok {
			t.Fatalf("event %d in new window must be admitted", i)
		}
	}
}

// TestFlowLogSinkAllowSampling：allow 事件按 1/128 采样（与限速器无关）。
func TestFlowLogSinkAllowSampling(t *testing.T) {
	s := &flowLogSink{}
	for i := 0; i < 128; i++ {
		s.Observe(ebpf.FlowRecord{FlowEvent: ebpf.FlowEvent{Verdict: ebpf.FlowAllow}})
	}
	if s.allowSeen != 128 {
		t.Fatalf("allowSeen=%d", s.allowSeen)
	}
}
