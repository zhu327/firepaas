package ebpf

import (
	"encoding/binary"
	"testing"
)

func TestDecodeFlowEvent(t *testing.T) {
	raw := make([]byte, 4+4+2+1+1+4+8)
	binary.LittleEndian.PutUint32(raw[0:4], 42)    // src
	binary.LittleEndian.PutUint32(raw[4:8], 99)    // dst
	binary.LittleEndian.PutUint16(raw[8:10], 80)   // dport
	raw[10] = 6                                    // TCP
	raw[11] = byte(FlowAllow)                      // verdict
	binary.LittleEndian.PutUint32(raw[12:16], 512) // len
	ev, ok := decodeFlowEvent(raw)
	if !ok || ev.SrcID != 42 || ev.DstID != 99 || ev.DPort != 80 || ev.Proto != 6 ||
		ev.Verdict != FlowAllow || ev.PktLen != 512 {
		t.Fatalf("decode = %+v ok=%v", ev, ok)
	}
	// 尺寸不匹配 → 拒绝（前向兼容：布局变化不得错位解析）。
	if _, ok := decodeFlowEvent(raw[:8]); ok {
		t.Fatal("short sample must be rejected")
	}
}

// fakeResolver 固定映射（correlate 单测）。
type fakeResolver map[uint32]FlowIdentity

func (f fakeResolver) Resolve(id uint32) (FlowIdentity, bool) {
	rec, ok := f[id]
	return rec, ok
}

func TestCorrelate(t *testing.T) {
	resolve := fakeResolver{
		42: {ProjectID: "p1", AppID: "web", MachineID: "m1", ExecutionID: "e1"},
		99: {ProjectID: "p2", AppID: "db", MachineID: "m2", ExecutionID: "e2"},
	}
	b := &Backend{}
	rec := b.correlate(FlowEvent{SrcID: 42, DstID: 99, Verdict: FlowDenyPolicy, DPort: 5432, Proto: 6}, resolve)
	if rec.SrcApp != "web" || rec.SrcProject != "p1" || rec.SrcMachine != "m1" || rec.SrcExecution != "e1" {
		t.Fatalf("src correlation = %+v", rec)
	}
	if rec.DstApp != "db" || rec.DstProject != "p2" {
		t.Fatalf("dst correlation = %+v", rec)
	}
	// 未知身份（残余流量）：坐标为空但 verdict 保留。
	rec2 := b.correlate(FlowEvent{SrcID: 777, Verdict: FlowDenySrc}, resolve)
	if rec2.SrcApp != "" || rec2.Verdict != FlowDenySrc {
		t.Fatalf("unknown identity = %+v", rec2)
	}
}

func TestFlowVerdictString(t *testing.T) {
	for v, want := range map[FlowVerdict]string{
		FlowAllow: "allow", FlowDenyPolicy: "deny_policy", FlowDenySrc: "deny_src",
		FlowDenyPort: "deny_port", FlowDenyDst: "deny_dst", FlowVerdict(99): "unknown",
	} {
		if got := v.String(); got != want {
			t.Fatalf("verdict %d = %q, want %q", v, got, want)
		}
	}
}
