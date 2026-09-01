package standby

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/kernel/hypeman/lib/autostandby"

	"github.com/zhu327/firepaas/internal/agent/probeflow"
)

type fakeStream struct {
	events chan autostandby.ConnectionEvent
	closed chan struct{}
}

func (f *fakeStream) Events() <-chan autostandby.ConnectionEvent { return f.events }
func (f *fakeStream) Errors() <-chan error                       { return nil }
func (f *fakeStream) Close() error {
	close(f.closed)
	return nil
}

type fakeSource struct {
	conns  []autostandby.Connection
	stream *fakeStream
}

func (f *fakeSource) ListConnections(context.Context) ([]autostandby.Connection, error) {
	return f.conns, nil
}

func (f *fakeSource) OpenStream(context.Context) (autostandby.ConnectionStream, error) {
	return f.stream, nil
}

func conn(src, dst string) autostandby.Connection {
	return autostandby.Connection{
		OriginalSourceIP:        netip.MustParseAddr(src),
		OriginalDestinationIP:   netip.MustParseAddr(dst),
		OriginalSourcePort:      40000,
		OriginalDestinationPort: 8080,
		TCPState:                autostandby.TCPStateEstablished,
	}
}

// v1.1（ADR-0017 决策 3 的实现形态）：探针连接（registry 登记）被剔除；
// 真实流量（proxy 转发）正常进入 conntrack 视图。
func TestFilteredSourceExcludesProbeFlows(t *testing.T) {
	reg := probeflow.NewRegistry(time.Hour)
	reg.Record(40000, netip.MustParseAddrPort("10.100.0.5:8080"))

	src := &fakeSource{
		conns: []autostandby.Connection{
			conn("10.12.0.1", "10.100.0.5"), // probe（登记）→ 剔除
			conn("10.12.0.1", "10.100.0.6"), // proxy 转发（未登记）→ 保留
		},
		stream: &fakeStream{events: make(chan autostandby.ConnectionEvent, 4), closed: make(chan struct{})},
	}
	filtered := NewFilteredSource(src, reg)

	conns, err := filtered.ListConnections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(conns) != 1 || conns[0].OriginalDestinationIP.String() != "10.100.0.6" {
		t.Fatalf("filtered dump: %+v", conns)
	}

	stream, err := filtered.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		src.stream.events <- autostandby.ConnectionEvent{Connection: conn("10.12.0.1", "10.100.0.5")}
		src.stream.events <- autostandby.ConnectionEvent{Connection: conn("10.12.0.1", "10.100.0.7")}
		close(src.stream.events)
	}()
	var got []string
	for ev := range stream.Events() {
		got = append(got, ev.Connection.OriginalDestinationIP.String())
	}
	if len(got) != 1 || got[0] != "10.100.0.7" {
		t.Fatalf("filtered events: %v", got)
	}
}
