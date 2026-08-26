package nodemanager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/example/firepaas/shared/gen/agent/v1"
)

func fakeNomad(t *testing.T, nodes []NomadNodeStub, allocs []NomadAllocStub, details map[string]NomadAllocStub) *httptest.Server {
	t.Helper()
	// P3-5：agentclient 现为 fail-closed mTLS；单测无证书，显式开 dev 明文。
	t.Setenv("FIREPAAS_AGENT_TLS_ALLOW_INSECURE", "true")
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nodes", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(nodes)
	})
	mux.HandleFunc("/v1/job/firepaas-agentd/allocations", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(allocs)
	})
	mux.HandleFunc("/v1/allocation/", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len("/v1/allocation/"):]
		if d, ok := details[id]; ok {
			_ = json.NewEncoder(w).Encode(d)
			return
		}
		http.NotFound(w, r)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func alloc(nodeID string, version uint64, running bool, grpcPort, proxyPort int) NomadAllocStub {
	status := "complete"
	if running {
		status = "running"
	}
	a := NomadAllocStub{ID: "alloc-" + nodeID, NodeID: nodeID, JobVersion: version, ClientStatus: status}
	a.AllocatedResources = &allocResources{}
	a.AllocatedResources.Shared.Ports = []struct {
		Label string `json:"Label"`
		Value int    `json:"Value"`
	}{
		{Label: "grpc", Value: grpcPort},
		{Label: "proxy", Value: proxyPort},
	}
	return a
}

func TestDiscoverBuildsNodeViews(t *testing.T) {
	nodes := []NomadNodeStub{
		{ID: "n1", Name: "node-1", NodePool: "compute", Status: "ready",
			SchedulingEligibility: "eligible", HTTPAddr: "10.0.0.1:4646"},
		{ID: "n2", Name: "node-2", NodePool: "compute", Status: "ready",
			SchedulingEligibility: "eligible", Drain: true, HTTPAddr: "10.0.0.2:4646"},
	}
	allocs := []NomadAllocStub{alloc("n1", 3, true, 5108, 5107), alloc("n2", 3, true, 5108, 5107)}
	srv := fakeNomad(t, nodes, allocs, nil)

	m, err := New(Config{NomadAddr: srv.URL, JobName: "firepaas-agentd"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := m.Nodes()
	if len(got) != 2 {
		t.Fatalf("want 2 nodes, got %d", len(got))
	}
	byID := map[string]Node{}
	for _, n := range got {
		byID[n.NomadNodeID] = n
	}
	if byID["n1"].GRPCAddr != "10.0.0.1:5108" || byID["n1"].ProxyAddr != "10.0.0.1:5107" {
		t.Errorf("node-1 addrs wrong: %+v", byID["n1"])
	}
	if byID["n1"].Status != "UNKNOWN" {
		t.Errorf("node-1 before ServiceInfo should be UNKNOWN, got %s", byID["n1"].Status)
	}
	if byID["n2"].Status != "DRAINING" {
		t.Errorf("draining node should be DRAINING, got %s", byID["n2"].Status)
	}
	if m.ClientFor("n1") == nil {
		t.Error("node-1 should have a dialed gRPC client")
	}
}

func TestDiscoverFillsPortsFromAllocDetail(t *testing.T) {
	nodes := []NomadNodeStub{
		{ID: "n1", Name: "node-1", NodePool: "compute", Status: "ready",
			SchedulingEligibility: "eligible", HTTPAddr: "10.0.0.1:4646"},
	}
	// 列表接口（Nomad 实际行为）不带 AllocatedResources。
	stub := alloc("n1", 3, true, 0, 0)
	stub.AllocatedResources = nil
	detail := alloc("n1", 3, true, 5108, 5107)
	srv := fakeNomad(t, nodes, []NomadAllocStub{stub}, map[string]NomadAllocStub{"alloc-n1": detail})

	m, err := New(Config{NomadAddr: srv.URL, JobName: "firepaas-agentd"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := m.Nodes()
	if len(got) != 1 || got[0].GRPCAddr != "10.0.0.1:5108" || got[0].ProxyAddr != "10.0.0.1:5107" {
		t.Fatalf("ports not filled from alloc detail: %+v", got)
	}
}

func TestCombinedStatusMachine(t *testing.T) {
	cases := []struct {
		name        string
		ready, elig bool
		drain       bool
		infoStatus  pb.ServiceInfoResponse_Status
		grpcAddr    string
		want        string
	}{
		{"healthy", true, true, false, pb.ServiceInfoResponse_HEALTHY, "10.0.0.1:5108", "HEALTHY"},
		{"draining", true, true, true, pb.ServiceInfoResponse_HEALTHY, "10.0.0.1:5108", "DRAINING"},
		{"not-ready", false, true, false, pb.ServiceInfoResponse_HEALTHY, "10.0.0.1:5108", "UNHEALTHY"},
		{"ineligible", true, false, false, pb.ServiceInfoResponse_HEALTHY, "10.0.0.1:5108", "UNHEALTHY"},
		{"agent-unhealthy", true, true, false, pb.ServiceInfoResponse_UNHEALTHY, "10.0.0.1:5108", "UNHEALTHY"},
		{"no-grpc", true, true, false, pb.ServiceInfoResponse_HEALTHY, "", "UNKNOWN"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := &Node{Ready: tc.ready, Eligible: tc.elig, Drain: tc.drain, GRPCAddr: tc.grpcAddr}
			info := &pb.ServiceInfoResponse{Status: tc.infoStatus}
			if got := combinedStatus(n, info); got != tc.want {
				t.Fatalf("want %s, got %s", tc.want, got)
			}
		})
	}
}

func TestDiscoverFailureMarksUnknown(t *testing.T) {
	nodes := []NomadNodeStub{
		{ID: "n1", Name: "node-1", NodePool: "compute", Status: "ready",
			SchedulingEligibility: "eligible", HTTPAddr: "10.0.0.1:4646"},
	}
	srv := fakeNomad(t, nodes, nil, nil)
	m, err := New(Config{NomadAddr: srv.URL, JobName: "firepaas-agentd"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv.Close() // 断开后再发现：保留快照并置 UNKNOWN
	if err := m.Discover(context.Background()); err == nil {
		t.Fatal("want error after nomad is gone")
	}
	got := m.Nodes()
	if len(got) != 1 || got[0].Status != "UNKNOWN" {
		t.Fatalf("snapshot should survive with UNKNOWN, got %+v", got)
	}
}
