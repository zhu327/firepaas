package nodemanager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

func fakeNomad(
	t *testing.T,
	nodes []NomadNodeStub,
	allocs []NomadAllocStub,
	details map[string]NomadAllocStub,
) *httptest.Server {
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
		HostIP string `json:"HostIP"`
		Label  string `json:"Label"`
		Value  int    `json:"Value"`
	}{
		{Label: "grpc", Value: grpcPort},
		{Label: "proxy", Value: proxyPort},
	}
	return a
}

func TestDiscoverBuildsNodeViews(t *testing.T) {
	nodes := []NomadNodeStub{
		{
			ID: "n1", Name: "node-1", NodePool: "compute", Status: "ready",
			SchedulingEligibility: "eligible", HTTPAddr: "10.0.0.1:4646",
		},
		{
			ID: "n2", Name: "node-2", NodePool: "compute", Status: "ready",
			SchedulingEligibility: "eligible", Drain: true, HTTPAddr: "10.0.0.2:4646",
		},
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

func TestDiscoverSelectsRoutableNomadNodeAddress(t *testing.T) {
	tests := []struct {
		name, address, httpAddr, grpcAddr, proxyAddr string
	}{
		{name: "IPv4 Address", address: "10.0.0.1", grpcAddr: "10.0.0.1:5108", proxyAddr: "10.0.0.1:5107"},
		{name: "IPv6 Address", address: "2001:db8::1", grpcAddr: "[2001:db8::1]:5108", proxyAddr: "[2001:db8::1]:5107"},
		{
			name:      "Address preferred",
			address:   "10.0.0.2",
			httpAddr:  "10.0.0.1:4646",
			grpcAddr:  "10.0.0.2:5108",
			proxyAddr: "10.0.0.2:5107",
		},
		{name: "legacy HTTPAddr", httpAddr: "10.0.0.1:4646", grpcAddr: "10.0.0.1:5108", proxyAddr: "10.0.0.1:5107"},
		{
			name:      "legacy hostname",
			httpAddr:  "compute-1.internal:4646",
			grpcAddr:  "compute-1.internal:5108",
			proxyAddr: "compute-1.internal:5107",
		},
		{name: "empty", grpcAddr: "", proxyAddr: ""},
		{name: "invalid", address: "not an IP", httpAddr: "://bad", grpcAddr: "", proxyAddr: ""},
		{name: "IPv4 loopback", address: "127.0.0.1", grpcAddr: "", proxyAddr: ""},
		{name: "IPv6 loopback", address: "::1", grpcAddr: "", proxyAddr: ""},
		{name: "localhost", httpAddr: "localhost:4646", grpcAddr: "", proxyAddr: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := []NomadNodeStub{{
				ID: "n1", Name: "node-1", NodePool: "compute", Status: "ready",
				SchedulingEligibility: "eligible", Address: tt.address, HTTPAddr: tt.httpAddr,
			}}
			srv := fakeNomad(t, nodes, []NomadAllocStub{alloc("n1", 3, true, 5108, 5107)}, nil)
			m, err := New(Config{NomadAddr: srv.URL, JobName: "firepaas-agentd"})
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if err := m.Discover(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := m.Nodes()
			if len(got) != 1 || got[0].GRPCAddr != tt.grpcAddr || got[0].ProxyAddr != tt.proxyAddr {
				t.Fatalf("unexpected addresses: %+v", got)
			}
		})
	}
}

func TestDiscoverUsesAllocHostIPWhenNodeAddressIsLoopback(t *testing.T) {
	nodes := []NomadNodeStub{{
		ID: "n1", Name: "node-1", NodePool: "compute", Status: "ready",
		SchedulingEligibility: "eligible", Address: "127.0.0.1",
	}}
	detail := alloc("n1", 3, true, 5108, 5107)
	for i := range detail.AllocatedResources.Shared.Ports {
		detail.AllocatedResources.Shared.Ports[i].HostIP = "10.0.0.1"
	}
	stub := alloc("n1", 3, true, 0, 0)
	stub.AllocatedResources = nil
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
		t.Fatalf("alloc host IP not used: %+v", got)
	}
}

func TestDiscoverFillsPortsFromAllocDetail(t *testing.T) {
	nodes := []NomadNodeStub{
		{
			ID: "n1", Name: "node-1", NodePool: "compute", Status: "ready",
			SchedulingEligibility: "eligible", HTTPAddr: "10.0.0.1:4646",
		},
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

func TestFollowerDiscoveryResolvesAgentClient(t *testing.T) {
	nodes := []NomadNodeStub{{
		ID: "nomad-1", Name: "node-1", Status: "ready",
		SchedulingEligibility: "eligible", HTTPAddr: "10.0.0.1:4646",
	}}
	srv := fakeNomad(t, nodes, []NomadAllocStub{alloc("nomad-1", 1, true, 5108, 5107)}, nil)
	m, err := New(Config{NomadAddr: srv.URL, JobName: "firepaas-agentd"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := m.ClientForNodeID("nomad-1"); got == nil {
		t.Fatal("follower discovery should resolve a client without ServiceInfo or PG sync")
	}
}

func TestLeaderHandoverDoesNotClearDiscoveryClients(t *testing.T) {
	nodes := []NomadNodeStub{{
		ID: "n1", Name: "node-1", Status: "ready",
		SchedulingEligibility: "eligible", HTTPAddr: "10.0.0.1:4646",
	}}
	srv := fakeNomad(t, nodes, []NomadAllocStub{alloc("n1", 1, true, 5108, 5107)}, nil)
	m, err := New(Config{NomadAddr: srv.URL, JobName: "firepaas-agentd", InfoEvery: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := m.ClientForNodeID("n1")
	if before == nil {
		t.Fatal("missing discovery client before handover")
	}

	// Losing leadership cancels only the PG-writing ServiceInfo loop. A new leader
	// term may start it again; neither transition owns or closes discovery clients.
	for i := 0; i < 2; i++ {
		term, cancel := context.WithCancel(context.Background())
		cancel()
		if err := m.RunServiceInfo(term); err == nil {
			t.Fatal("cancelled leader term should return an error")
		}
		if got := m.ClientForNodeID("n1"); got != before {
			t.Fatalf("handover %d replaced or cleared the follower discovery client", i+1)
		}
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
		{
			ID: "n1", Name: "node-1", NodePool: "compute", Status: "ready",
			SchedulingEligibility: "eligible", HTTPAddr: "10.0.0.1:4646",
		},
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
