package scheduler

import (
	"errors"
	"math/rand"
	"testing"
)

func healthyNode(id, pool string, cpu, mem uint64) Node {
	return Node{
		ID: id, Pool: pool, Labels: map[string]string{"arch": "x86_64"},
		Status: StatusHealthy, CPUTotal: cpu, MemTotalMib: mem,
	}
}

func TestFilterBeforeScore(t *testing.T) {
	nodes := []Node{
		func() Node {
			n := healthyNode("unhealthy-cheap", "compute", 64, 65536)
			n.Status = StatusUnhealthy // 分数再低也不能被采样打分
			return n
		}(),
		func() Node {
			n := healthyNode("busy", "compute", 64, 65536)
			n.CPUAllocated = 200 // 已超售但仍在 R=4 硬准入内
			return n
		}(),
		healthyNode("idle", "compute", 64, 65536),
	}
	var scored []string
	p := New(DefaultBestOfKConfig(), Options{ScoreHook: func(id string) { scored = append(scored, id) }})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512}, nodes, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if pl.NodeID != "idle" {
		t.Fatalf("want idle (lowest load), got %s", pl.NodeID)
	}
	for _, id := range scored {
		for _, n := range nodes {
			if n.ID == id && n.Status != StatusHealthy {
				t.Fatalf("unhealthy node %s was scored: filter must run before scoring", id)
			}
		}
	}
	if len(scored) == 0 {
		t.Fatal("score hook never called")
	}
}

func TestHardAdmissionRejectsOvercommit(t *testing.T) {
	nodes := []Node{
		func() Node {
			n := healthyNode("full-mem", "compute", 64, 4096)
			n.MemAllocated = 4096 // 已打满
			return n
		}(),
		func() Node {
			n := healthyNode("cpu-full", "compute", 8, 65536)
			n.CPUAllocated = 8 * 4 // R=4 已打满
			return n
		}(),
	}
	p := New(DefaultBestOfKConfig(), Options{})
	if _, err := p.Place(Request{VCPU: 1, MemMib: 512}, nodes, rand.New(rand.NewSource(1))); err == nil {
		t.Fatal("want no candidates when all nodes violate hard admission")
	} else {
		var nce ErrNoCandidates
		if !errors.As(err, &nce) {
			t.Fatalf("want ErrNoCandidates, got %T", err)
		}
	}
}

func TestAntiAffinitySpreadsReplicas(t *testing.T) {
	nodes := []Node{
		healthyNode("n1", "compute", 64, 65536),
		healthyNode("n2", "compute", 64, 65536),
	}
	p := New(DefaultBestOfKConfig(), Options{})

	// 第一个副本落点随机；之后强制把已占节点放入排除集。
	first, err := p.Place(Request{VCPU: 1, MemMib: 512, DeploymentID: "d1", AntiAffinity: true,
		ExistingDeploymentNodes: map[string]bool{}}, nodes, rand.New(rand.NewSource(7)))
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Place(Request{VCPU: 1, MemMib: 512, DeploymentID: "d1", AntiAffinity: true,
		ExistingDeploymentNodes: map[string]bool{first.NodeID: true}}, nodes, rand.New(rand.NewSource(7)))
	if err != nil {
		t.Fatal(err)
	}
	if second.NodeID == first.NodeID {
		t.Fatalf("anti-affinity broken: both replicas on %s", first.NodeID)
	}
}

func TestAntiAffinityDegradesWhenSingleNode(t *testing.T) {
	nodes := []Node{healthyNode("only", "compute", 64, 65536)}
	p := New(DefaultBestOfKConfig(), Options{})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512, DeploymentID: "d1", AntiAffinity: true,
		ExistingDeploymentNodes: map[string]bool{"only": true}}, nodes, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if pl.NodeID != "only" {
		t.Fatalf("degradation must fall back to the only node, got %s", pl.NodeID)
	}
	hasDegraded := false
	for _, ev := range pl.Events {
		if ev.Reason == "anti_affinity degraded: no node outside deployment set" {
			hasDegraded = true
		}
	}
	if !hasDegraded {
		t.Fatal("degradation must emit a scheduler event")
	}
}

func TestLostNodeExcluded(t *testing.T) {
	nodes := []Node{
		healthyNode("lost", "compute", 64, 65536),
		healthyNode("ok", "compute", 64, 65536),
	}
	nodes[0].Status = StatusUnhealthy
	p := New(DefaultBestOfKConfig(), Options{})
	for i := 0; i < 50; i++ {
		pl, err := p.Place(Request{VCPU: 1, MemMib: 512}, nodes, rand.New(rand.NewSource(int64(i))))
		if err != nil {
			t.Fatal(err)
		}
		if pl.NodeID == "lost" {
			t.Fatal("lost node must never be selected")
		}
	}
}

func TestPoolAndLabelsFilter(t *testing.T) {
	nodes := []Node{
		func() Node { n := healthyNode("control-node", "control", 64, 65536); return n }(),
		func() Node {
			n := healthyNode("wrong-arch", "compute", 64, 65536)
			n.Labels["arch"] = "arm64"
			return n
		}(),
		healthyNode("right", "compute", 64, 65536),
	}
	p := New(DefaultBestOfKConfig(), Options{})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512, Pool: "compute",
		Labels: map[string]string{"arch": "x86_64"}}, nodes, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if pl.NodeID != "right" {
		t.Fatalf("want right node, got %s", pl.NodeID)
	}
}

func TestZeroCapacityRejected(t *testing.T) {
	nodes := []Node{healthyNode("zero", "compute", 0, 65536)}
	p := New(DefaultBestOfKConfig(), Options{})
	if _, err := p.Place(Request{VCPU: 1, MemMib: 512}, nodes, rand.New(rand.NewSource(1))); err == nil {
		t.Fatal("zero-capacity node must be rejected (unknown capacity is unsafe)")
	}
}

func TestDeterministicTieBreak(t *testing.T) {
	nodes := []Node{
		healthyNode("b", "compute", 64, 65536),
		healthyNode("a", "compute", 64, 65536),
	}
	p := New(DefaultBestOfKConfig(), Options{})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512}, nodes, rand.New(rand.NewSource(42)))
	if err != nil {
		t.Fatal(err)
	}
	if pl.NodeID != "a" {
		t.Fatalf("equal scores must tie-break by node ID, got %s", pl.NodeID)
	}
}

func TestPendingAccountingCountsAgainstAdmission(t *testing.T) {
	nodes := []Node{
		func() Node {
			n := healthyNode("pending-full", "compute", 64, 65536)
			n.MemPending = 65536 // 在途承诺已打满
			return n
		}(),
		healthyNode("idle", "compute", 64, 65536),
	}
	p := New(DefaultBestOfKConfig(), Options{})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512}, nodes, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if pl.NodeID != "idle" {
		t.Fatalf("pending accounting must reject overcommitted node, got %s", pl.NodeID)
	}
}

// M5.5：排水节点不进候选（filter_rejection reason=draining）。
func TestPlacementExcludesDraining(t *testing.T) {
	nodes := []Node{
		{ID: "n1", Pool: "compute", Status: StatusHealthy, CPUTotal: 8, MemTotalMib: 16000},
		{ID: "n2", Pool: "compute", Status: StatusHealthy, Draining: true, CPUTotal: 8, MemTotalMib: 16000},
	}
	req := Request{VCPU: 1, MemMib: 128}
	p := New(DefaultBestOfKConfig(), Options{})
	placement, err := p.Place(req, nodes, nil)
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if placement.NodeID != "n1" {
		t.Fatalf("placed on %s, want n1", placement.NodeID)
	}
	found := false
	for _, e := range placement.Events {
		if e.Kind == "filter_rejection" && e.NodeID == "n2" && e.Reason == "draining" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing draining rejection event: %+v", placement.Events)
	}
}
