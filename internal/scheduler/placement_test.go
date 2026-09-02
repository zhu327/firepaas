package scheduler

import (
	"errors"
	"math/rand"
	"strings"
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

// TestCapabilityHardFilter 覆盖 v1.2-A（ADR-0023）：启动必需能力在资源
// 打分前硬过滤；缺能力的节点即使资源更空闲也不能被选中，且产生可解释的
// filter_rejection 事件。
func TestCapabilityHardFilter(t *testing.T) {
	nodes := []Node{
		healthyNode("no-cap", "compute", 64, 65536),
		func() Node {
			n := healthyNode("has-cap", "compute", 64, 65536)
			n.FeatureIDs = map[string]bool{"secret.oneshot.v1": true}
			return n
		}(),
	}
	p := New(DefaultBestOfKConfig(), Options{})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512, RequiredFeatures: []string{"secret.oneshot.v1"}},
		nodes, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatalf("want placement on capable node, got %v", err)
	}
	if pl.NodeID != "has-cap" {
		t.Fatalf("want has-cap, got %s", pl.NodeID)
	}
	var sawCapReject bool
	for _, ev := range pl.Events {
		if ev.Kind == "filter_rejection" && ev.NodeID == "no-cap" &&
			strings.Contains(ev.Reason, "secret.oneshot.v1") {
			sawCapReject = true
		}
	}
	if !sawCapReject {
		t.Fatalf("want capability filter rejection event, got %+v", pl.Events)
	}
}

// TestCapabilityNoSupportNodeFailsClosed 覆盖验收：无支持节点时返回可解释的
// terminal reason（ErrNoCandidates + capability 事件），不静默降级。
func TestCapabilityNoSupportNodeFailsClosed(t *testing.T) {
	nodes := []Node{healthyNode("only", "compute", 64, 65536)}
	p := New(DefaultBestOfKConfig(), Options{})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512, RequiredFeatures: []string{"secret.oneshot.v1"}},
		nodes, rand.New(rand.NewSource(1)))
	if err == nil {
		t.Fatal("want no candidates")
	}
	var nce ErrNoCandidates
	if !errors.As(err, &nce) {
		t.Fatalf("want ErrNoCandidates, got %T", err)
	}
	found := false
	for _, ev := range pl.Events {
		if ev.Kind == "filter_rejection" && strings.Contains(ev.Reason, "capability missing") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want capability rejection event, got %+v", pl.Events)
	}
}

// TestUnknownFeatureIgnored：未知 feature 不破坏旧客户端/节点（只按集合成员
// 匹配，required 为空时完全不影响放置）。
func TestUnknownFeatureIgnored(t *testing.T) {
	nodes := []Node{
		func() Node {
			n := healthyNode("n1", "compute", 64, 65536)
			n.FeatureIDs = map[string]bool{"future.gpu.v9": true}
			return n
		}(),
	}
	p := New(DefaultBestOfKConfig(), Options{})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512}, nodes, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatalf("unknown feature must be ignored: %v", err)
	}
	if pl.NodeID != "n1" {
		t.Fatalf("want n1, got %s", pl.NodeID)
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
	first, err := p.Place(Request{
		VCPU: 1, MemMib: 512, DeploymentID: "d1", AntiAffinity: true,
		ExistingDeploymentNodes: map[string]bool{},
	}, nodes, rand.New(rand.NewSource(7)))
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Place(Request{
		VCPU: 1, MemMib: 512, DeploymentID: "d1", AntiAffinity: true,
		ExistingDeploymentNodes: map[string]bool{first.NodeID: true},
	}, nodes, rand.New(rand.NewSource(7)))
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
	pl, err := p.Place(Request{
		VCPU: 1, MemMib: 512, DeploymentID: "d1", AntiAffinity: true,
		ExistingDeploymentNodes: map[string]bool{"only": true},
	}, nodes, rand.New(rand.NewSource(1)))
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
	pl, err := p.Place(Request{
		VCPU: 1, MemMib: 512, Pool: "compute",
		Labels: map[string]string{"arch": "x86_64"},
	}, nodes, rand.New(rand.NewSource(1)))
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

// v1.1（ADR-0018）：镜像缓存亲和——同资源分下已缓存节点必被选中；
// 未缓存节点不被过滤（新镜像永远有候选）；WeightImage=0 关闭罚项。
func TestImageAffinityPrefersCachedNode(t *testing.T) {
	cached := healthyNode("cached", "compute", 64, 65536)
	cached.CachedImageDigests = map[string]bool{"sha256:abc": true}
	cold := healthyNode("cold", "compute", 64, 65536)
	p := New(DefaultBestOfKConfig(), Options{})
	// K=2 且只有 2 个候选：两者都会被采样，缓存命中者得分更低。
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512, ImageDigest: "sha256:abc"},
		[]Node{cached, cold}, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if pl.NodeID != "cached" {
		t.Fatalf("want cached node, got %s", pl.NodeID)
	}
}

func TestImageAffinityDoesNotFilterUncached(t *testing.T) {
	cold := healthyNode("only", "compute", 64, 65536)
	p := New(DefaultBestOfKConfig(), Options{})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512, ImageDigest: "sha256:abc"},
		[]Node{cold}, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatalf("uncached-only cluster must still place: %v", err)
	}
	if pl.NodeID != "only" {
		t.Fatalf("want only node, got %s", pl.NodeID)
	}
}

func TestImageAffinityDisabledWithZeroWeight(t *testing.T) {
	// cached-busy：已缓存镜像但负载高；empty-cold：未缓存但更空。
	// 默认权重：亲和罚项（0.5）大于负载差 → 选 cached-busy；
	// WeightImage=0：纯资源分 → 选 empty-cold。
	cachedBusy := healthyNode("cached-busy", "compute", 64, 65536)
	cachedBusy.CachedImageDigests = map[string]bool{"sha256:abc": true}
	cachedBusy.CPUAllocated = 40
	emptyCold := healthyNode("empty-cold", "compute", 64, 65536)

	pOn := New(DefaultBestOfKConfig(), Options{})
	plOn, err := pOn.Place(Request{VCPU: 1, MemMib: 512, ImageDigest: "sha256:abc"},
		[]Node{cachedBusy, emptyCold}, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if plOn.NodeID != "cached-busy" {
		t.Fatalf("default weight must prefer cached node, got %s", plOn.NodeID)
	}

	// P1#14a：经 New 归一后 WeightImage 必被补成默认 0.5（0 视为未设置——
	// 半配置不再静默丢失镜像亲和）；打分层仍把 0 视作关闭，仅直接构造
	// Placer（不经 New）时可表达。
	cfg := DefaultBestOfKConfig()
	cfg.WeightImage = 0
	if got := New(cfg, Options{}).cfg.WeightImage; got != 0.5 {
		t.Fatalf("New must fill WeightImage default on partial config, got %v", got)
	}
	pOff := &Placer{cfg: cfg}
	plOff, err := pOff.Place(Request{VCPU: 1, MemMib: 512, ImageDigest: "sha256:abc"},
		[]Node{cachedBusy, emptyCold}, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if plOff.NodeID != "empty-cold" {
		t.Fatalf("weight=0 must ignore image cache (resource score decides), got %s", plOff.NodeID)
	}

	// 显式关闭的公共入口：Options.ImageAffinityDisabled 保留 WeightImage=0
	// （FIREPAAS_SCHED_WEIGHT_IMAGE=0 的接线路径）。
	pOffPublic := New(cfg, Options{ImageAffinityDisabled: true})
	if got := pOffPublic.cfg.WeightImage; got != 0 {
		t.Fatalf("ImageAffinityDisabled must keep WeightImage=0, got %v", got)
	}
	plOff2, err := pOffPublic.Place(Request{VCPU: 1, MemMib: 512, ImageDigest: "sha256:abc"},
		[]Node{cachedBusy, emptyCold}, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if plOff2.NodeID != "empty-cold" {
		t.Fatalf("disabled affinity must behave like weight=0, got %s", plOff2.NodeID)
	}
}

func TestImageAffinityNilCacheTreatedAsMiss(t *testing.T) {
	// 未上报缓存（nil）与上报空集合同罚（proto3 无法区分二者；不罚会让
	// 未上报者永远战胜已缓存节点）。
	other := healthyNode("other-cache", "compute", 64, 65536)
	other.CachedImageDigests = map[string]bool{"sha256:other": true}
	nilCache := healthyNode("nil-cache", "compute", 64, 65536)
	p := New(DefaultBestOfKConfig(), Options{})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512, ImageDigest: "sha256:abc"},
		[]Node{other, nilCache}, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	// 两者都 miss，同资源分 → 均可接受（tie-break 确定）。
	if pl.NodeID != "other-cache" && pl.NodeID != "nil-cache" {
		t.Fatalf("unexpected node %s", pl.NodeID)
	}
}

func TestPlacementAndPrefetchShareHardCandidatePolicy(t *testing.T) {
	base := healthyNode("eligible", "compute", 8, 8192)
	base.FeatureIDs = map[string]bool{"required": true}
	base.DiskTotalMib = 4096

	tests := []struct {
		name string
		node Node
	}{
		{
			name: "unhealthy",
			node: func() Node { n := base; n.ID = "unhealthy"; n.Status = StatusUnhealthy; return n }(),
		},
		{name: "draining", node: func() Node { n := base; n.ID = "draining"; n.Draining = true; return n }()},
		{name: "pool", node: func() Node { n := base; n.ID = "pool"; n.Pool = "other"; return n }()},
		{
			name: "labels",
			node: func() Node { n := base; n.ID = "labels"; n.Labels = map[string]string{"arch": "arm64"}; return n }(),
		},
		{name: "capability", node: func() Node { n := base; n.ID = "capability"; n.FeatureIDs = nil; return n }()},
		{
			name: "resources",
			node: func() Node { n := base; n.ID = "resources"; n.MemAllocated = n.MemTotalMib; return n }(),
		},
		{name: "disk", node: func() Node { n := base; n.ID = "disk"; n.DiskAllocated = n.DiskTotalMib; return n }()},
		{name: "locality", node: func() Node { n := base; n.ID = "locality"; return n }()},
	}

	p := New(DefaultBestOfKConfig(), Options{})
	req := Request{
		VCPU: 1, MemMib: 512, DiskMib: 512, Pool: "compute",
		Labels: map[string]string{"arch": "x86_64"}, RequiredFeatures: []string{"required"},
		RequiredNodeID: "eligible",
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := []Node{tt.node, base}
			prefetch := p.PrefetchTopK(req, nodes, len(nodes))
			if len(prefetch) != 1 || prefetch[0].ID != "eligible" {
				t.Fatalf("prefetch candidates = %+v, want only eligible", prefetch)
			}
			placement, err := p.Place(req, nodes, rand.New(rand.NewSource(1)))
			if err != nil {
				t.Fatal(err)
			}
			if placement.NodeID != "eligible" {
				t.Fatalf("placement = %q, want eligible", placement.NodeID)
			}
		})
	}
}

func TestPrefetchIgnoresPerAttemptRetryExclusions(t *testing.T) {
	p := New(DefaultBestOfKConfig(), Options{})
	node := healthyNode("only", "compute", 8, 8192)
	req := Request{VCPU: 1, MemMib: 512, ExcludedNodes: map[string]bool{"only": true}}
	if got := p.PrefetchTopK(req, []Node{node}, 1); len(got) != 1 || got[0].ID != "only" {
		t.Fatalf("prefetch must ignore retry exclusions, got %+v", got)
	}
	if _, err := p.Place(req, []Node{node}, rand.New(rand.NewSource(1))); err == nil {
		t.Fatal("placement must apply retry exclusions")
	}
}

func TestPrefetchTopKUsesDeploymentHardCandidates(t *testing.T) {
	busy := healthyNode("busy", "compute", 64, 65536)
	busy.CPUAllocated = 60
	idle := healthyNode("idle", "compute", 64, 65536)
	wrongPool := healthyNode("wrong-pool", "other", 64, 65536)
	wrongLabel := healthyNode("wrong-label", "compute", 64, 65536)
	wrongLabel.Labels["arch"] = "arm64"
	full := healthyNode("full", "compute", 64, 65536)
	full.MemAllocated = 65536
	occupied := healthyNode("occupied", "compute", 64, 65536)
	p := New(DefaultBestOfKConfig(), Options{})
	top := p.PrefetchTopK(Request{
		VCPU: 1, MemMib: 512, Pool: "compute", Labels: map[string]string{"arch": "x86_64"},
		DeploymentID: "d1", AntiAffinity: true,
		ExistingDeploymentNodes: map[string]bool{"occupied": true},
	}, []Node{busy, idle, wrongPool, wrongLabel, full, occupied}, 2)
	if len(top) != 2 || top[0].ID != "idle" || top[1].ID != "busy" {
		t.Fatalf("prefetch candidates = %+v, want idle,busy", top)
	}
}

func TestLocalRWLocalityWinsOverAntiAffinity(t *testing.T) {
	p := New(DefaultBestOfKConfig(), Options{})
	nodes := []Node{
		{
			ID:          "origin",
			Pool:        "compute",
			Status:      StatusHealthy,
			CPUTotal:    4,
			MemTotalMib: 4096,
			FeatureIDs:  map[string]bool{"volume.local_rw.v1": true},
		},
		{
			ID:          "spread",
			Pool:        "compute",
			Status:      StatusHealthy,
			CPUTotal:    4,
			MemTotalMib: 4096,
			FeatureIDs:  map[string]bool{"volume.local_rw.v1": true},
		},
	}
	pl, err := p.Place(Request{
		VCPU: 1, MemMib: 512, DeploymentID: "dep", AntiAffinity: true,
		ExistingDeploymentNodes: map[string]bool{"origin": true}, RequiredNodeID: "origin",
		RequiredFeatures: []string{"volume.local_rw.v1"},
	}, nodes, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if pl.NodeID != "origin" {
		t.Fatalf("placed on %q, want LOCAL_RW origin", pl.NodeID)
	}
}

// v1.2-E（ADR-0035）：磁盘维度硬过滤——requested 承诺不超售；旧 agent
// 未上报容量（DiskTotalMib==0）时跳过磁盘过滤。
func TestDiskHardFilter(t *testing.T) {
	nodes := []Node{
		func() Node {
			n := healthyNode("disk-full", "compute", 64, 65536)
			n.DiskTotalMib = 1024
			n.DiskAllocated = 500
			n.DiskPending = 500
			return n
		}(),
		func() Node {
			n := healthyNode("disk-ok", "compute", 64, 65536)
			n.DiskTotalMib = 2048
			n.DiskAllocated = 500
			return n
		}(),
		func() Node {
			// 未上报磁盘容量的旧节点：不能因磁盘被全拒（升级期安全）。
			n := healthyNode("disk-legacy", "compute", 64, 65536)
			return n
		}(),
	}
	p := New(DefaultBestOfKConfig(), Options{})
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512, DiskMib: 512}, nodes, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if pl.NodeID == "disk-full" {
		t.Fatal("disk-full (allocated+pending+req > total) must be filtered")
	}
	// 全部节点磁盘满 → ErrNoCandidates。
	full := []Node{nodes[0]}
	if _, err := p.Place(Request{VCPU: 1, MemMib: 512, DiskMib: 1000}, full, rand.New(rand.NewSource(1))); err == nil {
		t.Fatal("disk-exceeded single node must yield ErrNoCandidates")
	}
}

// P1#14a：New 必须补齐所有零值字段——全零配置等价默认配置。
func TestNewNormalizesFullyZeroConfig(t *testing.T) {
	p := New(BestOfKConfig{}, Options{})
	if p.cfg != DefaultBestOfKConfig() {
		t.Fatalf("zero config normalized to %+v, want default %+v", p.cfg, DefaultBestOfKConfig())
	}
}

// P1#14a：半配置不得丢失未指定维度。旧实现中 WeightImage 的默认填充条件
// 依赖 WeightCPU/WeightMem 仍为零，而它们在前面已被补成 0.5——条件永远不
// 触发，只设部分权重的调用方会静默丢失镜像亲和。
func TestNewPartialConfigKeepsDefaults(t *testing.T) {
	p := New(BestOfKConfig{WeightCPU: 0.8}, Options{})
	if p.cfg.WeightCPU != 0.8 {
		t.Fatalf("explicit WeightCPU lost: %+v", p.cfg)
	}
	if p.cfg.R != 4 || p.cfg.MemR != 1.0 || p.cfg.DiskR != 1.0 ||
		p.cfg.K != 3 || p.cfg.Alpha != 0.5 || p.cfg.WeightImage != 0.5 {
		t.Fatalf("partial config normalization = %+v", p.cfg)
	}
	// 行为断言：半配置下镜像缓存亲和仍参与打分（已缓存节点胜出）。
	cached := healthyNode("cached", "compute", 64, 65536)
	cached.CachedImageDigests = map[string]bool{"sha256:x": true}
	uncached := healthyNode("uncached", "compute", 64, 65536)
	uncached.CachedImageDigests = map[string]bool{}
	pl, err := p.Place(Request{VCPU: 1, MemMib: 1, ImageDigest: "sha256:x"},
		[]Node{uncached, cached}, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatal(err)
	}
	if pl.NodeID != "cached" {
		t.Fatalf("image affinity lost on partial config: placed on %s", pl.NodeID)
	}
}

// P1#14a：DiskR 漏补（=0）时 canFit 会把任何磁盘承诺判为超售，把
// DiskTotalMib>0 的节点全部硬过滤掉（集群不可调度）。New 必须补成 1.0。
func TestNewNormalizesDiskR(t *testing.T) {
	cfg := DefaultBestOfKConfig()
	cfg.DiskR = 0 // 模拟“只设了部分字段”的半配置输入
	p := New(cfg, Options{})
	if p.cfg.DiskR != 1.0 {
		t.Fatalf("DiskR must be defaulted to 1.0, got %v", p.cfg.DiskR)
	}
	node := healthyNode("disk-node", "compute", 64, 65536)
	node.DiskTotalMib = 51200
	pl, err := p.Place(Request{VCPU: 1, MemMib: 512, DiskMib: 10240},
		[]Node{node}, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatalf("DiskR=0 normalization must not hard-filter every node: %v", err)
	}
	if pl.NodeID != "disk-node" {
		t.Fatalf("placed on %s", pl.NodeID)
	}
}
