// Package scheduler 是 firepaas 的自研放置调度器（移植 e2b Best-of-K，ADR-0002）。
//
// 管线（ADR-0009，先过滤后打分）：
//
//	状态过滤 → node_pool/labels 过滤 → 资源硬准入过滤
//	→ DEPLOYMENT 反亲和过滤（尽力而为，候选为空回退）
//	→ 随机抽 K → Best-of-K 打分 → 最低分
//
// 打分公式（CPU/内存分维度，MVP 只对这两维打分）：
//
//	score = Σ_w w_i * (req_i + allocated_i + pending_i + alpha_i * usage_i) / (R_i * capacity_i)
package scheduler

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
)

// 节点状态（与 agent ServiceInfoResponse.Status 对齐，外加 UNKNOWN）。
const (
	StatusUnknown   = "UNKNOWN"
	StatusHealthy   = "HEALTHY"
	StatusDraining  = "DRAINING"
	StatusUnhealthy = "UNHEALTHY"
)

// BestOfKConfig 放置参数。
type BestOfKConfig struct {
	R         float64 // 集群级最大超售比（CPU 维度），默认 4
	MemR      float64 // 内存超售比，默认 1.0（内存不超售）
	DiskR     float64 // 磁盘超售比（v1.2-E，ADR-0035），默认 1.0（磁盘不超售）
	K         int     // 每轮随机采样数，默认 3
	Alpha     float64 // 实时用量的权重，默认 0.5
	WeightCPU float64 // 打分权重，默认 0.5
	WeightMem float64 // 打分权重，默认 0.5
	// WeightImage（v1.1，ADR-0018）：镜像缓存亲和罚项权重，默认 0.5。
	// 只在打分层生效（任何节点都不因镜像未缓存被过滤）。
	WeightImage float64
}

// DefaultBestOfKConfig 返回默认参数（对齐 e2b）。
func DefaultBestOfKConfig() BestOfKConfig {
	return BestOfKConfig{
		R:           4,
		MemR:        1.0,
		DiskR:       1.0,
		K:           3,
		Alpha:       0.5,
		WeightCPU:   0.5,
		WeightMem:   0.5,
		WeightImage: 0.5,
	}
}

// Options 是 Place 的可观测/可注入选项。
type Options struct {
	// ScoreHook 在每个候选节点进入打分前被调用一次；仿真用它在
	// “打分前”校验该节点已通过全部过滤（断言过滤先于打分）。
	ScoreHook func(nodeID string)
	// ImageAffinityDisabled 为 true 时保留 WeightImage=0：score 层据此
	// 整体跳过镜像亲和（见 score），而不是把它当作“未配置”归一化为
	// 默认权重。供显式关闭亲和的配置面使用（FIREPAAS_SCHED_WEIGHT_IMAGE=0）。
	ImageAffinityDisabled bool
}

// Node 是调度器看到的节点视图（nodemanager 同步，纯数据）。
type Node struct {
	ID     string
	Pool   string
	Labels map[string]string
	Status string
	// Draining：M5.5 手动排水——不再接受新放置（已有负载继续）。
	Draining bool

	CPUTotal    uint64
	MemTotalMib uint64

	CPUAllocated uint64  // 已承诺 vcpu
	MemAllocated uint64  // 已承诺 MiB（agent 上报校正）
	CPUPending   uint64  // 在途 create 的 vcpu 承诺
	MemPending   uint64  // 在途 create 的 MiB 承诺
	CPUPercent   float64 // 实时用量（0-100）
	MemUsedMib   uint64  // 实时用量

	// Disk（v1.2-E，ADR-0035）：requested overlay 承诺维度。
	// DiskTotalMib==0（旧 agent 未上报）时磁盘过滤跳过（混合版本安全）。
	DiskTotalMib  uint64 // 节点 data 盘总量
	DiskAllocated uint64 // 已承诺 MiB（requested 之和，agent 上报）
	DiskPending   uint64 // 在途 create 的 MiB 承诺

	// CachedImageDigests（v1.1，ADR-0018）：节点本地镜像缓存 digest 集合
	//（内含 name-digest 与 manifest digest 两种形态）。nil = 未知（不罚）。
	CachedImageDigests map[string]bool
	// FeatureIDs（v1.2-A，ADR-0023）：节点上报的 runtime capability 集合。
	// nil = 未上报/无能力（任何 required feature 都不满足）。
	FeatureIDs map[string]bool
}

// Request 是一次放置请求（来自 MachineSpec.placement + 资源需求）。
type Request struct {
	VCPU   uint64
	MemMib uint64
	// DiskMib（v1.2-E，ADR-0035）：有效磁盘承诺（MiB）。控制面必须传
	// EffectiveDiskMib（spec 0 → 默认 10GiB），保证调度/预约/准入同值。
	DiskMib uint64

	DeploymentID string            // 反亲和范围；空 = 不启用
	Pool         string            // 空 = compute
	Labels       map[string]string // key=value 硬过滤

	AntiAffinity bool
	// ExistingDeploymentNodes 是同一 deployment 已占节点集合
	// （控制面从 PG 推导，ADR-0009）。
	ExistingDeploymentNodes map[string]bool
	// ExcludedNodes 是本次放置不可选节点（如 ResourceExhausted 换节点重试）。
	ExcludedNodes map[string]bool
	// ImageDigest（v1.1，ADR-0018）：目标镜像 digest（image_ref 的 @ 后缀）。
	// 空 = 不启用镜像亲和罚项。
	ImageDigest string
	// RequiredFeatures（v1.2-A，ADR-0023）：启动正确性能力硬过滤。控制面从
	// deployment 语义推导（客户端不得直接声明内部 feature）。空 = 无要求。
	RequiredFeatures []string
	// RequiredNodeID is a hard locality constraint (LOCAL_RW origin node). It is
	// evaluated after resource/capability filters and before anti-affinity.
	RequiredNodeID string
}

// Event 是调度事件（写 scheduler_events，审计可解释）。
type Event struct {
	Kind   string // placement|filter_rejection
	NodeID string
	Reason string
}

// Placement 是一次放置的结果。
type Placement struct {
	NodeID string
	Score  float64
	Events []Event
}

// Placer 执行放置。
type Placer struct {
	cfg  BestOfKConfig
	opts Options
}

// New 构造 Placer。cfg 零值字段会被默认值补齐。
//
// P1#14a：DiskR 也必须归一——漏补时 DiskR=0 会把任何磁盘承诺都判为超售
// （canFit 把 DiskTotalMib>0 的节点全部硬过滤掉，集群不可调度）。
// WeightImage 的默认填充不得依赖 CPU/Mem 权重仍为零：旧条件在
// WeightCPU/WeightMem 默认值补齐后永远不触发，半配置（只设部分权重）会
// 静默丢失镜像亲和。归一后 WeightImage 不再能为 0（关闭镜像亲和的入口
// 不复存在）；score 中的 0 短路仅作为直接构造 Placer 时的防御保留。
func New(cfg BestOfKConfig, opts Options) *Placer {
	if cfg.R == 0 {
		cfg.R = DefaultBestOfKConfig().R
	}
	if cfg.MemR == 0 {
		cfg.MemR = DefaultBestOfKConfig().MemR
	}
	if cfg.DiskR == 0 {
		cfg.DiskR = DefaultBestOfKConfig().DiskR
	}
	if cfg.K == 0 {
		cfg.K = DefaultBestOfKConfig().K
	}
	if cfg.Alpha == 0 {
		cfg.Alpha = DefaultBestOfKConfig().Alpha
	}
	if cfg.WeightCPU == 0 && cfg.WeightMem == 0 {
		cfg.WeightCPU = 0.5
		cfg.WeightMem = 0.5
	}
	if cfg.WeightImage == 0 && !opts.ImageAffinityDisabled {
		cfg.WeightImage = DefaultBestOfKConfig().WeightImage
	}
	return &Placer{cfg: cfg, opts: opts}
}

// Config 返回归一化后的放置参数。reservations 的节点 CPU 超售双保险必须
// 与调度器使用同一个 R（契约 C-3），由调用方从这里取值传入。
func (p *Placer) Config() BestOfKConfig { return p.cfg }

// PrefetchTopK 按真实 deployment 放置的硬候选过滤后，按同一评分升序返回前 k
// 个节点。预取不做 reservation，但绝不向 pool/labels/资源/反亲和不合格的节点
// 发 PullImage；否则预热结果不能代表实际 create 的候选集。
func (p *Placer) PrefetchTopK(req Request, nodes []Node, k int) []Node {
	if k <= 0 {
		k = 3
	}
	candidates, _ := p.evaluateCandidates(req, nodes, candidateEvaluation{})
	sort.SliceStable(candidates, func(i, j int) bool {
		si, sj := p.score(candidates[i], req), p.score(candidates[j], req)
		if si != sj {
			return si < sj
		}
		return candidates[i].ID < candidates[j].ID
	})
	if len(candidates) > k {
		candidates = candidates[:k]
	}
	return candidates
}

type candidateEvaluation struct {
	applyRetryExclusions bool
	collectEvents        bool
}

// evaluateCandidates is the single hard-candidate pipeline used by placement
// and prefetch. Terminal sampling/ranking remains with the caller; prefetch
// deliberately omits per-attempt retry exclusions.
func (p *Placer) evaluateCandidates(req Request, nodes []Node, opts candidateEvaluation) ([]Node, []Event) {
	var events []Event
	addEvent := func(kind, nodeID, reason string) {
		if opts.collectEvents {
			events = append(events, Event{Kind: kind, NodeID: nodeID, Reason: reason})
		}
	}
	filter := func(nodes []Node, reject func(Node) string) []Node {
		passed := make([]Node, 0, len(nodes))
		for _, n := range nodes {
			if reason := reject(n); reason != "" {
				addEvent("filter_rejection", n.ID, reason)
				continue
			}
			passed = append(passed, n)
		}
		return passed
	}

	pool := req.Pool
	if pool == "" {
		pool = "compute"
	}

	candidates := filter(nodes, func(n Node) string {
		if n.Draining {
			return "draining"
		}
		if n.Status != StatusHealthy {
			return "status=" + n.Status
		}
		return ""
	})
	if opts.applyRetryExclusions {
		candidates = filter(candidates, func(n Node) string {
			if req.ExcludedNodes[n.ID] {
				return "excluded by retry policy"
			}
			return ""
		})
	}
	candidates = filter(candidates, func(n Node) string {
		if n.Pool != pool {
			return "pool mismatch"
		}
		if !labelsMatch(n.Labels, req.Labels) {
			return "labels mismatch"
		}
		return ""
	})
	candidates = filter(candidates, func(n Node) string {
		if missing := missingFeatures(n.FeatureIDs, req.RequiredFeatures); missing != "" {
			return "capability missing: " + missing
		}
		return ""
	})
	candidates = filter(candidates, func(n Node) string {
		if !canFit(n, req, p.cfg) {
			return fmt.Sprintf(
				"resources: need vcpu=%d mem=%dMiB disk=%dMiB (allocated+pending+req exceeds R*capacity)",
				req.VCPU,
				req.MemMib,
				req.DiskMib,
			)
		}
		return ""
	})
	candidates = filter(candidates, func(n Node) string {
		if req.RequiredNodeID != "" && n.ID != req.RequiredNodeID {
			return "volume locality mismatch"
		}
		return ""
	})

	if req.AntiAffinity && req.DeploymentID != "" && len(req.ExistingDeploymentNodes) > 0 {
		spread := make([]Node, 0, len(candidates))
		for _, n := range candidates {
			if !req.ExistingDeploymentNodes[n.ID] {
				spread = append(spread, n)
			}
		}
		if len(spread) == 0 {
			addEvent("placement", "", "anti_affinity degraded: no node outside deployment set")
		} else {
			candidates = spread
		}
	}
	return candidates, events
}

// ErrNoCandidates 表示过滤后无可用节点。
type ErrNoCandidates struct{ Reason string }

func (e ErrNoCandidates) Error() string { return "scheduler: no candidates: " + e.Reason }

// Place 执行一次放置。rnd 为 nil 时使用随机采样源。
func (p *Placer) Place(req Request, nodes []Node, rnd *rand.Rand) (Placement, error) {
	if rnd == nil {
		rnd = rand.New(rand.NewSource(rand.Int63()))
	}
	candidates, events := p.evaluateCandidates(req, nodes, candidateEvaluation{
		applyRetryExclusions: true,
		collectEvents:        true,
	})
	addEvent := func(kind, nodeID, reason string) {
		events = append(events, Event{Kind: kind, NodeID: nodeID, Reason: reason})
	}

	if len(candidates) == 0 {
		return Placement{Events: events}, ErrNoCandidates{Reason: "all filters exhausted"}
	}

	// 5) 随机抽 K → 打分 → 最低分。ScoreHook 在打分前调用（顺序断言）。
	sample := sampleK(candidates, p.cfg.K, rnd)
	type scored struct {
		node  Node
		score float64
	}
	scoredList := make([]scored, 0, len(sample))
	for _, n := range sample {
		if p.opts.ScoreHook != nil {
			p.opts.ScoreHook(n.ID)
		}
		scoredList = append(scoredList, scored{node: n, score: p.score(n, req)})
	}
	sort.Slice(scoredList, func(i, j int) bool {
		if scoredList[i].score != scoredList[j].score {
			return scoredList[i].score < scoredList[j].score
		}
		return scoredList[i].node.ID < scoredList[j].node.ID // 确定性 tie-break
	})
	best := scoredList[0]
	addEvent("placement", best.node.ID, fmt.Sprintf("score=%.6f candidates=%d sampled=%d",
		best.score, len(candidates), len(sample)))
	return Placement{NodeID: best.node.ID, Score: best.score, Events: events}, nil
}

// score 计算单节点放置分（Best-of-K，ADR-0002）。
// v1.1（ADR-0018）：增加镜像缓存亲和软罚项 WeightImage·imageMiss——目标镜像
// digest 在节点缓存内 = 0，不在 = 1；节点缓存未知（nil）不罚。亲和只出现在
// 打分层，任何节点都不因镜像未缓存被过滤（否则新镜像永远无候选）。
func (p *Placer) score(n Node, req Request) float64 {
	cpuCap := float64(n.CPUTotal) * p.cfg.R
	memCap := float64(n.MemTotalMib) * p.cfg.MemR
	usageCPU := n.CPUPercent / 100 * float64(n.CPUTotal) // 折算为“占用 vcpu 数”
	cpu := (float64(req.VCPU+uint64(n.CPUAllocated)+uint64(n.CPUPending)) + p.cfg.Alpha*usageCPU) / cpuCap
	mem := (float64(req.MemMib+n.MemAllocated+n.MemPending) + p.cfg.Alpha*float64(n.MemUsedMib)) / memCap
	base := p.cfg.WeightCPU*cpu + p.cfg.WeightMem*mem
	if req.ImageDigest == "" || p.cfg.WeightImage == 0 {
		return base
	}
	// 注：nil（未上报）与空集合同罚——proto3 repeated 无法区分“未知”与
	// “空”，且节点视图均来自已上报 ServiceInfo 的 agent（20s 内必有缓存
	// 视图）；不罚会让未上报者永远战胜已缓存节点，亲和失效。
	miss := 1.0
	if n.CachedImageDigests[req.ImageDigest] {
		miss = 0
	}
	return base + p.cfg.WeightImage*miss
}

// canFit 硬准入：allocated+pending+req ≤ R·capacity（内存 R=MemR）。
func canFit(n Node, req Request, cfg BestOfKConfig) bool {
	if n.CPUTotal == 0 || n.MemTotalMib == 0 {
		return false // 容量未知的节点不接新 machine（保守）
	}
	usedVCPU := uint64(n.CPUAllocated) + n.CPUPending + req.VCPU
	if float64(usedVCPU) > float64(n.CPUTotal)*cfg.R {
		return false
	}
	usedMem := n.MemAllocated + n.MemPending + req.MemMib
	if float64(usedMem) > float64(n.MemTotalMib)*cfg.MemR {
		return false
	}
	// v1.2-E（ADR-0035）：磁盘硬过滤（不超售）。旧 agent 未上报容量
	//（DiskTotalMib==0）时跳过，避免升级期全部节点不可调度。
	if n.DiskTotalMib > 0 {
		usedDisk := n.DiskAllocated + n.DiskPending + req.DiskMib
		if float64(usedDisk) > float64(n.DiskTotalMib)*cfg.DiskR {
			return false
		}
	}
	return true
}

func labelsMatch(nodeLabels, want map[string]string) bool {
	for k, v := range want {
		if nodeLabels[k] != v {
			return false
		}
	}
	return true
}

// missingFeatures 返回缺失能力名（逗号连接，事件审计用）。
func missingFeatures(nodeFeatures map[string]bool, required []string) string {
	var missing []string
	for _, id := range required {
		if !nodeFeatures[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return ""
	}
	return strings.Join(missing, ",")
}

func sampleK(nodes []Node, k int, rnd *rand.Rand) []Node {
	if k <= 0 || k >= len(nodes) {
		out := make([]Node, len(nodes))
		copy(out, nodes)
		return out
	}
	perm := rnd.Perm(len(nodes))
	out := make([]Node, 0, k)
	for _, idx := range perm[:k] {
		out = append(out, nodes[idx])
	}
	return out
}
