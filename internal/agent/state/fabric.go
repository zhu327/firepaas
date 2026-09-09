// fabric.go 实现节点级 fabric 快照持久化与 generation 高水位（ADR-0040
// §18 / G0-2）。与 fences.go（machine 级）同纪律但独立成册：
//
//   - fencing 键 = node_id + fabric_generation + operation_id；fabric_generation
//     只升不降，operation 幂等由 Ledger 承担；
//   - 全量快照替换：Apply 原子完成「检查 → 落盘 → 推进高水位」；
//   - 同 generation 携带不同内容同样拒绝（fail closed）；
//   - 崩溃后重启继续从持久化水位拒绝旧代请求。
//
// 内容只含可重建投影与公钥材料，绝不持久化任何私钥/凭证。
package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// ErrStaleFabricGeneration 表示请求的 fabric_generation 早于节点已知水位。
var ErrStaleFabricGeneration = errors.New("stale fabric generation")

// FabricPeer 是快照内的一条 WG peer（ADR-0040 §9：按 node /64 聚合）。
type FabricPeer struct {
	NodeID           string `json:"node_id"`
	Pubkey           string `json:"pubkey"`
	Endpoint         string `json:"endpoint"`
	NodePrefix       string `json:"node_prefix"`
	FabricGeneration uint64 `json:"fabric_generation"`
}

// FabricIdentity 是快照内的一条 ULA ↔ identity 映射（ADR-0040 §6/§7）。
type FabricIdentity struct {
	IdentityID  uint32 `json:"identity_id"`
	TrustDomain string `json:"trust_domain"`
	ProjectID   string `json:"project_id"`
	AppID       string `json:"app_id"`
	Service     string `json:"service"`
	ULA         string `json:"ula"`
	MachineID   string `json:"machine_id"`
	ExecutionID string `json:"execution_id"`
	Generation  uint64 `json:"generation"`
	// MeshDirect 是本 execution 主服务的直连声明（G2a，§15）：agent 建
	// policy 条目要求 EastWest 规则与 dst mesh_direct 缺一不可。
	MeshDirect bool `json:"mesh_direct,omitempty"`
}

// EastWestRule 是快照内的东西向放行规则（G2a，§15）：只表达“谁能连谁的
// 哪个服务端口”。全量随快照替换，generation 取表内规则最大水位。
type EastWestRule struct {
	SrcProject string   `json:"src_project"`
	SrcApp     string   `json:"src_app"`
	DstProject string   `json:"dst_project"`
	DstApp     string   `json:"dst_app"`
	DstService string   `json:"dst_service"`
	Ports      []uint32 `json:"ports"`
}

// DnsRecord 是快照内的一条 .internal 服务发现记录（G2c，§16）：
// name → ULA AAAA 集合。全量随快照替换，节点本地 DNS 直接服务。
type DnsRecord struct {
	Name       string   `json:"name"`
	AAAA       []string `json:"aaaa"`
	Generation uint64   `json:"generation"`
}

// FabricSnapshot 是 ApplyFabric 全量快照的持久化形态。
type FabricSnapshot struct {
	NodeID     string           `json:"node_id"`
	Generation uint64           `json:"generation"`
	NodePrefix string           `json:"node_prefix"`
	Peers      []FabricPeer     `json:"peers,omitempty"`
	Identities []FabricIdentity `json:"identities,omitempty"`
	// EastWest 是东西向放行全表（G2a；nil = 无放行，默认 deny）与表水位。
	EastWest           []EastWestRule `json:"eastwest,omitempty"`
	EastWestGeneration uint64         `json:"eastwest_generation,omitempty"`
	// DNS 是 .internal 服务发现全表（G2c；nil = 无记录）。节点本地 DNS
	// 按本表服务；同源门控（READY 非 draining mesh_direct）在控制面完成。
	DNS       []DnsRecord `json:"dns,omitempty"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// Fabric 持久化节点级 fabric 快照。mu 同时保护读取与「检查+推进」临界区：
// 并发 Apply 按持锁顺序串行，先 apply 的高代会拒绝后到的低代。
type Fabric struct {
	mu      sync.Mutex
	path    string
	current FabricSnapshot
}

// OpenFabric 加载 fabric 状态；文件不存在时从空状态开始。
func OpenFabric(path string) (*Fabric, error) {
	f := &Fabric{path: path}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return f, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read fabric state: %w", err)
	}
	if len(data) == 0 {
		return f, nil
	}
	if err := json.Unmarshal(data, &f.current); err != nil {
		return nil, fmt.Errorf("parse fabric state %s: %w", path, err)
	}
	return f, nil
}

// Current 返回当前已应用快照的深拷贝（零值 = 尚未应用任何快照）。
func (f *Fabric) Current() FabricSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneSnapshot(f.current)
}

// Check 校验 generation 不早于节点已知 fabric 高水位。
func (f *Fabric) Check(generation uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.current.Generation != 0 && generation < f.current.Generation {
		return fmt.Errorf("%w: node is at fabric generation %d, request carried %d",
			ErrStaleFabricGeneration, f.current.Generation, generation)
	}
	return nil
}

// CheckSnapshot 在串行化锁外预检整个快照：旧代拒绝；同代不同内容也拒绝
// （避免 stale 请求先写 ledger claim 再在 Effect 里失败）。Apply 内部仍
// 保持同样的原子强制（CheckSnapshot 只是减少孤儿 claim）。
func (f *Fabric) CheckSnapshot(snap FabricSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := f.current
	if cur.Generation == 0 {
		if snap.Generation == 0 {
			return fmt.Errorf("fabric snapshot generation must be > 0")
		}
		return nil
	}
	if snap.NodeID != cur.NodeID {
		return fmt.Errorf("fabric snapshot node %q does not match persisted node %q",
			snap.NodeID, cur.NodeID)
	}
	if snap.Generation < cur.Generation {
		return fmt.Errorf("%w: node is at fabric generation %d, request carried %d",
			ErrStaleFabricGeneration, cur.Generation, snap.Generation)
	}
	if snap.Generation == cur.Generation && !snapshotEqual(cur, snap) {
		return fmt.Errorf("%w: fabric generation %d already applied with different content",
			ErrStaleFabricGeneration, snap.Generation)
	}
	return nil
}

// Apply 原子应用全量快照：generation 只升不降、同代不同内容拒绝、落盘
// 成功才推进内存水位。返回 true 表示写入了新快照，false 表示同代同内容
// 幂等命中（不落盘）。
func (f *Fabric) Apply(snap FabricSnapshot) (bool, error) {
	if snap.Generation == 0 {
		return false, fmt.Errorf("fabric snapshot generation must be > 0")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := f.current
	if cur.Generation != 0 && snap.NodeID != cur.NodeID {
		return false, fmt.Errorf("fabric snapshot node %q does not match persisted node %q",
			snap.NodeID, cur.NodeID)
	}
	if snap.Generation < cur.Generation {
		return false, fmt.Errorf("%w: node is at fabric generation %d, request carried %d",
			ErrStaleFabricGeneration, cur.Generation, snap.Generation)
	}
	if snap.Generation == cur.Generation {
		if snapshotEqual(cur, snap) {
			return false, nil
		}
		return false, fmt.Errorf("%w: fabric generation %d already applied with different content",
			ErrStaleFabricGeneration, snap.Generation)
	}
	snap.UpdatedAt = time.Now().UTC()
	if err := f.persistLocked(snap); err != nil {
		return false, err
	}
	f.current = snap
	return true, nil
}

func snapshotEqual(a, b FabricSnapshot) bool {
	a.UpdatedAt = time.Time{}
	b.UpdatedAt = time.Time{}
	ra, err := json.Marshal(a)
	if err != nil {
		return false
	}
	rb, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return bytes.Equal(ra, rb)
}

func cloneSnapshot(s FabricSnapshot) FabricSnapshot {
	s.Peers = append([]FabricPeer(nil), s.Peers...)
	s.Identities = append([]FabricIdentity(nil), s.Identities...)
	return s
}

func (f *Fabric) persistLocked(snap FabricSnapshot) error {
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal fabric state: %w", err)
	}
	return writeFileDurable(f.path, "fabric", data)
}
