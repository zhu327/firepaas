// manager.go：egress 策略装配层——把 machine 生命周期（create/delete/restart）
// 映射为「数据面策略快照 + 透明代理注册」两步。策略落地走 network/api 的
// PolicyEngine 插件缝（ADR-0040 §11），单测可替身。
package egress

import (
	"context"
	"fmt"
	"sync"

	"github.com/zhu327/firepaas/internal/agent/network/api"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// Manager 把 proto 策略翻译为数据面策略快照并注册代理。
type Manager struct {
	mu sync.Mutex

	proxy   *Proxy
	policy  api.PolicyEngine
	port80  int
	port443 int
}

// NewManager 构造 Manager；policy 可为 nil（纯 CIDR 模式无数据面后端时禁用）。
func NewManager(proxy *Proxy, policy api.PolicyEngine) *Manager {
	return &Manager{proxy: proxy, policy: policy, port80: proxy.port80, port443: proxy.port43}
}

// Apply stages an invisible proxy generation, commits the datapath snapshot,
// then publishes policy and IP binding with one swap. Manager serializes the
// whole protocol so rollback cannot overwrite a concurrent generation.
func (m *Manager) Apply(
	ctx context.Context,
	machineID, executionID, projectID, appID, guestIP string,
	policy *Policy,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if policy == nil {
		return m.removeLocked(ctx, machineID)
	}
	staged, err := m.proxy.stage(machineID, executionID, projectID, appID, guestIP, policy)
	if err != nil {
		return fmt.Errorf("egress stage proxy: %w", err)
	}
	oldProxy, hadOldProxy := m.proxy.current(machineID)
	// Publish the new userspace policy first. During tightening this fails closed:
	// packets still following the old nft path cannot be authorized by the old
	// (broader) proxy generation.
	if err := m.proxy.swap(staged); err != nil {
		return fmt.Errorf("egress publish proxy: %w", err)
	}
	if m.policy != nil {
		if err := m.policy.ApplySnapshot(ctx, machineID, m.snapshotFor(policy)); err != nil {
			if hadOldProxy {
				_ = m.proxy.swap(oldProxy)
			} else {
				_ = m.proxy.Unregister(machineID)
			}
			return fmt.Errorf("egress apply datapath snapshot: %w", err)
		}
	}
	return nil
}

// Remove first clears the datapath snapshot. A failure leaves the proxy
// registration visible so traffic still uses the complete old policy; only a
// successful clear unregisters it.
func (m *Manager) Remove(ctx context.Context, machineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.removeLocked(ctx, machineID)
}

func (m *Manager) removeLocked(ctx context.Context, machineID string) error {
	if m.policy != nil {
		if err := m.policy.RemoveSnapshot(ctx, machineID); err != nil {
			return fmt.Errorf("egress remove datapath snapshot: %w", err)
		}
	}
	return m.proxy.Unregister(machineID)
}

// Restore 重启后重放持久化数据面快照并重建代理注册。身份信息（execution 等）
// 无法从持久化状态恢复：调用方须先经 hypeman 实例 tags 重建 Policy 并调用
// Apply；本方法只负责内核规则幂等重放。返回数据面侧错误（无持久化快照时不
// 报错）。
func (m *Manager) Restore(ctx context.Context, machineID string) error {
	if m.policy == nil {
		return nil
	}
	return m.policy.RestoreSnapshot(ctx, machineID)
}

// Stats 返回 machine 当前 execution 的审计聚合（Machine.EgressAudit）。
func (m *Manager) Stats(machineID string) *pb.EgressAuditStats {
	return m.proxy.Stats(machineID)
}

// snapshotFor 把归一化 Policy 翻译为数据面策略快照（全量替换 + generation 水位）。
func (m *Manager) snapshotFor(p *Policy) api.PolicySnapshot {
	allowed, denied := p.CIDRStrings()
	snap := api.PolicySnapshot{
		Mode:         p.ModeString(),
		AllowedCIDRs: allowed,
		DeniedCIDRs:  denied,
		Domains:      p.DomainStrings(),
		MaxTCPConns:  p.MaxTCPConns,
		AuditAll:     p.AuditAll,
		Generation:   p.Generation,
	}
	if p.ProxyNeeded() {
		snap.ProxyPort80 = m.port80
		snap.ProxyPort443 = m.port443
	}
	return snap
}
