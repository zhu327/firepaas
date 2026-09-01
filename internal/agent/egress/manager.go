// manager.go：egress 策略装配层——把 machine 生命周期（create/delete/restart）
// 映射为「slot nftables 规则 + 透明代理注册」两步。slot 侧接口化，单测可替身。
package egress

import (
	"context"
	"fmt"
	"sync"

	"github.com/zhu327/firepaas/internal/agent/network/slot"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// SlotApplier 是 Manager 需要的 slot 内核规则能力子集。
type SlotApplier interface {
	ApplyEgressPolicy(ctx context.Context, machineID string, rs slot.EgressRuleSet) error
	RemoveEgressPolicy(ctx context.Context, machineID string) error
	CurrentEgressPolicy(machineID string) (slot.EgressRuleSet, bool)
	RollbackEgressPolicy(ctx context.Context, machineID string, rs slot.EgressRuleSet, present bool) error
	RestoreEgress(ctx context.Context, machineID string) error
}

// SlotLister 提供重启恢复所需的全部 slot 视图。
type SlotLister interface {
	List() []slot.Slot
}

// Manager 把 proto 策略翻译为 slot 规则集并注册代理。
type Manager struct {
	mu sync.Mutex

	proxy   *Proxy
	slots   SlotApplier
	port80  int
	port443 int
}

// NewManager 构造 Manager；slots 可为 nil（纯 CIDR 模式无 slot 后端时禁用）。
func NewManager(proxy *Proxy, slots SlotApplier) *Manager {
	return &Manager{proxy: proxy, slots: slots, port80: proxy.port80, port443: proxy.port43}
}

// Apply stages an invisible proxy generation, commits the nft rules, then
// publishes policy and IP binding with one swap. Manager serializes the whole
// protocol so rollback cannot overwrite a concurrent generation.
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
	if m.slots != nil {
		if err := m.slots.ApplyEgressPolicy(ctx, machineID, m.ruleSetFor(policy)); err != nil {
			if hadOldProxy {
				_ = m.proxy.swap(oldProxy)
			} else {
				_ = m.proxy.Unregister(machineID)
			}
			return fmt.Errorf("egress apply slot rules: %w", err)
		}
	}
	return nil
}

// Remove first clears nft. A failure leaves the proxy registration visible so
// traffic still uses the complete old policy; only a successful clear unregisters it.
func (m *Manager) Remove(ctx context.Context, machineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.removeLocked(ctx, machineID)
}

func (m *Manager) removeLocked(ctx context.Context, machineID string) error {
	if m.slots != nil {
		if err := m.slots.RemoveEgressPolicy(ctx, machineID); err != nil {
			return fmt.Errorf("egress remove slot rules: %w", err)
		}
	}
	return m.proxy.Unregister(machineID)
}

// Restore 重启后重放持久化 slot 规则并重建代理注册。身份信息（execution 等）
// 无法从 slot 状态恢复：调用方须先经 hypeman 实例 tags 重建 Policy 并调用
// Apply；本方法只负责内核规则幂等重放。返回 slot 侧错误（无持久化规则时不
// 报错）。
func (m *Manager) Restore(ctx context.Context, machineID string) error {
	if m.slots == nil {
		return nil
	}
	return m.slots.RestoreEgress(ctx, machineID)
}

// Stats 返回 machine 当前 execution 的审计聚合（Machine.EgressAudit）。
func (m *Manager) Stats(machineID string) *pb.EgressAuditStats {
	return m.proxy.Stats(machineID)
}

// ruleSetFor 把归一化 Policy 翻译为 slot 规则集（全量替换 + generation 水位）。
func (m *Manager) ruleSetFor(p *Policy) slot.EgressRuleSet {
	allowed, denied := p.CIDRStrings()
	rs := slot.EgressRuleSet{
		Mode:         p.ModeString(),
		AllowedCIDRs: allowed,
		DeniedCIDRs:  denied,
		Domains:      p.DomainStrings(),
		MaxTCPConns:  p.MaxTCPConns,
		AuditAll:     p.AuditAll,
		Generation:   p.Generation,
	}
	if p.ProxyNeeded() {
		rs.ProxyPort80 = m.port80
		rs.ProxyPort443 = m.port443
	}
	return rs
}
