// metrics.go：eBPF 数据面的低基数观测缝（ADR-0040 §21，G3）。
//
// 三个指标（无 label——域名/IP/机器不得进 label，§21 低基数约束）：
//
//	firepaas_agent_ebpf_attach_errors_total   slot attach/ensure 失败计数
//	firepaas_agent_ebpf_fallback_total        探测失败回落 nft 事件计数
//	firepaas_agent_ebpf_policy_gen            当前已应用 fabric 策略代（gauge）
//
// Observer 由 agentd 注入（OTel 实现）；nil = no-op（测试/独立使用）。
package ebpf

// Observer 是 eBPF 数据面的观测缝（全部方法必须非阻塞、永不失败——
// 观测不得影响数据面）。
type Observer interface {
	// AttachError 记一次 slot 挂载/补齐失败（含 host/slot clsact 与 tc filter）。
	AttachError()
	// Fallback 记一次探测失败回落 nft emergency 事件。
	Fallback()
	// PolicyGen 更新当前已应用 fabric 策略代（gauge 语义）。
	PolicyGen(gen uint64)
}

// noopObserver 是默认观测者（全丢弃）。
type noopObserver struct{}

func (noopObserver) AttachError()     {}
func (noopObserver) Fallback()        {}
func (noopObserver) PolicyGen(uint64) {}
