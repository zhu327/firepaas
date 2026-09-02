package main

import (
	"sync"
	"time"
)

// resourceSample 是一次节点资源采集（硬准入与容量上报共享的输入，单位：MiB/核）。
type resourceSample struct {
	memMiB  uint64
	diskMiB uint64
	vcpus   int
	at      time.Time
}

// resourceSampler 采集 hypeman inventory 并维护新鲜的 last-known-good
// （R2 P0，契约 D-1）。ListInstances 失败不得伪装成 0 占用——硬准入会把
// “采集失败 = 空节点”的幻象放行成 ghost 超售。采集失败时的降级有序：
//  1. ≤ freshFor 的 last-known-good 仍视为有效（allocated 维度只随 delete
//     下降，短窗口内高估是保守方向，不会少售）；
//  2. 无 LKG 或过期 → invalid：capacity 上报退化为 0 并计失败指标，create
//     硬准入 fail-closed（codes.Unavailable），非 create RPC 不受影响。
//
// sample 只应做一次 inventory 读取（5s 级超时）；onError 是失败计数钩子。
type resourceSampler struct {
	mu       sync.Mutex
	lastGood *resourceSample
	freshFor time.Duration
	sample   func() (resourceSample, error)
	onError  func(err error)
	now      func() time.Time
}

func newResourceSampler(
	sample func() (resourceSample, error),
	freshFor time.Duration,
	onError func(err error),
) *resourceSampler {
	return &resourceSampler{freshFor: freshFor, sample: sample, onError: onError, now: time.Now}
}

// current 返回当前样本与有效性：live 采样优先；失败回退新鲜 LKG；再不行 invalid。
func (s *resourceSampler) current() (resourceSample, bool) {
	m, err := s.sample()
	if err == nil {
		m.at = s.now()
		s.mu.Lock()
		cp := m
		s.lastGood = &cp
		s.mu.Unlock()
		return m, true
	}
	if s.onError != nil {
		s.onError(err)
	}
	s.mu.Lock()
	lg := s.lastGood
	s.mu.Unlock()
	if lg != nil && s.now().Sub(lg.at) <= s.freshFor {
		return *lg, true
	}
	return resourceSample{}, false
}
