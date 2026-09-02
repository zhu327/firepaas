package main

import (
	"errors"
	"testing"
	"time"
)

// TestResourceSamplerLastKnownGood：R2（契约 D-1）降级序列——live 优先、
// 失败回退 ≤60s LKG、过期则 invalid（硬准入据此 fail-closed）。
func TestResourceSamplerLastKnownGood(t *testing.T) {
	inventoryDown := errors.New("hypeman inventory unavailable")
	errorsCounted := 0
	calls := 0
	clock := time.Now()
	s := newResourceSampler(func() (resourceSample, error) {
		calls++
		if calls == 1 {
			return resourceSample{memMiB: 512, diskMiB: 1024, vcpus: 2}, nil
		}
		return resourceSample{}, inventoryDown
	}, time.Minute, func(error) { errorsCounted++ })
	s.now = func() time.Time { return clock }

	m, ok := s.current()
	if !ok || m.memMiB != 512 || m.vcpus != 2 {
		t.Fatalf("live sample: %+v ok=%v", m, ok)
	}
	// live 失败回退 LKG：仍然有效，返回最近一次成功采集。
	m, ok = s.current()
	if !ok || m.memMiB != 512 || m.vcpus != 2 {
		t.Fatalf("last-known-good: %+v ok=%v", m, ok)
	}
	if errorsCounted != 1 {
		t.Fatalf("error hook calls = %d, want 1", errorsCounted)
	}
	// LKG 在新鲜窗口边界内有效。
	clock = clock.Add(59 * time.Second)
	if _, ok := s.current(); !ok {
		t.Fatal("LKG must stay valid within freshness bound")
	}
	// 超过新鲜边界 → invalid。
	clock = clock.Add(2 * time.Second)
	if _, ok := s.current(); ok {
		t.Fatal("stale LKG must be invalid")
	}
	// 从无成功采集时的直接 invalid（启动即 inventory 故障）。
	fresh := newResourceSampler(
		func() (resourceSample, error) { return resourceSample{}, inventoryDown },
		time.Minute, nil)
	if _, ok := fresh.current(); ok {
		t.Fatal("sampler without any successful sample must be invalid")
	}
}
