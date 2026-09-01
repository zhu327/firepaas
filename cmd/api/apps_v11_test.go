package main

import (
	"encoding/json"
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// v1.1（ADR-0022）：services 声明归一与校验。
func TestResolveServices(t *testing.T) {
	// 只给单端口：services=nil，port 透传。
	svcs, port, err := resolveServices(nil, 8080)
	if err != nil || svcs != nil || port != 8080 {
		t.Fatalf("single port: %+v %d %v", svcs, port, err)
	}

	// 多端口：主 service = 第一条。
	svcs, port, err = resolveServices([]serviceBody{
		{Name: "http", InternalPort: 80},
		{Name: "grpc", InternalPort: 8081},
	}, 0)
	if err != nil || len(svcs) != 2 || port != 80 {
		t.Fatalf("multi service: %+v %d %v", svcs, port, err)
	}
	if svcs[0].Name != "http" || svcs[1].InternalPort != 8081 {
		t.Fatalf("services order/content wrong: %+v", svcs)
	}
	// 非 8080 主端口是合法的；create handler 仅在没有 services 时默认 8080。
	if _, port, err := resolveServices([]serviceBody{{Name: "http", InternalPort: 8081}}, 0); err != nil ||
		port != 8081 {
		t.Fatalf("non-8080 primary service: port=%d err=%v", port, err)
	}

	// port 与 services[0] 一致时允许（互为别名）。
	if _, _, err := resolveServices([]serviceBody{{InternalPort: 80}}, 80); err != nil {
		t.Fatalf("consistent port/services must pass: %v", err)
	}
	// 冲突拒绝。
	if _, _, err := resolveServices([]serviceBody{{InternalPort: 80}}, 8080); err == nil {
		t.Fatal("port conflicting with services[0] must be rejected")
	}
	// 端口重复拒绝。
	if _, _, err := resolveServices([]serviceBody{{InternalPort: 80}, {InternalPort: 80}}, 0); err == nil {
		t.Fatal("duplicate ports must be rejected")
	}
	// 端口范围拒绝。
	if _, _, err := resolveServices(nil, 70000); err == nil {
		t.Fatal("out-of-range port must be rejected")
	}
	if _, _, err := resolveServices([]serviceBody{{InternalPort: 0}}, 0); err == nil {
		t.Fatal("zero internal_port must be rejected")
	}
	// 条目上限。
	tooMany := make([]serviceBody, 9)
	for i := range tooMany {
		tooMany[i] = serviceBody{InternalPort: 10000 + i}
	}
	if _, _, err := resolveServices(tooMany, 0); err == nil {
		t.Fatal("9 services must be rejected")
	}
	// 名称缺省生成。
	svcs, _, err = resolveServices([]serviceBody{{InternalPort: 8081}}, 0)
	if err != nil || svcs[0].Name != "svc-8081" {
		t.Fatalf("default name: %+v %v", svcs, err)
	}
}

// v1.1（ADR-0017）：auto_standby 策略校验与序列化。
func TestMarshalAutoStandby(t *testing.T) {
	if raw, err := marshalAutoStandby(nil); err != nil || raw != nil {
		t.Fatalf("nil policy: %v %v", raw, err)
	}
	if raw, err := marshalAutoStandby(&autoStandbyBody{Enabled: false}); err != nil || raw != nil {
		t.Fatalf("disabled policy: %v %v", raw, err)
	}
	if _, err := marshalAutoStandby(&autoStandbyBody{Enabled: true, IdleTimeoutSeconds: 3}); err == nil {
		t.Fatal("idle_timeout < 5s must be rejected")
	}
	raw, err := marshalAutoStandby(&autoStandbyBody{
		Enabled: true, IdleTimeoutSeconds: 60, IgnoreDestinationPorts: []uint32{9090},
	})
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed["enabled"] != true || parsed["idleTimeoutSeconds"] != float64(60) {
		t.Fatalf("serialized policy: %s", raw)
	}
}

func TestResolveStrategy(t *testing.T) {
	for in, want := range map[string]string{"": "bluegreen", "bluegreen": "bluegreen", "rolling": "rolling"} {
		got, err := resolveStrategy(in)
		if err != nil || got != want {
			t.Fatalf("resolveStrategy(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := resolveStrategy("canary"); err == nil {
		t.Fatal("canary must be rejected in v1.1")
	}
}

// v1.1（ADR-0022）：EffectiveServices 的单端口派生。
func TestEffectiveServicesDerivation(t *testing.T) {
	d := &store.Deployment{Port: 8080}
	svcs := d.EffectiveServices()
	if len(svcs) != 1 || svcs[0].Name != "default" || svcs[0].InternalPort != 8080 {
		t.Fatalf("derived services: %+v", svcs)
	}
	multi := &store.Deployment{Port: 80, Services: []store.ServiceSpec{
		{Name: "http", InternalPort: 80}, {Name: "grpc", InternalPort: 8081},
	}}
	svcs = multi.EffectiveServices()
	if len(svcs) != 2 || svcs[0].InternalPort != 80 {
		t.Fatalf("explicit services: %+v", svcs)
	}
	if multi.EffectiveStrategy() != "bluegreen" {
		t.Fatal("default strategy must be bluegreen")
	}
	if (&store.Deployment{Strategy: "rolling"}).EffectiveStrategy() != "rolling" {
		t.Fatal("rolling strategy must round-trip")
	}
}
