package machine

import (
	"context"
	"errors"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/network/api"
)

// TestAdapterCreateFabricULA：ADR-0040 T6 的 adapter 侧接线。
//
//   - lookup 未装配 → legacy 纯 v4（hreq/attach 均无 v6，零回归）；
//   - 快照命中 → hypeman 请求与 slot attach 同时携带 ULA（地址/前缀128/网关）；
//   - 快照已生效但无映射 → ErrFabricIdentityPending 且无任何副作用
//     （hypeman 实例未建，controller 重入列后收敛；方案 A）。
func TestAdapterCreateFabricULA(t *testing.T) {
	const ula = "fd7a:9a55:1::5"

	setup := func(lookup FabricULALookup) (*fakeInstances, *fakeDatapath, *Adapter) {
		im := &fakeInstances{}
		dp := &fakeDatapath{}
		a := New(im, &fakeImages{}, dp, nil)
		a.SetFabricULA(lookup)
		return im, dp, a
	}

	t.Run("legacy without lookup", func(t *testing.T) {
		im, dp, a := setup(nil)
		if _, err := a.Create(context.Background(), validCreateRequest()); err != nil {
			t.Fatal(err)
		}
		if im.lastReq.IPv6Address != "" || im.lastReq.IPv6Gateway != "" {
			t.Fatalf("legacy create must not inject v6: %+v", im.lastReq)
		}
		if dp.lastSpec.GuestIP6 != "" || dp.lastSpec.GuestGW6 != "" {
			t.Fatalf("legacy attach must not carry v6: %+v", dp.lastSpec)
		}
	})

	t.Run("inactive snapshot is legacy", func(t *testing.T) {
		im, dp, a := setup(func(string, string) (string, bool) { return "", false })
		if _, err := a.Create(context.Background(), validCreateRequest()); err != nil {
			t.Fatal(err)
		}
		if im.lastReq.IPv6Address != "" || dp.lastSpec.GuestIP6 != "" {
			t.Fatalf("inactive fabric must stay v4-only: %+v %+v", im.lastReq, dp.lastSpec)
		}
	})

	t.Run("hit injects to hypeman and slot", func(t *testing.T) {
		im, dp, a := setup(func(machineID, executionID string) (string, bool) {
			if machineID != "m1-test" || executionID != "e1" {
				t.Fatalf("lookup scope = %s/%s, want m1-test/e1", machineID, executionID)
			}
			return ula, true
		})
		if _, err := a.Create(context.Background(), validCreateRequest()); err != nil {
			t.Fatal(err)
		}
		if im.lastReq.IPv6Address != ula || im.lastReq.IPv6Prefix != 128 ||
			im.lastReq.IPv6Gateway != api.SlotBridgeLL6 {
			t.Fatalf("hypeman v6 injection = %+v, want %s/128 gw %s",
				im.lastReq, ula, api.SlotBridgeLL6)
		}
		if dp.lastSpec.GuestIP6 != ula || dp.lastSpec.GuestGW6 != api.SlotBridgeLL6 {
			t.Fatalf("slot attach v6 = %+v, want %s gw %s",
				dp.lastSpec, ula, api.SlotBridgeLL6)
		}
	})

	t.Run("active miss fails transient without side effects", func(t *testing.T) {
		im, dp, a := setup(func(string, string) (string, bool) { return "", true })
		_, err := a.Create(context.Background(), validCreateRequest())
		if !errors.Is(err, ErrFabricIdentityPending) {
			t.Fatalf("err = %v, want ErrFabricIdentityPending", err)
		}
		if im.created != nil {
			t.Fatal("pending create must not build a hypeman instance")
		}
		if dp.attachCalls != 0 {
			t.Fatal("pending create must not attach networking")
		}
	})
}
