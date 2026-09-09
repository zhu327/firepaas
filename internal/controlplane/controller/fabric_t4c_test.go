package controller

import (
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// fabricServiceName 必须与 store.FabricIdentities 的
// COALESCE(d.services->0->>'name','default') 同口径，否则分配的身份行与
// 快照查询对不上（快照侧 fail closed 报错）。
func TestFabricServiceNameMatchesSnapshotSemantics(t *testing.T) {
	for _, tc := range []struct {
		name string
		dep  *store.Deployment
		want string
	}{
		{"nil deployment", nil, "default"},
		{"no services", &store.Deployment{}, "default"},
		{"first service wins", &store.Deployment{Services: []store.ServiceSpec{
			{Name: "api", InternalPort: 8080},
			{Name: "worker", InternalPort: 9090},
		}}, "api"},
		// 空串原样保留：store 层 Name 无 omitempty，jsonb ->> 取到 ""，
		// 两边必须一致（不能一侧回退 default）。
		{"empty name preserved", &store.Deployment{Services: []store.ServiceSpec{
			{Name: "", InternalPort: 8080},
		}}, ""},
	} {
		if got := fabricServiceName(tc.dep); got != tc.want {
			t.Errorf("%s: fabricServiceName = %q, want %q", tc.name, got, tc.want)
		}
	}
}
