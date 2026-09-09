package state

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openTestFabric(t *testing.T) (*Fabric, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fabric.json")
	f, err := OpenFabric(path)
	if err != nil {
		t.Fatalf("open fabric: %v", err)
	}
	return f, path
}

func snapshot(gen uint64, prefix string, extra PeerMutation) FabricSnapshot {
	s := FabricSnapshot{
		NodeID:     "node-a",
		Generation: gen,
		NodePrefix: prefix,
		Peers: []FabricPeer{
			{NodeID: "node-b", Pubkey: "pk-b", Endpoint: "10.0.0.2:51820", NodePrefix: prefix, FabricGeneration: gen},
		},
		Identities: []FabricIdentity{
			{
				IdentityID: 7, TrustDomain: "td", ProjectID: "p", AppID: "a", Service: "s",
				ULA: "fd7a:9a55:1::5", MachineID: "m1", ExecutionID: "e1", Generation: 3,
			},
		},
	}
	if extra != nil {
		extra(&s)
	}
	return s
}

type PeerMutation func(*FabricSnapshot)

func TestFabricApplyFencingAndIdempotence(t *testing.T) {
	f, path := openTestFabric(t)

	// 首次应用推进水位并落盘。
	applied, err := f.Apply(snapshot(5, "fd7a:9a55:1::/64", nil))
	if err != nil || !applied {
		t.Fatalf("first apply = (%v, %v), want (true, nil)", applied, err)
	}
	got := f.Current()
	if got.Generation != 5 || len(got.Peers) != 1 {
		t.Fatalf("current = %+v", got)
	}

	// 同代同内容幂等（不落盘、不报错）。
	applied, err = f.Apply(snapshot(5, "fd7a:9a55:1::/64", nil))
	if err != nil || applied {
		t.Fatalf("idempotent apply = (%v, %v), want (false, nil)", applied, err)
	}

	// 旧代重放拒绝。
	_, err = f.Apply(snapshot(4, "fd7a:9a55:1::/64", nil))
	if !errors.Is(err, ErrStaleFabricGeneration) {
		t.Fatalf("stale err = %v, want ErrStaleFabricGeneration", err)
	}

	// 同代不同内容拒绝（fail closed）。
	_, err = f.Apply(snapshot(5, "fd7a:9a55:1::/64", func(s *FabricSnapshot) {
		s.Peers[0].Pubkey = "pk-rotated"
	}))
	if !errors.Is(err, ErrStaleFabricGeneration) {
		t.Fatalf("same-gen different content err = %v, want ErrStaleFabricGeneration", err)
	}

	// 更高代替换。
	applied, err = f.Apply(snapshot(6, "fd7a:9a55:2::/64", nil))
	if err != nil || !applied {
		t.Fatalf("next apply = (%v, %v), want (true, nil)", applied, err)
	}
	if got := f.Current(); got.Generation != 6 || got.NodePrefix != "fd7a:9a55:2::/64" {
		t.Fatalf("current = %+v", got)
	}

	// 重启后从持久化水位继续拒绝旧代（崩溃恢复）。
	reopened, err := OpenFabric(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := reopened.Current(); got.Generation != 6 {
		t.Fatalf("reopened current = %+v", got)
	}
	if _, err := reopened.Apply(snapshot(5, "fd7a:9a55:1::/64", nil)); !errors.Is(err, ErrStaleFabricGeneration) {
		t.Fatalf("reopened stale err = %v, want ErrStaleFabricGeneration", err)
	}
}

func TestFabricRejectsNodeIDSwitch(t *testing.T) {
	f, _ := openTestFabric(t)
	if _, err := f.Apply(snapshot(1, "fd7a:9a55:1::/64", nil)); err != nil {
		t.Fatal(err)
	}
	other := snapshot(2, "fd7a:9a55:1::/64", nil)
	other.NodeID = "node-other"
	if _, err := f.Apply(other); err == nil {
		t.Fatal("node id switch must be rejected")
	}
}

func TestFabricRejectsZeroGeneration(t *testing.T) {
	f, _ := openTestFabric(t)
	if _, err := f.Apply(FabricSnapshot{Generation: 0}); err == nil {
		t.Fatal("zero generation must be rejected")
	}
}

func TestOpenFabricCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fabric.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFabric(path); err == nil {
		t.Fatal("corrupt fabric file must fail open")
	}
}
