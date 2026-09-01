package store

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/secrets"
)

func mustBase64(t *testing.T) string {
	t.Helper()
	return base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
}

// M4（ADR-0010）：secrets 表 + 引用解析集成测试。
func TestSecretsCRUDAndResolve(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mgr, err := secrets.NewManager(mustBase64(t))
	if err != nil {
		t.Fatal(err)
	}
	const project, name = "dev", "test-secret-m4"

	v1, err := s.NextSecretVersion(ctx, project, name)
	if err != nil || v1 != 1 {
		t.Fatalf("first version: %v %d", err, v1)
	}
	s1, _ := mgr.Seal([]byte("value-1"), project, name, v1)
	if _, err := s.PutSecretVersion(ctx, project, name, v1, s1.Ciphertext, s1.WrappedDEK, "t"); err != nil {
		t.Fatal(err)
	}
	v2, _ := s.NextSecretVersion(ctx, project, name)
	if v2 != 2 {
		t.Fatalf("second version = %d", v2)
	}
	s2, _ := mgr.Seal([]byte("value-2"), project, name, v2)
	if _, err := s.PutSecretVersion(ctx, project, name, v2, s2.Ciphertext, s2.WrappedDEK, "t"); err != nil {
		t.Fatal(err)
	}

	// 默认最新版本；显式版本取历史值。
	pt, meta, err := ResolveSecretValue(ctx, s, mgr, project, name, nil)
	if err != nil || pt != "value-2" || meta.Version != 2 {
		t.Fatalf("latest resolve: %v %q", err, pt)
	}
	pt, meta, err = ResolveSecretValue(ctx, s, mgr, project, name, &v1)
	if err != nil || pt != "value-1" || meta.Version != 1 {
		t.Fatalf("v1 resolve: %v %q", err, pt)
	}

	// 缺失的 secret / 版本报错可识别。
	if _, _, err := ResolveSecretValue(ctx, s, mgr, project, "nope", nil); err == nil {
		t.Fatal("missing secret must fail")
	}

	// 列表只出每个 name 的最新版。
	metas, err := s.ListSecrets(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range metas {
		if m.Name == name && m.Version == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("list must show latest version")
	}

	n, err := s.DeleteSecret(ctx, project, name)
	if err != nil || n != 2 {
		t.Fatalf("delete: %v n=%d", err, n)
	}
}
